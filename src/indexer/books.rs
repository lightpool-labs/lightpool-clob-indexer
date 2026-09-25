// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use lightpool_sdk::lightpool_types::call::GetOrderBook;
use lightpool_sdk::OrderSide;
use tokio::sync::broadcast;

use crate::chain::{format_price_pieces, format_token_amount};
use crate::domain::{BookLevel, BookSnapshot};
use crate::persist::index_state_tables::BookMetaRow;
use crate::persist::index_write_set::IndexWriteSet;
use crate::spot_market::normalize_spot_market_key;
use crate::ws::models::{
    BookLevelDelta, OrderBookDelta, OrderBookSnapshot, QuoteDelta, QuoteSnapshot, RecentTrade,
};

const RECENT_TRADE_LIMIT: usize = 50;
const BROADCAST_CAPACITY: usize = 4096;

#[derive(Debug, Clone, Copy)]
struct FillKey {
    block_num: u64,
    price_raw: u64,
    size_raw: u64,
}

#[derive(Debug, Default)]
struct SpotBook {
    bids: BTreeMap<u64, u64>,
    asks: BTreeMap<u64, u64>,
    sequence: u64,
    last_trade_price: Option<u64>,
    chain_hydrated: bool,
    last_fill: Option<FillKey>,
    trades: VecDeque<RecentTrade>,
}

struct MarketShard {
    spot_market: String,
    book: Mutex<SpotBook>,
    orderbook_tx: broadcast::Sender<OrderBookDelta>,
    quote_tx: broadcast::Sender<QuoteDelta>,
}

impl MarketShard {
    fn new(spot_market: String) -> Self {
        let (orderbook_tx, _) = broadcast::channel(BROADCAST_CAPACITY);
        let (quote_tx, _) = broadcast::channel(BROADCAST_CAPACITY);
        Self {
            spot_market,
            book: Mutex::new(SpotBook::default()),
            orderbook_tx,
            quote_tx,
        }
    }
}

/// Per-market sharded order books. Cross-market applies do not share one lock.
pub struct Books {
    shards: DashMap<String, Arc<MarketShard>>,
    /// Globally unique recent-trade ids (cheap atomic; not under book locks).
    trade_seq: AtomicU64,
}


impl Books {
    pub fn new() -> Self {
        Self {
            shards: DashMap::new(),
            trade_seq: AtomicU64::new(0),
        }
    }

    pub fn clear(&self) {
        self.shards.clear();
        self.trade_seq.store(0, Ordering::Relaxed);
    }

    fn key(spot_market: &str) -> String {
        normalize_spot_market_key(spot_market)
    }

    fn shard(&self, spot_market: &str) -> Arc<MarketShard> {
        let key = Self::key(spot_market);
        if let Some(existing) = self.shards.get(&key) {
            return existing.clone();
        }
        let shard = Arc::new(MarketShard::new(key.clone()));
        match self.shards.entry(key) {
            dashmap::mapref::entry::Entry::Occupied(entry) => entry.get().clone(),
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(shard.clone());
                shard
            }
        }
    }

    fn try_shard(&self, spot_market: &str) -> Option<Arc<MarketShard>> {
        self.shards.get(&Self::key(spot_market)).map(|s| s.clone())
    }

    pub async fn export_for_persist(
        &self,
    ) -> (
        Vec<crate::persist::PersistBookLevel>,
        Vec<crate::persist::PersistBookMeta>,
    ) {
        self.export_for_persist_spots(None).await
    }

    pub async fn export_for_persist_spots(
        &self,
        only_spots: Option<&[String]>,
    ) -> (
        Vec<crate::persist::PersistBookLevel>,
        Vec<crate::persist::PersistBookMeta>,
    ) {
        let mut levels = Vec::new();
        let mut metas = Vec::new();
        for entry in self.shards.iter() {
            let shard = entry.value();
            if let Some(spots) = only_spots {
                if !spots.iter().any(|s| s == &shard.spot_market) {
                    continue;
                }
            }
            let Ok(book) = shard.book.lock() else {
                continue;
            };
            metas.push(crate::persist::PersistBookMeta {
                spot_market: shard.spot_market.clone(),
                sequence: book.sequence,
                last_trade_price: book.last_trade_price,
            });
            for (price_raw, size_raw) in &book.bids {
                levels.push(crate::persist::PersistBookLevel {
                    spot_market: shard.spot_market.clone(),
                    side: "buy".into(),
                    price_raw: *price_raw,
                    size_raw: *size_raw,
                });
            }
            for (price_raw, size_raw) in &book.asks {
                levels.push(crate::persist::PersistBookLevel {
                    spot_market: shard.spot_market.clone(),
                    side: "sell".into(),
                    price_raw: *price_raw,
                    size_raw: *size_raw,
                });
            }
        }
        (levels, metas)
    }

    pub async fn import_from_persist(
        &self,
        levels: Vec<crate::persist::PersistBookLevel>,
        metas: Vec<crate::persist::PersistBookMeta>,
    ) {
        self.shards.clear();

        for meta in metas {
            let shard = self.shard(&meta.spot_market);
            let Ok(mut book) = shard.book.lock() else {
                continue;
            };
            book.sequence = meta.sequence;
            book.last_trade_price = meta.last_trade_price;
            book.chain_hydrated = true;
        }

        for level in levels {
            if level.size_raw == 0 {
                continue;
            }
            let shard = self.shard(&level.spot_market);
            let Ok(mut book) = shard.book.lock() else {
                continue;
            };
            match level.side.as_str() {
                "buy" => {
                    book.bids.insert(level.price_raw, level.size_raw);
                }
                "sell" => {
                    book.asks.insert(level.price_raw, level.size_raw);
                }
                _ => {}
            }
            book.chain_hydrated = true;
        }
    }

    pub async fn subscribe(&self, spot_market: &str) -> broadcast::Receiver<OrderBookDelta> {
        self.shard(spot_market).orderbook_tx.subscribe()
    }

    pub async fn subscribe_quote(&self, spot_market: &str) -> broadcast::Receiver<QuoteDelta> {
        self.shard(spot_market).quote_tx.subscribe()
    }

    pub async fn snapshot(&self, spot_market: &str, depth: u32) -> Option<BookSnapshot> {
        let shard = self.try_shard(spot_market)?;
        let book = shard.book.lock().ok()?;
        Some(Self::book_to_response(&book, depth))
    }

    pub async fn ws_snapshot(&self, spot_market: &str, depth: u32) -> Option<OrderBookSnapshot> {
        let shard = self.try_shard(spot_market)?;
        let book = shard.book.lock().ok()?;
        Some(Self::book_to_ws_snapshot(&shard.spot_market, &book, depth))
    }

    pub async fn ws_quote_snapshot(&self, spot_market: &str) -> Option<QuoteSnapshot> {
        let shard = self.try_shard(spot_market)?;
        let book = shard.book.lock().ok()?;
        Some(Self::book_to_quote_snapshot(&shard.spot_market, &book))
    }

    pub async fn is_chain_hydrated(&self, spot_market: &str) -> bool {
        let Some(shard) = self.try_shard(spot_market) else {
            return false;
        };
        shard
            .book
            .lock()
            .map(|book| book.chain_hydrated)
            .unwrap_or(false)
    }

    pub async fn hydrate_from_chain(
        &self,
        spot_market: &str,
        chain_book: &GetOrderBook,
        last_trade_price: Option<u64>,
        mut ws: Option<&mut IndexWriteSet>,
    ) {
        let shard = self.shard(spot_market);
        let Ok(mut book) = shard.book.lock() else {
            return;
        };

        if let Some(ws) = ws.as_deref_mut() {
            for price in book.bids.keys().copied() {
                ws.delete_book_level(&shard.spot_market, "buy", price as i64);
            }
            for price in book.asks.keys().copied() {
                ws.delete_book_level(&shard.spot_market, "sell", price as i64);
            }
        }

        book.bids.clear();
        book.asks.clear();
        for level in &chain_book.best_bids {
            if level.total_quantity > 0 {
                book.bids.insert(level.price, level.total_quantity);
                if let Some(ws) = ws.as_deref_mut() {
                    ws.store_book_level(
                        &shard.spot_market,
                        "buy",
                        level.price as i64,
                        level.total_quantity as i64,
                    );
                }
            }
        }
        for level in &chain_book.best_asks {
            if level.total_quantity > 0 {
                book.asks.insert(level.price, level.total_quantity);
                if let Some(ws) = ws.as_deref_mut() {
                    ws.store_book_level(
                        &shard.spot_market,
                        "sell",
                        level.price as i64,
                        level.total_quantity as i64,
                    );
                }
            }
        }
        book.sequence = book.sequence.saturating_add(1);
        if let Some(price) = last_trade_price {
            book.last_trade_price = Some(price);
        }
        book.chain_hydrated = true;
        if let Some(ws) = ws {
            ws.store_book_meta(
                &shard.spot_market,
                BookMetaRow {
                    sequence: book.sequence as i64,
                    last_trade_price: book.last_trade_price.map(|p| p as i64),
                },
            );
        }
    }

    pub async fn apply_created(
        &self,
        spot_market: &str,
        side: OrderSide,
        price_raw: u64,
        amount_raw: u64,
        block_num: u64,
        publish_ws: bool,
        mut ws: Option<&mut IndexWriteSet>,
    ) {
        if price_raw == 0 || amount_raw == 0 {
            return;
        }
        let shard = self.shard(spot_market);
        let (delta, quote) = {
            let Ok(mut book) = shard.book.lock() else {
                return;
            };
            let delta = Self::apply_level_change(
                &mut book,
                &shard.spot_market,
                side,
                price_raw,
                amount_raw,
                true,
                block_num,
                None,
                ws,
            );
            let quote = delta.as_ref().map(|d| {
                Self::quote_from_book(&shard.spot_market, d.block_num, &book)
            });
            (delta, quote)
        };
        if publish_ws {
            Self::publish_delta(&shard, delta, quote);
        }
    }

    pub async fn apply_cancelled(
        &self,
        spot_market: &str,
        side: OrderSide,
        price_raw: u64,
        amount_raw: u64,
        block_num: u64,
        publish_ws: bool,
        mut ws: Option<&mut IndexWriteSet>,
    ) {
        if price_raw == 0 || amount_raw == 0 {
            return;
        }
        let shard = self.shard(spot_market);
        let (delta, quote) = {
            let Ok(mut book) = shard.book.lock() else {
                return;
            };
            let delta = Self::apply_level_change(
                &mut book,
                &shard.spot_market,
                side,
                price_raw,
                amount_raw,
                false,
                block_num,
                None,
                ws,
            );
            let quote = delta.as_ref().map(|d| {
                Self::quote_from_book(&shard.spot_market, d.block_num, &book)
            });
            (delta, quote)
        };
        if publish_ws {
            Self::publish_delta(&shard, delta, quote);
        }
    }

    pub async fn apply_updated(
        &self,
        spot_market: &str,
        side: OrderSide,
        price_raw: u64,
        old_amount_raw: u64,
        new_amount_raw: u64,
        new_remaining_raw: u64,
        block_num: u64,
        publish_ws: bool,
        mut ws: Option<&mut IndexWriteSet>,
    ) {
        if price_raw == 0 {
            return;
        }

        let filled = new_amount_raw.saturating_sub(new_remaining_raw);
        let old_remaining_raw = old_amount_raw.saturating_sub(filled);
        if old_remaining_raw == new_remaining_raw {
            return;
        }

        let shard = self.shard(spot_market);
        let (delta, quote) = {
            let Ok(mut book) = shard.book.lock() else {
                return;
            };
            let delta = if new_remaining_raw > old_remaining_raw {
                Self::apply_level_change(
                    &mut book,
                    &shard.spot_market,
                    side,
                    price_raw,
                    new_remaining_raw - old_remaining_raw,
                    true,
                    block_num,
                    None,
                    ws,
                )
            } else {
                Self::apply_level_change(
                    &mut book,
                    &shard.spot_market,
                    side,
                    price_raw,
                    old_remaining_raw - new_remaining_raw,
                    false,
                    block_num,
                    None,
                    ws,
                )
            };
            let quote = delta.as_ref().map(|d| {
                Self::quote_from_book(&shard.spot_market, d.block_num, &book)
            });
            (delta, quote)
        };
        if publish_ws {
            Self::publish_delta(&shard, delta, quote);
        }
    }

    pub async fn apply_filled(
        &self,
        spot_market: &str,
        side: OrderSide,
        price_raw: u64,
        fill_amount_raw: u64,
        block_num: u64,
        last_trade_price: u64,
        publish_ws: bool,
        mut ws: Option<&mut IndexWriteSet>,
        trade_time_ms: Option<u64>,
    ) {
        if price_raw == 0 || fill_amount_raw == 0 {
            return;
        }
        let shard = self.shard(spot_market);
        let (delta, quote) = {
            let Ok(mut book) = shard.book.lock() else {
                return;
            };
            let mut delta = Self::apply_level_change(
                &mut book,
                &shard.spot_market,
                side,
                price_raw,
                fill_amount_raw,
                false,
                block_num,
                Some(last_trade_price),
                ws,
            );
            let fill_key = FillKey {
                block_num,
                price_raw,
                size_raw: fill_amount_raw,
            };
            let duplicate = book.last_fill.is_some_and(|prev| {
                prev.block_num == fill_key.block_num
                    && prev.price_raw == fill_key.price_raw
                    && prev.size_raw == fill_key.size_raw
            });
            book.last_fill = Some(fill_key);
            if !duplicate {
                if let Some(trade) = Self::push_trade(
                    &self.trade_seq,
                    &mut book,
                    side,
                    price_raw,
                    fill_amount_raw,
                    block_num,
                    trade_time_ms,
                ) {
                    if let Some(delta) = delta.as_mut() {
                        delta.trade = Some(trade);
                    }
                }
            }
            let quote = delta.as_ref().map(|d| {
                Self::quote_from_book(&shard.spot_market, d.block_num, &book)
            });
            (delta, quote)
        };
        if publish_ws {
            Self::publish_delta(&shard, delta, quote);
        }
    }

    pub async fn recent_trades(&self, spot_market: &str) -> Vec<RecentTrade> {
        let Some(shard) = self.try_shard(spot_market) else {
            return Vec::new();
        };
        shard
            .book
            .lock()
            .map(|book| book.trades.iter().cloned().collect())
            .unwrap_or_default()
    }

    fn push_trade(
        trade_seq: &AtomicU64,
        book: &mut SpotBook,
        side: OrderSide,
        price_raw: u64,
        size_raw: u64,
        block_num: u64,
        trade_time_ms: Option<u64>,
    ) -> Option<RecentTrade> {
        let id = trade_seq.fetch_add(1, Ordering::Relaxed).saturating_add(1);
        let side = match side {
            OrderSide::Buy => "buy",
            OrderSide::Sell => "sell",
        };
        let time_ms = trade_time_ms.filter(|ms| *ms > 0).unwrap_or_else(|| {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|duration| duration.as_millis() as u64)
                .unwrap_or(0)
        });
        let trade = RecentTrade {
            id,
            side: side.into(),
            price: format_price_pieces(price_raw),
            size: format_token_amount(size_raw),
            time_ms,
            block_num,
        };
        book.trades.push_front(trade.clone());
        while book.trades.len() > RECENT_TRADE_LIMIT {
            book.trades.pop_back();
        }
        Some(trade)
    }

    fn apply_level_change(
        book: &mut SpotBook,
        spot_market: &str,
        side: OrderSide,
        price_raw: u64,
        amount_raw: u64,
        is_add: bool,
        block_num: u64,
        last_trade_price: Option<u64>,
        mut ws: Option<&mut IndexWriteSet>,
    ) -> Option<OrderBookDelta> {
        let levels = match side {
            OrderSide::Buy => &mut book.bids,
            OrderSide::Sell => &mut book.asks,
        };
        let side_str = match side {
            OrderSide::Buy => "buy",
            OrderSide::Sell => "sell",
        };

        let current = levels.get(&price_raw).copied().unwrap_or(0);
        let next = if is_add {
            current.saturating_add(amount_raw)
        } else {
            current.saturating_sub(amount_raw)
        };

        let level_delta = if next == 0 {
            levels.remove(&price_raw);
            if let Some(ws) = ws.as_deref_mut() {
                ws.delete_book_level(spot_market, side_str, price_raw as i64);
            }
            BookLevelDelta {
                price: format_price_pieces(price_raw),
                size: "0".into(),
            }
        } else {
            levels.insert(price_raw, next);
            if let Some(ws) = ws.as_deref_mut() {
                ws.store_book_level(spot_market, side_str, price_raw as i64, next as i64);
            }
            BookLevelDelta {
                price: format_price_pieces(price_raw),
                size: format_token_amount(next),
            }
        };

        book.sequence = book.sequence.saturating_add(1);
        if let Some(price) = last_trade_price {
            book.last_trade_price = Some(price);
        }
        if let Some(ws) = ws {
            ws.store_book_meta(
                spot_market,
                BookMetaRow {
                    sequence: book.sequence as i64,
                    last_trade_price: book.last_trade_price.map(|p| p as i64),
                },
            );
        }

        let (bids, asks) = match side {
            OrderSide::Buy => (vec![level_delta], Vec::new()),
            OrderSide::Sell => (Vec::new(), vec![level_delta]),
        };

        Some(OrderBookDelta {
            msg_type: "orderbook_delta".into(),
            spot_market: spot_market.to_string(),
            sequence: book.sequence,
            block_num,
            bids,
            asks,
            last_trade_price: book
                .last_trade_price
                .map(|price| format_price_pieces(price)),
            trade: None,
        })
    }

    fn publish_delta(
        shard: &MarketShard,
        delta: Option<OrderBookDelta>,
        quote: Option<QuoteDelta>,
    ) {
        if let Some(delta) = delta {
            let _ = shard.orderbook_tx.send(delta);
        }
        if let Some(quote) = quote {
            let _ = shard.quote_tx.send(quote);
        }
    }

    fn quote_from_book(spot_market: &str, block_num: u64, book: &SpotBook) -> QuoteDelta {
        let best_bid = book.bids.iter().next_back().map(|(price, size)| BookLevel {
            price: format_price_pieces(*price),
            size: format_token_amount(*size),
        });
        let best_ask = book.asks.iter().next().map(|(price, size)| BookLevel {
            price: format_price_pieces(*price),
            size: format_token_amount(*size),
        });
        QuoteDelta {
            msg_type: "quote".into(),
            spot_market: spot_market.to_string(),
            sequence: book.sequence,
            block_num,
            best_bid,
            best_ask,
            last_trade_price: book
                .last_trade_price
                .map(|price| format_price_pieces(price)),
        }
    }

    fn book_to_quote_snapshot(spot_market: &str, book: &SpotBook) -> QuoteSnapshot {
        let quote = Self::quote_from_book(spot_market, 0, book);
        QuoteSnapshot {
            msg_type: "quote_snapshot".into(),
            spot_market: quote.spot_market,
            sequence: quote.sequence,
            best_bid: quote.best_bid,
            best_ask: quote.best_ask,
            last_trade_price: quote.last_trade_price,
        }
    }

    fn book_to_response(book: &SpotBook, depth: u32) -> BookSnapshot {
        BookSnapshot {
            sequence: book.sequence,
            bids: book
                .bids
                .iter()
                .rev()
                .take(depth as usize)
                .map(|(price, size)| BookLevel {
                    price: format_price_pieces(*price),
                    size: format_token_amount(*size),
                })
                .collect(),
            asks: book
                .asks
                .iter()
                .take(depth as usize)
                .map(|(price, size)| BookLevel {
                    price: format_price_pieces(*price),
                    size: format_token_amount(*size),
                })
                .collect(),
            last_trade_price: book
                .last_trade_price
                .map(|price| format_price_pieces(price)),
        }
    }

    fn book_to_ws_snapshot(spot_market: &str, book: &SpotBook, depth: u32) -> OrderBookSnapshot {
        let response = Self::book_to_response(book, depth);
        OrderBookSnapshot {
            msg_type: "orderbook_snapshot".into(),
            spot_market: spot_market.to_string(),
            sequence: response.sequence,
            bids: response.bids,
            asks: response.asks,
            last_trade_price: response.last_trade_price,
        }
    }
}
