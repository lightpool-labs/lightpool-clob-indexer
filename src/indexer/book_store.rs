// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use lightpool_sdk::lightpool_types::call::GetOrderBook;
use lightpool_sdk::OrderSide;
use tokio::sync::{broadcast, RwLock};

use crate::chain::{format_price_pieces, format_token_amount};
use crate::domain::{BookLevel, BookSnapshot};
use crate::spot_market::normalize_spot_market_key;
use crate::ws::models::{
    BookLevelDelta, OrderBookDelta, OrderBookSnapshot, QuoteDelta, QuoteSnapshot, RecentTrade,
};

const RECENT_TRADE_LIMIT: usize = 50;

#[derive(Clone, Copy)]
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
}

#[derive(Default)]
struct BookStoreInner {
    books: HashMap<String, SpotBook>,
    chain_hydrated: HashSet<String>,
    publishers: HashMap<String, broadcast::Sender<OrderBookDelta>>,
    quote_publishers: HashMap<String, broadcast::Sender<QuoteDelta>>,
    trades: HashMap<String, VecDeque<RecentTrade>>,
    last_fill: HashMap<String, FillKey>,
    trade_seq: u64,
}

pub struct BookStore {
    inner: RwLock<BookStoreInner>,
}

pub type SharedBookStore = Arc<BookStore>;

impl BookStore {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(BookStoreInner::default()),
        }
    }

    pub async fn export_for_persist(
        &self,
    ) -> (
        Vec<crate::persist::PersistBookLevel>,
        Vec<crate::persist::PersistBookMeta>,
    ) {
        let inner = self.inner.read().await;
        let mut levels = Vec::new();
        let mut metas = Vec::new();
        for (spot_market, book) in &inner.books {
            metas.push(crate::persist::PersistBookMeta {
                spot_market: spot_market.clone(),
                sequence: book.sequence,
                last_trade_price: book.last_trade_price,
            });
            for (price_raw, size_raw) in &book.bids {
                levels.push(crate::persist::PersistBookLevel {
                    spot_market: spot_market.clone(),
                    side: "buy".into(),
                    price_raw: *price_raw,
                    size_raw: *size_raw,
                });
            }
            for (price_raw, size_raw) in &book.asks {
                levels.push(crate::persist::PersistBookLevel {
                    spot_market: spot_market.clone(),
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
        let mut inner = self.inner.write().await;
        inner.books.clear();
        inner.chain_hydrated.clear();

        for meta in metas {
            let key = Self::key(&meta.spot_market);
            let book = inner.books.entry(key.clone()).or_insert_with(SpotBook::default);
            book.sequence = meta.sequence;
            book.last_trade_price = meta.last_trade_price;
            inner.chain_hydrated.insert(key);
        }

        for level in levels {
            if level.size_raw == 0 {
                continue;
            }
            let key = Self::key(&level.spot_market);
            let book = inner.books.entry(key.clone()).or_insert_with(SpotBook::default);
            match level.side.as_str() {
                "buy" => {
                    book.bids.insert(level.price_raw, level.size_raw);
                }
                "sell" => {
                    book.asks.insert(level.price_raw, level.size_raw);
                }
                _ => {}
            }
            inner.chain_hydrated.insert(key);
        }
    }

    fn key(spot_market: &str) -> String {
        normalize_spot_market_key(spot_market)
    }

    pub async fn subscribe(&self, spot_market: &str) -> broadcast::Receiver<OrderBookDelta> {
        let key = Self::key(spot_market);
        let mut inner = self.inner.write().await;
        let sender = inner
            .publishers
            .entry(key.clone())
            .or_insert_with(|| broadcast::channel(4096).0);
        sender.subscribe()
    }

    pub async fn subscribe_quote(&self, spot_market: &str) -> broadcast::Receiver<QuoteDelta> {
        let key = Self::key(spot_market);
        let mut inner = self.inner.write().await;
        let sender = inner
            .quote_publishers
            .entry(key)
            .or_insert_with(|| broadcast::channel(4096).0);
        sender.subscribe()
    }

    pub async fn snapshot(&self, spot_market: &str, depth: u32) -> Option<BookSnapshot> {
        let key = Self::key(spot_market);
        let inner = self.inner.read().await;
        let book = inner.books.get(&key)?;
        Some(Self::book_to_response(book, depth))
    }

    pub async fn ws_snapshot(&self, spot_market: &str, depth: u32) -> Option<OrderBookSnapshot> {
        let key = Self::key(spot_market);
        let inner = self.inner.read().await;
        let book = inner.books.get(&key)?;
        Some(Self::book_to_ws_snapshot(&key, book, depth))
    }

    pub async fn ws_quote_snapshot(&self, spot_market: &str) -> Option<QuoteSnapshot> {
        let key = Self::key(spot_market);
        let inner = self.inner.read().await;
        let book = inner.books.get(&key)?;
        Some(Self::book_to_quote_snapshot(&key, book))
    }

    pub async fn is_chain_hydrated(&self, spot_market: &str) -> bool {
        let key = Self::key(spot_market);
        self.inner.read().await.chain_hydrated.contains(&key)
    }

    pub async fn hydrate_from_chain(
        &self,
        spot_market: &str,
        chain_book: &GetOrderBook,
        last_trade_price: Option<u64>,
    ) {
        let key = Self::key(spot_market);
        let mut inner = self.inner.write().await;
        let book = inner
            .books
            .entry(key.clone())
            .or_insert_with(SpotBook::default);

        book.bids.clear();
        book.asks.clear();
        for level in &chain_book.best_bids {
            if level.total_quantity > 0 {
                book.bids.insert(level.price, level.total_quantity);
            }
        }
        for level in &chain_book.best_asks {
            if level.total_quantity > 0 {
                book.asks.insert(level.price, level.total_quantity);
            }
        }
        book.sequence = book.sequence.saturating_add(1);
        if let Some(price) = last_trade_price {
            book.last_trade_price = Some(price);
        }
        inner.chain_hydrated.insert(key);
    }

    pub async fn apply_created(
        &self,
        spot_market: &str,
        side: OrderSide,
        price_raw: u64,
        amount_raw: u64,
        block_num: u64,
        publish_ws: bool,
    ) {
        if price_raw == 0 || amount_raw == 0 {
            return;
        }
        let key = Self::key(spot_market);
        let mut inner = self.inner.write().await;
        let delta = Self::apply_level_change(
            &mut inner,
            &key,
            side,
            price_raw,
            amount_raw,
            true,
            block_num,
            None,
        );
        if publish_ws {
            Self::publish_delta(&inner, delta);
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
    ) {
        if price_raw == 0 || amount_raw == 0 {
            return;
        }
        let key = Self::key(spot_market);
        let mut inner = self.inner.write().await;
        let delta = Self::apply_level_change(
            &mut inner,
            &key,
            side,
            price_raw,
            amount_raw,
            false,
            block_num,
            None,
        );
        if publish_ws {
            Self::publish_delta(&inner, delta);
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
    ) {
        if price_raw == 0 {
            return;
        }

        let filled = new_amount_raw.saturating_sub(new_remaining_raw);
        let old_remaining_raw = old_amount_raw.saturating_sub(filled);
        if old_remaining_raw == new_remaining_raw {
            return;
        }

        let key = Self::key(spot_market);
        let mut inner = self.inner.write().await;
        let delta = if new_remaining_raw > old_remaining_raw {
            Self::apply_level_change(
                &mut inner,
                &key,
                side,
                price_raw,
                new_remaining_raw - old_remaining_raw,
                true,
                block_num,
                None,
            )
        } else {
            Self::apply_level_change(
                &mut inner,
                &key,
                side,
                price_raw,
                old_remaining_raw - new_remaining_raw,
                false,
                block_num,
                None,
            )
        };
        if publish_ws {
            Self::publish_delta(&inner, delta);
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
    ) {
        if price_raw == 0 || fill_amount_raw == 0 {
            return;
        }
        let key = Self::key(spot_market);
        let mut inner = self.inner.write().await;
        let mut delta = Self::apply_level_change(
            &mut inner,
            &key,
            side,
            price_raw,
            fill_amount_raw,
            false,
            block_num,
            Some(last_trade_price),
        );
        let fill_key = FillKey {
            block_num,
            price_raw,
            size_raw: fill_amount_raw,
        };
        let duplicate = inner.last_fill.get(&key).is_some_and(|prev| {
            prev.block_num == fill_key.block_num
                && prev.price_raw == fill_key.price_raw
                && prev.size_raw == fill_key.size_raw
        });
        inner.last_fill.insert(key.clone(), fill_key);
        if !duplicate {
            if let Some(trade) = Self::push_trade(&mut inner, &key, side, price_raw, fill_amount_raw, block_num)
            {
                if let Some(delta) = delta.as_mut() {
                    delta.trade = Some(trade);
                }
            }
        }
        if publish_ws {
            Self::publish_delta(&inner, delta);
        }
    }

    pub async fn recent_trades(&self, spot_market: &str) -> Vec<RecentTrade> {
        let key = Self::key(spot_market);
        let inner = self.inner.read().await;
        inner
            .trades
            .get(&key)
            .map(|trades| trades.iter().cloned().collect())
            .unwrap_or_default()
    }

    fn push_trade(
        inner: &mut BookStoreInner,
        spot_market: &str,
        side: OrderSide,
        price_raw: u64,
        size_raw: u64,
        block_num: u64,
    ) -> Option<RecentTrade> {
        inner.trade_seq = inner.trade_seq.saturating_add(1);
        let side = match side {
            OrderSide::Buy => "buy",
            OrderSide::Sell => "sell",
        };
        let time_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis() as u64)
            .unwrap_or(0);
        let trade = RecentTrade {
            id: inner.trade_seq,
            side: side.into(),
            price: format_price_pieces(price_raw),
            size: format_token_amount(size_raw),
            time_ms,
            block_num,
        };
        let trades = inner.trades.entry(spot_market.to_string()).or_default();
        trades.push_front(trade.clone());
        while trades.len() > RECENT_TRADE_LIMIT {
            trades.pop_back();
        }
        Some(trade)
    }

    fn apply_level_change(
        inner: &mut BookStoreInner,
        spot_market: &str,
        side: OrderSide,
        price_raw: u64,
        amount_raw: u64,
        is_add: bool,
        block_num: u64,
        last_trade_price: Option<u64>,
    ) -> Option<OrderBookDelta> {
        let book = inner
            .books
            .entry(spot_market.to_string())
            .or_insert_with(SpotBook::default);
        let levels = match side {
            OrderSide::Buy => &mut book.bids,
            OrderSide::Sell => &mut book.asks,
        };

        let current = levels.get(&price_raw).copied().unwrap_or(0);
        let next = if is_add {
            current.saturating_add(amount_raw)
        } else {
            current.saturating_sub(amount_raw)
        };

        let level_delta = if next == 0 {
            levels.remove(&price_raw);
            BookLevelDelta {
                price: format_price_pieces(price_raw),
                size: "0".into(),
            }
        } else {
            levels.insert(price_raw, next);
            BookLevelDelta {
                price: format_price_pieces(price_raw),
                size: format_token_amount(next),
            }
        };

        book.sequence = book.sequence.saturating_add(1);
        if let Some(price) = last_trade_price {
            book.last_trade_price = Some(price);
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

    fn publish_delta(inner: &BookStoreInner, delta: Option<OrderBookDelta>) {
        let Some(delta) = delta else {
            return;
        };
        if let Some(sender) = inner.publishers.get(&delta.spot_market) {
            let _ = sender.send(delta.clone());
        }
        if let Some(book) = inner.books.get(&delta.spot_market) {
            Self::publish_quote(inner, &delta.spot_market, delta.block_num, book);
        }
    }

    fn publish_quote(inner: &BookStoreInner, spot_market: &str, block_num: u64, book: &SpotBook) {
        let quote = Self::quote_from_book(spot_market, block_num, book);
        if let Some(sender) = inner.quote_publishers.get(spot_market) {
            let _ = sender.send(quote);
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
