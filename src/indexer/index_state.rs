// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use dashmap::{DashMap, DashSet};
use tokio::sync::{broadcast, RwLock as TokioRwLock};
use uuid::Uuid;

use crate::domain::{
    Market, MarketCategory, MarketQuery, MarketSortOrder, Order, PaperActivityItem, PaperCloseKind,
    PaperHouseView, PaperInstrumentExposure, PaperPositionHistory, PaperPositionOpen,
    PaperPositionQuery, PaperPositionStatus, Vault, VaultQuery,
};
use lightpool_sdk::paper_events::PaperHouseAfter;
use crate::persist::{
    index_write_set::{order_row_from_parts, IndexWriteSet},
    PaperHouseHistoryRow, PersistOrderRow, SharedPersist, PAPER_HOUSE_HISTORY_LIMIT,
};
use crate::spot_market::{
    normalize_spot_market_key, onchain_order_id, split_onchain_order_id, OnchainOrderId,
};
use crate::bars::Bars;
use crate::ws::models::{
    OracleMarkWsMessage, PaperActivityWsMessage, PaperHouseWsMessage, PaperPositionWsMessage,
    PaperStakeWsMessage, PaperUserWsMessage,
};
use crate::book_hydrate::SharedChainClient;
use lightpool_sdk::{parse_token_contract, Address};
use crate::chain::format_token_amount;

pub const PAPER_ACTIVITY_LIMIT: usize = 120;

use super::books::Books;

#[derive(Debug, Clone, Default)]
pub struct IndexedBlockHead {
    pub block_num: u64,
    pub digest: String,
    pub tx_count: usize,
    pub connected: bool,
    pub catching_up: bool,
    /// Unix ms when apply last finished a block (0 = never).
    pub last_indexed_at_ms: u64,
}

pub type SharedIndexedBlockHead = Arc<TokioRwLock<IndexedBlockHead>>;

#[derive(Debug, Clone)]
struct SpotMarketRef {
    market_id: Uuid,
    outcome: String,
}

#[derive(Debug, Clone)]
pub(crate) struct StoredOrder {
    pub order: Order,
    pub user_address: String,
    pub chain_order_id: String,
    pub spot_market: String,
    pub filled_raw: u64,
    pub size_raw: u64,
}

#[derive(Debug, Clone)]
pub struct OrderQueryRecord {
    pub order: Order,
    pub chain_order_id: String,
    pub spot_market: String,
    pub user_address: String,
    pub size_raw: u64,
    pub filled_raw: u64,
}

#[derive(Debug, Clone, Default)]
struct PaperInstrumentMeta {
    name: String,
    price_market: String,
}

#[derive(Debug, Clone, Default)]
struct PaperInstrumentOi {
    long: u64,
    short: u64,
}

fn paper_instr_key(market: &str, instrument: u32) -> String {
    format!("{}:{instrument}", addr_key(market))
}

fn format_i128_token(raw: i128) -> String {
    use lightpool_sdk::TOKEN_SCALE;
    let sign = if raw < 0 { "-" } else { "" };
    let abs = raw.unsigned_abs();
    let whole = abs / TOKEN_SCALE as u128;
    let frac = abs % TOKEN_SCALE as u128;
    if frac == 0 {
        return format!("{sign}{whole}");
    }
    format!("{sign}{whole}.{frac:06}")
}

/// Max retained oracle/mark ticks per spot market (in-memory ring).
const ORACLE_HISTORY_LIMIT: usize = 3_600;
/// Max retained paper OI ticks per instrument (in-memory ring).
const PAPER_OI_HISTORY_LIMIT: usize = 3_600;

#[derive(Debug, Clone, serde::Serialize)]
pub struct OracleMark {
    pub spot_market: String,
    pub round: u64,
    pub oracle_price: u64,
    pub mark_price: u64,
    pub block_num: u64,
    /// Wall-clock ms when the tick was indexed (block timestamp when known).
    pub ts_ms: u64,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct OracleMarkTick {
    pub ts_ms: u64,
    pub round: u64,
    pub oracle_price: u64,
    pub mark_price: u64,
    pub block_num: u64,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct PaperOiTick {
    pub ts_ms: u64,
    pub long: u64,
    pub short: u64,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct PaperHouseTick {
    pub ts_ms: u64,
    pub house_funds: i128,
    pub lp_balance: u64,
    pub open_house_pnl: i64,
    pub queue_debt: u64,
}

/// 1h paper staking metrics (~1 tick/sec when dirty).
#[derive(Debug, Clone, serde::Serialize)]
pub struct PaperStakeTick {
    pub ts_ms: u64,
    /// USDC in staking reward_pool (claimable pool).
    pub staking_revenue: u64,
    /// PAPER token total supply.
    pub total_paper_supply: u64,
    /// Lifetime USDC distributed to stakers (fees_pushed staker_fee+swept).
    pub total_staking_reward: u64,
}

pub const PAPER_STAKE_HISTORY_LIMIT: usize = 3_600;

#[derive(Debug, Clone)]
struct PaperQueueClaim {
    market: String,
    index: u64,
    user: String,
    amount: u64,
    queued_at_ms: u64,
}

pub(crate) fn wall_clock_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Concurrent in-memory index (orders/markets/vaults) plus sharded order books and bars.
pub struct IndexState {
    pub books: Books,
    pub bars: Bars,
    markets: DashMap<Uuid, Market>,
    slug_to_market: DashMap<String, Uuid>,
    spot_to_market: DashMap<String, SpotMarketRef>,
    spot_by_name: DashMap<String, String>,
    last_trade_price: DashMap<String, u64>,
    /// Latest on-chain oracle/mark from `oracle_committed` events, keyed by spot market.
    oracle_marks: DashMap<String, OracleMark>,
    /// Recent oracle/mark ticks per spot market (newest at back).
    oracle_history: DashMap<String, VecDeque<OracleMarkTick>>,
    /// Per-market WS fan-out for oracle mark updates.
    oracle_publishers: DashMap<String, broadcast::Sender<OracleMarkWsMessage>>,
    /// Global WS fan-out for paper user position events (filter by user_address).
    paper_user_publisher: broadcast::Sender<PaperUserWsMessage>,
    /// Global WS fan-out for all paper position lifecycle events (keepers).
    paper_positions_publisher: broadcast::Sender<PaperPositionWsMessage>,
    /// Per-market WS fan-out for house overview snapshots.
    paper_house_publishers: DashMap<String, broadcast::Sender<PaperHouseWsMessage>>,
    /// Markets whose house view should be republished at end of block.
    paper_house_dirty: DashSet<String>,
    orders: DashMap<Uuid, StoredOrder>,
    orders_by_id: DashMap<OnchainOrderId, Uuid>,
    orders_by_user: DashMap<String, DashSet<Uuid>>,
    vaults: DashMap<Uuid, Vault>,
    vault_by_address: DashMap<String, Uuid>,
    vault_by_account: DashMap<String, Uuid>,
    vault_portfolio: DashMap<Uuid, HashMap<String, u64>>,
    /// Open paper positions keyed by `{market}:{position}`.
    paper_positions_open: DashMap<String, PaperPositionOpen>,
    /// Closed / liquidated paper positions (append-only history).
    paper_positions_history: DashMap<String, PaperPositionHistory>,
    /// Latest LP/queue snapshot from paper events (`PaperHouseAfter`), keyed by market.
    paper_house: DashMap<String, PaperHouseAfter>,
    /// Instrument meta from `paper_instrument_added`, keyed by `{market}:{instrument}`.
    paper_instruments: DashMap<String, PaperInstrumentMeta>,
    /// Per-instrument open interest (notional), keyed by `{market}:{instrument}`.
    paper_oi: DashMap<String, PaperInstrumentOi>,
    /// Recent long/short OI ticks per instrument (newest at back).
    paper_oi_history: DashMap<String, VecDeque<PaperOiTick>>,
    /// Recent house-metric ticks per paper market (newest at back).
    paper_house_history: DashMap<String, VecDeque<PaperHouseTick>>,
    /// Open payout-queue claims keyed by `{market}:{index}`.
    paper_queue_open: DashMap<String, PaperQueueClaim>,
    /// Lifetime traded notional per paper market (sum of opened notional).
    paper_volume: DashMap<String, u64>,
    /// Lifetime USDC pushed to stakers (`staker_fee + swept` from `paper_fees_pushed`).
    paper_total_staker_rewards: DashMap<String, u64>,
    /// Lifetime USDC claimed per `{market}:{user}` from `paper_claimed.reward`.
    paper_rewards_claimed: DashMap<String, u64>,
    /// Recent close-loss / liqd / stake / unstake feed (newest at back).
    paper_activity: Mutex<VecDeque<PaperActivityItem>>,
    paper_activity_publisher: broadcast::Sender<PaperActivityWsMessage>,
    paper_stake_dirty: DashSet<String>,
    paper_stake_history: DashMap<String, VecDeque<PaperStakeTick>>,
    paper_stake_publishers: DashMap<String, broadcast::Sender<PaperStakeWsMessage>>,
    persist: Option<SharedPersist>,
    /// Block unix-seconds while applying a ReceiptBlock (0 when idle / unknown).
    indexing_block_timestamp_secs: AtomicU64,
}

pub type SharedIndexState = Arc<IndexState>;

fn user_key(user_address: &str) -> String {
    user_address.trim().to_ascii_lowercase()
}

fn addr_key(address: &str) -> String {
    address.trim().to_ascii_lowercase()
}

fn is_hot_order_status(status: &str) -> bool {
    status == "open" || status == "partial_filled"
}

fn stored_to_persist_row(stored: &StoredOrder) -> PersistOrderRow {
    PersistOrderRow {
        order: stored.order.clone(),
        user_address: stored.user_address.clone(),
        chain_order_id: stored.chain_order_id.clone(),
        spot_market: stored.spot_market.clone(),
        size_raw: stored.size_raw,
        filled_raw: stored.filled_raw,
        status_ts_ms: 0,
    }
}

impl IndexState {
    pub fn new(persist: Option<SharedPersist>) -> Self {
        Self {
            books: Books::new(),
            bars: Bars::new(persist.clone()),
            markets: DashMap::new(),
            slug_to_market: DashMap::new(),
            spot_to_market: DashMap::new(),
            spot_by_name: DashMap::new(),
            last_trade_price: DashMap::new(),
            oracle_marks: DashMap::new(),
            oracle_history: DashMap::new(),
            oracle_publishers: DashMap::new(),
            paper_user_publisher: {
                let (sender, _) = broadcast::channel(512);
                sender
            },
            paper_positions_publisher: {
                let (sender, _) = broadcast::channel(512);
                sender
            },
            paper_house_publishers: DashMap::new(),
            paper_house_dirty: DashSet::new(),
            orders: DashMap::new(),
            orders_by_id: DashMap::new(),
            orders_by_user: DashMap::new(),
            vaults: DashMap::new(),
            vault_by_address: DashMap::new(),
            vault_by_account: DashMap::new(),
            vault_portfolio: DashMap::new(),
            paper_positions_open: DashMap::new(),
            paper_positions_history: DashMap::new(),
            paper_house: DashMap::new(),
            paper_instruments: DashMap::new(),
            paper_oi: DashMap::new(),
            paper_oi_history: DashMap::new(),
            paper_house_history: DashMap::new(),
            paper_queue_open: DashMap::new(),
            paper_volume: DashMap::new(),
            paper_total_staker_rewards: DashMap::new(),
            paper_rewards_claimed: DashMap::new(),
            paper_activity: Mutex::new(VecDeque::new()),
            paper_activity_publisher: {
                let (sender, _) = broadcast::channel(512);
                sender
            },
            paper_stake_dirty: DashSet::new(),
            paper_stake_history: DashMap::new(),
            paper_stake_publishers: DashMap::new(),
            persist,
            indexing_block_timestamp_secs: AtomicU64::new(0),
        }
    }

    pub fn set_indexing_block_timestamp(&self, timestamp_secs: u64) {
        self.indexing_block_timestamp_secs
            .store(timestamp_secs, Ordering::Relaxed);
    }

    /// Block wall time in ms for the block currently being applied; 0 if unknown.
    pub fn indexing_block_timestamp_ms(&self) -> u64 {
        let secs = self.indexing_block_timestamp_secs.load(Ordering::Relaxed);
        secs.saturating_mul(1000)
    }


    pub async fn clear_all(&self) {
        self.books.clear();
        self.bars.clear().await;
        self.markets.clear();
        self.slug_to_market.clear();
        self.spot_to_market.clear();
        self.spot_by_name.clear();
        self.last_trade_price.clear();
        self.oracle_marks.clear();
        self.oracle_history.clear();
        self.oracle_publishers.clear();
        self.orders.clear();
        self.orders_by_id.clear();
        self.orders_by_user.clear();
        self.vaults.clear();
        self.vault_by_address.clear();
        self.vault_by_account.clear();
        self.vault_portfolio.clear();
        self.paper_positions_open.clear();
        self.paper_positions_history.clear();
        self.paper_house.clear();
        self.paper_instruments.clear();
        self.paper_oi.clear();
        self.paper_oi_history.clear();
        self.paper_house_history.clear();
        self.paper_queue_open.clear();
        self.paper_volume.clear();
        self.paper_total_staker_rewards.clear();
        self.paper_rewards_claimed.clear();
        if let Ok(mut q) = self.paper_activity.lock() {
            q.clear();
        }
        self.paper_stake_dirty.clear();
        self.paper_stake_history.clear();
        self.paper_stake_publishers.clear();
        self.paper_house_dirty.clear();
        self.indexing_block_timestamp_secs.store(0, Ordering::Relaxed);
    }

    pub fn push_paper_activity(&self, mut item: PaperActivityItem) {
        if item.ts_ms == 0 {
            item.ts_ms = self.indexing_block_timestamp_ms();
            if item.ts_ms == 0 {
                item.ts_ms = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0);
            }
        }
        if let Ok(mut q) = self.paper_activity.lock() {
            q.push_back(item.clone());
            while q.len() > PAPER_ACTIVITY_LIMIT {
                q.pop_front();
            }
        }
        let _ = self.paper_activity_publisher.send(PaperActivityWsMessage {
            msg_type: "paper_activity".into(),
            activity: item,
        });
    }

    pub fn paper_activity(&self, market: Option<&str>, limit: usize) -> Vec<PaperActivityItem> {
        let limit = limit.clamp(1, PAPER_ACTIVITY_LIMIT);
        let market_key = market.map(addr_key);
        let Ok(q) = self.paper_activity.lock() else {
            return Vec::new();
        };
        q.iter()
            .rev()
            .filter(|a| {
                market_key
                    .as_ref()
                    .map(|m| addr_key(&a.market) == *m)
                    .unwrap_or(true)
            })
            .take(limit)
            .cloned()
            .collect()
    }

    pub fn subscribe_paper_activity(&self) -> broadcast::Receiver<PaperActivityWsMessage> {
        self.paper_activity_publisher.subscribe()
    }

    pub fn mark_paper_stake_dirty(&self, market: &str) {
        let key = addr_key(market);
        if !key.is_empty() {
            self.paper_stake_dirty.insert(key);
        }
    }

    fn record_paper_stake_tick(&self, market: &str, tick: PaperStakeTick) {
        let key = addr_key(market);
        let mut hist = self
            .paper_stake_history
            .entry(key)
            .or_insert_with(VecDeque::new);
        if let Some(last) = hist.back() {
            if last.ts_ms == tick.ts_ms {
                *hist.back_mut().unwrap() = tick;
                return;
            }
        }
        hist.push_back(tick);
        while hist.len() > PAPER_STAKE_HISTORY_LIMIT {
            hist.pop_front();
        }
    }

    fn paper_stake_publisher(&self, market: &str) -> broadcast::Sender<PaperStakeWsMessage> {
        let key = addr_key(market);
        self.paper_stake_publishers
            .entry(key)
            .or_insert_with(|| {
                let (sender, _) = broadcast::channel(64);
                sender
            })
            .clone()
    }

    pub fn subscribe_paper_stake(&self, market: &str) -> broadcast::Receiver<PaperStakeWsMessage> {
        self.paper_stake_publisher(market).subscribe()
    }

    pub fn paper_stake_history(
        &self,
        market: &str,
        from_ms: Option<u64>,
        limit: usize,
    ) -> Vec<PaperStakeTick> {
        let key = addr_key(market);
        let limit = limit.clamp(1, PAPER_STAKE_HISTORY_LIMIT);
        let Some(hist) = self.paper_stake_history.get(&key) else {
            return Vec::new();
        };
        hist.iter()
            .filter(|t| from_ms.map(|f| t.ts_ms >= f).unwrap_or(true))
            .cloned()
            .rev()
            .take(limit)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect()
    }

    /// Sample live stake metrics for dirty markets (needs chain for supply / reward_pool).
    pub async fn flush_paper_stake_history(&self, chain: &SharedChainClient, publish: bool) {
        let markets: Vec<String> = self
            .paper_stake_dirty
            .iter()
            .map(|e| e.key().clone())
            .collect();
        if markets.is_empty() {
            return;
        }
        self.paper_stake_dirty.clear();
        let mut ts_ms = self.indexing_block_timestamp_ms();
        if ts_ms == 0 {
            ts_ms = wall_clock_ms();
        }
        for market in markets {
            let Ok(paper_market) = parse_token_contract(&market) else {
                continue;
            };
            let paper_token = match chain.get_paper_market(paper_market).await {
                Ok(m) => m.paper_token,
                Err(err) => {
                    tracing::debug!(error = %err, %market, "flush stake history get_mkt failed");
                    None
                }
            };
            let total_paper_supply = if let Some(token) = paper_token {
                match chain.get_token_info(Address::ZERO, token).await {
                    Ok(info) => info.total_supply,
                    Err(_) => 0,
                }
            } else {
                0
            };
            let staking_revenue = match chain
                .get_paper_stake(Address::ZERO, paper_market)
                .await
            {
                Ok(s) => s.reward_pool,
                Err(_) => 0,
            };
            let total_staking_reward = self.paper_total_staker_rewards(&market);
            let tick = PaperStakeTick {
                ts_ms,
                staking_revenue,
                total_paper_supply,
                total_staking_reward,
            };
            self.record_paper_stake_tick(&market, tick.clone());
            if publish {
                let _ = self.paper_stake_publisher(&market).send(PaperStakeWsMessage {
                    msg_type: "paper_stake".into(),
                    market: market.clone(),
                    ts_ms: tick.ts_ms,
                    staking_revenue: format_token_amount(tick.staking_revenue),
                    staking_revenue_raw: tick.staking_revenue,
                    total_paper_supply: format_token_amount(tick.total_paper_supply),
                    total_paper_supply_raw: tick.total_paper_supply,
                    total_staking_reward: format_token_amount(tick.total_staking_reward),
                    total_staking_reward_raw: tick.total_staking_reward,
                    stake: None,
                });
            }
        }
    }

    fn paper_rewards_claimed_key(market: &str, user: &str) -> String {
        format!("{}:{}", addr_key(market), addr_key(user))
    }

    pub fn apply_paper_fees_pushed_rewards(
        &self,
        market: &str,
        staker_fee: u64,
        swept: u64,
    ) {
        let add = staker_fee.saturating_add(swept);
        if add == 0 {
            return;
        }
        self.paper_total_staker_rewards
            .entry(addr_key(market))
            .and_modify(|v| *v = v.saturating_add(add))
            .or_insert(add);
    }

    pub fn apply_paper_claimed_reward(&self, market: &str, user: &str, reward: u64) {
        if reward == 0 {
            return;
        }
        let key = Self::paper_rewards_claimed_key(market, user);
        self.paper_rewards_claimed
            .entry(key)
            .and_modify(|v| *v = v.saturating_add(reward))
            .or_insert(reward);
    }

    pub fn paper_total_staker_rewards(&self, market: &str) -> u64 {
        self.paper_total_staker_rewards
            .get(&addr_key(market))
            .map(|v| *v)
            .unwrap_or(0)
    }

    pub fn paper_rewards_claimed(&self, market: &str, user: &str) -> u64 {
        self.paper_rewards_claimed
            .get(&Self::paper_rewards_claimed_key(market, user))
            .map(|v| *v)
            .unwrap_or(0)
    }

    fn paper_position_key(market: &str, position: u64) -> String {
        format!("{}:{}", addr_key(market), position)
    }

    pub async fn upsert_paper_position_open(&self, row: PaperPositionOpen) {
        let key = Self::paper_position_key(&row.market, row.position);
        self.apply_paper_oi_delta(
            &row.market,
            row.instrument,
            row.is_long,
            row.notional_raw as i128,
        );
        self.record_paper_volume(&row.market, row.notional_raw);
        self.publish_paper_lifecycle(
            "opened",
            &row.owner,
            &row.market,
            row.position,
            row.opened_block,
            Some(row.instrument),
            Some(row.is_long),
            Some(row.bust_price_raw),
            Some(row.notional.clone()),
            Some(row.notional_raw),
            Some(row.entry_price.clone()),
            Some(row.entry_price_raw),
            None,
            None,
        );
        self.mark_paper_house_dirty(&row.market);
        self.paper_positions_open.insert(key, row);
    }

    fn record_paper_volume(&self, market: &str, notional: u64) {
        self.paper_volume
            .entry(addr_key(market))
            .and_modify(|v| *v = v.saturating_add(notional))
            .or_insert(notional);
    }

    pub fn apply_paper_house_after(&self, market: &str, house: PaperHouseAfter) {
        self.paper_house.insert(addr_key(market), house);
        self.mark_paper_house_dirty(market);
    }

    fn paper_queue_key(market: &str, index: u64) -> String {
        format!("{}:{}", addr_key(market), index)
    }

    pub fn apply_paper_queued(
        &self,
        market: &str,
        index: u64,
        user: &str,
        amount: u64,
        house: PaperHouseAfter,
    ) {
        let block_ms = self.indexing_block_timestamp_ms();
        let queued_at_ms = if block_ms > 0 {
            block_ms
        } else {
            wall_clock_ms()
        };
        self.paper_queue_open.insert(
            Self::paper_queue_key(market, index),
            PaperQueueClaim {
                market: market.to_string(),
                index,
                user: user_key(user),
                amount,
                queued_at_ms,
            },
        );
        self.apply_paper_house_after(market, house);
    }

    pub fn apply_paper_harvested(
        &self,
        market: &str,
        index: u64,
        house: PaperHouseAfter,
    ) {
        self.paper_queue_open
            .remove(&Self::paper_queue_key(market, index));
        self.apply_paper_house_after(market, house);
    }

    fn paper_queue_claims_for_market(&self, market: &str) -> Vec<PaperQueueClaim> {
        let market_key = addr_key(market);
        let mut claims: Vec<PaperQueueClaim> = self
            .paper_queue_open
            .iter()
            .filter(|e| addr_key(&e.value().market) == market_key)
            .map(|e| e.value().clone())
            .collect();
        claims.sort_by_key(|c| c.index);
        claims
    }

    pub async fn paper_queue_placement(
        &self,
        market: &str,
        owner: &str,
    ) -> crate::domain::PaperQueuePlacement {
        use crate::chain::format_token_amount;
        use crate::domain::PaperQueuePlacement;

        let owner = user_key(owner);
        let claims = self.paper_queue_claims_for_market(market);
        let total: u64 = claims.iter().map(|c| c.amount).sum();
        let your_entries = claims.iter().filter(|c| c.user == owner).count() as u32;
        let yours_raw: u64 = claims
            .iter()
            .filter(|c| c.user == owner)
            .map(|c| c.amount)
            .sum();
        let first_yours = claims
            .iter()
            .filter(|c| c.user == owner)
            .map(|c| c.index)
            .min();
        let (ahead_raw, behind_raw) = match first_yours {
            Some(first) => {
                let ahead_raw: u64 = claims
                    .iter()
                    .filter(|c| c.index < first)
                    .map(|c| c.amount)
                    .sum();
                let behind_raw = total
                    .saturating_sub(ahead_raw)
                    .saturating_sub(yours_raw);
                (ahead_raw, behind_raw)
            }
            None => (0, total),
        };

        PaperQueuePlacement {
            market: market.to_string(),
            address: owner,
            ahead: format_token_amount(ahead_raw),
            ahead_raw,
            yours: format_token_amount(yours_raw),
            yours_raw,
            behind: format_token_amount(behind_raw),
            behind_raw,
            your_entries,
            in_queue: your_entries > 0,
        }
    }

    fn push_paper_house_history(&self, market: &str, ts_ms: u64, house: &PaperHouseView) {
        let key = addr_key(market);
        if key.is_empty() {
            return;
        }
        let tick = PaperHouseTick {
            ts_ms,
            house_funds: house.house_funds_raw,
            lp_balance: house.lp_balance_raw,
            open_house_pnl: house.open_house_pnl_raw,
            queue_debt: house.queue_debt_raw,
        };
        let mut hist = self
            .paper_house_history
            .entry(key)
            .or_insert_with(VecDeque::new);
        if let Some(last) = hist.back() {
            if last.ts_ms == ts_ms {
                *hist.back_mut().expect("back exists") = tick;
            } else {
                hist.push_back(tick);
            }
        } else {
            hist.push_back(tick);
        }
        while hist.len() > PAPER_HOUSE_HISTORY_LIMIT {
            hist.pop_front();
        }
        if let Some(persist) = self.persist.as_ref() {
            persist.enqueue_paper_house_history(PaperHouseHistoryRow {
                market: market.to_string(),
                ts_ms,
                house_funds_raw: house.house_funds_raw,
                lp_balance_raw: house.lp_balance_raw,
                open_house_pnl_raw: house.open_house_pnl_raw,
                queue_debt_raw: house.queue_debt_raw,
            });
        }
    }

    pub fn hydrate_paper_house_history_from_persist(&self) {
        let Some(persist) = self.persist.as_ref() else {
            return;
        };
        let markets = match persist.list_paper_house_history_markets() {
            Ok(m) => m,
            Err(err) => {
                tracing::warn!(error = %err, "load paper_house_history markets failed");
                return;
            }
        };
        let mut loaded = 0usize;
        for market in markets {
            let rows = match persist.load_paper_house_history(
                &market,
                None,
                PAPER_HOUSE_HISTORY_LIMIT,
            ) {
                Ok(rows) => rows,
                Err(err) => {
                    tracing::warn!(%market, error = %err, "load paper_house_history failed");
                    continue;
                }
            };
            if rows.is_empty() {
                continue;
            }
            let key = addr_key(&market);
            let mut hist = VecDeque::with_capacity(rows.len());
            for row in rows {
                hist.push_back(PaperHouseTick {
                    ts_ms: row.ts_ms,
                    house_funds: row.house_funds_raw,
                    lp_balance: row.lp_balance_raw,
                    open_house_pnl: row.open_house_pnl_raw,
                    queue_debt: row.queue_debt_raw,
                });
            }
            loaded = loaded.saturating_add(hist.len());
            self.paper_house_history.insert(key, hist);
        }
        if loaded > 0 {
            tracing::info!(ticks = loaded, "hydrated paper_house_history from sqlite");
        }
    }

    pub async fn paper_house_history(
        &self,
        market: &str,
        from_ms: Option<u64>,
        limit: usize,
    ) -> Vec<PaperHouseTick> {
        let key = addr_key(market);
        let limit = limit.clamp(1, PAPER_HOUSE_HISTORY_LIMIT);
        if !self.paper_house_history.contains_key(&key) {
            if let Some(persist) = self.persist.as_ref() {
                if let Ok(rows) =
                    persist.load_paper_house_history(market, from_ms, PAPER_HOUSE_HISTORY_LIMIT)
                {
                    if !rows.is_empty() {
                        let mut hist = VecDeque::with_capacity(rows.len());
                        for row in rows {
                            hist.push_back(PaperHouseTick {
                                ts_ms: row.ts_ms,
                                house_funds: row.house_funds_raw,
                                lp_balance: row.lp_balance_raw,
                                open_house_pnl: row.open_house_pnl_raw,
                                queue_debt: row.queue_debt_raw,
                            });
                        }
                        self.paper_house_history.insert(key.clone(), hist);
                    }
                }
            }
        }
        let Some(hist) = self.paper_house_history.get(&key) else {
            return Vec::new();
        };
        let filtered: Vec<PaperHouseTick> = hist
            .iter()
            .filter(|t| from_ms.map(|f| t.ts_ms >= f).unwrap_or(true))
            .cloned()
            .collect();
        if filtered.len() <= limit {
            return filtered;
        }
        filtered[filtered.len() - limit..].to_vec()
    }

    pub fn mark_paper_house_dirty(&self, market: &str) {
        let key = addr_key(market);
        if !key.is_empty() {
            self.paper_house_dirty.insert(key);
        }
    }

    fn paper_house_publisher(&self, market: &str) -> broadcast::Sender<PaperHouseWsMessage> {
        let key = addr_key(market);
        self.paper_house_publishers
            .entry(key)
            .or_insert_with(|| {
                let (sender, _) = broadcast::channel(64);
                sender
            })
            .clone()
    }

    pub fn subscribe_paper_house(&self, market: &str) -> broadcast::Receiver<PaperHouseWsMessage> {
        self.paper_house_publisher(market).subscribe()
    }

    pub async fn paper_house_ws(&self, market: &str) -> Option<PaperHouseWsMessage> {
        let market = market.trim();
        if market.is_empty() {
            return None;
        }
        let mut ts_ms = self.indexing_block_timestamp_ms();
        if ts_ms == 0 {
            ts_ms = wall_clock_ms();
        }
        let house = self.paper_house_view(market).await;
        Some(PaperHouseWsMessage {
            msg_type: "paper_house".into(),
            market: house.market.clone(),
            ts_ms,
            house,
        })
    }

    pub fn clear_paper_house_dirty(&self) {
        self.paper_house_dirty.clear();
    }

    /// Coalesce dirty markets: always record history (+ sqlite); WS only if `publish`.
    pub async fn flush_paper_house_ws(&self, publish: bool) {
        let markets: Vec<String> = self
            .paper_house_dirty
            .iter()
            .map(|e| e.key().clone())
            .collect();
        if markets.is_empty() {
            return;
        }
        self.paper_house_dirty.clear();
        let mut ts_ms = self.indexing_block_timestamp_ms();
        if ts_ms == 0 {
            ts_ms = wall_clock_ms();
        }
        for market in markets {
            let house = self.paper_house_view(&market).await;
            self.push_paper_house_history(&market, ts_ms, &house);
            if !publish {
                continue;
            }
            let msg = PaperHouseWsMessage {
                msg_type: "paper_house".into(),
                market: house.market.clone(),
                ts_ms,
                house,
            };
            let _ = self.paper_house_publisher(&market).send(msg);
        }
    }

    fn mark_paper_house_dirty_for_price_market(&self, price_market: &str) {
        let price_key = normalize_spot_market_key(price_market);
        if price_key.is_empty() {
            return;
        }
        for entry in self.paper_instruments.iter() {
            if entry.value().price_market != price_key {
                continue;
            }
            let Some((market, _)) = entry.key().rsplit_once(':') else {
                continue;
            };
            self.mark_paper_house_dirty(market);
        }
    }

    pub fn upsert_paper_instrument(
        &self,
        market: &str,
        instrument: u32,
        name: impl Into<String>,
        price_market: impl Into<String>,
    ) {
        self.paper_instruments.insert(
            paper_instr_key(market, instrument),
            PaperInstrumentMeta {
                name: name.into(),
                price_market: normalize_spot_market_key(&price_market.into()),
            },
        );
    }

    fn apply_paper_oi_delta(&self, market: &str, instrument: u32, is_long: bool, delta: i128) {
        let key = paper_instr_key(market, instrument);
        let mut entry = self
            .paper_oi
            .entry(key)
            .or_insert_with(PaperInstrumentOi::default);
        if is_long {
            if delta >= 0 {
                entry.long = entry.long.saturating_add(delta as u64);
            } else {
                entry.long = entry.long.saturating_sub((-delta) as u64);
            }
        } else if delta >= 0 {
            entry.short = entry.short.saturating_add(delta as u64);
        } else {
            entry.short = entry.short.saturating_sub((-delta) as u64);
        }
        let long = entry.long;
        let short = entry.short;
        drop(entry);
        self.push_paper_oi_history(market, instrument, long, short);
    }

    fn push_paper_oi_history(&self, market: &str, instrument: u32, long: u64, short: u64) {
        let key = paper_instr_key(market, instrument);
        let block_ms = self.indexing_block_timestamp_ms();
        let ts_ms = if block_ms > 0 {
            block_ms
        } else {
            wall_clock_ms()
        };
        let tick = PaperOiTick { ts_ms, long, short };
        let mut hist = self
            .paper_oi_history
            .entry(key)
            .or_insert_with(VecDeque::new);
        if let Some(last) = hist.back() {
            if last.ts_ms == ts_ms {
                *hist.back_mut().expect("back exists") = tick;
            } else {
                hist.push_back(tick);
            }
        } else {
            hist.push_back(tick);
        }
        while hist.len() > PAPER_OI_HISTORY_LIMIT {
            hist.pop_front();
        }
    }

    pub fn paper_instrument_name(&self, market: &str, instrument: u32) -> String {
        self.paper_instruments
            .get(&paper_instr_key(market, instrument))
            .map(|m| m.name.clone())
            .unwrap_or_else(|| format!("#{instrument}"))
    }

    pub async fn paper_oi_history(
        &self,
        market: &str,
        instrument: u32,
        from_ms: Option<u64>,
        limit: usize,
    ) -> Vec<PaperOiTick> {
        let key = paper_instr_key(market, instrument);
        let limit = limit.clamp(1, PAPER_OI_HISTORY_LIMIT);
        let Some(hist) = self.paper_oi_history.get(&key) else {
            return Vec::new();
        };
        let filtered: Vec<PaperOiTick> = hist
            .iter()
            .filter(|t| from_ms.map(|f| t.ts_ms >= f).unwrap_or(true))
            .cloned()
            .collect();
        if filtered.len() <= limit {
            return filtered;
        }
        filtered[filtered.len() - limit..].to_vec()
    }

    /// Event-sourced house overview (no node call).
    pub async fn paper_house_view(&self, market: &str) -> PaperHouseView {
        use crate::chain::format_token_amount;

        let market_key = addr_key(market);
        let snap = self
            .paper_house
            .get(&market_key)
            .map(|e| e.value().clone())
            .unwrap_or(PaperHouseAfter {
                free_cash: 0,
                side_bucket: 0,
                tracked_lp: 0,
                tail_hwm: 0,
                total_queued: 0,
                queue_head: 0,
                queue_tail: 0,
                staker_fee_acc: 0,
                dev_fee_acc: 0,
            });

        let mut open_margin_raw = 0u64;
        let mut open_positions = 0u32;
        let mut open_house_pnl_raw: i128 = 0;
        for entry in self.paper_positions_open.iter() {
            let row = entry.value();
            if addr_key(&row.market) != market_key {
                continue;
            }
            open_positions += 1;
            open_margin_raw = open_margin_raw.saturating_add(row.margin_raw);
            let mark = self
                .paper_instruments
                .get(&paper_instr_key(&row.market, row.instrument))
                .and_then(|meta| {
                    self.oracle_marks
                        .get(&meta.price_market)
                        .map(|m| m.mark_price)
                })
                .filter(|m| *m > 0)
                .unwrap_or(row.entry_price_raw);
            if row.entry_price_raw > 0 {
                let diff = (mark as i128) - (row.entry_price_raw as i128);
                let signed = if row.is_long { diff } else { -diff };
                let user_pnl =
                    (row.notional_raw as i128) * signed / (row.entry_price_raw as i128);
                open_house_pnl_raw -= user_pnl;
            }
        }

        let mut instruments: Vec<PaperInstrumentExposure> = Vec::new();
        let mut open_interest_raw = 0u64;
        for entry in self.paper_oi.iter() {
            let key = entry.key();
            let prefix = format!("{market_key}:");
            let Some(instr_str) = key.strip_prefix(&prefix) else {
                continue;
            };
            let Ok(instrument) = instr_str.parse::<u32>() else {
                continue;
            };
            let oi = entry.value();
            if oi.long == 0 && oi.short == 0 {
                continue;
            }
            open_interest_raw = open_interest_raw
                .saturating_add(oi.long)
                .saturating_add(oi.short);
            let name = self
                .paper_instruments
                .get(key)
                .map(|m| m.name.clone())
                .unwrap_or_else(|| format!("#{instrument}"));
            let net_raw = oi.long as i128 - oi.short as i128;
            instruments.push(PaperInstrumentExposure {
                instrument,
                name,
                long: format_token_amount(oi.long),
                long_raw: oi.long,
                short: format_token_amount(oi.short),
                short_raw: oi.short,
                net: format_i128_token(net_raw),
                net_raw,
            });
        }
        instruments.sort_by_key(|i| i.instrument);

        let lp_balance_raw = snap.free_cash.saturating_add(snap.side_bucket);
        let queue_debt_raw = snap.total_queued;
        let house_funds_raw =
            (lp_balance_raw as i128) + open_house_pnl_raw - (queue_debt_raw as i128);
        let open_house_pnl_i64 = open_house_pnl_raw.clamp(i64::MIN as i128, i64::MAX as i128) as i64;
        let volume_raw = self
            .paper_volume
            .get(&market_key)
            .map(|v| *v)
            .unwrap_or(0);

        let claims = self.paper_queue_claims_for_market(market);
        let queue_debt_positions = claims.len() as u32;
        let queue_largest_raw = claims.iter().map(|c| c.amount).max().unwrap_or(0);
        let paid_next = claims
            .iter()
            .find(|c| c.index == snap.queue_head)
            .or_else(|| claims.first());
        let (queue_paid_next_raw, queue_paid_next_user) = match paid_next {
            Some(c) => (c.amount, c.user.clone()),
            None => (0, String::new()),
        };

        PaperHouseView {
            market: market.to_string(),
            free_cash: format_token_amount(snap.free_cash),
            free_cash_raw: snap.free_cash,
            side_bucket: format_token_amount(snap.side_bucket),
            side_bucket_raw: snap.side_bucket,
            tracked_lp: format_i128_token(snap.tracked_lp),
            tracked_lp_raw: snap.tracked_lp,
            tail_hwm: format_token_amount(snap.tail_hwm),
            tail_hwm_raw: snap.tail_hwm,
            queue_head: snap.queue_head,
            queue_tail: snap.queue_tail,
            total_queued: format_token_amount(snap.total_queued),
            total_queued_raw: snap.total_queued,
            staker_fee_acc: format_token_amount(snap.staker_fee_acc),
            staker_fee_acc_raw: snap.staker_fee_acc,
            dev_fee_acc: format_token_amount(snap.dev_fee_acc),
            dev_fee_acc_raw: snap.dev_fee_acc,
            queue_nonempty: snap.total_queued > 0,
            open_interest: format_token_amount(open_interest_raw),
            open_interest_raw,
            open_margin: format_token_amount(open_margin_raw),
            open_margin_raw,
            open_positions,
            instruments,
            lp_balance: format_token_amount(lp_balance_raw),
            lp_balance_raw,
            open_house_pnl: format_i128_token(open_house_pnl_raw),
            open_house_pnl_raw: open_house_pnl_i64,
            queue_debt: format_token_amount(queue_debt_raw),
            queue_debt_raw,
            queue_debt_positions,
            queue_largest: if queue_largest_raw > 0 {
                format_token_amount(queue_largest_raw)
            } else {
                String::new()
            },
            queue_largest_raw,
            queue_paid_next: if queue_paid_next_raw > 0 {
                format_token_amount(queue_paid_next_raw)
            } else {
                String::new()
            },
            queue_paid_next_raw,
            queue_paid_next_user,
            house_funds: format_i128_token(house_funds_raw),
            house_funds_raw,
            volume: format_token_amount(volume_raw),
            volume_raw,
        }
    }

    pub fn publish_paper_lifecycle(
        &self,
        event: &str,
        user_address: &str,
        market: &str,
        position: u64,
        block_num: u64,
        instrument: Option<u32>,
        is_long: Option<bool>,
        bust_price_raw: Option<u64>,
        notional: Option<String>,
        notional_raw: Option<u64>,
        entry_price: Option<String>,
        entry_price_raw: Option<u64>,
        pnl: Option<String>,
        pnl_raw: Option<i64>,
    ) {
        let instrument_name = instrument.and_then(|id| {
            self.paper_instruments
                .get(&paper_instr_key(market, id))
                .map(|m| m.name.clone())
        });
        let _ = self.paper_user_publisher.send(PaperUserWsMessage {
            msg_type: "paper_user".into(),
            event: event.to_string(),
            user_address: user_address.to_string(),
            market: market.to_string(),
            position,
            block_num,
        });
        let _ = self.paper_positions_publisher.send(PaperPositionWsMessage {
            msg_type: "paper_position".into(),
            event: event.to_string(),
            user_address: user_address.to_string(),
            market: market.to_string(),
            position,
            block_num,
            instrument,
            is_long,
            bust_price_raw,
            notional,
            notional_raw,
            entry_price,
            entry_price_raw,
            instrument_name,
            pnl,
            pnl_raw,
        });
    }

    pub fn subscribe_paper_user(&self) -> broadcast::Receiver<PaperUserWsMessage> {
        self.paper_user_publisher.subscribe()
    }

    pub fn subscribe_paper_positions(&self) -> broadcast::Receiver<PaperPositionWsMessage> {
        self.paper_positions_publisher.subscribe()
    }

    pub async fn close_paper_position(
        &self,
        market: &str,
        position: u64,
        owner_fallback: &str,
        closed_block: u64,
        exit_price: String,
        fee: String,
        payout: String,
        queued: String,
        minted: String,
        pnl: String,
        pnl_raw: i64,
        kind: PaperCloseKind,
        liquidator: Option<String>,
    ) {
        let key = Self::paper_position_key(market, position);
        let open = self.paper_positions_open.remove(&key).map(|(_, v)| v);
        let (
            instrument,
            is_long,
            margin,
            notional,
            notional_raw,
            entry_price,
            entry_price_raw,
            bust_price,
            owner,
            opened_block,
        ) = if let Some(o) = open {
            self.apply_paper_oi_delta(
                market,
                o.instrument,
                o.is_long,
                -(o.notional_raw as i128),
            );
            (
                o.instrument,
                o.is_long,
                o.margin,
                o.notional,
                o.notional_raw,
                o.entry_price,
                o.entry_price_raw,
                o.bust_price,
                o.owner,
                o.opened_block,
            )
        } else {
            (
                0,
                true,
                "0".into(),
                "0".into(),
                0,
                "0".into(),
                0,
                "0".into(),
                owner_fallback.to_string(),
                0,
            )
        };
        let event = match kind {
            PaperCloseKind::Liquidated => "liquidated",
            PaperCloseKind::Closed => "closed",
        };
        self.publish_paper_lifecycle(
            event,
            &owner,
            market,
            position,
            closed_block,
            Some(instrument),
            Some(is_long),
            None,
            Some(notional.clone()),
            Some(notional_raw),
            Some(entry_price.clone()),
            Some(entry_price_raw),
            Some(pnl.clone()),
            Some(pnl_raw),
        );
        self.mark_paper_house_dirty(market);
        let hist = PaperPositionHistory {
            market: addr_key(market),
            position,
            owner,
            instrument,
            is_long,
            margin,
            notional,
            entry_price,
            bust_price,
            exit_price,
            pnl,
            fee,
            payout,
            queued,
            minted,
            kind,
            liquidator,
            opened_block,
            closed_block,
        };
        self.paper_positions_history.insert(key, hist);
    }

    pub async fn query_paper_positions(
        &self,
        query: PaperPositionQuery,
    ) -> (Vec<PaperPositionOpen>, Vec<PaperPositionHistory>, u32) {
        let owner = query.owner.as_ref().map(|s| user_key(s));
        let market = query.market.as_ref().map(|s| addr_key(s));
        let limit = query.limit.max(1) as usize;
        let offset = query.offset as usize;

        let mut open_rows: Vec<PaperPositionOpen> = self
            .paper_positions_open
            .iter()
            .map(|e| e.value().clone())
            .filter(|row| {
                owner
                    .as_ref()
                    .map(|o| user_key(&row.owner) == *o)
                    .unwrap_or(true)
                    && market
                        .as_ref()
                        .map(|m| addr_key(&row.market) == *m)
                        .unwrap_or(true)
            })
            .collect();
        open_rows.sort_by(|a, b| {
            b.opened_block
                .cmp(&a.opened_block)
                .then(b.position.cmp(&a.position))
        });

        let mut hist_rows: Vec<PaperPositionHistory> = self
            .paper_positions_history
            .iter()
            .map(|e| e.value().clone())
            .filter(|row| {
                owner
                    .as_ref()
                    .map(|o| user_key(&row.owner) == *o)
                    .unwrap_or(true)
                    && market
                        .as_ref()
                        .map(|m| addr_key(&row.market) == *m)
                        .unwrap_or(true)
            })
            .collect();
        hist_rows.sort_by(|a, b| {
            b.closed_block
                .cmp(&a.closed_block)
                .then(b.position.cmp(&a.position))
        });

        match query.status {
            PaperPositionStatus::Open => {
                let total = open_rows.len() as u32;
                let page = open_rows.into_iter().skip(offset).take(limit).collect();
                (page, Vec::new(), total)
            }
            PaperPositionStatus::Closed => {
                let total = hist_rows.len() as u32;
                let page = hist_rows.into_iter().skip(offset).take(limit).collect();
                (Vec::new(), page, total)
            }
            PaperPositionStatus::All => {
                let open_len = open_rows.len();
                let total = (open_len + hist_rows.len()) as u32;
                let page_open: Vec<_> = open_rows.into_iter().skip(offset).take(limit).collect();
                let remain = limit.saturating_sub(page_open.len());
                let hist_skip = offset.saturating_sub(open_len);
                let page_hist: Vec<_> = hist_rows
                    .into_iter()
                    .skip(hist_skip)
                    .take(remain)
                    .collect();
                (page_open, page_hist, total)
            }
        }
    }

    pub async fn sum_open_margin(&self, owner: &str, market: Option<&str>) -> u64 {
        let owner = user_key(owner);
        let market = market.map(addr_key);
        self.paper_positions_open
            .iter()
            .filter(|e| {
                let row = e.value();
                user_key(&row.owner) == owner
                    && market
                        .as_ref()
                        .map(|m| addr_key(&row.market) == *m)
                        .unwrap_or(true)
            })
            .map(|e| e.value().margin_raw)
            .sum()
    }

    pub async fn count_open_positions(&self, owner: &str, market: Option<&str>) -> u32 {
        let owner = user_key(owner);
        let market = market.map(addr_key);
        self.paper_positions_open
            .iter()
            .filter(|e| {
                let row = e.value();
                user_key(&row.owner) == owner
                    && market
                        .as_ref()
                        .map(|m| addr_key(&row.market) == *m)
                        .unwrap_or(true)
            })
            .count() as u32
    }


    pub async fn market_count(&self) -> usize {
        self.markets.len()
    }

    pub async fn export_markets_for_persist(&self) -> Vec<Market> {
        self.markets.iter().map(|e| e.value().clone()).collect()
    }

    pub async fn export_orders_for_persist(&self) -> Vec<crate::persist::PersistOrderRow> {
        self.export_orders_for_persist_spots(None).await
    }

    pub async fn export_orders_for_persist_spots(
        &self,
        only_spots: Option<&[String]>,
    ) -> Vec<crate::persist::PersistOrderRow> {
        self.orders
            .iter()
            .filter(|e| {
                let spot = &e.value().spot_market;
                match only_spots {
                    Some(spots) => spots.iter().any(|s| s == spot),
                    None => true,
                }
            })
            .map(|e| {
                let stored = e.value();
                crate::persist::PersistOrderRow {
                    order: stored.order.clone(),
                    user_address: stored.user_address.clone(),
                    chain_order_id: stored.chain_order_id.clone(),
                    spot_market: stored.spot_market.clone(),
                    size_raw: stored.size_raw,
                    filled_raw: stored.filled_raw,
                    status_ts_ms: 0,
                }
            })
            .collect()
    }

    pub async fn export_last_trades_for_persist(&self) -> Vec<(String, u64)> {
        self.export_last_trades_for_persist_spots(None).await
    }

    pub async fn export_last_trades_for_persist_spots(
        &self,
        only_spots: Option<&[String]>,
    ) -> Vec<(String, u64)> {
        self.last_trade_price
            .iter()
            .filter(|e| match only_spots {
                Some(spots) => spots.iter().any(|s| s == e.key()),
                None => true,
            })
            .map(|e| (e.key().clone(), *e.value()))
            .collect()
    }

    pub async fn export_vaults_for_persist(&self) -> Vec<Vault> {
        self.vaults.iter().map(|e| e.value().clone()).collect()
    }

    pub async fn export_vault_portfolio_for_persist(
        &self,
    ) -> Vec<crate::persist::PersistVaultPortfolioRow> {
        let mut out = Vec::new();
        for entry in self.vault_portfolio.iter() {
            let vault_id = entry.key().to_string();
            for (spot_market, amount_raw) in entry.value().iter() {
                if *amount_raw == 0 {
                    continue;
                }
                out.push(crate::persist::PersistVaultPortfolioRow {
                    vault_id: vault_id.clone(),
                    spot_market: spot_market.clone(),
                    amount_raw: *amount_raw,
                });
            }
        }
        out
    }

    pub async fn import_vault_portfolio_for_persist(
        &self,
        rows: Vec<crate::persist::PersistVaultPortfolioRow>,
    ) {
        self.vault_portfolio.clear();
        for row in rows {
            let Ok(vault_id) = Uuid::parse_str(&row.vault_id) else {
                tracing::warn!(vault_id = %row.vault_id, "skip vault portfolio row with bad id");
                continue;
            };
            let spot = normalize_spot_market_key(&row.spot_market);
            if row.amount_raw == 0 {
                continue;
            }
            self.vault_portfolio
                .entry(vault_id)
                .or_default()
                .insert(spot, row.amount_raw);
        }
    }

    pub async fn vault_count(&self) -> usize {
        self.vaults.len()
    }

    pub async fn query_vaults(&self, query: VaultQuery) -> (Vec<Vault>, usize) {
        let mut vaults: Vec<Vault> = self.vaults.iter().map(|e| e.value().clone()).collect();

        if let Some(manager) = query.manager.as_deref() {
            let manager = manager.trim().to_ascii_lowercase();
            vaults.retain(|vault| vault.manager.trim().to_ascii_lowercase() == manager);
        }

        if !query.vault_addresses.is_empty() {
            let allowed: HashSet<String> = query
                .vault_addresses
                .iter()
                .map(|address| address.trim().to_ascii_lowercase())
                .collect();
            vaults.retain(|vault| {
                allowed.contains(&vault.vault_address.trim().to_ascii_lowercase())
            });
        }

        vaults.sort_by(|left, right| left.vault_address.cmp(&right.vault_address));

        let total = vaults.len();
        let offset = query.offset as usize;
        let page = vaults
            .into_iter()
            .skip(offset)
            .take(query.limit as usize)
            .collect();

        (page, total)
    }

    pub async fn get_vault(&self, id: Uuid) -> Option<Vault> {
        self.vaults.get(&id).map(|e| e.value().clone())
    }

    pub async fn get_vault_by_address(&self, vault_address: &str) -> Option<Vault> {
        let key = addr_key(vault_address);
        let id = self.vault_by_address.get(&key)?.clone();
        self.vaults.get(&id).map(|e| e.value().clone())
    }

    pub async fn upsert_vault(&self, vault: Vault, mut ws: Option<&mut IndexWriteSet>) {
        let address_key = addr_key(&vault.vault_address);
        let account_key = addr_key(&vault.vault_account);
        self.vault_by_address.insert(address_key, vault.id);
        self.vault_by_account.insert(account_key, vault.id);
        self.vault_portfolio.entry(vault.id).or_default();
        if let Some(ws) = ws {
            ws.store_vault(vault.clone());
        }
        self.vaults.insert(vault.id, vault);
    }

    pub async fn apply_vault_fill_to_portfolio(
        &self,
        user_address: &str,
        spot_market: &str,
        side: lightpool_sdk::OrderSide,
        fill_amount: u64,
        mut ws: Option<&mut IndexWriteSet>,
    ) {
        if fill_amount == 0 {
            return;
        }
        let account_key = user_key(user_address);
        let market_key = normalize_spot_market_key(spot_market);
        let Some(vault_id) = self.vault_by_account.get(&account_key).map(|e| *e) else {
            return;
        };
        let mut holdings = self.vault_portfolio.entry(vault_id).or_default();
        let current = holdings.get(&market_key).copied().unwrap_or(0);
        let next = match side {
            lightpool_sdk::OrderSide::Buy => current.saturating_add(fill_amount),
            lightpool_sdk::OrderSide::Sell => {
                if fill_amount > current {
                    tracing::warn!(
                        user_address,
                        spot_market = %market_key,
                        current,
                        fill_amount,
                        "vault portfolio sell underflow; clamping to zero"
                    );
                    0
                } else {
                    current - fill_amount
                }
            }
        };
        if next == 0 {
            holdings.remove(&market_key);
        } else {
            holdings.insert(market_key.clone(), next);
        }
        let vault_id_str = vault_id.to_string();
        drop(holdings);
        if let Some(ws) = ws {
            ws.store_vault_portfolio(&vault_id_str, &market_key, next as i64);
        }
    }

    pub async fn vault_portfolio_holdings(&self, vault_address: &str) -> Vec<(String, u64)> {
        let key = addr_key(vault_address);
        let Some(vault_id) = self.vault_by_address.get(&key).map(|e| *e) else {
            return Vec::new();
        };
        let Some(holdings) = self.vault_portfolio.get(&vault_id) else {
            return Vec::new();
        };
        let mut assets: Vec<(String, u64)> = holdings
            .iter()
            .filter(|(_, amount)| **amount > 0)
            .map(|(market, amount)| (market.clone(), *amount))
            .collect();
        assets.sort_by(|left, right| left.0.cmp(&right.0));
        assets
    }

    pub async fn update_vault_equity(&self, vault_address: &str, equity: &str, mut ws: Option<&mut IndexWriteSet>) {
        let key = addr_key(vault_address);
        let Some(id) = self.vault_by_address.get(&key).map(|e| *e) else {
            return;
        };
        if let Some(mut vault) = self.vaults.get_mut(&id) {
            vault.equity = equity.to_string();
            if let Some(ws) = ws { ws.store_vault(vault.clone()); }
        }
    }

    pub async fn update_vault_allow_deposit(&self, vault_address: &str, allow_deposit: bool, mut ws: Option<&mut IndexWriteSet>) {
        let key = addr_key(vault_address);
        let Some(id) = self.vault_by_address.get(&key).map(|e| *e) else {
            return;
        };
        if let Some(mut vault) = self.vaults.get_mut(&id) {
            vault.allow_deposit = allow_deposit;
            if let Some(ws) = ws { ws.store_vault(vault.clone()); }
        }
    }

    pub async fn update_vault_manager(&self, vault_address: &str, manager: &str, mut ws: Option<&mut IndexWriteSet>) {
        let key = addr_key(vault_address);
        let Some(id) = self.vault_by_address.get(&key).map(|e| *e) else {
            return;
        };
        if let Some(mut vault) = self.vaults.get_mut(&id) {
            vault.manager = manager.to_string();
            if let Some(ws) = ws { ws.store_vault(vault.clone()); }
        }
    }

    pub async fn mark_vault_closed(&self, vault_address: &str, mut ws: Option<&mut IndexWriteSet>) {
        let key = addr_key(vault_address);
        let Some(id) = self.vault_by_address.get(&key).map(|e| *e) else {
            return;
        };
        if let Some(mut vault) = self.vaults.get_mut(&id) {
            vault.is_closed = true;
            vault.allow_deposit = false;
            if let Some(ws) = ws { ws.store_vault(vault.clone()); }
        }
    }

    pub async fn query_markets(&self, query: MarketQuery) -> (Vec<Market>, usize) {
        let mut markets: Vec<Market> = self.markets.iter().map(|e| e.value().clone()).collect();

        if let Some(slug) = query.slug.as_deref() {
            markets.retain(|market| market.slug() == slug || market.name() == slug);
        }

        if !query.slugs.is_empty() {
            let allowed: HashSet<&str> = query.slugs.iter().map(String::as_str).collect();
            markets.retain(|market| {
                allowed.contains(market.slug()) || allowed.contains(market.name())
            });
        }

        if !query.market_ids.is_empty() {
            let allowed: HashSet<Uuid> = query.market_ids.iter().copied().collect();
            markets.retain(|market| allowed.contains(&market.id()));
        }

        if !query.market_addresses.is_empty() {
            let allowed: HashSet<String> = query
                .market_addresses
                .iter()
                .map(|address| address.trim().to_ascii_lowercase())
                .collect();
            markets.retain(|market| {
                allowed.contains(&market.market_address().trim().to_ascii_lowercase())
            });
        }

        if let Some(state) = query.state.as_deref() {
            markets.retain(|market| market.state().eq_ignore_ascii_case(state));
        }

        if let Some(category) = query.category {
            markets.retain(|market| market.category() == category);
        }

        if let Some(deployer) = query.deployer.as_deref() {
            let want = deployer.trim().to_ascii_lowercase();
            markets.retain(|market| market.deployer().trim().to_ascii_lowercase() == want);
        }

        match query.order {
            MarketSortOrder::ResolutionDeadline => {
                if query.ascending {
                    markets.sort_by_key(|market| market.resolution_deadline());
                } else {
                    markets.sort_by_key(|market| std::cmp::Reverse(market.resolution_deadline()));
                }
            }
            MarketSortOrder::Slug => {
                if query.ascending {
                    markets.sort_by(|left, right| {
                        left.slug()
                            .cmp(right.slug())
                            .then_with(|| left.name().cmp(right.name()))
                    });
                } else {
                    markets.sort_by(|left, right| {
                        right
                            .slug()
                            .cmp(left.slug())
                            .then_with(|| right.name().cmp(left.name()))
                    });
                }
            }
            MarketSortOrder::Question => {
                if query.ascending {
                    markets.sort_by(|left, right| left.label().cmp(right.label()));
                } else {
                    markets.sort_by(|left, right| right.label().cmp(left.label()));
                }
            }
        }

        let total = markets.len();
        let offset = query.offset as usize;
        let page = markets
            .into_iter()
            .skip(offset)
            .take(query.limit as usize)
            .collect();

        (page, total)
    }

    pub async fn get_market(&self, id: Uuid) -> Option<Market> {
        self.markets.get(&id).map(|e| e.value().clone())
    }

    pub async fn get_event_by_slug(&self, slug: &str) -> Option<Market> {
        let id = self.slug_to_market.get(slug)?.clone();
        self.markets.get(&id).map(|e| e.value().clone())
    }

    pub async fn allocate_market_slug(&self, question: &str) -> String {
        let existing_slugs: Vec<String> = self
            .slug_to_market
            .iter()
            .map(|e| e.key().clone())
            .collect();
        crate::slug::allocate_unique_slug(&existing_slugs, question)
    }

    pub async fn list_orders_for_user(&self, user_address: &str) -> Vec<OrderQueryRecord> {
        let key = user_key(user_address);
        let Some(ids) = self.orders_by_user.get(&key) else {
            return Vec::new();
        };
        let mut out = Vec::new();
        for id in ids.iter() {
            let Some(stored) = self.orders.get(&id) else {
                continue;
            };
            out.push(Self::order_query_record(stored.value()));
        }
        out
    }

    pub async fn position_token_specs(&self) -> Vec<(String, String)> {
        let mut specs = Vec::new();
        for market in self.markets.iter() {
            let yes = market.yes_token();
            let no = market.no_token();
            if !yes.is_empty() {
                specs.push(("YES".into(), yes.to_string()));
            }
            if !no.is_empty() {
                specs.push(("NO".into(), no.to_string()));
            }
        }
        specs
    }

    fn remove_slug_mappings_for_market(&self, market_id: Uuid) {
        self.slug_to_market.retain(|_, id| *id != market_id);
    }

    pub async fn upsert_market(&self, mut market: Market, mut ws: Option<&mut IndexWriteSet>) {
        if let Some(existing) = self.markets.get(&market.id()) {
            market.set_icon_url_if_empty(existing.icon_url().cloned());
            if let (Some(slot), existing_name) = (market.name_mut(), existing.name()) {
                if slot.is_empty() && !existing_name.is_empty() {
                    *slot = existing_name.to_string();
                }
            }
            if let (Some(slot), existing_slug) = (market.slug_mut(), existing.slug()) {
                if slot.is_empty() && !existing_slug.is_empty() {
                    *slot = existing_slug.to_string();
                }
            }
        }

        if !market.slug().is_empty() {
            self.remove_slug_mappings_for_market(market.id());
            self.slug_to_market
                .insert(market.slug().to_string(), market.id());
        }

        let yes_spot = normalize_spot_market_key(market.yes_spot_market());
        let no_spot = normalize_spot_market_key(market.no_spot_market());
        if let Some(slot) = market.yes_spot_market_mut() {
            *slot = yes_spot.clone();
        }
        if let Some(slot) = market.no_spot_market_mut() {
            *slot = no_spot.clone();
        }
        if let Market::Spot {
            market_address, ..
        } = &mut market
        {
            *market_address = yes_spot.clone();
        }
        if let Market::Perp {
            market_address, ..
        } = &mut market
        {
            *market_address = yes_spot.clone();
        }

        if matches!(market, Market::Event { .. }) && yes_spot == no_spot {
            tracing::warn!(
                market_id = %market.id(),
                slug = %market.slug(),
                spot_market = %yes_spot,
                "event market yes and no spot markets share the same address"
            );
        }

        let market_id = market.id();
        let category = market.category();
        if let Some(ws) = ws.as_deref_mut() {
            ws.store_market(market.clone());
        }
        self.markets.insert(market_id, market);

        match category {
            MarketCategory::Event => {
                self.bind_event_spot_legs(market_id, &yes_spot, &no_spot, ws);
            }
            MarketCategory::Spot => {
                self.spot_to_market.insert(
                    yes_spot,
                    SpotMarketRef {
                        market_id,
                        outcome: "spot".into(),
                    },
                );
            }
            MarketCategory::Perp => {
                self.spot_to_market.insert(
                    yes_spot,
                    SpotMarketRef {
                        market_id,
                        outcome: "perp".into(),
                    },
                );
            }
        }
    }

    /// Point yes/no spot venues at an event instrument; drop orphan spot shells for those addresses.
    fn bind_event_spot_legs(&self, event_id: Uuid, yes_spot: &str, no_spot: &str, mut ws: Option<&mut IndexWriteSet>) {
        for (spot, outcome) in [(yes_spot, "yes"), (no_spot, "no")] {
            self.remove_orphan_spot_shell(spot, ws.as_deref_mut());
            self.spot_to_market.insert(
                spot.to_string(),
                SpotMarketRef {
                    market_id: event_id,
                    outcome: outcome.into(),
                },
            );
        }
    }

    fn remove_orphan_spot_shell(&self, spot_market: &str, mut ws: Option<&mut IndexWriteSet>) {
        let orphan_id = market_uuid(spot_market);
        let Some((_, orphan)) = self.markets.remove(&orphan_id) else {
            return;
        };
        if orphan.category() != MarketCategory::Spot {
            self.markets.insert(orphan_id, orphan);
            return;
        }
        // Spot shells are not in slug_to_market; event slug entries are left untouched.
        if let Some(ws) = ws { ws.delete_market(&orphan_id.to_string()); }
    }

    pub async fn update_market_state(&self, market_address: &str, state: &str, mut ws: Option<&mut IndexWriteSet>) {
        for mut market in self.markets.iter_mut() {
            if market.market_address() == market_address {
                *market.state_mut() = state.to_string();
                if let Some(ws) = ws.as_deref_mut() {
                    ws.store_market(market.clone());
                }
            }
        }
    }

    pub async fn lookup_spot_market(&self, spot_market: &str) -> Option<(Uuid, String)> {
        let key = normalize_spot_market_key(spot_market);
        self.spot_to_market
            .get(&key)
            .map(|spot| (spot.market_id, spot.outcome.clone()))
    }

    pub async fn list_spot_markets(&self) -> Vec<String> {
        self.spot_to_market
            .iter()
            .map(|e| e.key().clone())
            .collect()
    }

    pub async fn register_named_spot_market(
        &self,
        name: &str,
        spot_market: &str,
        deployer: &str,
    ) {
        let spot = normalize_spot_market_key(spot_market);
        let name_key = name.trim().to_ascii_uppercase();
        if name_key.is_empty() || spot.is_empty() {
            return;
        }

        self.spot_by_name.insert(name_key, spot.clone());

        // Already an event (or other) leg — keep name indexes only, do not create a spot shell.
        if let Some(existing) = self.spot_to_market.get(&spot) {
            if let Some(market) = self.markets.get(&existing.market_id) {
                if market.category() != MarketCategory::Spot {
                    return;
                }
            }
        }

        let market_id = market_uuid(&spot);
        self.spot_to_market
            .entry(spot.clone())
            .or_insert(SpotMarketRef {
                market_id,
                outcome: "spot".into(),
            });
        self.markets.entry(market_id).or_insert_with(|| Market::Spot {
            id: market_id,
            name: name.trim().to_string(),
            icon_url: None,
            market_address: spot.clone(),
            state: "Active".into(),
            deployer: deployer.trim().to_string(),
            base_token: String::new(),
            quote_token: String::new(),
        });
    }

    pub async fn ensure_standalone_spot_market(&self, spot_market: &str) -> (Uuid, String) {
        let spot = normalize_spot_market_key(spot_market);
        if let Some(existing) = self.lookup_spot_market(&spot).await {
            return existing;
        }
        let name = self
            .spot_by_name
            .iter()
            .find(|e| e.value() == &spot)
            .map(|e| e.key().clone())
            .unwrap_or_else(|| spot.clone());
        self.register_named_spot_market(&name, &spot, "").await;
        self.lookup_spot_market(&spot)
            .await
            .unwrap_or_else(|| (market_uuid(&spot), "spot".into()))
    }

    pub async fn resolve_spot_market_key(&self, id_or_name: &str) -> Option<String> {
        let key = id_or_name.trim();
        if key.is_empty() {
            return None;
        }
        if key.starts_with("0x") || key.starts_with("0X") {
            return Some(normalize_spot_market_key(key));
        }

        let upper = key.to_ascii_uppercase();
        self.spot_by_name.get(&upper).map(|e| e.value().clone())
    }

    pub async fn record_last_trade_price(&self, spot_market: &str, price: u64, mut ws: Option<&mut IndexWriteSet>) {
        let key = normalize_spot_market_key(spot_market);
        self.last_trade_price.insert(key.clone(), price);
        if let Some(ws) = ws { ws.store_last_trade(&key, price as i64); }
    }

    pub async fn last_trade_price(&self, spot_market: &str) -> Option<u64> {
        let key = normalize_spot_market_key(spot_market);
        self.last_trade_price.get(&key).map(|e| *e)
    }

    fn oracle_ws_message(mark: &OracleMark) -> OracleMarkWsMessage {
        OracleMarkWsMessage {
            msg_type: "oracle".into(),
            spot_market: mark.spot_market.clone(),
            round: mark.round,
            oracle_price: crate::chain::format_oracle_human(mark.oracle_price),
            mark_price: crate::chain::format_oracle_human(mark.mark_price),
            oracle_price_raw: mark.oracle_price,
            mark_price_raw: mark.mark_price,
            block_num: mark.block_num,
            ts_ms: mark.ts_ms,
        }
    }

    fn oracle_publisher(&self, spot_market: &str) -> broadcast::Sender<OracleMarkWsMessage> {
        let key = normalize_spot_market_key(spot_market);
        self.oracle_publishers
            .entry(key)
            .or_insert_with(|| {
                let (sender, _) = broadcast::channel(256);
                sender
            })
            .clone()
    }

    pub async fn record_oracle_mark(
        &self,
        spot_market: &str,
        round: u64,
        oracle_price: u64,
        mark_price: u64,
        block_num: u64,
    ) {
        let key = normalize_spot_market_key(spot_market);
        let mut ts_ms = self.indexing_block_timestamp_ms();
        if ts_ms == 0 {
            ts_ms = wall_clock_ms();
        }
        let mark = OracleMark {
            spot_market: key.clone(),
            round,
            oracle_price,
            mark_price,
            block_num,
            ts_ms,
        };
        let tick = OracleMarkTick {
            ts_ms,
            round,
            oracle_price,
            mark_price,
            block_num,
        };
        {
            let mut hist = self
                .oracle_history
                .entry(key.clone())
                .or_insert_with(VecDeque::new);
            if let Some(last) = hist.back() {
                // Keep one sample per ms; overwrite same-timestamp updates.
                if last.ts_ms == ts_ms {
                    *hist.back_mut().expect("back exists") = tick.clone();
                } else {
                    hist.push_back(tick);
                }
            } else {
                hist.push_back(tick);
            }
            while hist.len() > ORACLE_HISTORY_LIMIT {
                hist.pop_front();
            }
        }
        let msg = Self::oracle_ws_message(&mark);
        self.oracle_marks.insert(key.clone(), mark);
        let _ = self.oracle_publisher(&key).send(msg);
        self.mark_paper_house_dirty_for_price_market(&key);
    }

    pub async fn oracle_mark(&self, spot_market: &str) -> Option<OracleMark> {
        let key = normalize_spot_market_key(spot_market);
        self.oracle_marks.get(&key).map(|e| e.value().clone())
    }

    pub async fn oracle_mark_history(
        &self,
        spot_market: &str,
        from_ms: Option<u64>,
        limit: usize,
    ) -> Vec<OracleMarkTick> {
        let key = normalize_spot_market_key(spot_market);
        let limit = limit.clamp(1, ORACLE_HISTORY_LIMIT);
        let Some(hist) = self.oracle_history.get(&key) else {
            return Vec::new();
        };
        let filtered: Vec<OracleMarkTick> = hist
            .iter()
            .filter(|t| from_ms.map(|f| t.ts_ms >= f).unwrap_or(true))
            .cloned()
            .collect();
        if filtered.len() <= limit {
            return filtered;
        }
        filtered[filtered.len() - limit..].to_vec()
    }

    pub async fn oracle_mark_ws(&self, spot_market: &str) -> Option<OracleMarkWsMessage> {
        self.oracle_mark(spot_market)
            .await
            .map(|mark| Self::oracle_ws_message(&mark))
    }

    pub async fn subscribe_oracle(
        &self,
        spot_market: &str,
    ) -> broadcast::Receiver<OracleMarkWsMessage> {
        self.oracle_publisher(spot_market).subscribe()
    }

    pub async fn has_chain_order(&self, spot_market: &str, chain_order_id: &str) -> bool {
        let key = onchain_order_id(spot_market, chain_order_id);
        self.orders_by_id.contains_key(&key)
    }

    pub async fn lookup_spot_market_for_chain_order(&self, chain_order_id: &str) -> Option<String> {
        let suffix = format!(":{chain_order_id}");
        let mut matches = self.orders_by_id.iter().filter_map(|e| {
            let key = e.key().as_str();
            if !key.ends_with(&suffix) {
                return None;
            }
            split_onchain_order_id(key).map(|(spot, _)| spot.to_string())
        });

        let spot = matches.next()?;
        if matches.next().is_some() {
            return None;
        }
        Some(spot)
    }

    pub async fn order_cancel_context(
        &self,
        order_id: Uuid,
        user_address: &str,
    ) -> Option<(Order, String, String)> {
        let stored = self.orders.get(&order_id)?;
        if !stored.user_address.eq_ignore_ascii_case(user_address) {
            return None;
        }
        if stored.order.status != "open" && stored.order.status != "partial_filled" {
            return None;
        }
        Some((
            stored.order.clone(),
            stored.chain_order_id.clone(),
            stored.spot_market.clone(),
        ))
    }

    pub async fn stored_order_context_by_id(
        &self,
        order_id: Uuid,
        user_address: &str,
    ) -> Option<(Order, String, String)> {
        let stored = self.orders.get(&order_id)?;
        if !stored.user_address.eq_ignore_ascii_case(user_address) {
            return None;
        }
        Some((
            stored.order.clone(),
            stored.chain_order_id.clone(),
            stored.spot_market.clone(),
        ))
    }

    pub async fn insert_order(
        &self,
        order: Order,
        user_address: String,
        spot_market: &str,
        chain_order_id: String,
        size_raw: u64,
        filled_raw: u64,
        mut ws: Option<&mut IndexWriteSet>,
    ) {
        let spot = normalize_spot_market_key(spot_market);
        let stored = StoredOrder {
            order: order.clone(),
            user_address,
            chain_order_id: chain_order_id.clone(),
            spot_market: spot.clone(),
            filled_raw,
            size_raw,
        };
        if !is_hot_order_status(&stored.order.status) {
            self.write_order_history(&stored_to_persist_row(&stored));
            return;
        }
        let user = user_key(&stored.user_address);
        let order_id = stored.order.id;
        let id = onchain_order_id(&spot, &chain_order_id);
        self.orders_by_id.insert(id, order_id);
        self.orders_by_user
            .entry(user)
            .or_default()
            .insert(order_id);
        if let Some(ws) = ws {
            ws.store_order(order_row_from_parts(
                stored.order.clone(),
                stored.user_address.clone(),
                stored.chain_order_id.clone(),
                stored.spot_market.clone(),
                stored.size_raw,
                stored.filled_raw,
            ));
        }
        self.orders.insert(order_id, stored);
    }

    fn write_order_history(&self, row: &PersistOrderRow) {
        let Some(persist) = &self.persist else {
            return;
        };
        let mut row = row.clone();
        if row.status_ts_ms == 0 {
            let ms = self.indexing_block_timestamp_ms();
            if ms > 0 {
                row.status_ts_ms = ms;
            }
        }
        persist.enqueue_order_history(row);
    }

    fn remove_hot_order(&self, order_id: Uuid, stored: &StoredOrder) {
        let id = onchain_order_id(&stored.spot_market, &stored.chain_order_id);
        self.orders_by_id.remove(&id);
        let user = user_key(&stored.user_address);
        if let Some(set) = self.orders_by_user.get(&user) {
            set.remove(&order_id);
        }
        self.orders.remove(&order_id);
    }

    fn archive_terminal_order(&self, order_id: Uuid, stored: StoredOrder, mut ws: Option<&mut IndexWriteSet>) {
        let row = stored_to_persist_row(&stored);
        self.remove_hot_order(order_id, &stored);
        if let Some(ws) = ws { ws.delete_order(&order_id.to_string()); }
        self.write_order_history(&row);
    }

    pub async fn query_order(
        &self,
        spot_market: &str,
        chain_order_id: &str,
        user_address: Option<&str>,
    ) -> Option<OrderQueryRecord> {
        let key = onchain_order_id(spot_market, chain_order_id);
        let order_id = self.orders_by_id.get(&key)?.clone();
        let stored = self.orders.get(&order_id)?;
        if let Some(user) = user_address {
            if !stored.user_address.eq_ignore_ascii_case(user) {
                return None;
            }
        }
        Some(Self::order_query_record(stored.value()))
    }

    pub async fn find_open_order_match(
        &self,
        spot_market: &str,
        user_address: &str,
        side: &str,
        price: &str,
        size_raw: u64,
    ) -> Option<OrderQueryRecord> {
        let spot = normalize_spot_market_key(spot_market);
        let key = user_key(user_address);
        let ids = self.orders_by_user.get(&key)?;
        for id in ids.iter() {
            let Some(stored) = self.orders.get(&id) else {
                continue;
            };
            if stored.order.status != "open" && stored.order.status != "partial_filled" {
                continue;
            }
            if stored.spot_market != spot {
                continue;
            }
            if stored.order.side != side || stored.order.price != price {
                continue;
            }
            if stored.size_raw != size_raw {
                continue;
            }
            return Some(Self::order_query_record(stored.value()));
        }
        None
    }

    fn order_query_record(stored: &StoredOrder) -> OrderQueryRecord {
        OrderQueryRecord {
            order: stored.order.clone(),
            chain_order_id: stored.chain_order_id.clone(),
            spot_market: stored.spot_market.clone(),
            user_address: stored.user_address.clone(),
            size_raw: stored.size_raw,
            filled_raw: stored.filled_raw,
        }
    }

    pub async fn stored_order_by_chain_id(
        &self,
        spot_market: &str,
        chain_order_id: &str,
    ) -> Option<(Order, String, String)> {
        let key = onchain_order_id(spot_market, chain_order_id);
        let order_id = self.orders_by_id.get(&key)?.clone();
        let stored = self.orders.get(&order_id)?;
        Some((
            stored.order.clone(),
            stored.user_address.clone(),
            stored.spot_market.clone(),
        ))
    }

    pub async fn update_order_cancelled(&self, spot_market: &str, chain_order_id: &str, mut ws: Option<&mut IndexWriteSet>) {
        let key = onchain_order_id(spot_market, chain_order_id);
        let Some(order_id) = self.orders_by_id.get(&key).map(|e| *e) else {
            return;
        };
        let Some(mut stored) = self.orders.get_mut(&order_id) else {
            return;
        };
        stored.order.status = "cancelled".into();
        let snapshot = stored.clone();
        drop(stored);
        self.archive_terminal_order(order_id, snapshot, ws);
    }

    pub async fn update_order_amount(
        &self,
        spot_market: &str,
        chain_order_id: &str,
        new_amount: u64,
        remaining_amount: u64,
        mut ws: Option<&mut IndexWriteSet>,
    ) {
        let key = onchain_order_id(spot_market, chain_order_id);
        let Some(order_id) = self.orders_by_id.get(&key).map(|e| *e) else {
            return;
        };
        let Some(mut stored) = self.orders.get_mut(&order_id) else {
            return;
        };

        stored.size_raw = new_amount;
        stored.order.size = crate::chain::format_token_amount(new_amount);
        stored.order.status = if remaining_amount == 0 {
            "filled".into()
        } else if stored.filled_raw > 0 {
            "partial_filled".into()
        } else {
            "open".into()
        };
        let terminal = !is_hot_order_status(&stored.order.status);
        let snapshot = if terminal {
            Some(stored.clone())
        } else {
            if let Some(ws) = ws.as_deref_mut() {
                ws.store_order(order_row_from_parts(
                    stored.order.clone(),
                    stored.user_address.clone(),
                    stored.chain_order_id.clone(),
                    stored.spot_market.clone(),
                    stored.size_raw,
                    stored.filled_raw,
                ));
            }
            None
        };
        drop(stored);
        if let Some(snapshot) = snapshot {
            self.archive_terminal_order(order_id, snapshot, ws);
        }
    }

    pub async fn update_order_fill(
        &self,
        spot_market: &str,
        chain_order_id: &str,
        fill_amount: u64,
        remaining_amount: u64,
        is_fully_filled: bool,
        mut ws: Option<&mut IndexWriteSet>,
    ) {
        let key = onchain_order_id(spot_market, chain_order_id);
        let Some(order_id) = self.orders_by_id.get(&key).map(|e| *e) else {
            return;
        };
        let Some(mut stored) = self.orders.get_mut(&order_id) else {
            return;
        };

        stored.filled_raw = stored.filled_raw.saturating_add(fill_amount);
        stored.order.status = if is_fully_filled || remaining_amount == 0 {
            "filled".into()
        } else {
            "partial_filled".into()
        };
        let terminal = !is_hot_order_status(&stored.order.status);
        let snapshot = if terminal {
            Some(stored.clone())
        } else {
            if let Some(ws) = ws.as_deref_mut() {
                ws.store_order(order_row_from_parts(
                    stored.order.clone(),
                    stored.user_address.clone(),
                    stored.chain_order_id.clone(),
                    stored.spot_market.clone(),
                    stored.size_raw,
                    stored.filled_raw,
                ));
            }
            None
        };
        drop(stored);
        if let Some(snapshot) = snapshot {
            self.archive_terminal_order(order_id, snapshot, ws);
        }
    }
}

/// Rebuild secondary order indexes after bulk-loading `orders` without `insert_order`.
#[allow(dead_code)]
pub fn rebuild_order_indexes(store: &IndexState) {
    store.orders_by_id.clear();
    store.orders_by_user.clear();
    for entry in store.orders.iter() {
        let order_uuid = *entry.key();
        let stored = entry.value();
        let id = onchain_order_id(&stored.spot_market, &stored.chain_order_id);
        store.orders_by_id.insert(id, order_uuid);
        store
            .orders_by_user
            .entry(user_key(&stored.user_address))
            .or_default()
            .insert(order_uuid);
    }
}

pub fn new_head() -> SharedIndexedBlockHead {
    Arc::new(TokioRwLock::new(IndexedBlockHead::default()))
}

pub fn market_uuid(market_address: &str) -> Uuid {
    Uuid::new_v5(&Uuid::NAMESPACE_OID, market_address.as_bytes())
}

pub fn vault_uuid(vault_address: &str) -> Uuid {
    Uuid::new_v5(
        &Uuid::NAMESPACE_OID,
        format!("vault:{vault_address}").as_bytes(),
    )
}
