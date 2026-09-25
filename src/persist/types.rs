// Copyright (c) LightPool Labs
// Author: xiaoyu1998


use crate::domain::{Market, Order, Vault};

pub const ORDER_HISTORY_LIMIT: usize = 2000;
pub const BAR_HISTORY_LIMIT: usize = 5000;
pub(crate) const DEFAULT_PERSIST_BATCH_MAX: usize = 128;
pub(crate) const DEFAULT_PERSIST_BATCH_WAIT_MS: u64 = 2;
/// Ingress queue holds cloned blocks (~MBs); keep this small.
pub(crate) const DEFAULT_PERSIST_QUEUE_CAPACITY: usize = 64;
/// Parallel encode workers (bincode / json) before the single sqlite writer.
pub(crate) const DEFAULT_PERSIST_ENCODE_WORKERS: usize = 4;
/// Drop order-history / bars when persist backlog exceeds this.
pub(crate) const SECONDARY_PENDING_LIMIT: u64 = 128;

#[derive(Debug, Clone)]
pub struct ClosedBarRow {
    pub spot_market: String,
    pub interval: String,
    pub start_ts: u64,
    pub open_raw: u64,
    pub high_raw: u64,
    pub low_raw: u64,
    pub close_raw: u64,
    pub volume_raw: u64,
    pub trade_count: u64,
}

#[derive(Debug, Clone)]
pub struct PersistMeta {
    pub block_num: u64,
    pub digest: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PersistOrderRow {
    pub order: Order,
    pub user_address: String,
    pub chain_order_id: String,
    pub spot_market: String,
    pub size_raw: u64,
    pub filled_raw: u64,
    /// Wall-clock ms when the order entered history (filled/cancelled). 0 if unknown.
    #[serde(default)]
    pub status_ts_ms: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PersistBookLevel {
    pub spot_market: String,
    pub side: String,
    pub price_raw: u64,
    pub size_raw: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PersistBookMeta {
    pub spot_market: String,
    pub sequence: u64,
    pub last_trade_price: Option<u64>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PersistVaultPortfolioRow {
    pub vault_id: String,
    pub spot_market: String,
    pub amount_raw: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CheckpointSnapshot {
    pub block_num: u64,
    pub digest: String,
    pub markets: Vec<Market>,
    pub orders: Vec<PersistOrderRow>,
    pub last_trades: Vec<(String, u64)>,
    pub book_levels: Vec<PersistBookLevel>,
    pub book_metas: Vec<PersistBookMeta>,
    #[serde(default)]
    pub vaults: Vec<Vault>,
    #[serde(default)]
    pub vault_portfolio: Vec<PersistVaultPortfolioRow>,
}
