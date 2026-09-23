// Copyright (c) LightPool Labs
// Author: xiaoyu1998


use crate::domain::{Market, Order, Vault};

pub const ORDER_HISTORY_LIMIT: usize = 2000;
pub const BAR_HISTORY_LIMIT: usize = 5000;
pub const DEFAULT_PERSIST_WORKERS: usize = 4;

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
