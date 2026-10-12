// Copyright (c) LightPool Labs
// Author: xiaoyu1998

pub use crate::domain::BookSnapshot as BookResponse;

use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct MarketInfoResponse {
    pub last_price: Option<String>,
    pub state: String,
    pub min_order_size: String,
    pub tick_size: String,
    pub maker_fee_bps: u16,
    pub taker_fee_bps: u16,
    pub allow_market_orders: bool,
}

#[derive(Debug, Serialize)]
pub struct OracleMarkResponse {
    pub spot_market: String,
    pub round: u64,
    pub oracle_price: String,
    pub mark_price: String,
    pub oracle_price_raw: u64,
    pub mark_price_raw: u64,
    pub block_num: u64,
    pub ts_ms: u64,
    pub source: &'static str,
}

#[derive(Debug, Serialize)]
pub struct OracleMarkHistoryTick {
    pub t: u64,
    pub mark_price: String,
    pub oracle_price: String,
    pub mark_price_raw: u64,
    pub oracle_price_raw: u64,
    pub round: u64,
    pub block_num: u64,
}

#[derive(Debug, Serialize)]
pub struct OracleMarkHistoryResponse {
    pub spot_market: String,
    pub ticks: Vec<OracleMarkHistoryTick>,
}
