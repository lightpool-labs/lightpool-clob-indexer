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
