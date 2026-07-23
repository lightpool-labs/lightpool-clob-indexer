// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct BookLevel {
    pub price: String,
    pub size: String,
}

#[derive(Debug, Serialize)]
pub struct BookSnapshot {
    pub sequence: u64,
    pub bids: Vec<BookLevel>,
    pub asks: Vec<BookLevel>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_trade_price: Option<String>,
}
