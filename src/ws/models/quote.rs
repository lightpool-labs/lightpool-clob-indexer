// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use serde::Serialize;

use crate::domain::BookLevel;

#[derive(Debug, Clone, Serialize)]
pub struct QuoteSnapshot {
    #[serde(rename = "type")]
    pub msg_type: String,
    pub spot_market: String,
    pub sequence: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub best_bid: Option<BookLevel>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub best_ask: Option<BookLevel>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_trade_price: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct QuoteDelta {
    #[serde(rename = "type")]
    pub msg_type: String,
    pub spot_market: String,
    pub sequence: u64,
    pub block_num: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub best_bid: Option<BookLevel>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub best_ask: Option<BookLevel>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_trade_price: Option<String>,
}
