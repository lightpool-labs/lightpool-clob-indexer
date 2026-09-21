// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use serde::Serialize;

use crate::domain::BookLevel;

#[derive(Debug, Clone, Serialize)]
pub struct BookLevelDelta {
    pub price: String,
    pub size: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct OrderBookSnapshot {
    #[serde(rename = "type")]
    pub msg_type: String,
    pub spot_market: String,
    pub sequence: u64,
    pub bids: Vec<BookLevel>,
    pub asks: Vec<BookLevel>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_trade_price: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RecentTrade {
    pub id: u64,
    pub side: String,
    pub price: String,
    pub size: String,
    pub time_ms: u64,
    pub block_num: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct OrderBookDelta {
    #[serde(rename = "type")]
    pub msg_type: String,
    pub spot_market: String,
    pub sequence: u64,
    pub block_num: u64,
    pub bids: Vec<BookLevelDelta>,
    pub asks: Vec<BookLevelDelta>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_trade_price: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trade: Option<RecentTrade>,
}
