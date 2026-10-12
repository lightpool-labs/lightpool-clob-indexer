// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct PaperUserWsMessage {
    #[serde(rename = "type")]
    pub msg_type: String,
    /// `opened` | `closed` | `liquidated`
    pub event: String,
    pub user_address: String,
    pub market: String,
    pub position: u64,
    pub block_num: u64,
}

/// Fan-out for keepers (all opens); richer than per-user `paper.user`.
#[derive(Debug, Clone, Serialize)]
pub struct PaperPositionWsMessage {
    #[serde(rename = "type")]
    pub msg_type: String,
    /// `opened` | `closed` | `liquidated`
    pub event: String,
    pub user_address: String,
    pub market: String,
    pub position: u64,
    pub block_num: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instrument: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_long: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bust_price_raw: Option<u64>,
}
