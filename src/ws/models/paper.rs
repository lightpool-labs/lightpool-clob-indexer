// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use serde::Serialize;

use crate::domain::{PaperAccountView, PaperActivityItem, PaperHouseView, PaperPositionOpen, PaperStakeView};

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

#[derive(Debug, Clone, Serialize)]
pub struct PaperUserStateWsMessage {
    #[serde(rename = "type")]
    pub msg_type: String,
    pub user_address: String,
    pub market: String,
    pub account: PaperAccountView,
    pub positions: Vec<PaperPositionOpen>,
    pub paper: PaperStakeView,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub notional: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub notional_raw: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub margin: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub margin_raw: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entry_price: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entry_price_raw: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instrument_name: Option<String>,
    /// Realized PnL human string on close/liqd; absent/None on open.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pnl: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pnl_raw: Option<i64>,
}

/// House overview snapshot (coalesced per block).
#[derive(Debug, Clone, Serialize)]
pub struct PaperHouseWsMessage {
    #[serde(rename = "type")]
    pub msg_type: String,
    pub market: String,
    pub ts_ms: u64,
    pub house: PaperHouseView,
}

#[derive(Debug, Clone, Serialize)]
pub struct PaperActivityWsMessage {
    #[serde(rename = "type")]
    pub msg_type: String,
    pub activity: PaperActivityItem,
}

#[derive(Debug, Clone, Serialize)]
pub struct PaperStakeWsMessage {
    #[serde(rename = "type")]
    pub msg_type: String,
    pub market: String,
    pub ts_ms: u64,
    pub staking_revenue: String,
    pub staking_revenue_raw: u64,
    pub total_paper_supply: String,
    pub total_paper_supply_raw: u64,
    pub total_staking_reward: String,
    pub total_staking_reward_raw: u64,
    /// Full stake snapshot when subscribed with `user_address` (house-style).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stake: Option<PaperStakeView>,
}
