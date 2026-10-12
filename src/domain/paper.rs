// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaperPositionOpen {
    pub market: String,
    pub position: u64,
    pub owner: String,
    pub instrument: u32,
    pub is_long: bool,
    pub margin: String,
    pub notional: String,
    pub entry_price: String,
    pub bust_price: String,
    pub margin_raw: u64,
    pub notional_raw: u64,
    pub entry_price_raw: u64,
    pub bust_price_raw: u64,
    pub funded_from_queued: bool,
    pub opened_block: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PaperCloseKind {
    Closed,
    Liquidated,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaperPositionHistory {
    pub market: String,
    pub position: u64,
    pub owner: String,
    pub instrument: u32,
    pub is_long: bool,
    pub margin: String,
    pub notional: String,
    pub entry_price: String,
    pub bust_price: String,
    pub exit_price: String,
    pub pnl: String,
    pub fee: String,
    pub payout: String,
    pub queued: String,
    pub minted: String,
    pub kind: PaperCloseKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub liquidator: Option<String>,
    pub opened_block: u64,
    pub closed_block: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaperHouseView {
    pub market: String,
    pub free_cash: String,
    pub free_cash_raw: u64,
    pub side_bucket: String,
    pub side_bucket_raw: u64,
    pub tracked_lp: String,
    pub tracked_lp_raw: i128,
    pub tail_hwm: String,
    pub tail_hwm_raw: u64,
    pub queue_head: u64,
    pub queue_tail: u64,
    pub total_queued: String,
    pub total_queued_raw: u64,
    pub staker_fee_acc: String,
    pub staker_fee_acc_raw: u64,
    pub dev_fee_acc: String,
    pub dev_fee_acc_raw: u64,
    /// True when payout queue has unpaid claims (`total_queued > 0`).
    pub queue_nonempty: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaperAccountView {
    pub market: String,
    pub address: String,
    pub available: String,
    pub queued: String,
    pub available_raw: u64,
    pub queued_raw: u64,
    pub entry_head: u64,
    pub entry_tail: u64,
    pub queued_truncated: bool,
    /// Sum of open margins (indexed).
    pub margin_in_use: String,
    pub margin_in_use_raw: u64,
    /// Unrealized PnL vs latest marks when available; else "0".
    pub open_pnl: String,
    pub open_pnl_raw: i64,
    pub equity: String,
    pub equity_raw: i64,
    pub open_positions: u32,
}

#[derive(Debug, Clone, Default)]
pub struct PaperPositionQuery {
    pub owner: Option<String>,
    pub market: Option<String>,
    pub status: PaperPositionStatus,
    pub limit: u32,
    pub offset: u32,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum PaperPositionStatus {
    #[default]
    Open,
    Closed,
    All,
}

impl PaperPositionStatus {
    pub fn parse(raw: Option<&str>) -> Self {
        match raw.map(|s| s.trim().to_ascii_lowercase()).as_deref() {
            Some("closed") | Some("history") => Self::Closed,
            Some("all") => Self::All,
            _ => Self::Open,
        }
    }
}

pub const DEFAULT_PAPER_PAGE_LIMIT: u32 = 100;
pub const MAX_PAPER_PAGE_LIMIT: u32 = 500;
