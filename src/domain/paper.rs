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
pub struct PaperInstrumentExposure {
    pub instrument: u32,
    pub name: String,
    pub long: String,
    pub long_raw: u64,
    pub short: String,
    pub short_raw: u64,
    pub net: String,
    pub net_raw: i128,
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
    pub buyback_cash: String,
    pub buyback_cash_raw: u64,
    /// True when payout queue has unpaid claims (`total_queued > 0`).
    pub queue_nonempty: bool,
    /// User Positions: sum of long+short notional across instruments.
    pub open_interest: String,
    pub open_interest_raw: u64,
    pub open_margin: String,
    pub open_margin_raw: u64,
    pub open_positions: u32,
    pub instruments: Vec<PaperInstrumentExposure>,
    /// Stats: free_cash + side_bucket + buyback_cash.
    pub lp_balance: String,
    pub lp_balance_raw: u64,
    /// Stats: −Σ trader unrealized PnL (house perspective).
    pub open_house_pnl: String,
    pub open_house_pnl_raw: i64,
    /// Stats: alias of total_queued.
    pub queue_debt: String,
    pub queue_debt_raw: u64,
    /// Open queue claim count (indexed `paper_queued` − harvested).
    pub queue_debt_positions: u32,
    /// Largest open claim amount; empty string when none.
    pub queue_largest: String,
    pub queue_largest_raw: u64,
    /// Next claim to be paid (queue head); empty when none.
    pub queue_paid_next: String,
    pub queue_paid_next_raw: u64,
    pub queue_paid_next_user: String,
    /// Stats: lp_balance + open_house_pnl − queue_debt.
    pub house_funds: String,
    pub house_funds_raw: i128,
    /// Lifetime open notional (sum of `paper_opened.notional`).
    pub volume: String,
    pub volume_raw: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaperQueuePlacement {
    pub market: String,
    pub address: String,
    pub ahead: String,
    pub ahead_raw: u64,
    pub yours: String,
    pub yours_raw: u64,
    pub behind: String,
    pub behind_raw: u64,
    pub your_entries: u32,
    pub in_queue: bool,
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

/// Recent PAPER-related activity (close loss / liqd / stake / unstake).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaperActivityItem {
    /// `close_loss` | `liquidated` | `staked` | `unstaked`
    pub kind: String,
    pub market: String,
    pub user: String,
    /// PAPER minted or stake/unstake amount.
    pub amount: String,
    pub amount_raw: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pnl: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pnl_raw: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub position: Option<u64>,
    pub block_num: u64,
    pub ts_ms: u64,
}

/// Live stake snapshot for `/paper` (wallet PAPER + stake + claimable USDC).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaperStakeView {
    pub market: String,
    pub address: String,
    pub paper_token: String,
    /// Wallet PAPER (unstaked).
    pub paper: String,
    pub paper_raw: u64,
    /// User staked PAPER.
    pub staked: String,
    pub staked_raw: u64,
    /// Claimable USDC (includes unsettled pending).
    pub claimable: String,
    pub claimable_raw: u64,
    /// Accrued since last settle, not yet folded into on-chain claimable storage.
    pub pending_reward: String,
    pub pending_reward_raw: u64,
    /// USDC still in the staking reward pool (all stakers).
    pub reward_pool: String,
    pub reward_pool_raw: u64,
    /// Quote reserved for PAPER buyback (half of excess sweep above staker_cap).
    pub buyback_cash: String,
    pub buyback_cash_raw: u64,
    pub total_staked: String,
    pub total_staked_raw: u64,
    /// User share of total_staked, e.g. "12.34" for 12.34%.
    pub stake_share_pct: String,
    /// Lifetime USDC this user has claimed (`paper_claimed` sum).
    pub rewards_claimed: String,
    pub rewards_claimed_raw: u64,
    /// Lifetime USDC distributed to stakers (`paper_fees_pushed` staker_fee+swept_stakers).
    pub total_staker_rewards: String,
    pub total_staker_rewards_raw: u64,
    /// Mint-curve style display, e.g. `0% / 12% / 99%`.
    pub reward_rate: String,
    /// Current mint rate in PAPER per $1 loss (`rate_ppm / 1e6`).
    pub mint_rate: String,
    pub mint_rate_raw: u64,
    /// Tracked LP (can be negative).
    pub tracked_lp: String,
    pub tracked_lp_raw: i64,
    pub tail_hwm: String,
    pub tail_hwm_raw: u64,
    pub flat_threshold: String,
    pub flat_threshold_raw: u64,
    pub flat_rate_ppm: u64,
    pub tail_decay_scale: String,
    pub tail_decay_scale_raw: u64,
    /// Always USDC for paper staking rewards.
    pub reward_token: String,
    /// PAPER token total supply.
    pub total_supply: String,
    pub total_supply_raw: u64,
    /// Alias of total_staked (protocol staked supply).
    pub staked_supply: String,
    pub staked_supply_raw: u64,
    /// `total_staked / total_supply`, e.g. "99.12".
    pub stake_ratio_pct: String,
    /// Paper market contract (holds staked PAPER ledger).
    pub staking_contract: String,
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
