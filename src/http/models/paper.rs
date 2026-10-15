// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use serde::Serialize;

use crate::domain::{
    PaperAccountView, PaperActivityItem, PaperHouseView, PaperPositionHistory, PaperPositionOpen,
    PaperQueuePlacement, PaperStakeView,
};

#[derive(Debug, Serialize)]
pub struct PaperPositionsResponse {
    pub open: Vec<PaperPositionOpen>,
    pub history: Vec<PaperPositionHistory>,
    pub total: u32,
    pub limit: u32,
    pub offset: u32,
}

#[derive(Debug, Serialize)]
pub struct PaperHistoryResponse {
    pub history: Vec<PaperPositionHistory>,
    pub total: u32,
    pub limit: u32,
    pub offset: u32,
}

#[derive(Debug, Serialize)]
pub struct PaperAccountResponse {
    pub account: PaperAccountView,
}

#[derive(Debug, Serialize)]
pub struct PaperHouseResponse {
    pub house: PaperHouseView,
}

#[derive(Debug, Serialize)]
pub struct PaperOiHistoryTick {
    pub t: u64,
    pub long: String,
    pub short: String,
    pub long_raw: u64,
    pub short_raw: u64,
}

#[derive(Debug, Serialize)]
pub struct PaperOiHistoryResponse {
    pub market: String,
    pub instrument: u32,
    pub name: String,
    pub ticks: Vec<PaperOiHistoryTick>,
}

#[derive(Debug, Serialize)]
pub struct PaperHouseHistoryTick {
    pub t: u64,
    pub house_funds: String,
    pub house_funds_raw: i128,
    pub lp_balance: String,
    pub lp_balance_raw: u64,
    pub open_house_pnl: String,
    pub open_house_pnl_raw: i64,
    pub queue_debt: String,
    pub queue_debt_raw: u64,
}

#[derive(Debug, Serialize)]
pub struct PaperHouseHistoryResponse {
    pub market: String,
    pub ticks: Vec<PaperHouseHistoryTick>,
}

#[derive(Debug, Serialize)]
pub struct PaperDebtHistoryTick {
    pub t: u64,
    pub debt: String,
    pub debt_raw: u64,
}

#[derive(Debug, Serialize)]
pub struct PaperDebtHistoryResponse {
    pub market: String,
    pub ticks: Vec<PaperDebtHistoryTick>,
}

#[derive(Debug, Serialize)]
pub struct PaperQueuePlacementResponse {
    pub placement: PaperQueuePlacement,
}

#[derive(Debug, Serialize)]
pub struct PaperStakeResponse {
    pub stake: PaperStakeView,
}

#[derive(Debug, Serialize)]
pub struct PaperActivityResponse {
    pub market: String,
    pub activities: Vec<PaperActivityItem>,
}

#[derive(Debug, Serialize)]
pub struct PaperStakeHistoryTick {
    pub t: u64,
    pub staking_revenue: String,
    pub staking_revenue_raw: u64,
    pub total_paper_supply: String,
    pub total_paper_supply_raw: u64,
    pub total_staking_reward: String,
    pub total_staking_reward_raw: u64,
}

#[derive(Debug, Serialize)]
pub struct PaperStakeHistoryResponse {
    pub market: String,
    pub ticks: Vec<PaperStakeHistoryTick>,
}
