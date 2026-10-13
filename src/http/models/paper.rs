// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use serde::Serialize;

use crate::domain::{
    PaperAccountView, PaperHouseView, PaperPositionHistory, PaperPositionOpen,
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
