// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use serde::Serialize;

use crate::domain::Market;

#[derive(Debug, Serialize)]
pub struct MarketsPageResponse {
    pub markets: Vec<Market>,
    pub total: usize,
    pub limit: u32,
    pub offset: u32,
}
