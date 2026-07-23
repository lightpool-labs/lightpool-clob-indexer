// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BalanceEntry {
    pub token: String,
    pub symbol: String,
    pub total: String,
    pub locked: String,
    pub available: String,
}

#[derive(Debug, Deserialize)]
pub struct BalancesRequest {
    pub tokens: Vec<BalanceTokenSpec>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BalanceTokenSpec {
    pub symbol: String,
    pub address: String,
}
