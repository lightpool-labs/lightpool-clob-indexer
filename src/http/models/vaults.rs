// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use serde::Serialize;

use crate::domain::Vault;

#[derive(Debug, Serialize)]
pub struct VaultsPageResponse {
    pub vaults: Vec<Vault>,
    pub total: usize,
    pub limit: u32,
    pub offset: u32,
}
