// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Vault {
    pub id: Uuid,
    pub vault_address: String,
    pub vault_account: String,
    pub manager: String,
    pub quote_token: String,
    pub share_token: String,
    pub equity: String,
    pub allow_deposit: bool,
    pub is_closed: bool,
}

pub const DEFAULT_VAULTS_PAGE_LIMIT: u32 = 100;
pub const MAX_VAULTS_PAGE_LIMIT: u32 = 100;

#[derive(Debug, Clone)]
pub struct VaultQuery {
    pub limit: u32,
    pub offset: u32,
    pub manager: Option<String>,
    pub vault_addresses: Vec<String>,
}
