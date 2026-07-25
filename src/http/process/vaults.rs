// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use serde::Deserialize;

use crate::domain::{VaultQuery, DEFAULT_VAULTS_PAGE_LIMIT, MAX_VAULTS_PAGE_LIMIT};
use crate::error::{AppError, AppResult};

#[derive(Debug, Deserialize)]
pub struct QueryVaultsParams {
    pub limit: Option<u32>,
    pub offset: Option<u32>,
    pub manager: Option<String>,
    pub vault_addresses: Option<String>,
}

fn parse_csv(value: Option<String>) -> Vec<String> {
    value
        .map(|items| {
            items
                .split(',')
                .map(str::trim)
                .filter(|item| !item.is_empty())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

pub fn build_vault_query(params: QueryVaultsParams) -> AppResult<VaultQuery> {
    let vault_addresses = parse_csv(params.vault_addresses);
    if vault_addresses.len() > MAX_VAULTS_PAGE_LIMIT as usize {
        return Err(AppError::BadRequest(format!(
            "vault_addresses accepts at most {MAX_VAULTS_PAGE_LIMIT} values"
        )));
    }

    let limit = params
        .limit
        .unwrap_or(DEFAULT_VAULTS_PAGE_LIMIT)
        .clamp(1, MAX_VAULTS_PAGE_LIMIT);
    let offset = params.offset.unwrap_or(0);

    Ok(VaultQuery {
        limit,
        offset,
        manager: params
            .manager
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty()),
        vault_addresses,
    })
}
