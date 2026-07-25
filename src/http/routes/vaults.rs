// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use axum::{
    extract::{Path, Query, State},
    routing::get,
    Json, Router,
};
use serde::Deserialize;

use crate::domain::Vault;
use crate::error::{AppError, AppResult};
use crate::http::models::VaultsPageResponse;
use crate::http::process::{build_vault_query, QueryVaultsParams};
use crate::state::AppState;
use crate::vault_enrich::{enrich_vault_for_account, enrich_vaults_for_account};

#[derive(Debug, Deserialize)]
pub struct GetVaultParams {
    pub account: Option<String>,
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", get(query_vaults))
        .route("/address/:address", get(get_vault_by_address))
}

async fn query_vaults(
    State(state): State<AppState>,
    Query(params): Query<QueryVaultsParams>,
) -> AppResult<Json<VaultsPageResponse>> {
    let account = params
        .account
        .as_ref()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    let query = build_vault_query(params)?;
    let limit = query.limit;
    let offset = query.offset;
    let (vaults, total) = state.index.query_vaults(query).await;
    let vaults = enrich_vaults_for_account(&state, vaults, account.as_deref()).await;

    Ok(Json(VaultsPageResponse {
        vaults,
        total,
        limit,
        offset,
    }))
}

async fn get_vault_by_address(
    State(state): State<AppState>,
    Path(address): Path<String>,
    Query(params): Query<GetVaultParams>,
) -> AppResult<Json<Vault>> {
    let vault = state
        .index
        .get_vault_by_address(&address)
        .await
        .ok_or_else(|| AppError::NotFound(format!("vault {address} not found")))?;
    let account = params
        .account
        .as_ref()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty());
    Ok(Json(
        enrich_vault_for_account(&state, vault, account).await,
    ))
}
