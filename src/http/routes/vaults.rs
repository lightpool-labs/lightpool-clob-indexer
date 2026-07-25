// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use axum::{
    extract::{Path, Query, State},
    routing::get,
    Json, Router,
};

use crate::domain::Vault;
use crate::error::{AppError, AppResult};
use crate::http::models::VaultsPageResponse;
use crate::http::process::{build_vault_query, QueryVaultsParams};
use crate::state::AppState;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", get(query_vaults))
        .route("/address/:address", get(get_vault_by_address))
}

async fn query_vaults(
    State(state): State<AppState>,
    Query(params): Query<QueryVaultsParams>,
) -> AppResult<Json<VaultsPageResponse>> {
    let query = build_vault_query(params)?;
    let limit = query.limit;
    let offset = query.offset;
    let (vaults, total) = state.index.query_vaults(query).await;

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
) -> AppResult<Json<Vault>> {
    state
        .index
        .get_vault_by_address(&address)
        .await
        .ok_or_else(|| AppError::NotFound(format!("vault {address} not found")))
        .map(Json)
}
