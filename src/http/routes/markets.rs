// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use axum::{
    extract::{Path, Query, State},
    routing::get,
    Json, Router,
};

use crate::domain::Market;
use crate::error::{AppError, AppResult};
use crate::http::models::{BalanceTokenSpec, MarketsPageResponse};
use crate::http::process::{build_market_query, QueryMarketsParams};
use crate::state::AppState;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", get(query_markets))
        .route("/slug/:slug", get(get_market_by_slug))
        .route("/index/position-token-specs", get(position_token_specs))
}

async fn query_markets(
    State(state): State<AppState>,
    Query(params): Query<QueryMarketsParams>,
) -> AppResult<Json<MarketsPageResponse>> {
    let query = build_market_query(params)?;
    let limit = query.limit;
    let offset = query.offset;
    let (markets, total) = state.index.query_markets(query).await;

    Ok(Json(MarketsPageResponse {
        markets,
        total,
        limit,
        offset,
    }))
}

async fn get_market_by_slug(
    State(state): State<AppState>,
    Path(slug): Path<String>,
) -> AppResult<Json<Market>> {
    state
        .index
        .get_event_by_slug(&slug)
        .await
        .ok_or_else(|| AppError::NotFound(format!("market {slug} not found")))
        .map(Json)
}

async fn position_token_specs(
    State(state): State<AppState>,
) -> Json<Vec<BalanceTokenSpec>> {
    let specs = state.index.position_token_specs().await;
    Json(
        specs
            .into_iter()
            .map(|(symbol, address)| BalanceTokenSpec { symbol, address })
            .collect(),
    )
}
