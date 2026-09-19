// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use axum::{
    extract::{Path, Query, State},
    routing::get,
    Json, Router,
};
use serde::Deserialize;

use crate::domain::Market;
use crate::error::{AppError, AppResult};
use crate::http::models::{BalanceTokenSpec, BookResponse, MarketsPageResponse};
use crate::http::process::{build_market_query, QueryMarketsParams};
use crate::state::AppState;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", get(query_markets))
        .route("/slug/:slug", get(get_market_by_slug))
        .route("/index/position-token-specs", get(position_token_specs))
        .route("/:symbol/book", get(get_book_by_symbol))
}

#[derive(Debug, Deserialize)]
pub struct SpotBookQuery {
    pub depth: Option<u32>,
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

async fn get_book_by_symbol(
    State(state): State<AppState>,
    Path(symbol): Path<String>,
    Query(query): Query<SpotBookQuery>,
) -> AppResult<Json<BookResponse>> {
    let depth = query.depth.unwrap_or(10).clamp(1, 50);
    let spot_market = resolve_spot_for_symbol(&state, &symbol).await?;

    crate::book_hydrate::rehydrate_spot_from_chain(
        &state.chain,
        &state.book_store,
        &state.index,
        &state.config.query_account,
        &spot_market,
        depth,
    )
    .await?;

    let book = state
        .book_store
        .snapshot(&spot_market, depth)
        .await
        .ok_or_else(|| AppError::NotFound(format!("order book for {spot_market} not found")))?;

    Ok(Json(book))
}

async fn resolve_spot_for_symbol(state: &AppState, symbol: &str) -> AppResult<String> {
    if let Some(spot) = state.index.resolve_spot_market_key(symbol).await {
        return Ok(spot);
    }

    // After indexer restart the in-memory name map is empty (not persisted). Discover
    // sequential spot markets on chain (0x0300…0001, …) and cache name → address.
    let want = symbol.trim().to_ascii_uppercase();
    if want.is_empty() {
        return Err(AppError::BadRequest("symbol is required".into()));
    }

    let account = crate::book_hydrate::parse_query_account(&state.config.query_account);
    for index in 1u64..=64 {
        let spot = match lightpool_sdk::lightpool_types::market_contract(index) {
            Ok(spot) => spot,
            Err(_) => break,
        };
        let spot_key = crate::spot_market::normalize_spot_market_key(&spot.to_string());
        let info = match state.chain.get_market_info(account, spot).await {
            Ok(info) => info,
            Err(_) => break,
        };
        let name = info.name.to_string();
        state
            .index
            .register_named_spot_market(&name, &spot_key)
            .await;

        let name_upper = name.to_ascii_uppercase();
        if name_upper == want || name_upper.starts_with(&format!("{want}/")) {
            return Ok(spot_key);
        }
    }

    Err(AppError::NotFound(format!(
        "spot market '{symbol}' not found; create the market (e.g. {want}/USDT) then retry"
    )))
}
