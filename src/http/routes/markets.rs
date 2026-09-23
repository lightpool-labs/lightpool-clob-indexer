// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use axum::{
    extract::{Path, Query, State},
    routing::get,
    Json, Router,
};
use serde::Deserialize;

use crate::book_hydrate::DEFAULT_BOOK_DEPTH;
use crate::domain::{Market, MarketCategory};
use crate::error::{AppError, AppResult};
use crate::http::models::{BalanceTokenSpec, BookResponse, MarketsPageResponse};
use crate::ws::models::RecentTrade;
use crate::http::process::{build_market_query, QueryMarketsParams};
use crate::state::AppState;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", get(query_markets))
        .route("/spot", get(query_spot_markets))
        .route("/event", get(query_event_markets))
        .route("/perp", get(query_perp_markets))
        .route("/slug/:slug", get(get_market_by_slug))
        .route("/index/position-token-specs", get(position_token_specs))
        .route("/:name/book", get(get_book_by_name))
        .route("/:name/trades", get(get_trades_by_name))
}

#[derive(Debug, Deserialize)]
pub struct SpotBookQuery {
    pub depth: Option<u32>,
}

async fn query_markets_with_category(
    state: AppState,
    mut params: QueryMarketsParams,
    category: MarketCategory,
) -> AppResult<Json<MarketsPageResponse>> {
    params.category = Some(category.as_str().to_string());
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

async fn query_spot_markets(
    State(state): State<AppState>,
    Query(params): Query<QueryMarketsParams>,
) -> AppResult<Json<MarketsPageResponse>> {
    query_markets_with_category(state, params, MarketCategory::Spot).await
}

async fn query_event_markets(
    State(state): State<AppState>,
    Query(params): Query<QueryMarketsParams>,
) -> AppResult<Json<MarketsPageResponse>> {
    query_markets_with_category(state, params, MarketCategory::Event).await
}

async fn query_perp_markets(
    State(state): State<AppState>,
    Query(params): Query<QueryMarketsParams>,
) -> AppResult<Json<MarketsPageResponse>> {
    query_markets_with_category(state, params, MarketCategory::Perp).await
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

async fn get_book_by_name(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Query(query): Query<SpotBookQuery>,
) -> AppResult<Json<BookResponse>> {
    let depth = query.depth.unwrap_or(10).clamp(1, DEFAULT_BOOK_DEPTH);
    let spot_market = resolve_spot_for_name(&state, &name).await?;

    if let Err(error) = crate::book_hydrate::rehydrate_spot_from_chain(
        &state.chain,
        &state.index,
        &state.config.query_account,
        &spot_market,
        depth,
    )
    .await
    {
        tracing::warn!(
            spot_market = %spot_market,
            error = %error,
            "chain book hydrate failed; serving in-memory snapshot"
        );
    }

    let book = state
        .index
        .books
        .snapshot(&spot_market, depth)
        .await
        .ok_or_else(|| AppError::NotFound(format!("order book for {spot_market} not found")))?;

    Ok(Json(book))
}

async fn get_trades_by_name(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> AppResult<Json<Vec<RecentTrade>>> {
    let spot_market = resolve_spot_for_name(&state, &name).await?;
    Ok(Json(state.index.books.recent_trades(&spot_market).await))
}

async fn resolve_spot_for_name(state: &AppState, name: &str) -> AppResult<String> {
    if let Some(spot) = state.index.resolve_spot_market_key(name).await {
        return Ok(spot);
    }

    // After indexer restart the in-memory name map is empty (not persisted). Discover
    // sequential spot markets on chain (0x0300…0001, …) and cache name → address.
    let want = name.trim().to_ascii_uppercase();
    if want.is_empty() {
        return Err(AppError::BadRequest("name is required".into()));
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
        let market_name = info.name.to_string();
        state
            .index
            .register_named_spot_market(&market_name, &spot_key, "")
            .await;

        if market_name.to_ascii_uppercase() == want {
            return Ok(spot_key);
        }
    }

    Err(AppError::NotFound(format!(
        "spot market '{name}' not found; create the market then retry"
    )))
}
