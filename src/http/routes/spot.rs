// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use axum::{
    extract::{Path, Query, State},
    routing::get,
    Json, Router,
};
use lightpool_sdk::{parse_token_contract, Address};
use serde::Deserialize;
use std::str::FromStr;

use crate::book_hydrate::DEFAULT_BOOK_DEPTH;
use crate::error::{AppError, AppResult};
use crate::http::models::{BookResponse, MarketInfoResponse};
use crate::state::AppState;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/:spot_market/book", get(get_book))
        .route("/:spot_market/info", get(get_market_info))
        .route("/:spot_market/bars", get(get_bars))
}

#[derive(Debug, Deserialize)]
pub struct SpotBookQuery {
    pub depth: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub struct SpotQuery {
    pub account: String,
}

#[derive(Debug, Deserialize)]
pub struct BarsQuery {
    pub interval: Option<String>,
    pub from: Option<u64>,
    pub to: Option<u64>,
    pub limit: Option<usize>,
}

async fn parse_account(account: &str) -> AppResult<Address> {
    Address::from_str(account.trim())
        .map_err(|e| AppError::BadRequest(format!("invalid account: {e}")))
}

async fn parse_spot_market(spot_market: &str) -> AppResult<lightpool_sdk::ContractAddress> {
    parse_token_contract(spot_market)
        .map_err(|e| AppError::BadRequest(format!("invalid spot market: {e}")))
}

async fn get_book(
    State(state): State<AppState>,
    Path(spot_market): Path<String>,
    Query(query): Query<SpotBookQuery>,
) -> AppResult<Json<BookResponse>> {
    let depth = query.depth.unwrap_or(10).clamp(1, DEFAULT_BOOK_DEPTH);

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

async fn get_market_info(
    State(state): State<AppState>,
    Path(spot_market): Path<String>,
    Query(query): Query<SpotQuery>,
) -> AppResult<Json<MarketInfoResponse>> {
    let account = parse_account(&query.account).await?;
    let spot_market = parse_spot_market(&spot_market).await?;

    let info = state.chain.get_market_info(account, spot_market).await?;

    Ok(Json(MarketInfoResponse {
        last_price: info.last_price.map(|price| crate::chain::format_price_pieces(price)),
        state: info.state.to_string(),
        min_order_size: crate::chain::format_token_amount(info.min_order_size),
        tick_size: crate::chain::format_token_amount(info.tick_size),
        maker_fee_bps: info.maker_fee_bps,
        taker_fee_bps: info.taker_fee_bps,
        allow_market_orders: info.allow_market_orders,
    }))
}

async fn get_bars(
    State(state): State<AppState>,
    Path(spot_market): Path<String>,
    Query(query): Query<BarsQuery>,
) -> AppResult<Json<serde_json::Value>> {
    let interval = query
        .interval
        .as_deref()
        .unwrap_or(crate::bars::INTERVAL_1M);
    if !crate::bars::is_supported_interval(interval) {
        return Err(AppError::BadRequest(format!(
            "unsupported interval `{interval}`; use {}",
            crate::bars::BAR_INTERVALS.join("|")
        )));
    }
    let limit = query
        .limit
        .unwrap_or(500)
        .clamp(1, crate::persist::BAR_HISTORY_LIMIT);
    let bars = state
        .index
        .bars
        .load_history(&spot_market, interval, query.from, query.to, limit)
        .await;
    let forming = state
        .index
        .bars
        .forming_interval(&spot_market, interval)
        .await
        .map(|b| b.to_ws_message("bar"));
    Ok(Json(serde_json::json!({
        "spot_market": crate::spot_market::normalize_spot_market_key(&spot_market),
        "interval": interval,
        "bars": bars.iter().map(|b| b.to_ws_message("bar_closed")).collect::<Vec<_>>(),
        "forming": forming,
    })))
}
