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

use crate::error::{AppError, AppResult};
use crate::http::models::{BookResponse, MarketInfoResponse};
use crate::state::AppState;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/:spot_market/book", get(get_book))
        .route("/:spot_market/info", get(get_market_info))
}

#[derive(Debug, Deserialize)]
pub struct SpotBookQuery {
    pub depth: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub struct SpotQuery {
    pub account: String,
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
    let depth = query.depth.unwrap_or(10).clamp(1, 50);

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
