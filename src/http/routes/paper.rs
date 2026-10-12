// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use axum::{
    extract::{Query, State},
    routing::get,
    Json, Router,
};
use lightpool_sdk::{parse_token_contract, Address};
use serde::Deserialize;
use std::str::FromStr;

use crate::chain::format_token_amount;
use crate::domain::{
    PaperAccountView, PaperHouseView, PaperPositionQuery, PaperPositionStatus,
    DEFAULT_PAPER_PAGE_LIMIT, MAX_PAPER_PAGE_LIMIT,
};
use crate::error::{AppError, AppResult};
use crate::http::models::{
    PaperAccountResponse, PaperHistoryResponse, PaperHouseResponse, PaperPositionsResponse,
};
use crate::state::AppState;

#[derive(Debug, Deserialize)]
pub struct PositionsQuery {
    pub owner: Option<String>,
    pub market: Option<String>,
    pub status: Option<String>,
    pub limit: Option<u32>,
    pub offset: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub struct HistoryQuery {
    pub owner: Option<String>,
    pub market: Option<String>,
    pub limit: Option<u32>,
    pub offset: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub struct AccountQuery {
    pub market: String,
    pub address: String,
}

#[derive(Debug, Deserialize)]
pub struct HouseQuery {
    pub market: String,
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/positions", get(list_positions))
        .route("/history", get(list_history))
        .route("/account", get(get_account))
        .route("/house", get(get_house))
}

fn page_limit(raw: Option<u32>) -> u32 {
    raw.unwrap_or(DEFAULT_PAPER_PAGE_LIMIT)
        .clamp(1, MAX_PAPER_PAGE_LIMIT)
}

async fn list_positions(
    State(state): State<AppState>,
    Query(params): Query<PositionsQuery>,
) -> AppResult<Json<PaperPositionsResponse>> {
    let limit = page_limit(params.limit);
    let offset = params.offset.unwrap_or(0);
    let query = PaperPositionQuery {
        owner: params.owner.filter(|s| !s.trim().is_empty()),
        market: params.market.filter(|s| !s.trim().is_empty()),
        status: PaperPositionStatus::parse(params.status.as_deref()),
        limit,
        offset,
    };
    let (open, history, total) = state.index.query_paper_positions(query).await;
    Ok(Json(PaperPositionsResponse {
        open,
        history,
        total,
        limit,
        offset,
    }))
}

async fn list_history(
    State(state): State<AppState>,
    Query(params): Query<HistoryQuery>,
) -> AppResult<Json<PaperHistoryResponse>> {
    let limit = page_limit(params.limit);
    let offset = params.offset.unwrap_or(0);
    let query = PaperPositionQuery {
        owner: params.owner.filter(|s| !s.trim().is_empty()),
        market: params.market.filter(|s| !s.trim().is_empty()),
        status: PaperPositionStatus::Closed,
        limit,
        offset,
    };
    let (_open, history, total) = state.index.query_paper_positions(query).await;
    Ok(Json(PaperHistoryResponse {
        history,
        total,
        limit,
        offset,
    }))
}

async fn get_account(
    State(state): State<AppState>,
    Query(params): Query<AccountQuery>,
) -> AppResult<Json<PaperAccountResponse>> {
    let market_raw = params.market.trim();
    let address_raw = params.address.trim();
    if market_raw.is_empty() || address_raw.is_empty() {
        return Err(AppError::BadRequest(
            "market and address are required".into(),
        ));
    }

    let paper_market = parse_token_contract(market_raw)
        .map_err(|e| AppError::BadRequest(format!("invalid paper market: {e}")))?;
    let user = Address::from_str(address_raw)
        .map_err(|e| AppError::BadRequest(format!("invalid address: {e}")))?;

    // Account object may not exist until first deposit — treat call failures as zeros.
    let (available, queued, entry_head, entry_tail, queued_truncated) =
        match state.chain.get_paper_account(user, paper_market).await {
            Ok(acct) => (
                acct.available,
                acct.queued,
                acct.entry_head,
                acct.entry_tail,
                acct.queued_truncated,
            ),
            Err(err) => {
                tracing::debug!(error = %err, %address_raw, %market_raw, "get_acct empty/fallback");
                (0, 0, 0, 0, false)
            }
        };

    let margin_in_use_raw = state
        .index
        .sum_open_margin(address_raw, Some(market_raw))
        .await;
    let open_positions = state
        .index
        .count_open_positions(address_raw, Some(market_raw))
        .await;
    let open_pnl_raw: i64 = 0;
    let equity_raw =
        available as i64 + queued as i64 + margin_in_use_raw as i64 + open_pnl_raw;

    Ok(Json(PaperAccountResponse {
        account: PaperAccountView {
            market: market_raw.to_string(),
            address: address_raw.to_string(),
            available: format_token_amount(available),
            queued: format_token_amount(queued),
            available_raw: available,
            queued_raw: queued,
            entry_head,
            entry_tail,
            queued_truncated,
            margin_in_use: format_token_amount(margin_in_use_raw),
            margin_in_use_raw,
            open_pnl: format_token_amount(0),
            open_pnl_raw,
            equity: format_i64_amount(equity_raw),
            equity_raw,
            open_positions,
        },
    }))
}

fn format_i64_amount(raw: i64) -> String {
    let sign = if raw < 0 { "-" } else { "" };
    format!("{sign}{}", format_token_amount(raw.unsigned_abs()))
}

fn format_i128_amount(raw: i128) -> String {
    let sign = if raw < 0 { "-" } else { "" };
    let abs = raw.unsigned_abs();
    // tracked_lp is also in TOKEN_SCALE units.
    use lightpool_sdk::TOKEN_SCALE;
    let whole = abs / TOKEN_SCALE as u128;
    let frac = abs % TOKEN_SCALE as u128;
    if frac == 0 {
        return format!("{sign}{whole}");
    }
    format!("{sign}{whole}.{frac:06}")
}

async fn get_house(
    State(state): State<AppState>,
    Query(params): Query<HouseQuery>,
) -> AppResult<Json<PaperHouseResponse>> {
    let market_raw = params.market.trim();
    if market_raw.is_empty() {
        return Err(AppError::BadRequest("market is required".into()));
    }
    let paper_market = parse_token_contract(market_raw)
        .map_err(|e| AppError::BadRequest(format!("invalid paper market: {e}")))?;

    let lp = state
        .chain
        .get_paper_lp(paper_market)
        .await
        .map_err(|e| AppError::Internal(format!("get_lp: {e}")))?;

    // Prefer get_queue tip when available; fall back to LP embedded heads.
    let (queue_head, queue_tail, total_queued) =
        match state.chain.get_paper_queue(paper_market).await {
            Ok(q) => (q.queue_head, q.queue_tail, q.total_queued),
            Err(err) => {
                tracing::debug!(error = %err, %market_raw, "get_queue fallback to get_lp");
                (lp.queue_head, lp.queue_tail, lp.total_queued)
            }
        };

    Ok(Json(PaperHouseResponse {
        house: PaperHouseView {
            market: market_raw.to_string(),
            free_cash: format_token_amount(lp.free_cash),
            free_cash_raw: lp.free_cash,
            side_bucket: format_token_amount(lp.side_bucket),
            side_bucket_raw: lp.side_bucket,
            tracked_lp: format_i128_amount(lp.tracked_lp),
            tracked_lp_raw: lp.tracked_lp,
            tail_hwm: format_token_amount(lp.tail_hwm),
            tail_hwm_raw: lp.tail_hwm,
            queue_head,
            queue_tail,
            total_queued: format_token_amount(total_queued),
            total_queued_raw: total_queued,
            staker_fee_acc: format_token_amount(lp.staker_fee_acc),
            staker_fee_acc_raw: lp.staker_fee_acc,
            dev_fee_acc: format_token_amount(lp.dev_fee_acc),
            dev_fee_acc_raw: lp.dev_fee_acc,
            queue_nonempty: total_queued > 0,
        },
    }))
}
