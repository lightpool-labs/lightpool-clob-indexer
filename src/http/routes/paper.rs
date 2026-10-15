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
    PaperAccountView, PaperPositionQuery, PaperPositionStatus, DEFAULT_PAPER_PAGE_LIMIT,
    MAX_PAPER_PAGE_LIMIT,
};
use crate::error::{AppError, AppResult};
use crate::http::models::{
    PaperAccountResponse, PaperActivityResponse, PaperDebtHistoryResponse, PaperDebtHistoryTick,
    PaperHistoryResponse, PaperHouseHistoryResponse, PaperHouseHistoryTick, PaperHouseResponse,
    PaperOiHistoryResponse, PaperOiHistoryTick, PaperPositionsResponse, PaperQueuePlacementResponse,
    PaperStakeHistoryResponse, PaperStakeHistoryTick, PaperStakeResponse,
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

#[derive(Debug, Deserialize)]
pub struct OiHistoryQuery {
    pub market: String,
    pub instrument: Option<u32>,
    pub from_ms: Option<u64>,
    pub limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub struct DebtHistoryQuery {
    pub market: String,
    pub from_ms: Option<u64>,
    pub limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub struct QueuePlacementQuery {
    pub market: String,
    pub owner: String,
}

#[derive(Debug, Deserialize)]
pub struct ActivityQuery {
    pub market: Option<String>,
    pub limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub struct StakeHistoryQuery {
    pub market: String,
    pub from_ms: Option<u64>,
    pub limit: Option<usize>,
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/positions", get(list_positions))
        .route("/history", get(list_history))
        .route("/account", get(get_account))
        .route("/stake", get(get_stake))
        .route("/activity", get(list_activity))
        .route("/stake/history", get(get_stake_history))
        .route("/house", get(get_house))
        .route("/house/oi/history", get(get_oi_history))
        .route("/house/history", get(get_house_history))
        .route("/house/queue/history", get(get_debt_history))
        .route("/house/queue/placement", get(get_queue_placement))
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
    use lightpool_sdk::TOKEN_SCALE;
    let sign = if raw < 0 { "-" } else { "" };
    let abs = raw.unsigned_abs();
    let whole = abs / TOKEN_SCALE as u128;
    let frac = abs % TOKEN_SCALE as u128;
    if frac == 0 {
        return format!("{sign}{whole}");
    }
    format!("{sign}{whole}.{frac:06}")
}

async fn get_stake_history(
    State(state): State<AppState>,
    Query(params): Query<StakeHistoryQuery>,
) -> AppResult<Json<PaperStakeHistoryResponse>> {
    let market_raw = params.market.trim();
    if market_raw.is_empty() {
        return Err(AppError::BadRequest("market is required".into()));
    }
    let _ = parse_token_contract(market_raw)
        .map_err(|e| AppError::BadRequest(format!("invalid paper market: {e}")))?;
    // Sample once so charts have a point after restart / quiet chain.
    state.index.mark_paper_stake_dirty(market_raw);
    state
        .index
        .flush_paper_stake_history(&state.chain, false)
        .await;
    let limit = params.limit.unwrap_or(600).clamp(1, 3_600);
    let ticks = state
        .index
        .paper_stake_history(market_raw, params.from_ms, limit);
    Ok(Json(PaperStakeHistoryResponse {
        market: market_raw.to_string(),
        ticks: ticks
            .into_iter()
            .map(|t| PaperStakeHistoryTick {
                t: t.ts_ms,
                staking_revenue: format_token_amount(t.staking_revenue),
                staking_revenue_raw: t.staking_revenue,
                total_paper_supply: format_token_amount(t.total_paper_supply),
                total_paper_supply_raw: t.total_paper_supply,
                total_staking_reward: format_token_amount(t.total_staking_reward),
                total_staking_reward_raw: t.total_staking_reward,
            })
            .collect(),
    }))
}

async fn list_activity(
    State(state): State<AppState>,
    Query(params): Query<ActivityQuery>,
) -> AppResult<Json<PaperActivityResponse>> {
    let market = params
        .market
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());
    if let Some(m) = market {
        let _ = parse_token_contract(m)
            .map_err(|e| AppError::BadRequest(format!("invalid paper market: {e}")))?;
    }
    let limit = params.limit.unwrap_or(60).clamp(1, 120);
    let activities = state.index.paper_activity(market, limit);
    Ok(Json(PaperActivityResponse {
        market: market.unwrap_or("").to_string(),
        activities,
    }))
}

/// Live stake + wallet PAPER via `get_stake` + `get_balance(paper_token)`.
async fn get_stake(
    State(state): State<AppState>,
    Query(params): Query<AccountQuery>,
) -> AppResult<Json<PaperStakeResponse>> {
    let market_raw = params.market.trim();
    let address_raw = params.address.trim();
    if market_raw.is_empty() || address_raw.is_empty() {
        return Err(AppError::BadRequest(
            "market and address are required".into(),
        ));
    }
    let _ = parse_token_contract(market_raw)
        .map_err(|e| AppError::BadRequest(format!("invalid paper market: {e}")))?;
    let _ = Address::from_str(address_raw)
        .map_err(|e| AppError::BadRequest(format!("invalid address: {e}")))?;

    let stake = state
        .index
        .paper_stake_view(&state.chain, market_raw, address_raw)
        .await
        .ok_or_else(|| AppError::BadRequest("failed to build stake view".into()))?;

    Ok(Json(PaperStakeResponse { stake }))
}

/// Event-sourced house snapshot — no `get_lp` / `get_mkt` node call.
async fn get_house(
    State(state): State<AppState>,
    Query(params): Query<HouseQuery>,
) -> AppResult<Json<PaperHouseResponse>> {
    let market_raw = params.market.trim();
    if market_raw.is_empty() {
        return Err(AppError::BadRequest("market is required".into()));
    }
    let _ = parse_token_contract(market_raw)
        .map_err(|e| AppError::BadRequest(format!("invalid paper market: {e}")))?;

    let house = state.index.paper_house_view(market_raw).await;
    Ok(Json(PaperHouseResponse { house }))
}

async fn get_oi_history(
    State(state): State<AppState>,
    Query(params): Query<OiHistoryQuery>,
) -> AppResult<Json<PaperOiHistoryResponse>> {
    let market_raw = params.market.trim();
    if market_raw.is_empty() {
        return Err(AppError::BadRequest("market is required".into()));
    }
    let _ = parse_token_contract(market_raw)
        .map_err(|e| AppError::BadRequest(format!("invalid paper market: {e}")))?;
    let instrument = params.instrument.unwrap_or(0);
    let limit = params.limit.unwrap_or(600).clamp(1, 3_600);
    let ticks = state
        .index
        .paper_oi_history(market_raw, instrument, params.from_ms, limit)
        .await;
    let name = state.index.paper_instrument_name(market_raw, instrument);
    Ok(Json(PaperOiHistoryResponse {
        market: market_raw.to_string(),
        instrument,
        name,
        ticks: ticks
            .into_iter()
            .map(|t| PaperOiHistoryTick {
                t: t.ts_ms,
                long: format_token_amount(t.long),
                short: format_token_amount(t.short),
                long_raw: t.long,
                short_raw: t.short,
            })
            .collect(),
    }))
}

async fn get_house_history(
    State(state): State<AppState>,
    Query(params): Query<DebtHistoryQuery>,
) -> AppResult<Json<PaperHouseHistoryResponse>> {
    let market_raw = params.market.trim();
    if market_raw.is_empty() {
        return Err(AppError::BadRequest("market is required".into()));
    }
    let _ = parse_token_contract(market_raw)
        .map_err(|e| AppError::BadRequest(format!("invalid paper market: {e}")))?;
    let limit = params.limit.unwrap_or(600).clamp(1, 3_600);
    let ticks = state
        .index
        .paper_house_history(market_raw, params.from_ms, limit)
        .await;
    Ok(Json(PaperHouseHistoryResponse {
        market: market_raw.to_string(),
        ticks: ticks
            .into_iter()
            .map(|t| PaperHouseHistoryTick {
                t: t.ts_ms,
                house_funds: format_i128_amount(t.house_funds),
                house_funds_raw: t.house_funds,
                lp_balance: format_token_amount(t.lp_balance),
                lp_balance_raw: t.lp_balance,
                open_house_pnl: format_i64_amount(t.open_house_pnl),
                open_house_pnl_raw: t.open_house_pnl,
                queue_debt: format_token_amount(t.queue_debt),
                queue_debt_raw: t.queue_debt,
            })
            .collect(),
    }))
}

async fn get_debt_history(
    State(state): State<AppState>,
    Query(params): Query<DebtHistoryQuery>,
) -> AppResult<Json<PaperDebtHistoryResponse>> {
    let market_raw = params.market.trim();
    if market_raw.is_empty() {
        return Err(AppError::BadRequest("market is required".into()));
    }
    let _ = parse_token_contract(market_raw)
        .map_err(|e| AppError::BadRequest(format!("invalid paper market: {e}")))?;
    let limit = params.limit.unwrap_or(600).clamp(1, 3_600);
    let ticks = state
        .index
        .paper_house_history(market_raw, params.from_ms, limit)
        .await;
    Ok(Json(PaperDebtHistoryResponse {
        market: market_raw.to_string(),
        ticks: ticks
            .into_iter()
            .map(|t| PaperDebtHistoryTick {
                t: t.ts_ms,
                debt: format_token_amount(t.queue_debt),
                debt_raw: t.queue_debt,
            })
            .collect(),
    }))
}

async fn get_queue_placement(
    State(state): State<AppState>,
    Query(params): Query<QueuePlacementQuery>,
) -> AppResult<Json<PaperQueuePlacementResponse>> {
    let market_raw = params.market.trim();
    let owner_raw = params.owner.trim();
    if market_raw.is_empty() || owner_raw.is_empty() {
        return Err(AppError::BadRequest(
            "market and owner are required".into(),
        ));
    }
    let _ = parse_token_contract(market_raw)
        .map_err(|e| AppError::BadRequest(format!("invalid paper market: {e}")))?;
    let _ = Address::from_str(owner_raw)
        .map_err(|e| AppError::BadRequest(format!("invalid owner: {e}")))?;
    let placement = state
        .index
        .paper_queue_placement(market_raw, owner_raw)
        .await;
    Ok(Json(PaperQueuePlacementResponse { placement }))
}
