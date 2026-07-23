// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use axum::{
    extract::{Path, State},
    routing::post,
    Json, Router,
};
use lightpool_sdk::parse_token_contract;
use std::str::FromStr;

use crate::chain::format_token_amount;
use crate::error::{AppError, AppResult};
use crate::http::models::{BalanceEntry, BalancesRequest};
use crate::state::AppState;

pub fn router() -> Router<AppState> {
    Router::new().route("/:address/balances", post(get_balances))
}

fn is_position_symbol(symbol: &str) -> bool {
    symbol == "YES" || symbol == "NO"
}

fn zero_balance_entry(symbol: String, token: String) -> BalanceEntry {
    BalanceEntry {
        token,
        symbol,
        total: "0".into(),
        locked: "0".into(),
        available: "0".into(),
    }
}

async fn get_balances(
    State(state): State<AppState>,
    Path(address): Path<String>,
    Json(body): Json<BalancesRequest>,
) -> AppResult<Json<Vec<BalanceEntry>>> {
    let account = lightpool_sdk::Address::from_str(address.trim())
        .map_err(|e| AppError::BadRequest(format!("invalid address: {e}")))?;

    let mut entries = Vec::new();

    for spec in body.tokens {
        let is_position = is_position_symbol(&spec.symbol);

        let token_contract = match parse_token_contract(&spec.address) {
            Ok(contract) => contract,
            Err(e) => {
                tracing::warn!(symbol = %spec.symbol, token = %spec.address, error = %e, "skip balance query");
                if !is_position {
                    entries.push(zero_balance_entry(spec.symbol, spec.address));
                }
                continue;
            }
        };

        match state.chain.get_balance(account, token_contract).await {
            Ok(balance) => {
                if is_position && balance.total == 0 && balance.locked == 0 {
                    continue;
                }
                entries.push(BalanceEntry {
                    token: spec.address,
                    symbol: spec.symbol,
                    total: format_token_amount(balance.total),
                    locked: format_token_amount(balance.locked),
                    available: format_token_amount(balance.available),
                });
            }
            Err(e) => {
                tracing::warn!(symbol = %spec.symbol, error = %e, "get_balance failed, returning zero");
                if !is_position {
                    entries.push(zero_balance_entry(spec.symbol, spec.address));
                }
            }
        }
    }

    Ok(Json(entries))
}
