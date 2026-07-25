// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use std::str::FromStr;

use lightpool_sdk::{Address, ContractAddress, TOKEN_SCALE};
use lightpool_sdk::parse_token_contract;

use crate::domain::{Vault, VaultAsset};
use crate::error::{AppError, AppResult};
use crate::state::AppState;

fn mul_quote(amount: u64, price: u64) -> u64 {
    ((amount as u128 * price as u128) / TOKEN_SCALE as u128) as u64
}

fn mul_div(a: u64, b: u64, d: u64) -> u64 {
    if d == 0 {
        return 0;
    }
    ((a as u128 * b as u128) / d as u128) as u64
}

/// Format raw token/price units (6 decimals) as a fixed 2-decimal display string.
fn format_amount_2dp(raw: u64) -> String {
    let cents = (raw.saturating_add(5_000)) / 10_000;
    let whole = cents / 100;
    let frac = cents % 100;
    format!("{whole}.{frac:02}")
}

fn parse_address(value: &str) -> AppResult<Address> {
    Address::from_str(value.trim())
        .map_err(|e| AppError::BadRequest(format!("invalid address '{value}': {e}")))
}

fn parse_contract(value: &str) -> AppResult<ContractAddress> {
    parse_token_contract(value.trim())
        .map_err(|e| AppError::BadRequest(format!("invalid contract '{value}': {e}")))
}

pub async fn enrich_vault(state: &AppState, vault: Vault) -> Vault {
    enrich_vault_for_account(state, vault, None).await
}

pub async fn enrich_vault_for_account(
    state: &AppState,
    mut vault: Vault,
    user_account: Option<&str>,
) -> Vault {
    match enrich_vault_inner(state, &vault, user_account).await {
        Ok((equity, user_deposit, portfolio)) => {
            vault.equity = equity;
            vault.user_deposit = user_deposit;
            vault.portfolio = portfolio;
        }
        Err(error) => {
            tracing::warn!(
                vault = %vault.vault_address,
                error = %error,
                "failed to enrich vault equity/portfolio"
            );
            vault.portfolio = Vec::new();
            if vault.user_deposit.is_empty() {
                vault.user_deposit = "0.00".into();
            }
        }
    }
    vault
}

async fn enrich_vault_inner(
    state: &AppState,
    vault: &Vault,
    user_account: Option<&str>,
) -> AppResult<(String, String, Vec<VaultAsset>)> {
    let query_account = parse_address(&state.config.query_account)?;
    let vault_account = parse_address(&vault.vault_account)?;
    let quote_token = parse_contract(&vault.quote_token)?;
    let share_token = parse_contract(&vault.share_token)?;

    let holdings = state
        .index
        .vault_portfolio_holdings(&vault.vault_address)
        .await;

    let quote_balance = state
        .chain
        .get_balance(vault_account, quote_token)
        .await
        .map(|balance| balance.total)
        .unwrap_or(0);

    let mut equity = quote_balance;
    let mut assets = Vec::with_capacity(holdings.len() + 1);

    let cash_amount = format_amount_2dp(quote_balance);
    assets.push(VaultAsset {
        market: format!("{}(Cash)", vault.quote_token),
        amount: cash_amount.clone(),
        last_price: Some("1.00".into()),
        quote_value: Some(cash_amount),
    });

    for (market, amount) in holdings {
        let mut last_price: Option<String> = None;
        let mut quote_value: Option<String> = None;

        if amount > 0 {
            let price = match state.index.last_trade_price(&market).await {
                Some(price) => Some(price),
                None => match parse_contract(&market) {
                    Ok(market_contract) => state
                        .chain
                        .get_market_info(query_account, market_contract)
                        .await
                        .ok()
                        .and_then(|info| info.last_price),
                    Err(_) => None,
                },
            };

            if let Some(price) = price {
                let value = mul_quote(amount, price);
                equity = equity.saturating_add(value);
                last_price = Some(format_amount_2dp(price));
                quote_value = Some(format_amount_2dp(value));
            }
        }

        assets.push(VaultAsset {
            market,
            amount: format_amount_2dp(amount),
            last_price,
            quote_value,
        });
    }

    let user_deposit = match user_account {
        Some(account) if !account.trim().is_empty() => {
            let user = parse_address(account)?;
            let user_shares = state
                .chain
                .get_balance(user, share_token)
                .await
                .map(|balance| balance.total)
                .unwrap_or(0);
            if user_shares == 0 || equity == 0 {
                "0.00".into()
            } else {
                let total_supply = state
                    .chain
                    .get_token_info(query_account, share_token)
                    .await
                    .map(|info| info.total_supply)
                    .unwrap_or(0);
                if total_supply == 0 {
                    "0.00".into()
                } else {
                    format_amount_2dp(mul_div(user_shares, equity, total_supply))
                }
            }
        }
        _ => "0.00".into(),
    };

    Ok((format_amount_2dp(equity), user_deposit, assets))
}

pub async fn enrich_vaults(state: &AppState, vaults: Vec<Vault>) -> Vec<Vault> {
    enrich_vaults_for_account(state, vaults, None).await
}

pub async fn enrich_vaults_for_account(
    state: &AppState,
    vaults: Vec<Vault>,
    user_account: Option<&str>,
) -> Vec<Vault> {
    let mut enriched = Vec::with_capacity(vaults.len());
    for vault in vaults {
        enriched.push(enrich_vault_for_account(state, vault, user_account).await);
    }
    enriched
}
