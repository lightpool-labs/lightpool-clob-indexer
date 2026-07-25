// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use lightpool_sdk::lightpool_types::call::{GetBalance, GetBalanceParams};
use lightpool_sdk::lightpool_types::call::{
    GetMarket, GetMarketInfoParams, GetOrderBook, GetOrderBookParams, GetTokenInfo,
    GetTokenInfoParams,
};
use lightpool_sdk::lightpool_types::SignedTransaction;
use lightpool_sdk::types::SubmitTransactionResponse;
use lightpool_sdk::{
    ActionBuilder, Address, ContractAddress, LightPoolClient, TransactionBuilder,
};

use crate::error::{AppError, AppResult};

pub struct ChainClient {
    client: LightPoolClient,
}

impl ChainClient {
    pub fn new(rpc_url: &str) -> Self {
        Self {
            client: LightPoolClient::new(rpc_url),
        }
    }

    pub async fn health_check(&self) -> AppResult<bool> {
        self.client
            .health_check()
            .await
            .map_err(|e| AppError::Internal(format!("node health check failed: {e}")))
    }

    pub async fn submit_transaction(&self, tx: SignedTransaction) -> AppResult<SubmitTransactionResponse> {
        self.client
            .submit_transaction(tx)
            .await
            .map_err(|e| AppError::Internal(format!("submit transaction failed: {e}")))
    }

    pub async fn get_balance(
        &self,
        account: Address,
        token_contract: ContractAddress,
    ) -> AppResult<GetBalance> {
        let action = ActionBuilder::get_balance(token_contract, account, GetBalanceParams {})
            .map_err(|e| AppError::Internal(format!("build get_balance action: {e}")))?;

        let call_tx = TransactionBuilder::new()
            .account(account)
            .expiration(u64::MAX)
            .add_action(action)
            .build_and_without_sign()
            .map_err(|e| AppError::Internal(format!("build get_balance call tx: {e}")))?;

        let bytes = self
            .client
            .call(call_tx)
            .await
            .map_err(|e| AppError::Internal(format!("call get_balance failed: {e}")))?;

        bincode::deserialize(&bytes)
            .map_err(|e| AppError::Internal(format!("decode GetBalance: {e}")))
    }

    pub async fn get_book(
        &self,
        account: Address,
        spot_market: ContractAddress,
        depth: u32,
    ) -> AppResult<GetOrderBook> {
        let action = ActionBuilder::get_orderbook(
            spot_market,
            GetOrderBookParams {
                depth,
                aggregated: true,
            },
        )
        .map_err(|e| AppError::Internal(format!("build get_book action: {e}")))?;

        let call_tx = TransactionBuilder::new()
            .account(account)
            .expiration(u64::MAX)
            .add_action(action)
            .build_and_without_sign()
            .map_err(|e| AppError::Internal(format!("build get_book call tx: {e}")))?;

        let bytes = self
            .client
            .call(call_tx)
            .await
            .map_err(|e| AppError::Internal(format!("call get_book failed: {e}")))?;

        bincode::deserialize(&bytes)
            .map_err(|e| AppError::Internal(format!("decode GetOrderBook: {e}")))
    }

    pub async fn get_market_info(
        &self,
        account: Address,
        spot_market: ContractAddress,
    ) -> AppResult<GetMarket> {
        let action = ActionBuilder::get_market_info(spot_market, GetMarketInfoParams {})
            .map_err(|e| AppError::Internal(format!("build get_market_info action: {e}")))?;

        let call_tx = TransactionBuilder::new()
            .account(account)
            .expiration(u64::MAX)
            .add_action(action)
            .build_and_without_sign()
            .map_err(|e| AppError::Internal(format!("build get_market_info call tx: {e}")))?;

        let bytes = self
            .client
            .call(call_tx)
            .await
            .map_err(|e| AppError::Internal(format!("call get_market_info failed: {e}")))?;

        bincode::deserialize(&bytes)
            .map_err(|e| AppError::Internal(format!("decode GetMarket: {e}")))
    }

    pub async fn get_token_info(
        &self,
        account: Address,
        token_contract: ContractAddress,
    ) -> AppResult<GetTokenInfo> {
        let action = ActionBuilder::get_token_info(token_contract, GetTokenInfoParams {})
            .map_err(|e| AppError::Internal(format!("build get_token_info action: {e}")))?;

        let call_tx = TransactionBuilder::new()
            .account(account)
            .expiration(u64::MAX)
            .add_action(action)
            .build_and_without_sign()
            .map_err(|e| AppError::Internal(format!("build get_token_info call tx: {e}")))?;

        let bytes = self
            .client
            .call(call_tx)
            .await
            .map_err(|e| AppError::Internal(format!("call get_token_info failed: {e}")))?;

        bincode::deserialize(&bytes)
            .map_err(|e| AppError::Internal(format!("decode GetTokenInfo: {e}")))
    }
}

pub fn format_token_amount(raw: u64) -> String {
    use lightpool_sdk::TOKEN_SCALE;
    let whole = raw / TOKEN_SCALE;
    let frac = raw % TOKEN_SCALE;
    if frac == 0 {
        return whole.to_string();
    }
    format!("{whole}.{frac:06}", frac = frac)
}

pub fn format_price_pieces(raw: u64) -> String {
    use lightpool_sdk::TOKEN_SCALE;
    let numerator = raw.saturating_mul(100);
    let whole = numerator / TOKEN_SCALE;
    let frac = numerator % TOKEN_SCALE;
    if frac == 0 {
        return whole.to_string();
    }
    let frac_str = format!("{frac:06}");
    let trimmed = frac_str.trim_end_matches('0');
    format!("{whole}.{trimmed}")
}
