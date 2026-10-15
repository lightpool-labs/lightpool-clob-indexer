// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use lightpool_sdk::lightpool_types::call::{GetBalance, GetBalanceParams};
use lightpool_sdk::lightpool_types::call::{
    GetMarket, GetMarketInfoParams, GetOrderBook, GetOrderBookParams, GetPaperAccount,
    GetPaperAccountParams, GetPaperLp, GetPaperLpParams, GetPaperMarket, GetPaperMarketParams,
    GetPaperQueue, GetPaperQueueParams, GetPaperStake, GetPaperStakeParams, GetTokenInfo,
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

    /// Live paper account via node `get_acct` (inputs resolved from warmup).
    pub async fn get_paper_account(
        &self,
        user: Address,
        paper_market: ContractAddress,
    ) -> AppResult<GetPaperAccount> {
        let action = ActionBuilder::get_paper_account(paper_market, GetPaperAccountParams {})
            .map_err(|e| AppError::Internal(format!("build get_paper_account action: {e}")))?;

        let call_tx = TransactionBuilder::new()
            .sender(user)
            .account(user)
            .expiration(u64::MAX)
            .add_action(action)
            .build_and_without_sign()
            .map_err(|e| AppError::Internal(format!("build get_paper_account call tx: {e}")))?;

        let bytes = self
            .client
            .call(call_tx)
            .await
            .map_err(|e| AppError::Internal(format!("call get_acct failed: {e}")))?;

        bincode::deserialize(&bytes)
            .map_err(|e| AppError::Internal(format!("decode GetPaperAccount: {e}")))
    }

    /// Live paper LP buckets + queue heads via node `get_lp`.
    pub async fn get_paper_lp(
        &self,
        paper_market: ContractAddress,
    ) -> AppResult<GetPaperLp> {
        let action = ActionBuilder::get_paper_lp(paper_market, GetPaperLpParams {})
            .map_err(|e| AppError::Internal(format!("build get_paper_lp action: {e}")))?;

        let caller = Address::ZERO;
        let call_tx = TransactionBuilder::new()
            .sender(caller)
            .account(caller)
            .expiration(u64::MAX)
            .add_action(action)
            .build_and_without_sign()
            .map_err(|e| AppError::Internal(format!("build get_paper_lp call tx: {e}")))?;

        let bytes = self
            .client
            .call(call_tx)
            .await
            .map_err(|e| AppError::Internal(format!("call get_lp failed: {e}")))?;

        bincode::deserialize(&bytes)
            .map_err(|e| AppError::Internal(format!("decode GetPaperLp: {e}")))
    }

    /// Live paper queue tip via node `get_queue`.
    pub async fn get_paper_queue(
        &self,
        paper_market: ContractAddress,
    ) -> AppResult<GetPaperQueue> {
        let action = ActionBuilder::get_paper_queue(paper_market, GetPaperQueueParams {})
            .map_err(|e| AppError::Internal(format!("build get_paper_queue action: {e}")))?;

        let caller = Address::ZERO;
        let call_tx = TransactionBuilder::new()
            .sender(caller)
            .account(caller)
            .expiration(u64::MAX)
            .add_action(action)
            .build_and_without_sign()
            .map_err(|e| AppError::Internal(format!("build get_paper_queue call tx: {e}")))?;

        let bytes = self
            .client
            .call(call_tx)
            .await
            .map_err(|e| AppError::Internal(format!("call get_queue failed: {e}")))?;

        bincode::deserialize(&bytes)
            .map_err(|e| AppError::Internal(format!("decode GetPaperQueue: {e}")))
    }

    /// Live paper market config via node `get_mkt` (includes `paper_token`).
    pub async fn get_paper_market(
        &self,
        paper_market: ContractAddress,
    ) -> AppResult<GetPaperMarket> {
        let action = ActionBuilder::get_paper_market(paper_market, GetPaperMarketParams {})
            .map_err(|e| AppError::Internal(format!("build get_paper_market action: {e}")))?;

        let caller = Address::ZERO;
        let call_tx = TransactionBuilder::new()
            .sender(caller)
            .account(caller)
            .expiration(u64::MAX)
            .add_action(action)
            .build_and_without_sign()
            .map_err(|e| AppError::Internal(format!("build get_paper_market call tx: {e}")))?;

        let bytes = self
            .client
            .call(call_tx)
            .await
            .map_err(|e| AppError::Internal(format!("call get_mkt failed: {e}")))?;

        bincode::deserialize(&bytes)
            .map_err(|e| AppError::Internal(format!("decode GetPaperMarket: {e}")))
    }

    /// Live paper stake via node `get_stake` (account = user).
    pub async fn get_paper_stake(
        &self,
        user: Address,
        paper_market: ContractAddress,
    ) -> AppResult<GetPaperStake> {
        let action = ActionBuilder::get_paper_stake(paper_market, GetPaperStakeParams {})
            .map_err(|e| AppError::Internal(format!("build get_paper_stake action: {e}")))?;

        let call_tx = TransactionBuilder::new()
            .sender(user)
            .account(user)
            .expiration(u64::MAX)
            .add_action(action)
            .build_and_without_sign()
            .map_err(|e| AppError::Internal(format!("build get_paper_stake call tx: {e}")))?;

        let bytes = self
            .client
            .call(call_tx)
            .await
            .map_err(|e| AppError::Internal(format!("call get_stake failed: {e}")))?;

        bincode::deserialize(&bytes)
            .map_err(|e| AppError::Internal(format!("decode GetPaperStake: {e}")))
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

/// Human dollars for spot/oracle fixed-point (`raw / TOKEN_SCALE`).
pub fn format_oracle_human(raw: u64) -> String {
    use lightpool_sdk::TOKEN_SCALE;
    let whole = raw / TOKEN_SCALE;
    let frac = raw % TOKEN_SCALE;
    if frac == 0 {
        return whole.to_string();
    }
    let frac_str = format!("{frac:06}");
    let trimmed = frac_str.trim_end_matches('0');
    format!("{whole}.{trimmed}")
}
