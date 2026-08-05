// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use std::str::FromStr;
use std::sync::Arc;

use lightpool_sdk::{parse_token_contract, Address};
use lightpool_sdk::lightpool_types::call::GetOrderBook;
use lightpool_sdk::ContractAddress;

use crate::chain::ChainClient;
use crate::error::{AppError, AppResult};
use crate::indexer::{BookStore, SharedIndexStore};

pub const DEFAULT_BOOK_DEPTH: u32 = 50;

pub fn parse_query_account(value: &str) -> Address {
    Address::from_str(value.trim()).unwrap_or_else(|error| {
        tracing::warn!(
            query_account = %value.trim(),
            error = %error,
            "invalid QUERY_ACCOUNT, falling back to zero address"
        );
        Address::ZERO
    })
}

pub async fn hydrate_spot_from_chain(
    chain: &ChainClient,
    book_store: &BookStore,
    index: &SharedIndexStore,
    query_account: &str,
    spot_market: &str,
    depth: u32,
) -> AppResult<()> {
    let account = parse_query_account(query_account);
    let spot = parse_token_contract(spot_market)
        .map_err(|e| AppError::BadRequest(format!("invalid spot market: {e}")))?;

    let depth = depth.clamp(1, DEFAULT_BOOK_DEPTH);
    let chain_book = get_book_with_depth_fallback(chain, account, spot, depth).await?;
    let last_trade_price = if let Some(price) = index.last_trade_price(spot_market).await {
        Some(price)
    } else {
        chain.get_market_info(account, spot).await?.last_price
    };

    book_store
        .hydrate_from_chain(spot_market, &chain_book, last_trade_price)
        .await;

    tracing::debug!(
        spot_market,
        depth,
        bids = chain_book.best_bids.len(),
        asks = chain_book.best_asks.len(),
        "hydrated order book from chain"
    );

    Ok(())
}

async fn get_book_with_depth_fallback(
    chain: &ChainClient,
    account: Address,
    spot: ContractAddress,
    depth: u32,
) -> AppResult<GetOrderBook> {
    let mut attempt_depth = depth;
    loop {
        match chain.get_book(account, spot, attempt_depth).await {
            Ok(book) => {
                if attempt_depth < depth {
                    tracing::warn!(
                        requested_depth = depth,
                        used_depth = attempt_depth,
                        "get_book failed at higher depth; hydrated with reduced depth"
                    );
                }
                return Ok(book);
            }
            Err(error) if attempt_depth > 10 => {
                tracing::warn!(
                    depth = attempt_depth,
                    error = %error,
                    "get_book failed; retrying with smaller depth"
                );
                attempt_depth = (attempt_depth / 2).max(10);
            }
            Err(error) => return Err(error),
        }
    }
}

pub async fn ensure_chain_hydrated(
    chain: &ChainClient,
    book_store: &BookStore,
    index: &SharedIndexStore,
    query_account: &str,
    spot_market: &str,
    depth: u32,
) -> AppResult<()> {
    if book_store.is_chain_hydrated(spot_market).await {
        return Ok(());
    }
    hydrate_spot_from_chain(chain, book_store, index, query_account, spot_market, depth).await
}

pub async fn rehydrate_spot_from_chain(
    chain: &ChainClient,
    book_store: &BookStore,
    index: &SharedIndexStore,
    query_account: &str,
    spot_market: &str,
    depth: u32,
) -> AppResult<()> {
    hydrate_spot_from_chain(
        chain,
        book_store,
        index,
        query_account,
        spot_market,
        depth,
    )
    .await
}

pub async fn hydrate_all_spot_markets(
    chain: &ChainClient,
    book_store: &BookStore,
    index: &SharedIndexStore,
    query_account: &str,
) -> AppResult<()> {
    let spots = index.list_spot_markets().await;
    if spots.is_empty() {
        return Ok(());
    }

    tracing::info!(count = spots.len(), "hydrating spot markets from chain");
    for spot_market in spots {
        if let Err(error) = hydrate_spot_from_chain(
            chain,
            book_store,
            index,
            query_account,
            &spot_market,
            DEFAULT_BOOK_DEPTH,
        )
        .await
        {
            tracing::warn!(
                spot_market,
                error = %error,
                "failed to hydrate spot market from chain"
            );
        }
    }
    Ok(())
}

pub async fn hydrate_market_spots(
    chain: &ChainClient,
    book_store: &BookStore,
    index: &SharedIndexStore,
    query_account: &str,
    yes_spot_market: &str,
    no_spot_market: &str,
) {
    for spot_market in [yes_spot_market, no_spot_market] {
        if let Err(error) = hydrate_spot_from_chain(
            chain,
            book_store,
            index,
            query_account,
            spot_market,
            DEFAULT_BOOK_DEPTH,
        )
        .await
        {
            tracing::warn!(
                spot_market,
                error = %error,
                "failed to hydrate market spot from chain"
            );
        }
    }
}

pub type SharedChainClient = Arc<ChainClient>;
