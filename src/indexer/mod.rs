// Copyright (c) LightPool Labs
// Author: xiaoyu1998

mod book_store;
mod processor;
mod store;

use std::time::Duration;

use lightpool_sdk::{Message, Subscription, WebSocketClient};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

pub use book_store::BookStore;
pub use processor::{apply_order_created_to_book, index_order_created, publish_user_order_created};
pub use store::{IndexStore, SharedIndexStore, SharedIndexedBlockHead, new_head};

pub use book_store::SharedBookStore;

use crate::book_hydrate::{hydrate_all_spot_markets, SharedChainClient};
use crate::error::{AppError, AppResult};
use crate::submit_wait::SharedSubmitWaitRegistry;
use crate::ws::process::SharedUserEventHub;

use processor::process_block;

pub fn spawn(
    ws_url: String,
    chain: SharedChainClient,
    query_account: String,
    head: SharedIndexedBlockHead,
    index: SharedIndexStore,
    book_store: SharedBookStore,
    user_hub: SharedUserEventHub,
    submit_wait: SharedSubmitWaitRegistry,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            match run_once(
                &ws_url,
                &chain,
                &query_account,
                head.clone(),
                index.clone(),
                book_store.clone(),
                user_hub.clone(),
                submit_wait.clone(),
            )
            .await
            {
                Ok(()) => {
                    tracing::warn!("indexer stream ended, reconnecting in 5s");
                }
                Err(e) => {
                    tracing::error!("indexer error: {e}, reconnecting in 5s");
                }
            }

            {
                let mut state = head.write().await;
                state.connected = false;
            }

            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    })
}

async fn run_once(
    ws_url: &str,
    chain: &SharedChainClient,
    query_account: &str,
    head: SharedIndexedBlockHead,
    index: SharedIndexStore,
    book_store: SharedBookStore,
    user_hub: SharedUserEventHub,
    submit_wait: SharedSubmitWaitRegistry,
) -> AppResult<()> {
    if let Err(error) = hydrate_all_spot_markets(chain, &book_store, &index, query_account).await {
        tracing::warn!(error = %error, "startup spot market hydration failed");
    }

    let mut client = WebSocketClient::new(Some(ws_url.to_string()))
        .await
        .map_err(|e| AppError::Internal(format!("create ws client: {e}")))?;

    let (sender, mut receiver) = mpsc::unbounded_channel();
    let subscription_id = client
        .subscribe(Subscription::NewBlocks, sender)
        .await
        .map_err(|e| AppError::Internal(format!("subscribe NewBlocks: {e}")))?;

    tracing::info!(subscription_id, "indexer subscribed to NewBlocks");

    {
        let mut state = head.write().await;
        state.connected = true;
    }

    while let Some(message) = receiver.recv().await {
        match message {
            Message::NewBlock(block) => {
                let block_num = block.block_num;
                let digest = hex::encode(block.digest.as_bytes());
                let tx_count = block.transaction_outputs.len();

                process_block(
                    chain,
                    query_account,
                    &index,
                    &book_store,
                    &user_hub,
                    &submit_wait,
                    block,
                )
                .await;

                let mut state = head.write().await;
                state.block_num = block_num;
                state.digest = digest;
                state.tx_count = tx_count;
            }
            Message::Error(err) => {
                return Err(AppError::Internal(format!("ws error: {err}")));
            }
        }
    }

    Ok(())
}
