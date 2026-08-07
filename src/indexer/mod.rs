// Copyright (c) LightPool Labs
// Author: xiaoyu1998

mod book_store;
mod processor;
mod store;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use lightpool_sdk::{Message, ReceiptBlock, Subscription, WebSocketClient};
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;
use tokio::time::{interval, MissedTickBehavior};

pub use book_store::BookStore;
pub use processor::{apply_order_created_to_book, index_order_created, publish_user_order_created};
pub use store::{IndexStore, SharedIndexStore, SharedIndexedBlockHead, new_head};

pub use book_store::SharedBookStore;

use crate::book_hydrate::{hydrate_all_spot_markets, SharedChainClient};
use crate::error::{AppError, AppResult};
use crate::peer::{
    decode_block_payload, digest_set_from_blocks, select_catchup_peer, PeerClient,
};
use crate::persist::{PersistMeta, SharedPersist};
use crate::submit_wait::SharedSubmitWaitRegistry;
use crate::ws::process::SharedUserEventHub;

use processor::process_block;

pub type IndexApplyGate = Arc<Mutex<()>>;

#[derive(Clone)]
pub struct PeerCatchupConfig {
    pub peer_urls: Vec<String>,
    pub threshold: u64,
}

pub fn new_apply_gate() -> IndexApplyGate {
    Arc::new(Mutex::new(()))
}

pub fn spawn(
    ws_url: String,
    chain: SharedChainClient,
    query_account: String,
    head: SharedIndexedBlockHead,
    index: SharedIndexStore,
    book_store: SharedBookStore,
    user_hub: SharedUserEventHub,
    submit_wait: SharedSubmitWaitRegistry,
    persist: Option<SharedPersist>,
    apply_gate: Option<IndexApplyGate>,
    mut peer_catchup: Option<PeerCatchupConfig>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut first = true;
        loop {
            let catchup = if first {
                first = false;
                peer_catchup.take()
            } else {
                None
            };
            match run_once(
                &ws_url,
                &chain,
                &query_account,
                head.clone(),
                index.clone(),
                book_store.clone(),
                user_hub.clone(),
                submit_wait.clone(),
                persist.clone(),
                apply_gate.clone(),
                catchup,
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
                state.catching_up = false;
            }

            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    })
}

pub fn spawn_checkpoint_worker(
    interval_ms: u64,
    persist: SharedPersist,
    head: SharedIndexedBlockHead,
    index: SharedIndexStore,
    book_store: SharedBookStore,
    apply_gate: IndexApplyGate,
) -> JoinHandle<()> {
    let period = Duration::from_millis(interval_ms.max(1));
    tokio::spawn(async move {
        let mut ticker = interval(period);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut last_block_num = u64::MAX;
        let mut last_digest = String::new();

        loop {
            ticker.tick().await;

            let snapshot = {
                let _apply = apply_gate.lock().await;
                let (block_num, digest, catching_up) = {
                    let state = head.read().await;
                    (state.block_num, state.digest.clone(), state.catching_up)
                };

                if catching_up || digest.is_empty() {
                    None
                } else if block_num == last_block_num && digest == last_digest {
                    None
                } else {
                    let markets = index.export_markets_for_persist().await;
                    let orders = index.export_orders_for_persist().await;
                    let last_trades = index.export_last_trades_for_persist().await;
                    let vaults = index.export_vaults_for_persist().await;
                    let vault_portfolio = index.export_vault_portfolio_for_persist().await;
                    let (levels, metas) = book_store.export_for_persist().await;
                    Some((
                        block_num,
                        digest,
                        markets,
                        orders,
                        last_trades,
                        levels,
                        metas,
                        vaults,
                        vault_portfolio,
                    ))
                }
            };

            let Some((
                block_num,
                digest,
                markets,
                orders,
                last_trades,
                levels,
                metas,
                vaults,
                vault_portfolio,
            )) = snapshot
            else {
                continue;
            };

            match persist.checkpoint_exported(
                block_num,
                &digest,
                &markets,
                &orders,
                &last_trades,
                &levels,
                &metas,
                &vaults,
                &vault_portfolio,
            ) {
                Ok(()) => {
                    tracing::info!(
                        block_num,
                        digest = %digest,
                        "periodic sqlite checkpoint completed"
                    );
                    last_block_num = block_num;
                    last_digest = digest;
                }
                Err(error) => {
                    tracing::error!(
                        block_num,
                        error = %error,
                        "periodic sqlite checkpoint failed"
                    );
                }
            }
        }
    })
}

pub async fn recover_from_persist(
    persist: &SharedPersist,
    chain: &SharedChainClient,
    query_account: &str,
    index: &SharedIndexStore,
    book_store: &SharedBookStore,
    user_hub: &SharedUserEventHub,
    submit_wait: &SharedSubmitWaitRegistry,
) -> AppResult<Option<PersistMeta>> {
    let snapshot = persist.load_into(index, book_store).await?;
    let blocks = persist.load_blocks_after(snapshot.as_ref().map(|m| m.block_num))?;

    if blocks.is_empty() {
        return Ok(snapshot);
    }

    tracing::info!(
        after_block_num = snapshot.as_ref().map(|m| m.block_num),
        replay_count = blocks.len(),
        "replaying persisted blocks after sqlite snapshot"
    );

    let mut head = snapshot;
    for (block_num, digest, payload) in blocks {
        let block: ReceiptBlock = serde_json::from_slice(&payload).map_err(|e| {
            AppError::Internal(format!("decode persisted block {block_num}: {e}"))
        })?;
        process_block(
            chain,
            query_account,
            index,
            book_store,
            user_hub,
            submit_wait,
            block,
        )
        .await;
        head = Some(PersistMeta { block_num, digest });
    }

    if let Some(ref meta) = head {
        tracing::info!(
            block_num = meta.block_num,
            digest = %meta.digest,
            "sqlite recovery replay finished"
        );
    }

    Ok(head)
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
    persist: Option<SharedPersist>,
    apply_gate: Option<IndexApplyGate>,
    peer_catchup: Option<PeerCatchupConfig>,
) -> AppResult<()> {
    {
        let _apply = match &apply_gate {
            Some(gate) => Some(gate.lock().await),
            None => None,
        };
        if let Err(error) = hydrate_all_spot_markets(chain, &book_store, &index, query_account).await
        {
            tracing::warn!(error = %error, "startup spot market hydration failed");
        }
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

    if let (Some(peer_cfg), Some(persist)) = (peer_catchup, persist.clone()) {
        if !peer_cfg.peer_urls.is_empty() {
            run_peer_catchup(
                &mut receiver,
                &peer_cfg,
                chain,
                query_account,
                &head,
                &index,
                &book_store,
                &user_hub,
                &submit_wait,
                &persist,
                apply_gate.as_ref(),
            )
            .await?;
        }
    }

    while let Some(message) = receiver.recv().await {
        match message {
            Message::NewBlock(block) => {
                apply_live_block(
                    block,
                    chain,
                    query_account,
                    &head,
                    &index,
                    &book_store,
                    &user_hub,
                    &submit_wait,
                    persist.as_ref(),
                    apply_gate.as_ref(),
                )
                .await;
            }
            Message::Error(err) => {
                return Err(AppError::Internal(format!("ws error: {err}")));
            }
            Message::ReceiptBlock(_) => {}
        }
    }

    Ok(())
}

async fn run_peer_catchup(
    receiver: &mut mpsc::UnboundedReceiver<Message>,
    peer_cfg: &PeerCatchupConfig,
    chain: &SharedChainClient,
    query_account: &str,
    head: &SharedIndexedBlockHead,
    index: &SharedIndexStore,
    book_store: &SharedBookStore,
    user_hub: &SharedUserEventHub,
    submit_wait: &SharedSubmitWaitRegistry,
    persist: &SharedPersist,
    apply_gate: Option<&IndexApplyGate>,
) -> AppResult<()> {
    let local_tip = head.read().await.block_num;
    let client = PeerClient::new();

    let mut tips = Vec::new();
    for url in &peer_cfg.peer_urls {
        match client.tip(url).await {
            Ok(tip) => tips.push((url.clone(), tip)),
            Err(error) => {
                tracing::warn!(peer = %url, error = %error, "peer tip probe failed");
            }
        }
    }

    let Some((peer_url, peer_tip)) =
        select_catchup_peer(&tips, local_tip, peer_cfg.threshold).cloned()
    else {
        tracing::info!(
            local_tip,
            threshold = peer_cfg.threshold,
            "peer catch-up not needed"
        );
        return Ok(());
    };

    tracing::info!(
        peer = %peer_url,
        local_tip,
        peer_tip = peer_tip.block_num,
        checkpoint = ?peer_tip.checkpoint_block_num,
        "peer catch-up starting; buffering live NewBlocks"
    );

    {
        let mut state = head.write().await;
        state.catching_up = true;
    }

    let peer_url_download = peer_url.clone();
    let download = tokio::spawn(async move {
        let client = PeerClient::new();
        let snapshot = client.checkpoint(&peer_url_download).await?;
        let blocks = client
            .download_blocks_until_empty(&peer_url_download, snapshot.block_num)
            .await?;
        Ok::<_, AppError>((snapshot, blocks))
    });
    tokio::pin!(download);

    let mut live_buffer: Vec<ReceiptBlock> = Vec::new();
    let (snapshot, peer_blocks) = loop {
        tokio::select! {
            biased;
            msg = receiver.recv() => {
                match msg {
                    Some(Message::NewBlock(block)) => live_buffer.push(block),
                    Some(Message::Error(err)) => {
                        let mut state = head.write().await;
                        state.catching_up = false;
                        return Err(AppError::Internal(format!(
                            "ws error during peer catch-up: {err}"
                        )));
                    }
                    Some(Message::ReceiptBlock(_)) => {}
                    None => {
                        let mut state = head.write().await;
                        state.catching_up = false;
                        return Err(AppError::Internal(
                            "ws closed during peer catch-up".into(),
                        ));
                    }
                }
            }
            result = &mut download => {
                match result {
                    Ok(Ok(data)) => break data,
                    Ok(Err(error)) => {
                        let mut state = head.write().await;
                        state.catching_up = false;
                        return Err(error);
                    }
                    Err(error) => {
                        let mut state = head.write().await;
                        state.catching_up = false;
                        return Err(AppError::Internal(format!(
                            "peer download join: {error}"
                        )));
                    }
                }
            }
        }
    };

    tracing::info!(
        checkpoint_block = snapshot.block_num,
        peer_blocks = peer_blocks.len(),
        buffered_live = live_buffer.len(),
        "peer checkpoint/blocks downloaded; applying"
    );

    let mut applied_digests = HashSet::new();
    applied_digests.insert(snapshot.digest.clone());
    applied_digests.extend(digest_set_from_blocks(&peer_blocks));

    {
        let _apply = match apply_gate {
            Some(gate) => Some(gate.lock().await),
            None => None,
        };

        persist
            .apply_checkpoint_snapshot(&snapshot, index, book_store)
            .await?;

        let mut tip = PersistMeta {
            block_num: snapshot.block_num,
            digest: snapshot.digest.clone(),
        };

        for (block_num, digest, payload) in &peer_blocks {
            let block = decode_block_payload(*block_num, payload)?;
            if let Err(error) = persist.save_block(*block_num, digest, payload) {
                tracing::error!(block_num, error = %error, "failed to persist peer block");
            }
            process_block(
                chain,
                query_account,
                index,
                book_store,
                user_hub,
                submit_wait,
                block,
            )
            .await;
            tip = PersistMeta {
                block_num: *block_num,
                digest: digest.clone(),
            };
        }

        for block in live_buffer {
            let digest = hex::encode(block.digest.as_bytes());
            if applied_digests.contains(&digest) {
                continue;
            }
            let block_num = block.block_num;
            let tx_count = block.transaction_outputs.len();
            if let Ok(payload) = serde_json::to_vec(&block) {
                if let Err(error) = persist.save_block(block_num, &digest, &payload) {
                    tracing::error!(block_num, error = %error, "failed to persist buffered block");
                }
            }
            process_block(
                chain,
                query_account,
                index,
                book_store,
                user_hub,
                submit_wait,
                block,
            )
            .await;
            applied_digests.insert(digest.clone());
            tip = PersistMeta {
                block_num,
                digest: digest.clone(),
            };
            let mut state = head.write().await;
            state.block_num = block_num;
            state.digest = digest;
            state.tx_count = tx_count;
        }

        {
            let mut state = head.write().await;
            state.block_num = tip.block_num;
            state.digest = tip.digest;
            state.catching_up = false;
        }
    }

    let finished_tip = head.read().await.block_num;
    tracing::info!(
        block_num = finished_tip,
        "peer catch-up finished; continuing with live NewBlocks"
    );
    Ok(())
}

async fn apply_live_block(
    block: ReceiptBlock,
    chain: &SharedChainClient,
    query_account: &str,
    head: &SharedIndexedBlockHead,
    index: &SharedIndexStore,
    book_store: &SharedBookStore,
    user_hub: &SharedUserEventHub,
    submit_wait: &SharedSubmitWaitRegistry,
    persist: Option<&SharedPersist>,
    apply_gate: Option<&IndexApplyGate>,
) {
    let block_num = block.block_num;
    let digest = hex::encode(block.digest.as_bytes());
    let tx_count = block.transaction_outputs.len();

    if let Some(persist) = persist {
        match serde_json::to_vec(&block) {
            Ok(payload) => {
                if let Err(error) = persist.save_block(block_num, &digest, &payload) {
                    tracing::error!(
                        block_num,
                        error = %error,
                        "failed to persist raw block"
                    );
                }
            }
            Err(error) => {
                tracing::error!(
                    block_num,
                    error = %error,
                    "failed to serialize block for sqlite"
                );
            }
        }
    }

    {
        let _apply = match apply_gate {
            Some(gate) => Some(gate.lock().await),
            None => None,
        };
        process_block(
            chain,
            query_account,
            index,
            book_store,
            user_hub,
            submit_wait,
            block,
        )
        .await;

        let mut state = head.write().await;
        state.block_num = block_num;
        state.digest = digest;
        state.tx_count = tx_count;
    }
}
