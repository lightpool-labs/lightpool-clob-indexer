// Copyright (c) LightPool Labs
// Author: xiaoyu1998

mod books;
mod pipeline;
mod processor;
mod index_state;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use lightpool_sdk::{Message, ReceiptBlock, Subscription, WebSocketClient};
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;
use tokio::time::interval;
use tokio_util::sync::CancellationToken;

pub use books::Books;
pub use processor::{
    apply_order_created_to_book, index_order_created, publish_user_order_created, ProcessOpts,
};
pub use index_state::{
    IndexState, OrderQueryRecord, SharedIndexState, SharedIndexedBlockHead, new_head,
};

use crate::book_hydrate::{hydrate_all_spot_markets, SharedChainClient};
use crate::error::{AppError, AppResult};
use crate::peer::{
    decode_block_payload, digest_set_from_blocks, select_catchup_peer, PeerClient,
};
use crate::persist::{PersistMeta, SharedPersist};
use crate::submit_wait::SharedSubmitWaitRegistry;
use crate::ws::process::SharedUserEventHub;

use processor::process_block;

/// When apply backlog exceeds this, skip websocket fan-out to catch up faster.
pub(crate) const QUIET_APPLY_BACKLOG: u64 = 8;
/// Cap in-flight ReceiptBlocks between receive and decode.
pub(crate) const PIPELINE_RAW_CAPACITY: usize = 64;
/// Cap prepared blocks between decode and apply.
pub(crate) const PIPELINE_PREPARED_CAPACITY: usize = 64;
/// Cap WS NewBlocks before TCP/read backpressure.
pub(crate) const WS_NEWBLOCKS_CAPACITY: usize = 128;
/// Cap buffered live NewBlocks while downloading a peer checkpoint.
pub(crate) const PEER_LIVE_BUFFER_MAX: usize = 256;

pub type IndexApplyGate = Arc<Mutex<()>>;

#[derive(Clone)]
pub struct PeerCatchupConfig {
    pub peer_urls: Vec<String>,
    pub threshold: u64,
}

pub struct IndexerSpawnConfig {
    pub ws_url: String,
    pub chain: SharedChainClient,
    pub query_account: String,
    pub head: SharedIndexedBlockHead,
    pub index: SharedIndexState,
    pub user_hub: SharedUserEventHub,
    pub submit_wait: SharedSubmitWaitRegistry,
    pub persist: Option<SharedPersist>,
    pub apply_gate: Option<IndexApplyGate>,
    pub peer_catchup: Option<PeerCatchupConfig>,
}

pub fn new_apply_gate() -> IndexApplyGate {
    Arc::new(Mutex::new(()))
}

pub fn spawn(cfg: IndexerSpawnConfig, cancel: CancellationToken) -> JoinHandle<()> {
    let IndexerSpawnConfig {
        ws_url,
        chain,
        query_account,
        head,
        index,
        user_hub,
        submit_wait,
        persist,
        apply_gate,
        mut peer_catchup,
    } = cfg;
    tokio::spawn(async move {
        let mut first = true;
        loop {
            if cancel.is_cancelled() {
                break;
            }

            let catchup = if first {
                first = false;
                peer_catchup.take()
            } else {
                None
            };
            let result = tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    tracing::info!("indexer cancelled");
                    break;
                }
                result = run_once(
                    &ws_url,
                    &chain,
                    &query_account,
                    head.clone(),
                    index.clone(),
                    user_hub.clone(),
                    submit_wait.clone(),
                    persist.clone(),
                    apply_gate.clone(),
                    catchup,
                    cancel.clone(),
                ) => result,
            };

            {
                let mut state = head.write().await;
                state.connected = false;
                state.catching_up = false;
            }
            if let Some(persist) = &persist {
                persist.set_secondary_persist(true);
                persist.set_epoch_checkpoint(true);
            }

            if cancel.is_cancelled() {
                break;
            }

            match result {
                Ok(()) => {
                    tracing::warn!("indexer stream ended, reconnecting in 5s");
                }
                Err(e) => {
                    tracing::error!("indexer error: {e}, reconnecting in 5s");
                }
            }

            tokio::select! {
                biased;
                _ = cancel.cancelled() => break,
                _ = tokio::time::sleep(Duration::from_secs(5)) => {}
            }
        }
    })
}

pub fn spawn_bars_closer(index: SharedIndexState, cancel: CancellationToken) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(1));
        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => break,
                _ = ticker.tick() => {
                    index.bars.close_expired(crate::bars::Bars::now_ts()).await;
                }
            }
        }
    })
}

#[derive(Debug)]
pub enum CheckpointOutcome {
    Skipped,
    Written { block_num: u64, digest: String },
}

/// Epoch checkpoint: wait for live state mirror to catch up, then clone sqlite file.
pub async fn checkpoint_once(
    persist: &SharedPersist,
    head: &SharedIndexedBlockHead,
    index: &SharedIndexState,
    apply_gate: &IndexApplyGate,
    skip_if: Option<(u64, &str)>,
) -> AppResult<CheckpointOutcome> {
    // Hold apply gate for the whole epoch so live state cannot race the clone.
    let _apply = apply_gate.lock().await;
    let (block_num, digest, catching_up) = {
        let state = head.read().await;
        (state.block_num, state.digest.clone(), state.catching_up)
    };

    if digest.is_empty() {
        return Ok(CheckpointOutcome::Skipped);
    }

    if let Some((last_block_num, last_digest)) = skip_if {
        if catching_up || (block_num == last_block_num && digest == last_digest) {
            return Ok(CheckpointOutcome::Skipped);
        }
    } else if catching_up {
        tracing::warn!(
            block_num,
            "final epoch checkpoint while catching_up; cloning current state db"
        );
    }

    if !persist.wait_idle(Duration::from_secs(60)).await {
        tracing::warn!(
            block_num,
            "persist still busy before epoch clone; forcing full state sync"
        );
        force_full_index_write(persist, index, block_num, digest.clone()).await;
        let _ = persist.wait_idle(Duration::from_secs(60)).await;
    } else {
        let meta = persist.meta()?;
        let needs_sync = match meta {
            Some(m) => m.block_num != block_num || m.digest != digest,
            None => true,
        };
        if needs_sync {
            force_full_index_write(persist, index, block_num, digest.clone()).await;
            let _ = persist.wait_idle(Duration::from_secs(60)).await;
        }
    }

    let persist_clone = persist.clone();
    let digest_clone = digest.clone();
    tokio::task::spawn_blocking(move || {
        persist_clone.clone_epoch_checkpoint(block_num, &digest_clone)
    })
    .await
    .map_err(|e| AppError::Internal(format!("epoch clone join: {e}")))??;

    Ok(CheckpointOutcome::Written { block_num, digest })
}

async fn force_full_index_write(
    persist: &SharedPersist,
    index: &SharedIndexState,
    block_num: u64,
    digest: String,
) {
    let markets = index.export_markets_for_persist().await;
    let orders = index.export_orders_for_persist().await;
    let last_trades = index.export_last_trades_for_persist().await;
    let vaults = index.export_vaults_for_persist().await;
    let vault_portfolio = index.export_vault_portfolio_for_persist().await;
    let (levels, metas) = index.books.export_for_persist().await;

    let mut ws = crate::persist::IndexWriteSet {
        block_num,
        digest,
        full_replace: true,
        ..Default::default()
    };
    for market in markets {
        ws.markets_to_store
            .insert(market.id().to_string(), market);
    }
    for row in orders {
        let id = row.order.id.to_string();
        ws.orders_to_store.insert(
            id.clone(),
            crate::persist::index_write_set::order_row_from_parts(
                row.order,
                row.user_address,
                row.chain_order_id,
                row.spot_market,
                row.size_raw,
                row.filled_raw,
            ),
        );
    }
    for level in levels {
        if level.size_raw == 0 {
            continue;
        }
        ws.book_levels_to_store.insert(
            (
                crate::spot_market::normalize_spot_market_key(&level.spot_market),
                level.side,
                level.price_raw as i64,
            ),
            level.size_raw as i64,
        );
    }
    for meta in metas {
        ws.book_meta_to_store.insert(
            crate::spot_market::normalize_spot_market_key(&meta.spot_market),
            crate::persist::index_state_tables::BookMetaRow {
                sequence: meta.sequence as i64,
                last_trade_price: meta.last_trade_price.map(|v| v as i64),
            },
        );
    }
    for (spot, price) in last_trades {
        ws.last_trades_to_store.insert(
            crate::spot_market::normalize_spot_market_key(&spot),
            price as i64,
        );
    }
    for mut vault in vaults {
        vault.portfolio.clear();
        ws.vaults_to_store.insert(vault.id.to_string(), vault);
    }
    for row in vault_portfolio {
        if row.amount_raw == 0 {
            continue;
        }
        ws.vault_portfolio_to_store.insert(
            (
                row.vault_id,
                crate::spot_market::normalize_spot_market_key(&row.spot_market),
            ),
            row.amount_raw as i64,
        );
    }
    persist.enqueue_index_write(ws).await;
}

pub async fn recover_from_persist(
    persist: &SharedPersist,
    chain: &SharedChainClient,
    query_account: &str,
    index: &SharedIndexState,
    user_hub: &SharedUserEventHub,
    submit_wait: &SharedSubmitWaitRegistry,
) -> AppResult<Option<PersistMeta>> {
    let snapshot = persist.load_into(index).await?;
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
        let block = decode_block_payload(block_num, &payload)?;
        process_block(
            chain,
            query_account,
            index,
            user_hub,
            submit_wait,
            block,
            ProcessOpts::quiet(),
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
    index: SharedIndexState,
    user_hub: SharedUserEventHub,
    submit_wait: SharedSubmitWaitRegistry,
    persist: Option<SharedPersist>,
    apply_gate: Option<IndexApplyGate>,
    peer_catchup: Option<PeerCatchupConfig>,
    cancel: CancellationToken,
) -> AppResult<()> {
    if cancel.is_cancelled() {
        return Ok(());
    }

    {
        let _apply = match &apply_gate {
            Some(gate) => Some(gate.lock().await),
            None => None,
        };
        if let Err(error) = hydrate_all_spot_markets(chain, &index, query_account, None).await
        {
            tracing::warn!(error = %error, "startup spot market hydration failed");
        }
    }

    if cancel.is_cancelled() {
        return Ok(());
    }

    let mut client = WebSocketClient::new(Some(ws_url.to_string()))
        .await
        .map_err(|e| AppError::Internal(format!("create ws client: {e}")))?;

    let (sender, mut receiver) = mpsc::channel(WS_NEWBLOCKS_CAPACITY);
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
                &user_hub,
                &submit_wait,
                &persist,
                apply_gate.as_ref(),
                &cancel,
            )
            .await?;
        }
    }

    if cancel.is_cancelled() {
        return Ok(());
    }

    pipeline::run_live_pipeline(
        receiver,
        pipeline::LivePipelineConfig {
            chain: chain.clone(),
            query_account: query_account.to_string(),
            head: head.clone(),
            index: index.clone(),
            user_hub: user_hub.clone(),
            submit_wait: submit_wait.clone(),
            persist: persist.clone(),
            apply_gate: apply_gate.clone(),
            cancel: cancel.clone(),
        },
    )
    .await
}

async fn run_peer_catchup(
    receiver: &mut mpsc::Receiver<Message>,
    peer_cfg: &PeerCatchupConfig,
    chain: &SharedChainClient,
    query_account: &str,
    head: &SharedIndexedBlockHead,
    index: &SharedIndexState,
    user_hub: &SharedUserEventHub,
    submit_wait: &SharedSubmitWaitRegistry,
    persist: &SharedPersist,
    apply_gate: Option<&IndexApplyGate>,
    cancel: &CancellationToken,
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
    persist.set_secondary_persist(false);

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
            _ = cancel.cancelled() => {
                let mut state = head.write().await;
                state.catching_up = false;
                persist.set_secondary_persist(true);
                return Ok(());
            }
            msg = receiver.recv() => {
                match msg {
                    Some(Message::NewBlock(block)) => {
                        if live_buffer.len() >= PEER_LIVE_BUFFER_MAX {
                            let _ = live_buffer.remove(0);
                            tracing::warn!(
                                buffered = live_buffer.len() + 1,
                                limit = PEER_LIVE_BUFFER_MAX,
                                "peer catch-up live buffer full; dropping oldest NewBlock"
                            );
                        }
                        live_buffer.push(block);
                    }
                    Some(Message::Error(err)) => {
                        let mut state = head.write().await;
                        state.catching_up = false;
                        persist.set_secondary_persist(true);
                        return Err(AppError::Internal(format!(
                            "ws error during peer catch-up: {err}"
                        )));
                    }
                    Some(Message::ReceiptBlock(_)) => {}
                    None => {
                        let mut state = head.write().await;
                        state.catching_up = false;
                        persist.set_secondary_persist(true);
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
                        persist.set_secondary_persist(true);
                        return Err(error);
                    }
                    Err(error) => {
                        let mut state = head.write().await;
                        state.catching_up = false;
                        persist.set_secondary_persist(true);
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
            .apply_checkpoint_snapshot(&snapshot, index)
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
                user_hub,
                submit_wait,
                block,
                ProcessOpts::quiet(),
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
            persist.enqueue_receipt_block(&block).await;
            process_block(
                chain,
                query_account,
                index,
                user_hub,
                submit_wait,
                block,
                ProcessOpts::quiet(),
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
            state.last_indexed_at_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
        }

        {
            let mut state = head.write().await;
            state.block_num = tip.block_num;
            state.digest = tip.digest.clone();
            state.catching_up = false;
        }
        force_full_index_write(persist, index, tip.block_num, tip.digest).await;
        persist.set_secondary_persist(true);
    }

    let finished_tip = head.read().await.block_num;
    tracing::info!(
        block_num = finished_tip,
        "peer catch-up finished; continuing with live NewBlocks"
    );
    Ok(())
}
