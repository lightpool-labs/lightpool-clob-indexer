// Copyright (c) LightPool Labs
// Author: xiaoyu1998

//! Live NewBlocks pipeline:
//! 1. receive  — pull WS messages, enqueue blocks only
//! 2. decode   — prepare/classify (parse event payloads, group by market)
//! 3. apply    — ordered by block; within a block, markets apply in parallel
//!
//! All stage channels are bounded so a slow persist/apply cannot unbounded-grow RAM.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use lightpool_sdk::{Message, ReceiptBlock};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::book_hydrate::SharedChainClient;
use crate::error::{AppError, AppResult};
use crate::persist::SharedPersist;
use crate::submit_wait::SharedSubmitWaitRegistry;
use crate::ws::process::SharedUserEventHub;

use super::processor::{prepare_block, process_prepared_block, ProcessOpts, PreparedBlock};
use super::index_state::{SharedIndexState, SharedIndexedBlockHead};
use super::{
    IndexApplyGate, PIPELINE_PREPARED_CAPACITY, PIPELINE_RAW_CAPACITY, QUIET_APPLY_BACKLOG,
};

pub struct LivePipelineConfig {
    pub chain: SharedChainClient,
    pub query_account: String,
    pub head: SharedIndexedBlockHead,
    pub index: SharedIndexState,
    pub user_hub: SharedUserEventHub,
    pub submit_wait: SharedSubmitWaitRegistry,
    pub persist: Option<SharedPersist>,
    pub apply_gate: Option<IndexApplyGate>,
    pub cancel: CancellationToken,
}

/// Run receive → decode → apply until cancel or WS ends.
pub async fn run_live_pipeline(
    mut ws_rx: mpsc::Receiver<Message>,
    cfg: LivePipelineConfig,
) -> AppResult<()> {
    let (raw_tx, mut raw_rx) = mpsc::channel::<ReceiptBlock>(PIPELINE_RAW_CAPACITY);
    let (prepared_tx, mut prepared_rx) = mpsc::channel::<PreparedBlock>(PIPELINE_PREPARED_CAPACITY);
    let decode_backlog = Arc::new(AtomicU64::new(0));
    let apply_backlog = Arc::new(AtomicU64::new(0));

    let cancel_recv = cfg.cancel.clone();
    let decode_backlog_recv = decode_backlog.clone();
    let receive_worker: JoinHandle<AppResult<()>> = tokio::spawn(async move {
        loop {
            tokio::select! {
                biased;
                _ = cancel_recv.cancelled() => return Ok(()),
                message = ws_rx.recv() => {
                    match message {
                        Some(Message::NewBlock(block)) => {
                            decode_backlog_recv.fetch_add(1, Ordering::Relaxed);
                            if raw_tx.send(block).await.is_err() {
                                decode_backlog_recv.fetch_sub(1, Ordering::Relaxed);
                                return Ok(());
                            }
                        }
                        Some(Message::Error(err)) => {
                            return Err(AppError::Internal(format!("ws error: {err}")));
                        }
                        Some(Message::ReceiptBlock(_)) => {}
                        None => return Ok(()),
                    }
                }
            }
        }
    });

    let cancel_decode = cfg.cancel.clone();
    let decode_backlog_dec = decode_backlog.clone();
    let apply_backlog_enc = apply_backlog.clone();
    let persist_decode = cfg.persist.clone();
    let index_decode = cfg.index.clone();
    let decode_worker = tokio::spawn(async move {
        while let Some(block) = raw_rx.recv().await {
            if cancel_decode.is_cancelled() {
                break;
            }
            decode_backlog_dec.fetch_sub(1, Ordering::Relaxed);

            if let Some(persist) = persist_decode.as_ref() {
                // Awaits when persist queue is full → backpressure into raw/WS.
                persist.enqueue_receipt_block(&block).await;
            }

            let prepared = prepare_block(&index_decode, block).await;
            apply_backlog_enc.fetch_add(1, Ordering::Relaxed);
            if prepared_tx.send(prepared).await.is_err() {
                apply_backlog_enc.fetch_sub(1, Ordering::Relaxed);
                break;
            }
        }
    });

    let cancel_apply = cfg.cancel.clone();
    let apply_backlog_dec = apply_backlog.clone();
    let apply_head = cfg.head.clone();
    let apply_chain = cfg.chain.clone();
    let apply_query = cfg.query_account.clone();
    let apply_index = cfg.index.clone();
    let apply_hub = cfg.user_hub.clone();
    let apply_wait = cfg.submit_wait.clone();
    let apply_gate = cfg.apply_gate.clone();
    let persist_apply = cfg.persist.clone();
    let apply_worker = tokio::spawn(async move {
        while let Some(prepared) = prepared_rx.recv().await {
            if cancel_apply.is_cancelled() {
                break;
            }
            let remaining = apply_backlog_dec
                .fetch_sub(1, Ordering::Relaxed)
                .saturating_sub(1);
            let catching_up = apply_head.read().await.catching_up;
            let quiet = catching_up || remaining >= QUIET_APPLY_BACKLOG;
            if let Some(persist) = persist_apply.as_ref() {
                persist.set_secondary_persist(!quiet);
                persist.set_epoch_checkpoint(!catching_up);
            }
            if quiet && remaining > 0 && remaining.is_multiple_of(50) {
                tracing::warn!(
                    apply_backlog = remaining + 1,
                    quiet,
                    "indexer apply backlog; using quiet mode (no ws fan-out)"
                );
            }

            let block_num = prepared.block_num;
            let digest = prepared.block_digest.clone();
            let tx_count = prepared.tx_count;
            let opts = if quiet {
                ProcessOpts::quiet()
            } else {
                ProcessOpts::default()
            };

            let write_set = {
                let _apply = match &apply_gate {
                    Some(gate) => Some(gate.lock().await),
                    None => None,
                };
                let write_set = process_prepared_block(
                    &apply_chain,
                    &apply_query,
                    &apply_index,
                    &apply_hub,
                    &apply_wait,
                    prepared,
                    opts,
                )
                .await;

                let mut state = apply_head.write().await;
                state.block_num = block_num;
                state.digest = digest.clone();
                state.tx_count = tx_count;
                state.last_indexed_at_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0);
                write_set
            };

            if let Some(persist) = persist_apply.as_ref() {
                persist.enqueue_index_write(write_set).await;
            }
        }
    });

    let recv_result = receive_worker
        .await
        .map_err(|e| AppError::Internal(format!("receive worker join: {e}")))?;
    let _ = decode_worker.await;
    let _ = apply_worker.await;

    recv_result
}
