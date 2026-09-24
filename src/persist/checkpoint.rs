// Copyright (c) LightPool Labs
// Author: xiaoyu1998


//! Dedicated checkpoint persist pipeline (encode → coalesce → write), isolated from
//! block / secondary (history|bars) lanes so checkpoint CPU/IO does not stall them.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::timeout_at;

use crate::error::AppError;

use super::op::{encode_checkpoint_op, EncodedCheckpoint};
use super::store::{BlockStore, StateStore};
use super::timing::{persist_timing, TimedPersistOp};

const CKPT_BATCH_MAX: usize = 4;
const CKPT_BATCH_WAIT_MS: u64 = 2;
const CKPT_ENCODED_CAPACITY: usize = 4;
const CKPT_BATCH_CAPACITY: usize = 2;

struct TimedEncodedCheckpoint {
    op_id: u64,
    encoded: EncodedCheckpoint,
}

struct CheckpointBatch {
    ops: Vec<TimedEncodedCheckpoint>,
    batch_sent_at: Instant,
}

/// Spawn checkpoint encode / coalesce / write tasks.
pub fn spawn(
    rx: mpsc::Receiver<TimedPersistOp>,
    state: Arc<Mutex<StateStore>>,
    blocks: Arc<Mutex<BlockStore>>,
    pending: Arc<AtomicU64>,
) -> Vec<(String, JoinHandle<()>)> {
    let batch_wait = Duration::from_millis(CKPT_BATCH_WAIT_MS);
    tracing::info!("persist checkpoint pipeline starting (encode → coalesce → write)");

    let (encoded_tx, encoded_rx) =
        mpsc::channel::<TimedEncodedCheckpoint>(CKPT_ENCODED_CAPACITY);
    let (batch_tx, batch_rx) = mpsc::channel::<CheckpointBatch>(CKPT_BATCH_CAPACITY);

    let encode = {
        let pending = Arc::clone(&pending);
        tokio::spawn(async move {
            run_checkpoint_encode(rx, encoded_tx, pending).await;
        })
    };

    let coalesce = tokio::spawn(async move {
        run_checkpoint_coalesce(encoded_rx, batch_tx, batch_wait).await;
    });

    let write = tokio::spawn(async move {
        run_checkpoint_write(batch_rx, state, blocks, pending).await;
    });

    vec![
        ("persist_ckpt_encode".into(), encode),
        ("persist_ckpt_coalesce".into(), coalesce),
        ("persist_ckpt_write".into(), write),
    ]
}

async fn run_checkpoint_encode(
    mut rx: mpsc::Receiver<TimedPersistOp>,
    encoded_tx: mpsc::Sender<TimedEncodedCheckpoint>,
    pending: Arc<AtomicU64>,
) {
    while let Some(TimedPersistOp { op_id, op }) = rx.recv().await {
        persist_timing().mark_encode_start(op_id, 0);

        let encoded = tokio::task::spawn_blocking(move || {
            let encoded = encode_checkpoint_op(op)?;
            let bincode_bytes = bincode::serialize(&encoded).map_err(|e| {
                AppError::Internal(format!("checkpoint bincode serialize: {e}"))
            })?;
            tracing::info!(
                block_num = encoded.block_num,
                bincode_bytes = bincode_bytes.len(),
                markets = encoded.markets.len(),
                orders = encoded.orders.len(),
                book_levels = encoded.levels.len(),
                vaults = encoded.vaults.len(),
                "checkpoint encoded (bincode size)"
            );
            Ok::<_, AppError>(encoded)
        })
        .await;

        let encoded = match encoded {
            Ok(Ok(encoded)) => encoded,
            Ok(Err(error)) => {
                pending.fetch_sub(1, Ordering::Relaxed);
                persist_timing().cancel(op_id);
                tracing::error!(op_id, error = %error, "checkpoint encode failed");
                continue;
            }
            Err(error) => {
                pending.fetch_sub(1, Ordering::Relaxed);
                persist_timing().cancel(op_id);
                tracing::error!(op_id, error = %error, "checkpoint encode join failed");
                continue;
            }
        };
        persist_timing().mark_encode_done(op_id);

        if encoded_tx
            .send(TimedEncodedCheckpoint { op_id, encoded })
            .await
            .is_err()
        {
            pending.fetch_sub(1, Ordering::Relaxed);
            persist_timing().cancel(op_id);
            tracing::error!("checkpoint coalesce stopped; dropping encoded checkpoint");
            break;
        }
        persist_timing().mark_encoded_sent(op_id);
    }
    tracing::info!("persist checkpoint encode stopped");
}

async fn run_checkpoint_coalesce(
    mut rx: mpsc::Receiver<TimedEncodedCheckpoint>,
    batch_tx: mpsc::Sender<CheckpointBatch>,
    batch_wait: Duration,
) {
    loop {
        let Some(first) = rx.recv().await else {
            break;
        };
        persist_timing().mark_coalesce_recv(first.op_id);

        let mut ops = vec![first];
        let deadline = tokio::time::Instant::now() + batch_wait;
        while ops.len() < CKPT_BATCH_MAX {
            match timeout_at(deadline, rx.recv()).await {
                Ok(Some(op)) => {
                    persist_timing().mark_coalesce_recv(op.op_id);
                    ops.push(op);
                }
                Ok(None) => break,
                Err(_) => break,
            }
        }

        let op_ids: Vec<u64> = ops.iter().map(|o| o.op_id).collect();
        persist_timing().mark_batch_sent(&op_ids);
        if batch_tx
            .send(CheckpointBatch {
                ops,
                batch_sent_at: Instant::now(),
            })
            .await
            .is_err()
        {
            for id in op_ids {
                persist_timing().cancel(id);
            }
            tracing::error!("checkpoint write stopped; dropping batch");
            break;
        }
    }
    tracing::info!("persist checkpoint coalesce stopped");
}

async fn run_checkpoint_write(
    mut rx: mpsc::Receiver<CheckpointBatch>,
    state: Arc<Mutex<StateStore>>,
    blocks: Arc<Mutex<BlockStore>>,
    pending: Arc<AtomicU64>,
) {
    while let Some(batch) = rx.recv().await {
        let batch_wait = batch.batch_sent_at.elapsed();
        let op_ids: Vec<u64> = batch.ops.iter().map(|o| o.op_id).collect();
        let op_count = batch.ops.len() as u64;
        let ops: Vec<EncodedCheckpoint> = batch.ops.into_iter().map(|o| o.encoded).collect();
        let state = Arc::clone(&state);
        let blocks = Arc::clone(&blocks);
        let write_started = Instant::now();
        let result = tokio::task::spawn_blocking(move || {
            for encoded in ops {
                let block_num = encoded.block_num;
                {
                    let guard = state
                        .lock()
                        .map_err(|_| AppError::Internal("state sqlite mutex poisoned".into()))?;
                    guard.checkpoint_encoded(&encoded)?;
                }
                let guard = blocks
                    .lock()
                    .map_err(|_| AppError::Internal("blocks sqlite mutex poisoned".into()))?;
                guard.delete_blocks_through(block_num)?;
            }
            Ok::<_, AppError>(())
        })
        .await;
        let write = write_started.elapsed();
        pending.fetch_sub(op_count, Ordering::Relaxed);
        match result {
            Ok(Ok(())) => persist_timing().finish_write(&op_ids, write, batch_wait),
            Ok(Err(error)) => {
                for id in &op_ids {
                    persist_timing().cancel(*id);
                }
                tracing::error!(error = %error, "checkpoint write failed");
            }
            Err(error) => {
                for id in &op_ids {
                    persist_timing().cancel(*id);
                }
                tracing::error!(error = %error, "checkpoint write join failed");
            }
        }
    }
    tracing::info!("persist checkpoint write stopped");
}
