// Copyright (c) LightPool Labs
// Author: xiaoyu1998


use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::timeout_at;

use crate::error::AppError;

use super::checkpoint;
use super::op::{encode_persist_op, EncodedPersistOp};
use super::store::{BlockStore, StateStore};
use super::timing::{persist_timing, TimedEncodedOp, TimedPersistOp};
use super::types::{
    DEFAULT_PERSIST_BATCH_MAX, DEFAULT_PERSIST_BATCH_WAIT_MS, DEFAULT_PERSIST_ENCODE_WORKERS,
};

struct EncodedBatch {
    ops: Vec<TimedEncodedOp>,
    batch_sent_at: Instant,
}

pub struct PersistWorkers {
    pub(crate) blocks: Arc<Mutex<BlockStore>>,
    pub(crate) state: Arc<Mutex<StateStore>>,
    pub(crate) pending: Arc<AtomicU64>,
    pub(crate) rx: mpsc::Receiver<TimedPersistOp>,
    pub(crate) ckpt_rx: mpsc::Receiver<TimedPersistOp>,
}

impl PersistWorkers {
    /// Spawn block/secondary encode×N + dual write, plus dedicated checkpoint pipeline.
    pub fn spawn(self) -> Vec<(String, JoinHandle<()>)> {
        let encode_workers = DEFAULT_PERSIST_ENCODE_WORKERS.max(1);
        let batch_max = DEFAULT_PERSIST_BATCH_MAX.max(1);
        let batch_wait = Duration::from_millis(DEFAULT_PERSIST_BATCH_WAIT_MS);
        tracing::info!(
            encode_workers,
            batch_max,
            batch_wait_ms = batch_wait.as_millis(),
            "persist pipeline starting (encode×N → blocks|secondary; checkpoint dedicated)"
        );

        let (block_enc_tx, block_enc_rx) =
            mpsc::channel::<TimedEncodedOp>(encode_workers * 2);
        let (secondary_enc_tx, secondary_enc_rx) =
            mpsc::channel::<TimedEncodedOp>(encode_workers * 2);
        let (block_batch_tx, block_batch_rx) = mpsc::channel::<EncodedBatch>(2);
        let (secondary_batch_tx, secondary_batch_rx) = mpsc::channel::<EncodedBatch>(2);

        let op_rx = Arc::new(tokio::sync::Mutex::new(self.rx));
        let mut handles = Vec::new();

        for worker_id in 0..encode_workers {
            let op_rx = Arc::clone(&op_rx);
            let block_enc_tx = block_enc_tx.clone();
            let secondary_enc_tx = secondary_enc_tx.clone();
            let pending = Arc::clone(&self.pending);
            handles.push((
                format!("persist_encode_{worker_id}"),
                tokio::spawn(async move {
                    run_persist_encode(
                        worker_id,
                        op_rx,
                        block_enc_tx,
                        secondary_enc_tx,
                        pending,
                    )
                    .await;
                }),
            ));
        }
        drop(block_enc_tx);
        drop(secondary_enc_tx);

        handles.push((
            "persist_coalesce_blocks".into(),
            tokio::spawn(async move {
                run_persist_coalesce(block_enc_rx, block_batch_tx, batch_max, batch_wait, "blocks")
                    .await;
            }),
        ));
        handles.push((
            "persist_coalesce_secondary".into(),
            tokio::spawn(async move {
                run_persist_coalesce(
                    secondary_enc_rx,
                    secondary_batch_tx,
                    batch_max,
                    batch_wait,
                    "secondary",
                )
                .await;
            }),
        ));

        let blocks = self.blocks.clone();
        let pending_blocks = Arc::clone(&self.pending);
        handles.push((
            "persist_write_blocks".into(),
            tokio::spawn(async move {
                run_persist_write_blocks(block_batch_rx, blocks, pending_blocks).await;
            }),
        ));

        let state_secondary = self.state.clone();
        let pending_secondary = Arc::clone(&self.pending);
        handles.push((
            "persist_write_secondary".into(),
            tokio::spawn(async move {
                run_persist_write_secondary(secondary_batch_rx, state_secondary, pending_secondary)
                    .await;
            }),
        ));

        handles.extend(checkpoint::spawn(
            self.ckpt_rx,
            self.state,
            self.blocks,
            self.pending,
        ));

        handles
    }
}

async fn run_persist_encode(
    worker_id: usize,
    op_rx: Arc<tokio::sync::Mutex<mpsc::Receiver<TimedPersistOp>>>,
    block_enc_tx: mpsc::Sender<TimedEncodedOp>,
    secondary_enc_tx: mpsc::Sender<TimedEncodedOp>,
    pending: Arc<AtomicU64>,
) {
    loop {
        let timed = {
            let mut guard = op_rx.lock().await;
            guard.recv().await
        };
        let Some(timed) = timed else {
            break;
        };
        let TimedPersistOp { op_id, op } = timed;
        persist_timing().mark_encode_start(op_id, worker_id);

        let encoded = tokio::task::spawn_blocking(move || encode_persist_op(op)).await;
        let encoded = match encoded {
            Ok(Ok(encoded)) => encoded,
            Ok(Err(error)) => {
                pending.fetch_sub(1, Ordering::Relaxed);
                persist_timing().cancel(op_id);
                tracing::error!(worker_id, op_id, error = %error, "persist encode failed");
                continue;
            }
            Err(error) => {
                pending.fetch_sub(1, Ordering::Relaxed);
                persist_timing().cancel(op_id);
                tracing::error!(worker_id, op_id, error = %error, "persist encode join failed");
                continue;
            }
        };
        persist_timing().mark_encode_done(op_id);

        let is_block = encoded.is_block();
        let timed_encoded = TimedEncodedOp {
            op_id,
            op: encoded,
        };
        let send_result = if is_block {
            block_enc_tx.send(timed_encoded).await
        } else {
            secondary_enc_tx.send(timed_encoded).await
        };
        if send_result.is_err() {
            pending.fetch_sub(1, Ordering::Relaxed);
            persist_timing().cancel(op_id);
            tracing::error!(worker_id, "persist coalesce stopped; dropping encoded op");
            break;
        }
        persist_timing().mark_encoded_sent(op_id);
    }
    tracing::info!(worker_id, "persist encode stopped");
}

async fn run_persist_coalesce(
    mut rx: mpsc::Receiver<TimedEncodedOp>,
    batch_tx: mpsc::Sender<EncodedBatch>,
    batch_max: usize,
    batch_wait: Duration,
    lane: &'static str,
) {
    loop {
        let Some(first) = rx.recv().await else {
            break;
        };
        persist_timing().mark_coalesce_recv(first.op_id);

        let mut ops = vec![first];
        let deadline = tokio::time::Instant::now() + batch_wait;

        while ops.len() < batch_max {
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
            .send(EncodedBatch {
                ops,
                batch_sent_at: Instant::now(),
            })
            .await
            .is_err()
        {
            for id in op_ids {
                persist_timing().cancel(id);
            }
            tracing::error!(lane, "persist write stopped; dropping coalesced batch");
            break;
        }
    }
    tracing::info!(lane, "persist coalesce stopped");
}

async fn run_persist_write_blocks(
    mut rx: mpsc::Receiver<EncodedBatch>,
    blocks: Arc<Mutex<BlockStore>>,
    pending: Arc<AtomicU64>,
) {
    while let Some(batch) = rx.recv().await {
        let batch_wait = batch.batch_sent_at.elapsed();
        let op_ids: Vec<u64> = batch.ops.iter().map(|o| o.op_id).collect();
        let op_count = batch.ops.len() as u64;
        let ops: Vec<EncodedPersistOp> = batch.ops.into_iter().map(|o| o.op).collect();
        let blocks = Arc::clone(&blocks);
        let write_started = Instant::now();
        let result = tokio::task::spawn_blocking(move || {
            let guard = blocks
                .lock()
                .map_err(|_| AppError::Internal("blocks sqlite mutex poisoned".into()))?;
            guard.apply_encoded_blocks_batch(&ops)
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
                tracing::error!(error = %error, "persist blocks write failed");
            }
            Err(error) => {
                for id in &op_ids {
                    persist_timing().cancel(*id);
                }
                tracing::error!(error = %error, "persist blocks write join failed");
            }
        }
    }
    tracing::info!("persist write blocks stopped");
}

async fn run_persist_write_secondary(
    mut rx: mpsc::Receiver<EncodedBatch>,
    state: Arc<Mutex<StateStore>>,
    pending: Arc<AtomicU64>,
) {
    while let Some(batch) = rx.recv().await {
        let batch_wait = batch.batch_sent_at.elapsed();
        let op_ids: Vec<u64> = batch.ops.iter().map(|o| o.op_id).collect();
        let op_count = batch.ops.len() as u64;
        let ops: Vec<EncodedPersistOp> = batch.ops.into_iter().map(|o| o.op).collect();
        let state = Arc::clone(&state);
        let write_started = Instant::now();
        let result = tokio::task::spawn_blocking(move || {
            let guard = state
                .lock()
                .map_err(|_| AppError::Internal("state sqlite mutex poisoned".into()))?;
            guard.apply_encoded_state_batch(&ops)
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
                tracing::error!(error = %error, "persist secondary write failed");
            }
            Err(error) => {
                for id in &op_ids {
                    persist_timing().cancel(*id);
                }
                tracing::error!(error = %error, "persist secondary write join failed");
            }
        }
    }
    tracing::info!("persist write secondary stopped");
}
