// Copyright (c) LightPool Labs
// Author: xiaoyu1998


use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::timeout_at;

use crate::error::AppError;

use super::index_state_write;
use super::op::{encode_persist_op, EncodedPersistOp};
use super::index_state_tables::IndexStateStore;
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
    /// Sqlite for order_history + bars.
    pub(crate) state: Arc<Mutex<StateStore>>,
    pub(crate) index: Arc<IndexStateStore>,
    pub(crate) index_write_gate: Arc<Mutex<()>>,
    pub(crate) pending: Arc<AtomicU64>,
    /// Blocks sqlite ingress; None when disabled.
    pub(crate) blocks_rx: Option<mpsc::Receiver<TimedPersistOp>>,
    /// History (order_history / bars) sqlite ingress.
    pub(crate) history_rx: mpsc::Receiver<TimedPersistOp>,
    /// IndexWriteSet ingress (RocksDB index-state); None when disabled.
    pub(crate) index_state_write_rx: Option<mpsc::Receiver<TimedPersistOp>>,
    pub(crate) index_path: std::path::PathBuf,
    pub(crate) epoch_length: u64,
    pub(crate) epoch_ckpt_enabled: Arc<std::sync::atomic::AtomicBool>,
}

impl PersistWorkers {
    /// Spawn persist pipelines: history, and optionally blocks / index-state.
    pub fn spawn(self) -> Vec<(String, JoinHandle<()>)> {
        let pending = Arc::clone(&self.pending);
        let mut handles = Vec::new();
        if let Some(rx) = self.blocks_rx {
            handles.extend(spawn_blocks_persist(
                rx,
                self.blocks,
                Arc::clone(&pending),
            ));
        } else {
            tracing::info!("persist blocks sqlite pipeline disabled");
        }
        handles.extend(spawn_history_persist(
            self.history_rx,
            self.state,
            Arc::clone(&pending),
        ));
        if let Some(rx) = self.index_state_write_rx {
            handles.extend(spawn_index_state_write(
                rx,
                self.index,
                self.index_write_gate,
                pending,
                self.index_path,
                self.epoch_length,
                self.epoch_ckpt_enabled,
            ));
        } else {
            tracing::info!("persist index-state WriteBatch pipeline disabled");
        }
        handles
    }
}

fn spawn_blocks_persist(
    rx: mpsc::Receiver<TimedPersistOp>,
    blocks: Arc<Mutex<BlockStore>>,
    pending: Arc<AtomicU64>,
) -> Vec<(String, JoinHandle<()>)> {
    let encode_workers = DEFAULT_PERSIST_ENCODE_WORKERS.max(1);
    let batch_max = DEFAULT_PERSIST_BATCH_MAX.max(1);
    let batch_wait = Duration::from_millis(DEFAULT_PERSIST_BATCH_WAIT_MS);
    tracing::info!(
        encode_workers,
        batch_max,
        batch_wait_ms = batch_wait.as_millis(),
        "persist blocks pipeline starting (encode → coalesce → write)"
    );

    let (enc_tx, enc_rx) = mpsc::channel::<TimedEncodedOp>(encode_workers * 2);
    let (batch_tx, batch_rx) = mpsc::channel::<EncodedBatch>(2);
    let op_rx = Arc::new(tokio::sync::Mutex::new(rx));
    let mut handles = Vec::new();

    for worker_id in 0..encode_workers {
        let op_rx = Arc::clone(&op_rx);
        let enc_tx = enc_tx.clone();
        let pending = Arc::clone(&pending);
        handles.push((
            format!("persist_sqlite_encode_blocks_{worker_id}"),
            tokio::spawn(async move {
                run_sqlite_encode(worker_id, op_rx, enc_tx, pending, "blocks").await;
            }),
        ));
    }
    drop(enc_tx);

    handles.push((
        "persist_blocks_coalesce".into(),
        tokio::spawn(async move {
            run_coalesce(enc_rx, batch_tx, batch_max, batch_wait, "blocks").await;
        }),
    ));
    handles.push((
        "persist_blocks_write".into(),
        tokio::spawn(async move {
            run_blocks_write(batch_rx, blocks, pending).await;
        }),
    ));
    handles
}

fn spawn_history_persist(
    rx: mpsc::Receiver<TimedPersistOp>,
    state: Arc<Mutex<StateStore>>,
    pending: Arc<AtomicU64>,
) -> Vec<(String, JoinHandle<()>)> {
    let encode_workers = DEFAULT_PERSIST_ENCODE_WORKERS.max(1);
    let batch_max = DEFAULT_PERSIST_BATCH_MAX.max(1);
    let batch_wait = Duration::from_millis(DEFAULT_PERSIST_BATCH_WAIT_MS);
    tracing::info!(
        encode_workers,
        batch_max,
        batch_wait_ms = batch_wait.as_millis(),
        "persist history pipeline starting (encode → coalesce → write)"
    );

    let (enc_tx, enc_rx) = mpsc::channel::<TimedEncodedOp>(encode_workers * 2);
    let (batch_tx, batch_rx) = mpsc::channel::<EncodedBatch>(2);
    let op_rx = Arc::new(tokio::sync::Mutex::new(rx));
    let mut handles = Vec::new();

    for worker_id in 0..encode_workers {
        let op_rx = Arc::clone(&op_rx);
        let enc_tx = enc_tx.clone();
        let pending = Arc::clone(&pending);
        handles.push((
            format!("persist_sqlite_encode_history_{worker_id}"),
            tokio::spawn(async move {
                run_sqlite_encode(worker_id, op_rx, enc_tx, pending, "history").await;
            }),
        ));
    }
    drop(enc_tx);

    handles.push((
        "persist_history_coalesce".into(),
        tokio::spawn(async move {
            run_coalesce(enc_rx, batch_tx, batch_max, batch_wait, "history").await;
        }),
    ));
    handles.push((
        "persist_history_write".into(),
        tokio::spawn(async move {
            run_history_write(batch_rx, state, pending).await;
        }),
    ));
    handles
}

fn spawn_index_state_write(
    rx: mpsc::Receiver<TimedPersistOp>,
    index: Arc<IndexStateStore>,
    write_gate: Arc<Mutex<()>>,
    pending: Arc<AtomicU64>,
    index_path: std::path::PathBuf,
    epoch_length: u64,
    epoch_ckpt_enabled: Arc<std::sync::atomic::AtomicBool>,
) -> Vec<(String, JoinHandle<()>)> {
    index_state_write::spawn(
        rx,
        index,
        write_gate,
        pending,
        index_path,
        epoch_length,
        epoch_ckpt_enabled,
    )
}

async fn run_sqlite_encode(
    worker_id: usize,
    op_rx: Arc<tokio::sync::Mutex<mpsc::Receiver<TimedPersistOp>>>,
    enc_tx: mpsc::Sender<TimedEncodedOp>,
    pending: Arc<AtomicU64>,
    lane: &'static str,
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
                tracing::error!(
                    worker_id,
                    lane,
                    op_id,
                    error = %error,
                    "persist sqlite encode failed"
                );
                continue;
            }
            Err(error) => {
                pending.fetch_sub(1, Ordering::Relaxed);
                persist_timing().cancel(op_id);
                tracing::error!(
                    worker_id,
                    lane,
                    op_id,
                    error = %error,
                    "persist sqlite encode join failed"
                );
                continue;
            }
        };
        persist_timing().mark_encode_done(op_id);

        let timed_encoded = TimedEncodedOp {
            op_id,
            op: encoded,
        };
        if enc_tx.send(timed_encoded).await.is_err() {
            pending.fetch_sub(1, Ordering::Relaxed);
            persist_timing().cancel(op_id);
            tracing::error!(worker_id, lane, "persist coalesce stopped; dropping encoded op");
            break;
        }
        persist_timing().mark_encoded_sent(op_id);
    }
    tracing::info!(worker_id, lane, "persist sqlite encode stopped");
}

async fn run_coalesce(
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

async fn run_blocks_write(
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
    tracing::info!("persist blocks write stopped");
}

async fn run_history_write(
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
                .map_err(|_| AppError::Internal("history sqlite mutex poisoned".into()))?;
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
                tracing::error!(error = %error, "persist history write failed");
            }
            Err(error) => {
                for id in &op_ids {
                    persist_timing().cancel(*id);
                }
                tracing::error!(error = %error, "persist history write join failed");
            }
        }
    }
    tracing::info!("persist history write stopped");
}
