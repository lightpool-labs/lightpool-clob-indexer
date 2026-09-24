// Copyright (c) LightPool Labs
// Author: xiaoyu1998


//! Ordered RocksDB index-state writer:
//! recv IndexWriteSet → build WriteBatch → write → optional epoch clone.
//!
//! Build does not take the write gate (can run during clone).
//! Write + epoch clone share the write gate (no concurrent writes into a clean ckpt).

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use typed_store::rocks::DBBatch;

use crate::error::{AppError, AppResult};

use super::checkpoint::{ckpt_path_for, is_epoch_end};
use super::index_state_tables::IndexStateStore;
use super::op::PersistOp;
use super::timing::{persist_timing, TimedPersistOp};

struct BuiltIndexBatch {
    op_id: u64,
    block_num: u64,
    digest: String,
    batch: DBBatch,
    batch_sent_at: Instant,
}

pub fn spawn(
    mut rx: mpsc::Receiver<TimedPersistOp>,
    index: Arc<IndexStateStore>,
    write_gate: Arc<Mutex<()>>,
    pending: Arc<AtomicU64>,
    index_path: PathBuf,
    epoch_length: u64,
    epoch_ckpt_enabled: Arc<AtomicBool>,
) -> Vec<(String, JoinHandle<()>)> {
    tracing::info!(
        epoch_length,
        "persist index-state pipeline starting (build WriteBatch → rocksdb write → epoch clone)"
    );

    // Buffer built batches so build can continue while write holds the gate for clone.
    let (batch_tx, mut batch_rx) = mpsc::channel::<BuiltIndexBatch>(32);
    let mut handles = Vec::new();

    let build_index = Arc::clone(&index);
    let build_pending = Arc::clone(&pending);
    handles.push((
        "persist_index_state_build".into(),
        tokio::spawn(async move {
            while let Some(TimedPersistOp { op_id, op }) = rx.recv().await {
                persist_timing().mark_encode_start(op_id, 0);

                let index = Arc::clone(&build_index);
                let built = tokio::task::spawn_blocking(move || {
                    let PersistOp::IndexWrite(ws) = op else {
                        return Err(AppError::Internal(
                            "non-index-write op on index-state-build pipeline".into(),
                        ));
                    };
                    let block_num = ws.block_num;
                    let digest = ws.digest.clone();
                    // No write gate: building a WriteBatch must not wait on epoch clone.
                    let batch = index.build_index_write_batch(&ws)?;
                    Ok((block_num, digest, batch))
                })
                .await;

                let (block_num, digest, batch) = match built {
                    Ok(Ok(v)) => v,
                    Ok(Err(error)) => {
                        build_pending.fetch_sub(1, Ordering::Relaxed);
                        persist_timing().cancel(op_id);
                        tracing::error!(error = %error, "index-state build WriteBatch failed");
                        continue;
                    }
                    Err(error) => {
                        build_pending.fetch_sub(1, Ordering::Relaxed);
                        persist_timing().cancel(op_id);
                        tracing::error!(error = %error, "index-state build WriteBatch join failed");
                        continue;
                    }
                };
                persist_timing().mark_encode_done(op_id);

                let sent = BuiltIndexBatch {
                    op_id,
                    block_num,
                    digest,
                    batch,
                    batch_sent_at: Instant::now(),
                };
                if batch_tx.send(sent).await.is_err() {
                    build_pending.fetch_sub(1, Ordering::Relaxed);
                    persist_timing().cancel(op_id);
                    tracing::error!("index-state write stopped; dropping built WriteBatch");
                    break;
                }
                persist_timing().mark_encoded_sent(op_id);
                persist_timing().mark_coalesce_recv(op_id);
                persist_timing().mark_batch_sent(&[op_id]);
            }
            tracing::info!("persist index-state build stopped");
        }),
    ));

    let write_index = Arc::clone(&index);
    handles.push((
        "persist_index_state_write".into(),
        tokio::spawn(async move {
            while let Some(BuiltIndexBatch {
                op_id,
                block_num,
                digest,
                batch,
                batch_sent_at,
            }) = batch_rx.recv().await
            {
                let batch_wait = batch_sent_at.elapsed();
                let index = Arc::clone(&write_index);
                let write_gate = Arc::clone(&write_gate);
                let index_path = index_path.clone();
                let epoch_enabled = epoch_ckpt_enabled.load(Ordering::Relaxed);
                let do_epoch = epoch_enabled && is_epoch_end(block_num, epoch_length);
                let write_started = Instant::now();
                let result = tokio::task::spawn_blocking(move || -> AppResult<()> {
                    let _gate = write_gate
                        .lock()
                        .map_err(|_| AppError::Internal("index-state write gate poisoned".into()))?;
                    batch
                        .write()
                        .map_err(|e| AppError::Internal(format!("index-state rocksdb: {e}")))?;
                    if do_epoch {
                        let dest = ckpt_path_for(&index_path, block_num);
                        index.checkpoint_db(&dest)?;
                        tracing::info!(
                            block_num,
                            digest = %digest,
                            ckpt = %dest.display(),
                            "epoch checkpoint cloned after index WriteBatch"
                        );
                    }
                    Ok(())
                })
                .await;
                let write = write_started.elapsed();
                pending.fetch_sub(1, Ordering::Relaxed);
                match result {
                    Ok(Ok(())) => persist_timing().finish_write(&[op_id], write, batch_wait),
                    Ok(Err(error)) => {
                        persist_timing().cancel(op_id);
                        tracing::error!(error = %error, "index-state WriteBatch write failed");
                    }
                    Err(error) => {
                        persist_timing().cancel(op_id);
                        tracing::error!(error = %error, "index-state WriteBatch write join failed");
                    }
                }
            }
            tracing::info!("persist index-state write stopped");
        }),
    ));

    handles
}
