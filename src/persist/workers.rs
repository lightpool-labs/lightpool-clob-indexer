// Copyright (c) LightPool Labs
// Author: xiaoyu1998


use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::error::{AppError, AppResult};
use crate::peer::encode_block_payload;

use super::op::PersistOp;
use super::store::PersistStore;
use super::types::BAR_HISTORY_LIMIT;

pub struct PersistWorkers {
    pub(crate) inner: Arc<Mutex<PersistStore>>,
    pub(crate) pending: Arc<AtomicU64>,
    pub(crate) rx: mpsc::UnboundedReceiver<PersistOp>,
    pub(crate) worker_count: usize,
}

impl PersistWorkers {
    pub fn spawn(self) -> Vec<JoinHandle<()>> {
        let worker_count = self.worker_count.max(1);
        tracing::info!(workers = worker_count, "persist workers starting");
        let rx = Arc::new(tokio::sync::Mutex::new(self.rx));
        (0..worker_count)
            .map(|worker_id| {
                let rx = Arc::clone(&rx);
                let inner = Arc::clone(&self.inner);
                let pending = Arc::clone(&self.pending);
                tokio::spawn(async move {
                    run_persist_worker(worker_id, rx, inner, pending).await;
                })
            })
            .collect()
    }
}

async fn run_persist_worker(
    worker_id: usize,
    rx: Arc<tokio::sync::Mutex<mpsc::UnboundedReceiver<PersistOp>>>,
    inner: Arc<Mutex<PersistStore>>,
    pending: Arc<AtomicU64>,
) {
    loop {
        let op = {
            let mut guard = rx.lock().await;
            guard.recv().await
        };
        let Some(op) = op else {
            break;
        };

        let inner = Arc::clone(&inner);
        let result = tokio::task::spawn_blocking(move || apply_persist_op(&inner, op)).await;
        pending.fetch_sub(1, Ordering::Relaxed);
        match result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                tracing::error!(worker_id, error = %error, "persist op failed");
            }
            Err(error) => {
                tracing::error!(worker_id, error = %error, "persist worker join failed");
            }
        }
    }
    tracing::info!(worker_id, "persist worker stopped");
}

fn apply_persist_op(inner: &Mutex<PersistStore>, op: PersistOp) -> AppResult<()> {
    match op {
        PersistOp::SaveReceiptBlock(block) => {
            let block_num = block.block_num;
            let digest = hex::encode(block.digest.as_bytes());
            let payload = encode_block_payload(&block)?;
            let guard = inner
                .lock()
                .map_err(|_| AppError::Internal("sqlite mutex poisoned".into()))?;
            guard.save_block(block_num, &digest, &payload)
        }
        PersistOp::SaveBlockBytes {
            block_num,
            digest,
            payload,
        } => {
            let guard = inner
                .lock()
                .map_err(|_| AppError::Internal("sqlite mutex poisoned".into()))?;
            guard.save_block(block_num, &digest, &payload)
        }
        PersistOp::UpsertOrderHistory(row) => {
            let guard = inner
                .lock()
                .map_err(|_| AppError::Internal("sqlite mutex poisoned".into()))?;
            guard.upsert_order_history(&row)
        }
        PersistOp::SaveClosedBar(bar) => {
            let guard = inner
                .lock()
                .map_err(|_| AppError::Internal("sqlite mutex poisoned".into()))?;
            guard.save_closed_bar(&bar)?;
            guard.trim_bars(&bar.spot_market, &bar.interval, BAR_HISTORY_LIMIT)
        }
        PersistOp::Checkpoint {
            block_num,
            digest,
            markets,
            orders,
            last_trades,
            levels,
            metas,
            vaults,
            vault_portfolio,
        } => {
            let guard = inner
                .lock()
                .map_err(|_| AppError::Internal("sqlite mutex poisoned".into()))?;
            guard.checkpoint(
                block_num,
                &digest,
                &markets,
                &orders,
                &last_trades,
                &levels,
                &metas,
                &vaults,
                &vault_portfolio,
            )
        }
    }
}
