// Copyright (c) LightPool Labs
// Author: xiaoyu1998

//! Submit path split into two stages:
//!
//! 1. **Ingress task** — register waiter, push bytes to mempool (wire ack only).
//! 2. **Receipt wait tasks** — each accepted tx gets its own task that parks on
//!    `SubmitWaitRegistry` until `process_block` completes the digest (or timeout).
//!
//! HTTP still returns the receipt; waiting no longer shares a `FuturesUnordered`
//! with the mempool ingress loop (that coupling starved send under load).

use std::time::Duration;

use lightpool_sdk::lightpool_types::SignedTransaction;
use lightpool_sdk::types::SubmitTransactionResponse;
use tokio::sync::{mpsc, oneshot};

use crate::error::{AppError, AppResult};
use crate::mempool_client::MempoolClient;
use crate::submit_wait::{SharedSubmitWaitRegistry, SubmitWaitResult};

struct SubmitJob {
    tx: SignedTransaction,
    respond_to: oneshot::Sender<AppResult<SubmitTransactionResponse>>,
}

pub struct SubmitQueueConfig {
    pub capacity: usize,
    pub wait_timeout: Duration,
}

#[derive(Clone)]
pub struct SubmitQueue {
    sender: mpsc::Sender<SubmitJob>,
}

impl SubmitQueue {
    pub fn spawn(
        mempool: MempoolClient,
        submit_wait: SharedSubmitWaitRegistry,
        config: SubmitQueueConfig,
    ) -> Self {
        let (sender, mut receiver) = mpsc::channel::<SubmitJob>(config.capacity);
        let wait_timeout = config.wait_timeout;

        tokio::spawn(async move {
            while let Some(job) = receiver.recv().await {
                let digest_hex = hex::encode(job.tx.digest().as_bytes());
                let receipt_rx = submit_wait.register(&digest_hex);
                let sender_addr = job.tx.transaction().sender();
                let respond_to = job.respond_to;

                if let Err(error) = mempool.submit_transaction(&job.tx).await {
                    submit_wait.cancel(&digest_hex);
                    tracing::warn!(
                        digest = %digest_hex,
                        sender = %sender_addr,
                        error = %error,
                        "mempool submit failed"
                    );
                    let _ = respond_to.send(Err(error));
                    continue;
                }

                tracing::debug!(
                    digest = %digest_hex,
                    sender = %sender_addr,
                    wait_timeout_ms = wait_timeout.as_millis(),
                    "mempool submit accepted; handing off to receipt wait task"
                );

                let submit_wait = submit_wait.clone();
                tokio::spawn(async move {
                    let result = wait_for_receipt(
                        &submit_wait,
                        &digest_hex,
                        sender_addr,
                        receipt_rx,
                        wait_timeout,
                    )
                    .await;
                    if respond_to.send(result).is_err() {
                        tracing::warn!(
                            digest = %digest_hex,
                            "submit HTTP client disconnected before receipt response was sent"
                        );
                    }
                });
            }

            tracing::info!("submit queue ingress stopped");
        });

        tracing::info!(
            capacity = config.capacity,
            wait_timeout_ms = wait_timeout.as_millis(),
            "submit queue started (mempool ingress + per-tx receipt wait tasks)"
        );
        Self { sender }
    }

    pub async fn submit(&self, tx: SignedTransaction) -> AppResult<SubmitTransactionResponse> {
        let (respond_to, response_rx) = oneshot::channel();
        self.sender
            .send(SubmitJob { tx, respond_to })
            .await
            .map_err(|_| AppError::ServiceUnavailable("submit queue unavailable".into()))?;

        response_rx
            .await
            .map_err(|_| AppError::Internal("submit task dropped".into()))?
    }
}

async fn wait_for_receipt(
    submit_wait: &SharedSubmitWaitRegistry,
    digest_hex: &str,
    sender: lightpool_sdk::Address,
    receipt_rx: oneshot::Receiver<SubmitWaitResult>,
    wait_timeout: Duration,
) -> AppResult<SubmitTransactionResponse> {
    match tokio::time::timeout(wait_timeout, receipt_rx).await {
        Ok(Ok(wait_result)) => {
            tracing::debug!(
                digest = digest_hex,
                sender = %sender,
                block_num = wait_result.block_num,
                success = wait_result.receipt.is_success(),
                event_count = wait_result.receipt.event_count(),
                "submit receipt received; ready for HTTP response"
            );
            Ok(SubmitTransactionResponse {
                digest: digest_hex.to_string(),
                block_num: wait_result.block_num,
                receipt: wait_result.receipt,
            })
        }
        Ok(Err(_)) => {
            submit_wait.cancel(digest_hex);
            tracing::warn!(
                digest = digest_hex,
                sender = %sender,
                "submit waiter dropped before receipt arrived"
            );
            Err(AppError::Internal(format!(
                "submit waiter dropped for transaction {digest_hex}"
            )))
        }
        Err(_) => {
            submit_wait.cancel(digest_hex);
            tracing::warn!(
                digest = digest_hex,
                sender = %sender,
                wait_timeout_ms = wait_timeout.as_millis(),
                "timed out waiting for transaction receipt"
            );
            Err(AppError::Timeout(format!(
                "timed out waiting for transaction {digest_hex} to be committed"
            )))
        }
    }
}
