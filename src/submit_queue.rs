// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use std::time::Duration;

use futures_util::stream::{FuturesUnordered, StreamExt};
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
            let mut waiting = FuturesUnordered::new();

            loop {
                tokio::select! {
                    Some(job) = receiver.recv() => {
                        let digest_hex = hex::encode(job.tx.digest().as_bytes());
                        let receipt_rx = submit_wait.register(&digest_hex);
                        let mempool = mempool.clone();
                        let submit_wait = submit_wait.clone();
                        let tx = job.tx;
                        let respond_to = job.respond_to;

                        waiting.push(async move {
                            let result = submit_and_wait(
                                &mempool,
                                &submit_wait,
                                &digest_hex,
                                tx,
                                receipt_rx,
                                wait_timeout,
                            )
                            .await;
                            (respond_to, result)
                        });
                    }
                    Some((respond_to, result)) = waiting.next() => {
                        if respond_to.send(result).is_err() {
                            tracing::warn!(
                                "submit HTTP client disconnected before receipt response was sent"
                            );
                        }
                    }
                    else => break,
                }
            }

            tracing::info!("submit queue dispatcher stopped");
        });

        tracing::info!(
            capacity = config.capacity,
            wait_timeout_ms = wait_timeout.as_millis(),
            "submit queue dispatcher started"
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

async fn submit_and_wait(
    mempool: &MempoolClient,
    submit_wait: &SharedSubmitWaitRegistry,
    digest_hex: &str,
    tx: SignedTransaction,
    receipt_rx: oneshot::Receiver<SubmitWaitResult>,
    wait_timeout: Duration,
) -> AppResult<SubmitTransactionResponse> {
    let sender = tx.transaction().sender();
    if let Err(error) = mempool.submit_transaction(&tx).await {
        submit_wait.cancel(digest_hex);
        tracing::warn!(
            digest = digest_hex,
            sender = %sender,
            error = %error,
            "mempool submit failed"
        );
        return Err(error);
    }

    tracing::info!(
        digest = digest_hex,
        sender = %sender,
        wait_timeout_ms = wait_timeout.as_millis(),
        "mempool submit accepted; waiting for receipt"
    );

    match tokio::time::timeout(wait_timeout, receipt_rx).await {
        Ok(Ok(wait_result)) => {
            tracing::info!(
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
