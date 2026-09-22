// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use std::sync::Arc;

use dashmap::DashMap;
use lightpool_sdk::lightpool_types::TransactionReceipt;
use tokio::sync::oneshot;

#[derive(Clone, Debug)]
pub struct SubmitWaitResult {
    pub block_num: u64,
    pub receipt: TransactionReceipt,
}

#[derive(Default)]
pub struct SubmitWaitRegistry {
    pending: DashMap<String, oneshot::Sender<SubmitWaitResult>>,
}

pub type SharedSubmitWaitRegistry = Arc<SubmitWaitRegistry>;

impl SubmitWaitRegistry {
    pub fn new() -> Self {
        Self {
            pending: DashMap::new(),
        }
    }

    pub fn shared() -> SharedSubmitWaitRegistry {
        Arc::new(Self::new())
    }

    pub fn register(&self, digest_hex: &str) -> oneshot::Receiver<SubmitWaitResult> {
        let (sender, receiver) = oneshot::channel();
        self.pending.insert(digest_hex.to_string(), sender);
        receiver
    }

    pub fn cancel(&self, digest_hex: &str) {
        self.pending.remove(digest_hex);
    }

    /// Completes a pending submit waiter. Clones `receipt` only when a waiter exists.
    pub fn complete(
        &self,
        digest_hex: &str,
        block_num: u64,
        receipt: &TransactionReceipt,
    ) -> bool {
        let Some((_, sender)) = self.pending.remove(digest_hex) else {
            return false;
        };
        sender
            .send(SubmitWaitResult {
                block_num,
                receipt: receipt.clone(),
            })
            .is_ok()
    }
}
