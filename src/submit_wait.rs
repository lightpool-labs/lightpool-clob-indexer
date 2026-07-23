// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use std::sync::Arc;

use dashmap::DashMap;
use lightpool_sdk::lightpool_types::TransactionReceipt;
use tokio::sync::oneshot;

#[derive(Default)]
pub struct SubmitWaitRegistry {
    pending: DashMap<String, oneshot::Sender<TransactionReceipt>>,
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

    pub fn register(&self, digest_hex: &str) -> oneshot::Receiver<TransactionReceipt> {
        let (sender, receiver) = oneshot::channel();
        self.pending.insert(digest_hex.to_string(), sender);
        receiver
    }

    pub fn cancel(&self, digest_hex: &str) {
        self.pending.remove(digest_hex);
    }

    pub fn complete(&self, digest_hex: &str, receipt: TransactionReceipt) -> bool {
        let Some((_, sender)) = self.pending.remove(digest_hex) else {
            return false;
        };
        sender.send(receipt).is_ok()
    }
}
