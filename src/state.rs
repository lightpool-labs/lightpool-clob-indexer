// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use std::sync::Arc;
use std::time::Duration;

use crate::chain::ChainClient;
use crate::config::Config;
use crate::indexer::{
    BookStore, IndexStore, SharedBookStore, SharedIndexStore, SharedIndexedBlockHead, new_head,
};
use crate::mempool_client::MempoolClient;
use crate::submit_queue::{SubmitQueue, SubmitQueueConfig};
use crate::submit_wait::SharedSubmitWaitRegistry;
use crate::ws::process::{SharedUserEventHub, UserEventHub};

#[derive(Clone)]
pub struct AppState {
    pub config: Config,
    pub chain: Arc<ChainClient>,
    pub submit_queue: SubmitQueue,
    pub submit_wait: SharedSubmitWaitRegistry,
    pub indexed_head: SharedIndexedBlockHead,
    pub index: SharedIndexStore,
    pub book_store: SharedBookStore,
    pub user_hub: SharedUserEventHub,
}

impl AppState {
    pub fn new(config: Config) -> Self {
        let chain = Arc::new(ChainClient::new(&config.lightpool_rpc_url));
        let submit_wait = crate::submit_wait::SubmitWaitRegistry::shared();
        let mempool = MempoolClient::new(&config.lightpool_mempool_addr)
            .expect("invalid LIGHTPOOL_MEMPOOL_ADDR");
        let submit_queue = SubmitQueue::spawn(
            mempool,
            submit_wait.clone(),
            SubmitQueueConfig {
                capacity: config.submit_queue_capacity,
                wait_timeout: Duration::from_millis(config.submit_wait_timeout_ms),
            },
        );
        Self {
            config,
            chain,
            submit_queue,
            submit_wait,
            indexed_head: new_head(),
            index: Arc::new(IndexStore::new()),
            book_store: Arc::new(BookStore::new()),
            user_hub: Arc::new(UserEventHub::new()),
        }
    }
}
