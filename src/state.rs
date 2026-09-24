// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use std::sync::Arc;
use std::time::Duration;

use crate::chain::ChainClient;
use crate::config::Config;
use crate::indexer::{IndexState, SharedIndexState, SharedIndexedBlockHead, new_head};
use crate::mempool_client::MempoolClient;
use crate::persist::{PersistWorkers, SharedPersist};
use crate::submit_queue::{SubmitQueue, SubmitQueueConfig, SubmitQueueIngress};
use crate::submit_wait::SharedSubmitWaitRegistry;
use crate::ws::process::{SharedUserEventHub, UserEventHub};

#[derive(Clone)]
pub struct AppState {
    pub config: Config,
    pub chain: Arc<ChainClient>,
    pub submit_queue: SubmitQueue,
    pub submit_wait: SharedSubmitWaitRegistry,
    pub indexed_head: SharedIndexedBlockHead,
    pub index: SharedIndexState,
    pub user_hub: SharedUserEventHub,
    pub persist: Option<SharedPersist>,
}

impl AppState {
    /// Build shared state without starting background tasks.
    /// Returns deferred workers for [`crate::app::App::start_workers`].
    pub fn build(
        config: Config,
    ) -> (Self, SubmitQueueIngress, Option<PersistWorkers>) {
        let chain = Arc::new(ChainClient::new(&config.lightpool_rpc_url));
        let submit_wait = crate::submit_wait::SubmitWaitRegistry::shared();
        let mempool = MempoolClient::new(&config.lightpool_mempool_addr)
            .expect("invalid LIGHTPOOL_MEMPOOL_ADDR");
        let (submit_queue, submit_ingress) = SubmitQueue::create(
            mempool,
            submit_wait.clone(),
            SubmitQueueConfig {
                capacity: config.submit_queue_capacity,
                wait_timeout: Duration::from_millis(config.submit_wait_timeout_ms),
            },
        );

        let (persist, persist_workers) = if config.enable_sqlite {
            let (persist, workers) =
                SharedPersist::open(
                    &config.sqlite_path,
                    config.checkpoint_every_blocks,
                    config.enable_blocks_persist,
                    config.enable_index_state_persist,
                    config.enable_epoch_checkpoint,
                )
                    .expect("failed to open SQLITE_PATH");
            (Some(persist), Some(workers))
        } else {
            (None, None)
        };

        let state = Self {
            config,
            chain,
            submit_queue,
            submit_wait,
            indexed_head: new_head(),
            index: Arc::new(IndexState::new(persist.clone())),
            user_hub: Arc::new(UserEventHub::new()),
            persist,
        };
        (state, submit_ingress, persist_workers)
    }
}
