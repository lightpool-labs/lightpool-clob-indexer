// Copyright (c) LightPool Labs

//! Application lifecycle: build state, recover, start named workers, serve HTTP.

use std::net::SocketAddr;
use std::time::Duration;

use axum::Router;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tower_http::cors::{Any, CorsLayer};

use crate::config::Config;
use crate::indexer::{self, IndexApplyGate, IndexerSpawnConfig};
use crate::persist::PersistWorkers;
use crate::state::AppState;
use crate::submit_queue::SubmitQueueIngress;

struct NamedTask {
    name: String,
    handle: JoinHandle<()>,
}

#[derive(Default)]
struct BackgroundSet {
    tasks: Vec<NamedTask>,
}

impl BackgroundSet {
    fn push(&mut self, name: impl Into<String>, handle: JoinHandle<()>) {
        let name = name.into();
        tracing::info!(worker = %name, "background worker started");
        self.tasks.push(NamedTask { name, handle });
    }

    async fn join_all(self) {
        for task in self.tasks {
            match task.handle.await {
                Ok(()) => tracing::debug!(worker = %task.name, "background worker stopped"),
                Err(error) => {
                    tracing::warn!(worker = %task.name, error = %error, "background worker join failed")
                }
            }
        }
    }
}

/// Owns shared state plus deferred workers started from one place.
pub struct App {
    state: AppState,
    cancel: CancellationToken,
    background: BackgroundSet,
    submit_ingress: Option<SubmitQueueIngress>,
    persist_workers: Option<PersistWorkers>,
    apply_gate: Option<IndexApplyGate>,
}

impl App {
    pub fn build(config: Config) -> Self {
        let (state, submit_ingress, persist_workers) = AppState::build(config);
        let apply_gate = if state.config.enable_indexer && state.persist.is_some() {
            Some(indexer::new_apply_gate())
        } else {
            None
        };
        Self {
            state,
            cancel: CancellationToken::new(),
            background: BackgroundSet::default(),
            submit_ingress: Some(submit_ingress),
            persist_workers,
            apply_gate,
        }
    }

    pub async fn recover(&self) {
        let Some(persist) = &self.state.persist else {
            return;
        };
        match indexer::recover_from_persist(
            persist,
            &self.state.chain,
            &self.state.config.query_account,
            &self.state.index,
            &self.state.user_hub,
            &self.state.submit_wait,
        )
        .await
        {
            Ok(Some(meta)) => {
                let mut head = self.state.indexed_head.write().await;
                head.block_num = meta.block_num;
                head.digest = meta.digest;
            }
            Ok(None) => {}
            Err(error) => {
                tracing::error!(error = %error, "failed to recover state from sqlite");
            }
        }
    }

    /// Single place that starts all long-lived background tasks.
    pub fn start_workers(&mut self) {
        if let Some(ingress) = self.submit_ingress.take() {
            self.background.push("submit_queue", ingress.spawn());
        }

        if let Some(workers) = self.persist_workers.take() {
            for (i, handle) in workers.spawn().into_iter().enumerate() {
                self.background.push(format!("persist_{i}"), handle);
            }
        }

        if !self.state.config.enable_indexer {
            tracing::info!("block indexer disabled");
            return;
        }

        let config = &self.state.config;
        let peer_catchup = if config.peer_index_urls.is_empty() {
            None
        } else {
            Some(indexer::PeerCatchupConfig {
                peer_urls: config.peer_index_urls.clone(),
                threshold: config.peer_catchup_threshold,
            })
        };

        self.background.push(
            "indexer",
            indexer::spawn(
                IndexerSpawnConfig {
                    ws_url: config.lightpool_ws_url.clone(),
                    chain: self.state.chain.clone(),
                    query_account: config.query_account.clone(),
                    head: self.state.indexed_head.clone(),
                    index: self.state.index.clone(),
                    user_hub: self.state.user_hub.clone(),
                    submit_wait: self.state.submit_wait.clone(),
                    persist: self.state.persist.clone(),
                    apply_gate: self.apply_gate.clone(),
                    peer_catchup,
                },
                self.cancel.clone(),
            ),
        );

        self.background.push(
            "bars_closer",
            indexer::spawn_bars_closer(self.state.index.clone(), self.cancel.clone()),
        );

        if let (Some(persist), Some(apply_gate)) =
            (self.state.persist.clone(), self.apply_gate.clone())
        {
            self.background.push(
                "checkpoint",
                indexer::spawn_checkpoint_worker(
                    config.checkpoint_interval_ms,
                    persist,
                    self.state.indexed_head.clone(),
                    self.state.index.clone(),
                    apply_gate,
                    self.cancel.clone(),
                ),
            );
        }
    }

    pub async fn serve(mut self) {
        let config = self.state.config.clone();
        let addr: SocketAddr = format!("{}:{}", config.host, config.port)
            .parse()
            .expect("invalid listen address");

        let router = Router::new()
            .nest("/api", crate::http::router())
            .layer(
                CorsLayer::new()
                    .allow_origin(Any)
                    .allow_methods(Any)
                    .allow_headers(Any),
            )
            .with_state(self.state.clone());

        tracing::info!("lightpool-clob-indexer listening on http://{addr}");

        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .expect("failed to bind");

        let cancel = self.cancel.clone();
        let shutdown_persist = self.state.persist.clone();
        let shutdown_apply_gate = self.apply_gate.clone();
        let shutdown_head = self.state.indexed_head.clone();
        let shutdown_index = self.state.index.clone();
        let background = std::mem::take(&mut self.background);

        let shutdown = async move {
            wait_shutdown_signal().await;
            tracing::info!("shutdown signal received");
            cancel.cancel();

            if tokio::time::timeout(Duration::from_secs(5), background.join_all())
                .await
                .is_err()
            {
                tracing::warn!("background tasks did not finish within 5s; continuing shutdown");
            }

            if let (Some(persist), Some(apply_gate)) = (shutdown_persist, shutdown_apply_gate) {
                match indexer::checkpoint_once(
                    &persist,
                    &shutdown_head,
                    &shutdown_index,
                    &apply_gate,
                    None,
                )
                .await
                {
                    Ok(indexer::CheckpointOutcome::Written { block_num, digest }) => {
                        if !persist
                            .wait_idle(Duration::from_secs(10))
                            .await
                        {
                            tracing::warn!("persist queue still busy after final checkpoint enqueue");
                        }
                        tracing::info!(
                            block_num,
                            digest = %digest,
                            "final sqlite checkpoint completed"
                        );
                    }
                    Ok(indexer::CheckpointOutcome::Skipped) => {
                        tracing::info!("final sqlite checkpoint skipped");
                    }
                    Err(error) => {
                        tracing::error!(error = %error, "final sqlite checkpoint failed");
                    }
                }
            }

            tracing::info!("graceful shutdown complete");
        };

        axum::serve(listener, router)
            .with_graceful_shutdown(shutdown)
            .await
            .expect("server failed");
    }
}

async fn wait_shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
