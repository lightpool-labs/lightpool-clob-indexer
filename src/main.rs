// Copyright (c) LightPool Labs
// Author: xiaoyu1998

mod book_hydrate;
mod bars;
mod chain;
mod config;
mod domain;
mod error;
mod http;
mod indexer;
mod mempool_client;
mod peer;
mod persist;
mod slug;
mod spot_market;
mod state;
mod submit_queue;
mod submit_wait;
mod vault_enrich;
mod ws;

use std::net::SocketAddr;
use std::time::Duration;

use axum::Router;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tower_http::cors::{Any, CorsLayer};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::config::Config;
use crate::state::AppState;

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();

    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(
            |_| "lightpool_clob_index=info,tower_http=warn".into(),
        ))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let config = Config::from_env();
    let state = AppState::new(config.clone());
    let cancel = CancellationToken::new();
    let mut background: Vec<JoinHandle<()>> = Vec::new();

    if let Some(persist) = &state.persist {
        match indexer::recover_from_persist(
            persist,
            &state.chain,
            &config.query_account,
            &state.index,
            &state.book_store,
            &state.user_hub,
            &state.submit_wait,
        )
        .await
        {
            Ok(Some(meta)) => {
                let mut head = state.indexed_head.write().await;
                head.block_num = meta.block_num;
                head.digest = meta.digest;
            }
            Ok(None) => {}
            Err(error) => {
                tracing::error!(error = %error, "failed to recover state from sqlite");
            }
        }
    }

    let shutdown_persist = state.persist.clone();
    let shutdown_apply_gate = if config.enable_indexer {
        state.persist.as_ref().map(|_| indexer::new_apply_gate())
    } else {
        None
    };
    let shutdown_head = state.indexed_head.clone();
    let shutdown_index = state.index.clone();
    let shutdown_book_store = state.book_store.clone();

    if config.enable_indexer {
        let ws_url = config.lightpool_ws_url.clone();
        let chain = state.chain.clone();
        let query_account = config.query_account.clone();
        let head = state.indexed_head.clone();
        let index = state.index.clone();
        let book_store = state.book_store.clone();
        let user_hub = state.user_hub.clone();
        let persist = state.persist.clone();
        let apply_gate = shutdown_apply_gate.clone();
        let peer_catchup = if config.peer_index_urls.is_empty() {
            None
        } else {
            Some(indexer::PeerCatchupConfig {
                peer_urls: config.peer_index_urls.clone(),
                threshold: config.peer_catchup_threshold,
            })
        };
        let bar_store = state.bar_store.clone();
        background.push(indexer::spawn(
            ws_url,
            chain,
            query_account,
            head,
            index,
            book_store,
            user_hub,
            state.submit_wait.clone(),
            persist.clone(),
            apply_gate.clone(),
            peer_catchup,
            bar_store.clone(),
            cancel.clone(),
        ));
        tracing::info!("block indexer started");

        let bar_store_closer = bar_store.clone();
        let bar_cancel = cancel.clone();
        background.push(tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(1));
            loop {
                tokio::select! {
                    biased;
                    _ = bar_cancel.cancelled() => break,
                    _ = ticker.tick() => {
                        bar_store_closer
                            .close_expired(crate::bars::BarStore::now_ts())
                            .await;
                    }
                }
            }
        }));

        if let (Some(persist), Some(apply_gate)) = (persist, apply_gate) {
            background.push(indexer::spawn_checkpoint_worker(
                config.checkpoint_interval_ms,
                persist,
                state.indexed_head.clone(),
                state.index.clone(),
                state.book_store.clone(),
                apply_gate,
                cancel.clone(),
            ));
            tracing::info!(
                interval_ms = config.checkpoint_interval_ms,
                "periodic sqlite checkpoint worker started"
            );
        }
    } else {
        tracing::info!("block indexer disabled");
    }

    let app = Router::new()
        .nest("/api", http::router())
        .layer(CorsLayer::new().allow_origin(Any).allow_methods(Any).allow_headers(Any))
        .with_state(state);

    let addr: SocketAddr = format!("{}:{}", config.host, config.port)
        .parse()
        .expect("invalid listen address");

    tracing::info!("lightpool-clob-index listening on http://{addr}");

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("failed to bind");

    let shutdown = async move {
        wait_shutdown_signal().await;
        tracing::info!("shutdown signal received");
        cancel.cancel();

        let join_all = async {
            for handle in background {
                let _ = handle.await;
            }
        };
        if tokio::time::timeout(Duration::from_secs(5), join_all)
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
                &shutdown_book_store,
                &apply_gate,
                None,
            )
            .await
            {
                Ok(indexer::CheckpointOutcome::Written { block_num, digest }) => {
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

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await
        .expect("server failed");
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
