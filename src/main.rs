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

use axum::Router;
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

    if config.enable_indexer {
        let ws_url = config.lightpool_ws_url.clone();
        let chain = state.chain.clone();
        let query_account = config.query_account.clone();
        let head = state.indexed_head.clone();
        let index = state.index.clone();
        let book_store = state.book_store.clone();
        let user_hub = state.user_hub.clone();
        let persist = state.persist.clone();
        let apply_gate = persist.as_ref().map(|_| indexer::new_apply_gate());
        let peer_catchup = if config.peer_index_urls.is_empty() {
            None
        } else {
            Some(indexer::PeerCatchupConfig {
                peer_urls: config.peer_index_urls.clone(),
                threshold: config.peer_catchup_threshold,
            })
        };
        let bar_store = state.bar_store.clone();
        let _indexer_handle = indexer::spawn(
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
        );
        tracing::info!("block indexer started");

        let bar_store_closer = bar_store.clone();
        let _bar_close_handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(1));
            loop {
                ticker.tick().await;
                bar_store_closer
                    .close_expired(crate::bars::BarStore::now_ts())
                    .await;
            }
        });

        if let (Some(persist), Some(apply_gate)) = (persist, apply_gate) {
            let _checkpoint_handle = indexer::spawn_checkpoint_worker(
                config.checkpoint_interval_ms,
                persist,
                state.indexed_head.clone(),
                state.index.clone(),
                state.book_store.clone(),
                apply_gate,
            );
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
    axum::serve(listener, app).await.expect("server failed");
}
