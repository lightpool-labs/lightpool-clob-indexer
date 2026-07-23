// Copyright (c) LightPool Labs
// Author: xiaoyu1998

mod book_hydrate;
mod chain;
mod config;
mod domain;
mod error;
mod http;
mod indexer;
mod mempool_client;
mod slug;
mod spot_market;
mod state;
mod submit_queue;
mod submit_wait;
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

    if config.enable_indexer {
        let ws_url = config.lightpool_ws_url.clone();
        let chain = state.chain.clone();
        let query_account = config.query_account.clone();
        let head = state.indexed_head.clone();
        let index = state.index.clone();
        let book_store = state.book_store.clone();
        let user_hub = state.user_hub.clone();
        let _indexer_handle = indexer::spawn(
            ws_url,
            chain,
            query_account,
            head,
            index,
            book_store,
            user_hub,
            state.submit_wait.clone(),
        );
        tracing::info!("block indexer started");
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
