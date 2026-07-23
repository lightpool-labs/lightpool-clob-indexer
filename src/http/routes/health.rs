// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use axum::{extract::State, routing::get, Json, Router};
use serde_json::json;

use crate::error::AppResult;
use crate::state::AppState;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
}

async fn health() -> Json<serde_json::Value> {
    Json(json!({ "status": "ok" }))
}

async fn ready(State(state): State<AppState>) -> AppResult<Json<serde_json::Value>> {
    let node_ok = state.chain.health_check().await?;
    let head = state.indexed_head.read().await.clone();
    let market_count = state.index.market_count().await;

    Ok(Json(json!({
        "status": if node_ok { "ready" } else { "degraded" },
        "node": node_ok,
        "indexer": {
            "connected": head.connected,
            "block_num": head.block_num,
            "digest": head.digest,
            "tx_count": head.tx_count,
            "market_count": market_count,
        },
    })))
}
