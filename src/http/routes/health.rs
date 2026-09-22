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
        .route("/client_version", get(get_client_version))
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

async fn health() -> Json<serde_json::Value> {
    Json(json!({ "status": "ok" }))
}

async fn get_client_version() -> Json<serde_json::Value> {
    Json(json!({
        "client_version": format!(
            "{}/{}",
            env!("CARGO_PKG_NAME"),
            env!("CARGO_PKG_VERSION")
        ),
    }))
}

async fn ready(State(state): State<AppState>) -> AppResult<Json<serde_json::Value>> {
    let node_ok = state.chain.health_check().await?;
    let head = state.indexed_head.read().await.clone();
    let market_count = state.index.market_count().await;
    let vault_count = state.index.vault_count().await;
    let persist_pending = state
        .persist
        .as_ref()
        .map(|p| p.persist_pending())
        .unwrap_or(0);
    let idle_ms = if head.last_indexed_at_ms == 0 {
        0
    } else {
        now_ms().saturating_sub(head.last_indexed_at_ms)
    };
    // Apply is idle when no recent block apply; persist may still drain.
    let apply_busy = head.catching_up || (head.last_indexed_at_ms > 0 && idle_ms < 1_000);
    let persist_busy = persist_pending > 0;
    let busy = apply_busy || persist_busy;

    Ok(Json(json!({
        "status": if node_ok && !head.catching_up && !busy { "ready" } else { "degraded" },
        "node": node_ok,
        "indexer": {
            "connected": head.connected,
            "catching_up": head.catching_up,
            "block_num": head.block_num,
            "digest": head.digest,
            "tx_count": head.tx_count,
            "market_count": market_count,
            "vault_count": vault_count,
            "last_indexed_at_ms": head.last_indexed_at_ms,
            "idle_ms": idle_ms,
            "persist_pending": persist_pending,
            "apply_busy": apply_busy,
            "persist_busy": persist_busy,
            "busy": busy,
        },
    })))
}
