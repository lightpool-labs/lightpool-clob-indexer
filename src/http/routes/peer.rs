// Copyright (c) LightPool Labs

use axum::{
    extract::{Query, State},
    routing::get,
    Json, Router,
};
use base64::{engine::general_purpose::STANDARD as B64, Engine};
use serde::Deserialize;

use crate::error::{AppError, AppResult};
use crate::peer::{PeerBlockRow, PeerBlocksResponse, PeerTip};
use crate::persist::CheckpointSnapshot;
use crate::state::AppState;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/tip", get(tip))
        .route("/checkpoint", get(checkpoint))
        .route("/blocks", get(blocks))
}

async fn tip(State(state): State<AppState>) -> AppResult<Json<PeerTip>> {
    let head = state.indexed_head.read().await.clone();
    let (checkpoint_block_num, checkpoint_digest) = match &state.persist {
        Some(persist) => match persist.meta()? {
            Some(meta) => (Some(meta.block_num), Some(meta.digest)),
            None => (None, None),
        },
        None => (None, None),
    };
    Ok(Json(PeerTip {
        block_num: head.block_num,
        digest: head.digest,
        connected: head.connected,
        checkpoint_block_num,
        checkpoint_digest,
    }))
}

async fn checkpoint(State(state): State<AppState>) -> AppResult<Json<CheckpointSnapshot>> {
    let persist = state
        .persist
        .as_ref()
        .ok_or_else(|| AppError::ServiceUnavailable("sqlite persist disabled".into()))?;
    let snapshot = persist
        .export_checkpoint_snapshot()?
        .ok_or_else(|| AppError::NotFound("no checkpoint materialized yet".into()))?;
    Ok(Json(snapshot))
}

#[derive(Debug, Deserialize)]
struct BlocksQuery {
    after: Option<u64>,
    limit: Option<usize>,
}

async fn blocks(
    State(state): State<AppState>,
    Query(query): Query<BlocksQuery>,
) -> AppResult<Json<PeerBlocksResponse>> {
    let persist = state
        .persist
        .as_ref()
        .ok_or_else(|| AppError::ServiceUnavailable("sqlite persist disabled".into()))?;
    let limit = query.limit.unwrap_or(200).clamp(1, 500);
    let rows = persist.load_blocks_after_limited(query.after, limit)?;
    let blocks = rows
        .into_iter()
        .map(|(block_num, digest, payload)| PeerBlockRow {
            block_num,
            digest,
            payload_b64: B64.encode(payload),
        })
        .collect();
    Ok(Json(PeerBlocksResponse { blocks }))
}
