// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use axum::{extract::State, routing::post, Json, Router};
use lightpool_sdk::lightpool_types::SignedTransaction;

use crate::error::{AppError, AppResult};
use crate::http::models::{InjectTxResponse, SubmitTxRequest, SubmitTxResponse};
use crate::state::AppState;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/submit", post(submit_transaction))
        .route("/inject", post(inject_transaction))
}

fn submit_action_summary(tx: &SignedTransaction) -> String {
    tx.transaction()
        .actions()
        .iter()
        .map(|action| action.action.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

/// Fire-and-forget: push to mempool and return digest. Does **not** wait for receipt/block.
async fn inject_transaction(
    State(state): State<AppState>,
    Json(body): Json<SubmitTxRequest>,
) -> AppResult<Json<InjectTxResponse>> {
    let digest = hex::encode(body.tx.digest().as_bytes());
    let sender = body.tx.transaction().sender();
    let actions = submit_action_summary(&body.tx);

    tracing::debug!(
        digest,
        sender = %sender,
        actions,
        "inject HTTP request received"
    );

    state.mempool.submit_transaction(&body.tx).await?;

    Ok(Json(InjectTxResponse { digest }))
}

async fn submit_transaction(
    State(state): State<AppState>,
    Json(body): Json<SubmitTxRequest>,
) -> AppResult<Json<SubmitTxResponse>> {
    let digest = hex::encode(body.tx.digest().as_bytes());
    let sender = body.tx.transaction().sender();
    let actions = submit_action_summary(&body.tx);

    tracing::info!(
        digest,
        sender = %sender,
        actions,
        "submit HTTP request received"
    );

    let response = state.submit_queue.submit(body.tx).await?;

    if !response.receipt.is_success() {
        tracing::warn!(
            digest = %response.digest,
            block_num = response.block_num,
            status = ?response.receipt.status,
            "submit HTTP response failed transaction"
        );
        return Err(AppError::Internal(format!(
            "transaction failed: {:?}",
            response.receipt.status
        )));
    }

    tracing::info!(
        digest = %response.digest,
        sender = %sender,
        actions,
        block_num = response.block_num,
        event_count = response.receipt.event_count(),
        "submit HTTP response sent to client with receipt"
    );

    Ok(Json(SubmitTxResponse {
        digest: response.digest,
        block_num: response.block_num,
        receipt: response.receipt,
    }))
}
