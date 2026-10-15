// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use axum::extract::ws::Message;
use futures_util::stream::SplitSink;
use futures_util::SinkExt;

use crate::state::AppState;
use crate::ws::models::{ws_error, ws_subscribed, ws_unsubscribed, CHANNEL_PAPER_STAKE};
use crate::ws::process::WsSession;

pub async fn handle_subscribe(
    state: &AppState,
    sender: &mut SplitSink<axum::extract::ws::WebSocket, Message>,
    session: &mut WsSession,
    market: &str,
    user_address: &str,
) -> bool {
    if market.trim().is_empty() {
        let _ = sender
            .send(Message::Text(ws_error("missing market").into()))
            .await;
        return true;
    }
    if user_address.trim().is_empty() {
        let _ = sender
            .send(Message::Text(ws_error("missing user_address").into()))
            .await;
        return true;
    }

    // Snapshot on subscribe (same pattern as paper.house).
    if let Some(snapshot) = state
        .index
        .paper_stake_ws(&state.chain, market, user_address)
        .await
    {
        let text = serde_json::to_string(&snapshot).unwrap_or_default();
        if sender.send(Message::Text(text.into())).await.is_err() {
            return false;
        }
    }

    let rx = state.index.subscribe_paper_stake(market);
    session.subscribe_paper_stake(
        state.clone(),
        market.to_string(),
        user_address.to_string(),
        rx,
    );

    let _ = sender
        .send(Message::Text(
            ws_subscribed(CHANNEL_PAPER_STAKE, market).into(),
        ))
        .await;
    true
}

pub async fn handle_unsubscribe(
    sender: &mut SplitSink<axum::extract::ws::WebSocket, Message>,
    session: &mut WsSession,
    market: Option<&str>,
) {
    session.cancel_channel(CHANNEL_PAPER_STAKE, market);
    if let Some(key) = market {
        let _ = sender
            .send(Message::Text(
                ws_unsubscribed(CHANNEL_PAPER_STAKE, key).into(),
            ))
            .await;
    }
}
