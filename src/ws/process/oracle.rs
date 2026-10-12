// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use axum::extract::ws::Message;
use futures_util::stream::SplitSink;
use futures_util::SinkExt;

use crate::state::AppState;
use crate::ws::models::{ws_error, ws_subscribed, ws_unsubscribed, CHANNEL_ORACLE};
use crate::ws::process::WsSession;

pub async fn handle_subscribe(
    state: &AppState,
    sender: &mut SplitSink<axum::extract::ws::WebSocket, Message>,
    session: &mut WsSession,
    spot_market: &str,
) -> bool {
    if spot_market.trim().is_empty() {
        let _ = sender
            .send(Message::Text(ws_error("missing spot_market").into()))
            .await;
        return true;
    }

    if let Some(snapshot) = state.index.oracle_mark_ws(spot_market).await {
        let text = serde_json::to_string(&snapshot).unwrap_or_default();
        if sender.send(Message::Text(text.into())).await.is_err() {
            return false;
        }
    }

    let rx = state.index.subscribe_oracle(spot_market).await;
    session.subscribe_oracle(spot_market.to_string(), rx);

    let _ = sender
        .send(Message::Text(ws_subscribed(CHANNEL_ORACLE, spot_market).into()))
        .await;
    true
}

pub async fn handle_unsubscribe(
    sender: &mut SplitSink<axum::extract::ws::WebSocket, Message>,
    session: &mut WsSession,
    spot_market: Option<&str>,
) {
    session.cancel_channel(CHANNEL_ORACLE, spot_market);
    if let Some(key) = spot_market {
        let _ = sender
            .send(Message::Text(ws_unsubscribed(CHANNEL_ORACLE, key).into()))
            .await;
    }
}
