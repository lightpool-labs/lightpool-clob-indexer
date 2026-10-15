// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use axum::extract::ws::Message;
use futures_util::stream::SplitSink;
use futures_util::SinkExt;

use crate::state::AppState;
use crate::ws::models::{ws_error, ws_subscribed, ws_unsubscribed, CHANNEL_PAPER_HOUSE};
use crate::ws::process::WsSession;

pub async fn handle_subscribe(
    state: &AppState,
    sender: &mut SplitSink<axum::extract::ws::WebSocket, Message>,
    session: &mut WsSession,
    market: &str,
) -> bool {
    if market.trim().is_empty() {
        let _ = sender
            .send(Message::Text(ws_error("missing market").into()))
            .await;
        return true;
    }

    if let Some(snapshot) = state.index.paper_house_ws(market).await {
        let text = serde_json::to_string(&snapshot).unwrap_or_default();
        if sender.send(Message::Text(text.into())).await.is_err() {
            return false;
        }
    }

    let rx = state.index.subscribe_paper_house(market);
    session.subscribe_paper_house(market.to_string(), rx);

    let _ = sender
        .send(Message::Text(ws_subscribed(CHANNEL_PAPER_HOUSE, market).into()))
        .await;
    true
}

pub async fn handle_unsubscribe(
    sender: &mut SplitSink<axum::extract::ws::WebSocket, Message>,
    session: &mut WsSession,
    market: Option<&str>,
) {
    session.cancel_channel(CHANNEL_PAPER_HOUSE, market);
    if let Some(key) = market {
        let _ = sender
            .send(Message::Text(ws_unsubscribed(CHANNEL_PAPER_HOUSE, key).into()))
            .await;
    }
}
