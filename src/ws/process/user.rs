// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use axum::extract::ws::Message;
use futures_util::stream::SplitSink;
use futures_util::SinkExt;

use crate::state::AppState;
use crate::ws::models::{ws_subscribed, ws_unsubscribed, CHANNEL_USER};
use crate::ws::process::WsSession;

pub async fn handle_subscribe(
    state: &AppState,
    sender: &mut SplitSink<axum::extract::ws::WebSocket, Message>,
    session: &mut WsSession,
    user_address: &str,
) -> bool {
    let rx = state.user_hub.subscribe().await;
    session.subscribe_user(user_address.to_string(), rx);

    let _ = sender
        .send(Message::Text(
            ws_subscribed(CHANNEL_USER, user_address).into(),
        ))
        .await;
    true
}

pub async fn handle_unsubscribe(
    sender: &mut SplitSink<axum::extract::ws::WebSocket, Message>,
    session: &mut WsSession,
    user_address: Option<&str>,
) {
    session.cancel_channel(CHANNEL_USER, user_address);
    if let Some(key) = user_address {
        let _ = sender
            .send(Message::Text(ws_unsubscribed(CHANNEL_USER, key).into()))
            .await;
    }
}
