// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use axum::extract::ws::Message;
use futures_util::stream::SplitSink;
use futures_util::SinkExt;

use crate::state::AppState;
use crate::ws::models::{
    ws_subscribed, ws_unsubscribed, CHANNEL_PAPER_POSITIONS, CHANNEL_PAPER_USER,
};
use crate::ws::process::WsSession;

pub async fn handle_subscribe_user(
    state: &AppState,
    sender: &mut SplitSink<axum::extract::ws::WebSocket, Message>,
    session: &mut WsSession,
    user_address: &str,
) -> bool {
    let rx = state.index.subscribe_paper_user();
    session.subscribe_paper_user(user_address.to_string(), rx);

    let _ = sender
        .send(Message::Text(
            ws_subscribed(CHANNEL_PAPER_USER, user_address).into(),
        ))
        .await;
    true
}

pub async fn handle_unsubscribe_user(
    sender: &mut SplitSink<axum::extract::ws::WebSocket, Message>,
    session: &mut WsSession,
    user_address: Option<&str>,
) {
    session.cancel_channel(CHANNEL_PAPER_USER, user_address);
    if let Some(key) = user_address {
        let _ = sender
            .send(Message::Text(
                ws_unsubscribed(CHANNEL_PAPER_USER, key).into(),
            ))
            .await;
    }
}

pub async fn handle_subscribe_positions(
    state: &AppState,
    sender: &mut SplitSink<axum::extract::ws::WebSocket, Message>,
    session: &mut WsSession,
) -> bool {
    let rx = state.index.subscribe_paper_positions();
    session.subscribe_paper_positions(rx);

    let _ = sender
        .send(Message::Text(
            ws_subscribed(CHANNEL_PAPER_POSITIONS, "*").into(),
        ))
        .await;
    true
}

pub async fn handle_unsubscribe_positions(
    sender: &mut SplitSink<axum::extract::ws::WebSocket, Message>,
    session: &mut WsSession,
) {
    session.cancel_channel(CHANNEL_PAPER_POSITIONS, None);
    let _ = sender
        .send(Message::Text(
            ws_unsubscribed(CHANNEL_PAPER_POSITIONS, "*").into(),
        ))
        .await;
}
