// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use axum::extract::ws::Message;
use futures_util::stream::SplitSink;
use futures_util::SinkExt;

use crate::state::AppState;
use crate::ws::models::{
    ws_error, ws_subscribed, ws_unsubscribed, CHANNEL_PAPER_ACTIVITY, CHANNEL_PAPER_POSITIONS,
    CHANNEL_PAPER_USER, CHANNEL_PAPER_USER_STATE,
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

pub async fn handle_subscribe_activity(
    state: &AppState,
    sender: &mut SplitSink<axum::extract::ws::WebSocket, Message>,
    session: &mut WsSession,
) -> bool {
    let rx = state.index.subscribe_paper_activity();
    session.subscribe_paper_activity(rx);

    let _ = sender
        .send(Message::Text(
            ws_subscribed(CHANNEL_PAPER_ACTIVITY, "*").into(),
        ))
        .await;
    true
}

pub async fn handle_unsubscribe_activity(
    sender: &mut SplitSink<axum::extract::ws::WebSocket, Message>,
    session: &mut WsSession,
) {
    session.cancel_channel(CHANNEL_PAPER_ACTIVITY, None);
    let _ = sender
        .send(Message::Text(
            ws_unsubscribed(CHANNEL_PAPER_ACTIVITY, "*").into(),
        ))
        .await;
}

pub async fn handle_subscribe_user_state(
    state: &AppState,
    sender: &mut SplitSink<axum::extract::ws::WebSocket, Message>,
    session: &mut WsSession,
    user_address: &str,
    market: &str,
) -> bool {
    if user_address.trim().is_empty() {
        let _ = sender
            .send(Message::Text(ws_error("missing user_address").into()))
            .await;
        return true;
    }
    if market.trim().is_empty() {
        let _ = sender
            .send(Message::Text(ws_error("missing market").into()))
            .await;
        return true;
    }

    let user_rx = state.index.subscribe_paper_user();
    let stake_rx = state.index.subscribe_paper_stake(market);
    if let Some(snapshot) = state
        .index
        .paper_user_state(&state.chain, user_address, market)
        .await
    {
        let text = serde_json::to_string(&snapshot).unwrap_or_default();
        if sender.send(Message::Text(text.into())).await.is_err() {
            return false;
        }
    }
    session.subscribe_paper_user_state(
        state.clone(),
        user_address.to_string(),
        market.to_string(),
        user_rx,
        stake_rx,
    );
    let _ = sender
        .send(Message::Text(
            ws_subscribed(CHANNEL_PAPER_USER_STATE, user_address).into(),
        ))
        .await;
    true
}

pub async fn handle_unsubscribe_user_state(
    sender: &mut SplitSink<axum::extract::ws::WebSocket, Message>,
    session: &mut WsSession,
    user_address: Option<&str>,
    market: Option<&str>,
) {
    let key = match (user_address, market) {
        (Some(user), Some(market)) => Some(format!("{user}|{market}")),
        (Some(user), None) => Some(user.to_string()),
        _ => None,
    };
    session.cancel_channel(CHANNEL_PAPER_USER_STATE, key.as_deref());
    if let Some(user) = user_address {
        let _ = sender
            .send(Message::Text(
                ws_unsubscribed(CHANNEL_PAPER_USER_STATE, user).into(),
            ))
            .await;
    }
}
