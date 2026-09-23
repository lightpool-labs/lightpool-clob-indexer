// Copyright (c) LightPool Labs

use axum::extract::ws::Message;
use futures_util::stream::SplitSink;
use futures_util::SinkExt;

use crate::bars::INTERVAL_1M;
use crate::state::AppState;
use crate::ws::models::{ws_error, ws_subscribed, ws_unsubscribed, CHANNEL_BARS};
use crate::ws::process::WsSession;

pub async fn handle_subscribe(
    state: &AppState,
    sender: &mut SplitSink<axum::extract::ws::WebSocket, Message>,
    session: &mut WsSession,
    spot_market: &str,
    interval: Option<&str>,
) -> bool {
    let interval = interval.unwrap_or(INTERVAL_1M);
    if interval != INTERVAL_1M {
        let _ = sender
            .send(Message::Text(
                ws_error("subscribe bars currently supports interval=1m only; use HTTP history for 5m/1h").into(),
            ))
            .await;
        return true;
    }

    let snapshot = state.index.bars.snapshot_ws(spot_market, 200).await;
    let text = serde_json::to_string(&snapshot).unwrap_or_default();
    if sender.send(Message::Text(text.into())).await.is_err() {
        return false;
    }

    let rx = state.index.bars.subscribe(spot_market).await;
    session.subscribe_bars(spot_market.to_string(), rx);

    let _ = sender
        .send(Message::Text(ws_subscribed(CHANNEL_BARS, spot_market).into()))
        .await;
    true
}

pub async fn handle_unsubscribe(
    sender: &mut SplitSink<axum::extract::ws::WebSocket, Message>,
    session: &mut WsSession,
    spot_market: Option<&str>,
) {
    session.cancel_channel(CHANNEL_BARS, spot_market);
    if let Some(key) = spot_market {
        let _ = sender
            .send(Message::Text(ws_unsubscribed(CHANNEL_BARS, key).into()))
            .await;
    }
}
