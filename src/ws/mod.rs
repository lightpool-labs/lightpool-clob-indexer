// Copyright (c) LightPool Labs
// Author: xiaoyu1998

pub mod models;
pub mod process;

use axum::{
    extract::{
        ws::{Message, WebSocket},
        State, WebSocketUpgrade,
    },
    response::IntoResponse,
    routing::get,
    Router,
};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;

use crate::book_hydrate::DEFAULT_BOOK_DEPTH;
use crate::state::AppState;

use models::{
    ws_error, WsRequest, CHANNEL_BARS, CHANNEL_ORDERBOOK_DELTA, CHANNEL_QUOTE, CHANNEL_USER,
};
use process::WsSession;

pub fn router() -> Router<AppState> {
    Router::new().route("/", get(ws_handler))
}

async fn ws_handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: AppState) {
    let (mut sender, mut receiver) = socket.split();
    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<Message>();
    let mut session = WsSession::new(out_tx.clone());

    loop {
        tokio::select! {
            incoming = receiver.next() => {
                let Some(result) = incoming else {
                    break;
                };

                match result {
                    Ok(Message::Text(text)) => {
                        let request = match serde_json::from_str::<WsRequest>(&text) {
                            Ok(value) => value,
                            Err(error) => {
                                if out_tx
                                    .send(Message::Text(
                                        ws_error(format!("invalid message: {error}")).into(),
                                    ))
                                    .is_err()
                                {
                                    break;
                                }
                                continue;
                            }
                        };

                        let keep_alive =
                            handle_request(&state, &mut sender, &mut session, request).await;
                        if !keep_alive {
                            break;
                        }
                    }
                    Ok(Message::Ping(payload)) => {
                        if sender.send(Message::Pong(payload)).await.is_err() {
                            break;
                        }
                    }
                    Ok(Message::Pong(_)) => {}
                    Ok(Message::Close(_)) => break,
                    Ok(Message::Binary(_)) => {}
                    Err(_) => break,
                }
            }
            outbound = out_rx.recv() => {
                let Some(message) = outbound else {
                    break;
                };
                if sender.send(message).await.is_err() {
                    break;
                }
            }
        }
    }
}

async fn handle_request(
    state: &AppState,
    sender: &mut futures_util::stream::SplitSink<WebSocket, Message>,
    session: &mut WsSession,
    request: WsRequest,
) -> bool {
    match request.op.as_str() {
        "subscribe" => {
            let Some(channel) = request.channel.as_deref() else {
                let _ = sender
                    .send(Message::Text(ws_error("missing channel").into()))
                    .await;
                return true;
            };

            match channel {
                CHANNEL_ORDERBOOK_DELTA => {
                    let Some(spot_market) =
                        request.spot_market.filter(|value| !value.trim().is_empty())
                    else {
                        let _ = sender
                            .send(Message::Text(ws_error("missing spot_market").into()))
                            .await;
                        return true;
                    };
                    let depth = request.depth.unwrap_or(10).clamp(1, DEFAULT_BOOK_DEPTH);
                    process::subscribe_orderbook(state, sender, session, &spot_market, depth).await
                }
                CHANNEL_QUOTE => {
                    let Some(spot_market) =
                        request.spot_market.filter(|value| !value.trim().is_empty())
                    else {
                        let _ = sender
                            .send(Message::Text(ws_error("missing spot_market").into()))
                            .await;
                        return true;
                    };
                    process::subscribe_quote(state, sender, session, &spot_market).await
                }
                CHANNEL_USER => {
                    let Some(user_address) =
                        request.user_address.filter(|value| !value.trim().is_empty())
                    else {
                        let _ = sender
                            .send(Message::Text(ws_error("missing user_address").into()))
                            .await;
                        return true;
                    };
                    process::subscribe_user(state, sender, session, &user_address).await
                }
                CHANNEL_BARS => {
                    let Some(spot_market) =
                        request.spot_market.filter(|value| !value.trim().is_empty())
                    else {
                        let _ = sender
                            .send(Message::Text(ws_error("missing spot_market").into()))
                            .await;
                        return true;
                    };
                    process::subscribe_bars(
                        state,
                        sender,
                        session,
                        &spot_market,
                        request.interval.as_deref(),
                    )
                    .await
                }
                _ => {
                    let _ = sender
                        .send(Message::Text(
                            ws_error(format!("unknown channel: {channel}")).into(),
                        ))
                        .await;
                    true
                }
            }
        }
        "unsubscribe" => {
            let Some(channel) = request.channel.as_deref() else {
                return true;
            };
            match channel {
                CHANNEL_ORDERBOOK_DELTA => {
                    let spot_market = request
                        .spot_market
                        .as_deref()
                        .filter(|value| !value.trim().is_empty());
                    process::unsubscribe_orderbook(sender, session, spot_market).await;
                }
                CHANNEL_QUOTE => {
                    let spot_market = request
                        .spot_market
                        .as_deref()
                        .filter(|value| !value.trim().is_empty());
                    process::unsubscribe_quote(sender, session, spot_market).await;
                }
                CHANNEL_USER => {
                    let user_address = request
                        .user_address
                        .as_deref()
                        .filter(|value| !value.trim().is_empty());
                    process::unsubscribe_user(sender, session, user_address).await;
                }
                CHANNEL_BARS => {
                    let spot_market = request
                        .spot_market
                        .as_deref()
                        .filter(|value| !value.trim().is_empty());
                    process::unsubscribe_bars(sender, session, spot_market).await;
                }
                _ => {}
            }
            true
        }
        _ => {
            let _ = sender
                .send(Message::Text(ws_error(format!("unknown op: {}", request.op)).into()))
                .await;
            true
        }
    }
}
