// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use std::collections::HashMap;

use axum::extract::ws::Message;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;

use crate::indexer::SharedIndexState;
use crate::spot_market::normalize_spot_market_key;
use crate::ws::models::{
    OracleMarkWsMessage, OrderBookDelta, PaperActivityWsMessage, PaperHouseWsMessage,
    PaperPositionWsMessage, PaperStakeWsMessage, PaperUserWsMessage, QuoteDelta, UserWsMessage,
};
use crate::bars::BarWsMessage;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SubscriptionKey {
    Orderbook(String),
    Quote(String),
    User(String),
    PaperUser(String),
    PaperPositions,
    PaperActivity,
    PaperStake(String),
    PaperHouse(String),
    Bars(String),
    Oracle(String),
}

pub struct WsSession {
    outbound: mpsc::UnboundedSender<Message>,
    tasks: HashMap<SubscriptionKey, JoinHandle<()>>,
}

impl WsSession {
    pub fn new(outbound: mpsc::UnboundedSender<Message>) -> Self {
        Self {
            outbound,
            tasks: HashMap::new(),
        }
    }

    pub fn subscribe_orderbook(
        &mut self,
        spot_market: String,
        mut rx: broadcast::Receiver<OrderBookDelta>,
        index: SharedIndexState,
        depth: u32,
    ) {
        self.cancel(SubscriptionKey::Orderbook(spot_market.clone()));
        let outbound = self.outbound.clone();
        let normalized_spot_market = normalize_spot_market_key(&spot_market);
        let handle = tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(delta) => {
                        let text = serde_json::to_string(&delta).unwrap_or_default();
                        if outbound.send(Message::Text(text.into())).is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        tracing::warn!(
                            spot_market = normalized_spot_market,
                            skipped,
                            "orderbook delta receiver lagged; sending snapshot resync"
                        );
                        if let Some(snapshot) = index
                            .books
                            .ws_snapshot(&normalized_spot_market, depth)
                            .await
                        {
                            let text = serde_json::to_string(&snapshot).unwrap_or_default();
                            if outbound.send(Message::Text(text.into())).is_err() {
                                break;
                            }
                        }
                    }
                }
            }
        });
        self.tasks
            .insert(SubscriptionKey::Orderbook(spot_market), handle);
    }

    pub fn subscribe_quote(
        &mut self,
        spot_market: String,
        mut rx: broadcast::Receiver<QuoteDelta>,
    ) {
        self.cancel(SubscriptionKey::Quote(spot_market.clone()));
        let outbound = self.outbound.clone();
        let handle = tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(quote) => {
                        let text = serde_json::to_string(&quote).unwrap_or_default();
                        if outbound.send(Message::Text(text.into())).is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                    Err(broadcast::error::RecvError::Lagged(_)) => {}
                }
            }
        });
        self.tasks.insert(SubscriptionKey::Quote(spot_market), handle);
    }

    pub fn subscribe_user(
        &mut self,
        user_address: String,
        mut rx: broadcast::Receiver<UserWsMessage>,
    ) {
        self.cancel(SubscriptionKey::User(user_address.clone()));
        let outbound = self.outbound.clone();
        let filter = user_address.clone();
        let handle = tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(message) => {
                        let value = serde_json::to_value(&message).unwrap_or_default();
                        let msg_user = value
                            .get("user_address")
                            .and_then(|v| v.as_str())
                            .unwrap_or_default();
                        if !msg_user.eq_ignore_ascii_case(&filter) {
                            continue;
                        }
                        let text = value.to_string();
                        if outbound.send(Message::Text(text.into())).is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                    Err(broadcast::error::RecvError::Lagged(_)) => {}
                }
            }
        });
        self.tasks.insert(SubscriptionKey::User(user_address), handle);
    }

    pub fn subscribe_paper_user(
        &mut self,
        user_address: String,
        mut rx: broadcast::Receiver<PaperUserWsMessage>,
    ) {
        self.cancel(SubscriptionKey::PaperUser(user_address.clone()));
        let outbound = self.outbound.clone();
        let filter = user_address.clone();
        let handle = tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(message) => {
                        if !message.user_address.eq_ignore_ascii_case(&filter) {
                            continue;
                        }
                        let text = serde_json::to_string(&message).unwrap_or_default();
                        if outbound.send(Message::Text(text.into())).is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                    Err(broadcast::error::RecvError::Lagged(_)) => {}
                }
            }
        });
        self.tasks
            .insert(SubscriptionKey::PaperUser(user_address), handle);
    }

    pub fn subscribe_paper_positions(
        &mut self,
        mut rx: broadcast::Receiver<PaperPositionWsMessage>,
    ) {
        self.cancel(SubscriptionKey::PaperPositions);
        let outbound = self.outbound.clone();
        let handle = tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(message) => {
                        let text = serde_json::to_string(&message).unwrap_or_default();
                        if outbound.send(Message::Text(text.into())).is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                    Err(broadcast::error::RecvError::Lagged(_)) => {}
                }
            }
        });
        self.tasks.insert(SubscriptionKey::PaperPositions, handle);
    }

    pub fn subscribe_paper_activity(
        &mut self,
        mut rx: broadcast::Receiver<PaperActivityWsMessage>,
    ) {
        self.cancel(SubscriptionKey::PaperActivity);
        let outbound = self.outbound.clone();
        let handle = tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(message) => {
                        let text = serde_json::to_string(&message).unwrap_or_default();
                        if outbound.send(Message::Text(text.into())).is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                    Err(broadcast::error::RecvError::Lagged(_)) => {}
                }
            }
        });
        self.tasks.insert(SubscriptionKey::PaperActivity, handle);
    }

    pub fn subscribe_paper_stake(
        &mut self,
        state: crate::state::AppState,
        market: String,
        user_address: String,
        mut rx: broadcast::Receiver<PaperStakeWsMessage>,
    ) {
        self.cancel(SubscriptionKey::PaperStake(market.clone()));
        let outbound = self.outbound.clone();
        let market_key = market.clone();
        let handle = tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(mut message) => {
                        // Enrich market tick with per-user stake snapshot.
                        if let Some(stake) = state
                            .index
                            .paper_stake_view(&state.chain, &market, &user_address)
                            .await
                        {
                            message.stake = Some(stake);
                        }
                        let text = serde_json::to_string(&message).unwrap_or_default();
                        if outbound.send(Message::Text(text.into())).is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                    Err(broadcast::error::RecvError::Lagged(_)) => {}
                }
            }
        });
        self.tasks
            .insert(SubscriptionKey::PaperStake(market_key), handle);
    }

    pub fn subscribe_paper_house(
        &mut self,
        market: String,
        mut rx: broadcast::Receiver<PaperHouseWsMessage>,
    ) {
        self.cancel(SubscriptionKey::PaperHouse(market.clone()));
        let outbound = self.outbound.clone();
        let handle = tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(message) => {
                        let text = serde_json::to_string(&message).unwrap_or_default();
                        if outbound.send(Message::Text(text.into())).is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                    Err(broadcast::error::RecvError::Lagged(_)) => {}
                }
            }
        });
        self.tasks
            .insert(SubscriptionKey::PaperHouse(market), handle);
    }

    pub fn subscribe_bars(
        &mut self,
        spot_market: String,
        mut rx: broadcast::Receiver<BarWsMessage>,
    ) {
        self.cancel(SubscriptionKey::Bars(spot_market.clone()));
        let outbound = self.outbound.clone();
        let handle = tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(bar) => {
                        let text = serde_json::to_string(&bar).unwrap_or_default();
                        if outbound.send(Message::Text(text.into())).is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                    Err(broadcast::error::RecvError::Lagged(_)) => {}
                }
            }
        });
        self.tasks.insert(SubscriptionKey::Bars(spot_market), handle);
    }

    pub fn subscribe_oracle(
        &mut self,
        spot_market: String,
        mut rx: broadcast::Receiver<OracleMarkWsMessage>,
    ) {
        self.cancel(SubscriptionKey::Oracle(spot_market.clone()));
        let outbound = self.outbound.clone();
        let handle = tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(mark) => {
                        let text = serde_json::to_string(&mark).unwrap_or_default();
                        if outbound.send(Message::Text(text.into())).is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                    Err(broadcast::error::RecvError::Lagged(_)) => {}
                }
            }
        });
        self.tasks.insert(SubscriptionKey::Oracle(spot_market), handle);
    }

    pub fn cancel(&mut self, key: SubscriptionKey) {
        if let Some(handle) = self.tasks.remove(&key) {
            handle.abort();
        }
    }

    pub fn cancel_channel(&mut self, channel: &str, key: Option<&str>) {
        let keys: Vec<SubscriptionKey> = self
            .tasks
            .keys()
            .filter(|subscription| match (channel, key, subscription) {
                ("orderbook_delta", Some(spot_market), SubscriptionKey::Orderbook(current))
                    if current.eq_ignore_ascii_case(spot_market) =>
                {
                    true
                }
                ("quote", Some(spot_market), SubscriptionKey::Quote(current))
                    if current.eq_ignore_ascii_case(spot_market) =>
                {
                    true
                }
                ("user", Some(user_address), SubscriptionKey::User(current))
                    if current.eq_ignore_ascii_case(user_address) =>
                {
                    true
                }
                ("paper.user", Some(user_address), SubscriptionKey::PaperUser(current))
                    if current.eq_ignore_ascii_case(user_address) =>
                {
                    true
                }
                ("paper.positions", _, SubscriptionKey::PaperPositions) => true,
                ("paper.activity", _, SubscriptionKey::PaperActivity) => true,
                ("paper.stake", Some(market), SubscriptionKey::PaperStake(current))
                    if current.eq_ignore_ascii_case(market) =>
                {
                    true
                }
                ("paper.house", Some(market), SubscriptionKey::PaperHouse(current))
                    if current.eq_ignore_ascii_case(market) =>
                {
                    true
                }
                ("bars", Some(spot_market), SubscriptionKey::Bars(current))
                    if current.eq_ignore_ascii_case(spot_market) =>
                {
                    true
                }
                ("oracle", Some(spot_market), SubscriptionKey::Oracle(current))
                    if current.eq_ignore_ascii_case(spot_market) =>
                {
                    true
                }
                (channel, None, subscription) => match channel {
                    "orderbook_delta" => matches!(subscription, SubscriptionKey::Orderbook(_)),
                    "quote" => matches!(subscription, SubscriptionKey::Quote(_)),
                    "user" => matches!(subscription, SubscriptionKey::User(_)),
                    "paper.user" => matches!(subscription, SubscriptionKey::PaperUser(_)),
                    "paper.positions" => matches!(subscription, SubscriptionKey::PaperPositions),
                    "paper.activity" => matches!(subscription, SubscriptionKey::PaperActivity),
                    "paper.stake" => matches!(subscription, SubscriptionKey::PaperStake(_)),
                    "paper.house" => matches!(subscription, SubscriptionKey::PaperHouse(_)),
                    "bars" => matches!(subscription, SubscriptionKey::Bars(_)),
                    "oracle" => matches!(subscription, SubscriptionKey::Oracle(_)),
                    _ => false,
                },
                _ => false,
            })
            .cloned()
            .collect();

        for subscription_key in keys {
            self.cancel(subscription_key);
        }
    }
}
