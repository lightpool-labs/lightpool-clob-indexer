// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use std::sync::Arc;

use tokio::sync::{broadcast, RwLock};
use uuid::Uuid;

use crate::domain::Order;
use crate::ws::models::{UserOrderMessage, UserTradeMessage, UserWsMessage};

#[derive(Default)]
struct UserHubInner {
    sender: Option<broadcast::Sender<UserWsMessage>>,
}

pub struct UserEventHub {
    inner: RwLock<UserHubInner>,
}

pub type SharedUserEventHub = Arc<UserEventHub>;

impl UserEventHub {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(UserHubInner::default()),
        }
    }

    async fn sender(&self) -> broadcast::Sender<UserWsMessage> {
        let mut inner = self.inner.write().await;
        if let Some(sender) = inner.sender.clone() {
            return sender;
        }
        let (sender, _) = broadcast::channel(512);
        inner.sender = Some(sender.clone());
        sender
    }

    pub async fn subscribe(&self) -> broadcast::Receiver<UserWsMessage> {
        self.sender().await.subscribe()
    }

    pub async fn publish(&self, message: UserWsMessage) {
        let sender = self.sender().await;
        let _ = sender.send(message);
    }

    pub async fn publish_order(
        &self,
        event: &str,
        user_address: &str,
        chain_order_id: &str,
        order: Order,
        block_num: u64,
    ) {
        self.publish(UserWsMessage::Order(UserOrderMessage {
            msg_type: "order".into(),
            event: event.to_string(),
            user_address: user_address.to_string(),
            chain_order_id: chain_order_id.to_string(),
            block_num,
            order,
        }))
        .await;
    }

    pub async fn publish_trade(
        &self,
        user_address: &str,
        chain_order_id: &str,
        order_id: Uuid,
        market_slug: &str,
        outcome: &str,
        side: &str,
        price: &str,
        fill_amount: &str,
        remaining_amount: &str,
        is_fully_filled: bool,
        spot_market: &str,
        block_num: u64,
    ) {
        self.publish(UserWsMessage::Trade(UserTradeMessage {
            msg_type: "trade".into(),
            user_address: user_address.to_string(),
            chain_order_id: chain_order_id.to_string(),
            order_id,
            market_slug: market_slug.to_string(),
            outcome: outcome.to_string(),
            side: side.to_string(),
            price: price.to_string(),
            fill_amount: fill_amount.to_string(),
            remaining_amount: remaining_amount.to_string(),
            is_fully_filled,
            spot_market: spot_market.to_string(),
            block_num,
        }))
        .await;
    }
}
