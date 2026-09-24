// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use axum::{
    extract::{Path, Query, State},
    routing::{get, post},
    Json, Router,
};
use lightpool_sdk::lightpool_types::{Address, ContractAddress};
use lightpool_sdk::parse_token_contract;
use lightpool_sdk::spot_events::{OrderCreatedEvent, OrderEventType};
use lightpool_sdk::{OrderId, OrderSide};
use serde::Deserialize;
use std::str::FromStr;
use uuid::Uuid;

use crate::domain::Order;
use crate::error::{AppError, AppResult};
use crate::http::models::{CancelContextResponse, ListedOrder, OrderQueryResponse};
use crate::indexer::{apply_order_created_to_book, index_order_created, publish_user_order_created};
use crate::spot_market::normalize_spot_market_key;
use crate::state::AppState;

#[derive(Debug, Deserialize)]
struct IndexFromEventRequest {
    event: WireOrderCreatedEvent,
    #[serde(default)]
    skip_book: bool,
    #[serde(default = "default_open_status")]
    status: String,
    #[serde(default)]
    filled_raw: u64,
}

#[derive(Debug, Deserialize)]
struct WireOrderCreatedEvent {
    order_id: OrderId,
    side: OrderSide,
    amount: u64,
    creator: FlexibleAddress,
    market: FlexibleAddress,
    order_type: OrderEventType,
    #[serde(default)]
    cloid: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum FlexibleAddress {
    Hex(String),
    Bytes(Vec<u8>),
}

fn default_open_status() -> String {
    "open".into()
}

fn parse_flexible_address(value: FlexibleAddress) -> AppResult<Address> {
    match value {
        FlexibleAddress::Hex(raw) => {
            let trimmed = raw.trim();
            if let Ok(address) = Address::from_str(trimmed) {
                return Ok(address);
            }
            if let Ok(contract) = parse_token_contract(trimmed) {
                return Ok(contract.to_address());
            }
            Err(AppError::BadRequest(format!(
                "invalid address `{trimmed}`"
            )))
        }
        FlexibleAddress::Bytes(bytes) => {
            if bytes.len() == Address::ADDRESS_LENGTH {
                Address::from_slice(&bytes)
                    .map_err(|e| AppError::BadRequest(format!("invalid address bytes: {e}")))
            } else if bytes.len() == ContractAddress::CONTRACT_ADDRESS_LENGTH {
                let mut arr = [0u8; ContractAddress::CONTRACT_ADDRESS_LENGTH];
                arr.copy_from_slice(&bytes);
                Ok(ContractAddress::from_bytes(arr).to_address())
            } else {
                Err(AppError::BadRequest(format!(
                    "invalid address byte length {}",
                    bytes.len()
                )))
            }
        }
    }
}

impl WireOrderCreatedEvent {
    fn into_event(self) -> AppResult<OrderCreatedEvent> {
        Ok(OrderCreatedEvent {
            order_id: self.order_id,
            side: self.side,
            amount: self.amount,
            creator: parse_flexible_address(self.creator)?,
            market: parse_flexible_address(self.market)?,
            order_type: self.order_type,
            cloid: self.cloid,
        })
    }
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", get(list_orders))
        .route("/openOrders", get(list_orders))
        .route("/historicalOrders", get(list_historical_orders))
        .route("/query", get(query_order))
        .route("/:id/cancel-context", get(cancel_context))
        .route("/:id/cancelled", post(mark_cancelled))
        .route("/index/from-event", post(index_from_event))
}

#[derive(Debug, Deserialize)]
pub struct ListOrdersQuery {
    pub user_address: String,
}

#[derive(Debug, Deserialize)]
pub struct CancelContextQuery {
    pub user_address: String,
}

#[derive(Debug, Deserialize)]
pub struct QueryOrderQuery {
    pub spot_market: String,
    pub chain_order_id: Option<String>,
    pub user_address: Option<String>,
    pub side: Option<String>,
    pub price: Option<String>,
    pub size_raw: Option<u64>,
}

async fn list_orders(
    State(state): State<AppState>,
    Query(query): Query<ListOrdersQuery>,
) -> Json<Vec<ListedOrder>> {
    let mut records = state
        .index
        .list_orders_for_user(&query.user_address)
        .await;

    for record in &mut records {
        enrich_order_record(&state, record).await;
    }

    Json(records.into_iter().map(listed_from_record).collect())
}

async fn list_historical_orders(
    State(state): State<AppState>,
    Query(query): Query<ListOrdersQuery>,
) -> AppResult<Json<Vec<ListedOrder>>> {
    let Some(persist) = state.persist.as_ref() else {
        return Ok(Json(Vec::new()));
    };
    let rows = persist.list_order_history(
        &query.user_address,
        crate::persist::ORDER_HISTORY_LIMIT,
    )?;
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let mut record = crate::indexer::OrderQueryRecord {
            order: row.order,
            chain_order_id: row.chain_order_id,
            spot_market: row.spot_market,
            user_address: row.user_address,
            size_raw: row.size_raw,
            filled_raw: row.filled_raw,
        };
        enrich_order_record(&state, &mut record).await;
        out.push(listed_from_record(record));
    }
    Ok(Json(out))
}

async fn enrich_order_record(state: &AppState, record: &mut crate::indexer::OrderQueryRecord) {
    if record.order.question.is_empty() || record.order.market_slug.is_empty() {
        if let Some(market) = state.index.get_market(record.order.market_id).await {
            if record.order.question.is_empty() {
                record.order.question = market.label().to_string();
            }
            if record.order.market_slug.is_empty() {
                record.order.market_slug = if !market.slug().is_empty() {
                    market.slug().to_string()
                } else {
                    market.name().to_string()
                };
            }
        }
    }
}

fn listed_from_record(record: crate::indexer::OrderQueryRecord) -> ListedOrder {
    ListedOrder {
        order: record.order,
        chain_order_id: record.chain_order_id,
        spot_market: record.spot_market,
        user_address: record.user_address,
        size_raw: record.size_raw,
        filled_raw: record.filled_raw,
    }
}

async fn query_order(
    State(state): State<AppState>,
    Query(query): Query<QueryOrderQuery>,
) -> AppResult<Json<OrderQueryResponse>> {
    let spot_market = normalize_spot_market_key(&query.spot_market);

    let record = if let Some(chain_order_id) = query.chain_order_id.as_deref() {
        state
            .index
            .query_order(
                &spot_market,
                chain_order_id,
                query.user_address.as_deref(),
            )
            .await
    } else {
        let user_address = query
            .user_address
            .as_deref()
            .ok_or_else(|| AppError::BadRequest("user_address is required".into()))?;
        let side = query
            .side
            .as_deref()
            .ok_or_else(|| AppError::BadRequest("side is required".into()))?;
        let price = query
            .price
            .as_deref()
            .ok_or_else(|| AppError::BadRequest("price is required".into()))?;
        let size_raw = query
            .size_raw
            .ok_or_else(|| AppError::BadRequest("size_raw is required".into()))?;
        state
            .index
            .find_open_order_match(&spot_market, user_address, side, price, size_raw)
            .await
    };

    let Some(record) = record else {
        return Err(AppError::NotFound(format!(
            "order not found for spot market {spot_market}"
        )));
    };

    Ok(Json(OrderQueryResponse {
        order: record.order,
        chain_order_id: record.chain_order_id,
        spot_market: record.spot_market,
        user_address: record.user_address,
        size_raw: record.size_raw,
        filled_raw: record.filled_raw,
    }))
}

async fn cancel_context(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Query(query): Query<CancelContextQuery>,
) -> AppResult<Json<CancelContextResponse>> {
    let (order, chain_order_id, spot_market) = state
        .index
        .order_cancel_context(id, &query.user_address)
        .await
        .ok_or_else(|| AppError::NotFound(format!("open order {id} not found")))?;

    Ok(Json(CancelContextResponse {
        order,
        chain_order_id,
        spot_market,
    }))
}

async fn mark_cancelled(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Query(query): Query<CancelContextQuery>,
) -> AppResult<Json<serde_json::Value>> {
    let (order, chain_order_id, spot_market) = state
        .index
        .stored_order_context_by_id(id, &query.user_address)
        .await
        .ok_or_else(|| AppError::NotFound(format!("order {id} not found")))?;

    if order.status == "cancelled" {
        return Ok(Json(serde_json::json!({ "ok": true })));
    }

    if order.status != "open" && order.status != "partial_filled" {
        return Err(AppError::BadRequest(format!(
            "order {id} is not cancellable"
        )));
    }

    state
        .index
        .update_order_cancelled(&spot_market, &chain_order_id, None)
        .await;
    Ok(Json(serde_json::json!({ "ok": true })))
}

async fn index_from_event(
    State(state): State<AppState>,
    Json(body): Json<serde_json::Value>,
) -> AppResult<Json<Order>> {
    let request: IndexFromEventRequest = if body.get("event").is_some() {
        serde_json::from_value(body)
            .map_err(|error| AppError::BadRequest(format!("invalid index request: {error}")))?
    } else {
        IndexFromEventRequest {
            event: serde_json::from_value(body)
                .map_err(|error| AppError::BadRequest(format!("invalid order event: {error}")))?,
            skip_book: false,
            status: default_open_status(),
            filled_raw: 0,
        }
    };

    let event = request.event.into_event()?;
    let chain_order_id = event.order_id.to_string();
    let spot_market = normalize_spot_market_key(&event.market.to_string());
    let is_new = !state
        .index
        .has_chain_order(&spot_market, &chain_order_id)
        .await;
    if is_new && !request.skip_book {
        apply_order_created_to_book(
            &state.index,
            0,
            &event,
            &spot_market,
            true,
            None,
        )
        .await;
    }

    let order = index_order_created(
        &state.index,
        event,
        &spot_market,
        Some((request.status, request.filled_raw)),
        None,
    )
    .await
    .ok_or_else(|| AppError::Internal("failed to index order from event".into()))?;
    if is_new {
        publish_user_order_created(
            &state.user_hub,
            &state.index,
            &spot_market,
            &chain_order_id,
            0,
        )
        .await;
    }
    Ok(Json(order))
}
