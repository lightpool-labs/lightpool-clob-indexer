// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use std::collections::HashMap;

use futures_util::future::join_all;
use lightpool_sdk::event_contract_events::{
    EventContractBurnedEvent, EventContractCreatedEvent, EventContractMintedEvent,
    EventContractRedeemedEvent, EventContractResolvedEvent,
};
use lightpool_sdk::spot_events::{
    MarketCreatedEvent, MarketOrderExecutedEvent, OrderCancelledEvent, OrderCreatedEvent,
    OrderEventType, OrderFilledEvent, OrderUpdatedEvent, parse_spot_event_data,
};
use lightpool_sdk::token_events::{
    TokenCreatedEvent, TokenMintedEvent, TransferEvent, parse_event_data,
};
use lightpool_sdk::vault_events::{
    VaultClosedEvent, VaultCreatedEvent, VaultDepositedEvent,
    VaultDepositPermissionUpdatedEvent, VaultManagerUpdatedEvent, VaultWithdrawnEvent,
};
use lightpool_sdk::{vault_account, EventData, EventType, ExecutionStatus, TransactionEvent, ReceiptBlock};
use lightpool_sdk::lightpool_types::TransactionResult;
use uuid::Uuid;

use crate::book_hydrate::{
    ensure_chain_hydrated, hydrate_all_spot_markets, hydrate_market_spots, SharedChainClient,
    DEFAULT_BOOK_DEPTH,
};
use crate::chain::{format_price_pieces, format_token_amount};
use crate::domain::{Market, Order, Vault};
use crate::persist::IndexWriteSet;
use crate::submit_wait::SharedSubmitWaitRegistry;
use crate::ws::process::SharedUserEventHub;

use super::index_state::{market_uuid, vault_uuid, SharedIndexState};

fn spot_market_from_event_contract(event: &TransactionEvent) -> Option<String> {
    event
        .contract
        .as_ref()
        .map(|contract| crate::spot_market::normalize_spot_market_key(&contract.to_string()))
}

async fn resolve_spot_market_for_order_event(
    store: &SharedIndexState,
    event: &TransactionEvent,
    chain_order_id: &str,
) -> Option<String> {
    if let Some(spot_market) = spot_market_from_event_contract(event) {
        return Some(spot_market);
    }
    store.lookup_spot_market_for_chain_order(chain_order_id).await
}

/// Options for block apply throughput vs live fan-out.
#[derive(Debug, Clone, Copy)]
pub struct ProcessOpts {
    /// When false, update book/index but skip book/user/bars websocket fan-out.
    pub publish_ws: bool,
}

impl Default for ProcessOpts {
    fn default() -> Self {
        Self { publish_ws: true }
    }
}

impl ProcessOpts {
    pub fn quiet() -> Self {
        Self { publish_ws: false }
    }
}

/// Per-market order work decoded in the pipeline decode stage.
#[derive(Debug)]
pub enum MarketOrderEvent {
    Created(OrderCreatedEvent),
    Cancelled(OrderCancelledEvent),
    Updated(OrderUpdatedEvent),
    Filled {
        filled: OrderFilledEvent,
        tx_sender: String,
    },
    /// Taker market order summary (chain never emits `order_created` for market orders).
    Executed(MarketOrderExecutedEvent),
}

#[derive(Debug)]
pub enum UnmappedOrderEvent {
    Cancelled { chain_order_id: String },
    Updated { chain_order_id: String },
}

/// Block ready for apply: globals still on `block`; order events grouped by market.
#[derive(Debug)]
pub struct PreparedBlock {
    pub block_num: u64,
    pub block_digest: String,
    pub tx_count: usize,
    pub ok_count: usize,
    pub block: ReceiptBlock,
    pub markets: HashMap<String, Vec<MarketOrderEvent>>,
    pub unmapped: Vec<UnmappedOrderEvent>,
}

/// Decode/classify stage: parse order payloads and group by `spot_market`.
pub async fn prepare_block(store: &SharedIndexState, block: ReceiptBlock) -> PreparedBlock {
    let block_num = block.block_num;
    let block_digest = hex::encode(block.digest.as_bytes());
    let tx_count = block.transaction_outputs.len();
    let ok_count = block
        .transaction_outputs
        .iter()
        .filter(|tx| tx.is_success())
        .count();

    let mut markets: HashMap<String, Vec<MarketOrderEvent>> = HashMap::new();
    let mut unmapped = Vec::new();

    for tx_result in &block.transaction_outputs {
        if !tx_result.is_success() {
            continue;
        }
        let tx_sender = tx_result.sender().to_string();
        for event in &tx_result.receipt.events {
            let EventType::Call(action_name) = &event.event_type else {
                continue;
            };
            let EventData::Bytes(data) = &event.data else {
                continue;
            };
            match action_name.as_str() {
                "order_created" => {
                    if let Ok(created) = bincode::deserialize::<OrderCreatedEvent>(data) {
                        let spot_market = crate::spot_market::normalize_spot_market_key(
                            &created.market.to_string(),
                        );
                        markets
                            .entry(spot_market)
                            .or_default()
                            .push(MarketOrderEvent::Created(created));
                    }
                }
                "order_cancelled" => {
                    if let Ok(cancelled) = bincode::deserialize::<OrderCancelledEvent>(data) {
                        let chain_order_id = cancelled.order_id.to_string();
                        match resolve_spot_market_for_order_event(store, event, &chain_order_id)
                            .await
                        {
                            Some(spot_market) => {
                                markets
                                    .entry(spot_market)
                                    .or_default()
                                    .push(MarketOrderEvent::Cancelled(cancelled));
                            }
                            None => unmapped.push(UnmappedOrderEvent::Cancelled { chain_order_id }),
                        }
                    }
                }
                "order_updated" => {
                    if let Ok(updated) = bincode::deserialize::<OrderUpdatedEvent>(data) {
                        let chain_order_id = updated.order_id.to_string();
                        match resolve_spot_market_for_order_event(store, event, &chain_order_id)
                            .await
                        {
                            Some(spot_market) => {
                                markets
                                    .entry(spot_market)
                                    .or_default()
                                    .push(MarketOrderEvent::Updated(updated));
                            }
                            None => unmapped.push(UnmappedOrderEvent::Updated { chain_order_id }),
                        }
                    }
                }
                "order_filled" => {
                    if let Ok(filled) = bincode::deserialize::<OrderFilledEvent>(data) {
                        let spot_market = crate::spot_market::normalize_spot_market_key(
                            &filled.market.to_string(),
                        );
                        markets.entry(spot_market).or_default().push(
                            MarketOrderEvent::Filled {
                                filled,
                                tx_sender: tx_sender.clone(),
                            },
                        );
                    }
                }
                "market_order_executed" => {
                    if let Ok(executed) = bincode::deserialize::<MarketOrderExecutedEvent>(data) {
                        if let Some(spot_market) = spot_market_from_event_contract(event) {
                            markets
                                .entry(spot_market)
                                .or_default()
                                .push(MarketOrderEvent::Executed(executed));
                        }
                    }
                }
                _ => {}
            }
        }
    }

    tracing::debug!(
        block_num,
        markets = markets.len(),
        order_events = markets.values().map(|v| v.len()).sum::<usize>(),
        unmapped = unmapped.len(),
        "prepared block for parallel market apply"
    );

    PreparedBlock {
        block_num,
        block_digest,
        tx_count,
        ok_count,
        block,
        markets,
        unmapped,
    }
}

pub async fn process_block(
    chain: &SharedChainClient,
    query_account: &str,
    store: &SharedIndexState,
    user_hub: &SharedUserEventHub,
    submit_wait: &SharedSubmitWaitRegistry,
    block: ReceiptBlock,
    opts: ProcessOpts,
) {
    let prepared = prepare_block(store, block).await;
    let _ = process_prepared_block(
        chain,
        query_account,
        store,
        user_hub,
        submit_wait,
        prepared,
        opts,
    )
    .await;
}

pub async fn process_prepared_block(
    chain: &SharedChainClient,
    query_account: &str,
    store: &SharedIndexState,
    user_hub: &SharedUserEventHub,
    submit_wait: &SharedSubmitWaitRegistry,
    prepared: PreparedBlock,
    opts: ProcessOpts,
) -> IndexWriteSet {
    let block_num = prepared.block_num;
    let digest = prepared.block_digest.clone();
    let mut ws = IndexWriteSet::default();
    let tx_count = prepared.tx_count;
    let ok_count = prepared.ok_count;
    let markets = prepared.markets;
    let unmapped = prepared.unmapped;
    let block = prepared.block;

    store.set_indexing_block_timestamp(block.timestamp);

    for tx_result in &block.transaction_outputs {
        let digest = hex::encode(tx_result.signed_digest.as_bytes());
        if !submit_wait.complete(&digest, block_num, &tx_result.receipt) {
            tracing::debug!(
                digest,
                "no pending submit waiter for transaction digest"
            );
        }
    }

    // Global / non-order events stay serial (shared index metadata).
    for tx_result in &block.transaction_outputs {
        log_tx_result(tx_result);
        if !tx_result.is_success() {
            continue;
        }
        for event in &tx_result.receipt.events {
            let EventType::Call(action_name) = &event.event_type else {
                continue;
            };
            if matches!(
                action_name.as_str(),
                "order_created"
                    | "order_cancelled"
                    | "order_updated"
                    | "order_filled"
                    | "market_order_executed"
            ) {
                continue;
            }
            if tracing::enabled!(tracing::Level::DEBUG) {
                tracing::debug!(
                    action = action_name.as_str(),
                    detail = %format_event_detail(event),
                    "processing tx event"
                );
            }
            apply_global_event(
                chain,
                query_account,
                store,
                action_name.as_str(),
                event,
                &mut ws,
            )
            .await;
        }
    }

    for item in &unmapped {
        match item {
            UnmappedOrderEvent::Cancelled { chain_order_id } => {
                tracing::warn!(
                    order_id = %chain_order_id,
                    "order_cancelled without indexed spot market; skipping hydrate_all in quiet apply"
                );
                if opts.publish_ws {
                    if let Err(error) =
                        hydrate_all_spot_markets(chain, store, query_account, Some(&mut ws)).await
                    {
                        tracing::warn!(
                            error = %error,
                            "failed to rehydrate spot books after order_cancelled mapping miss"
                        );
                    }
                }
            }
            UnmappedOrderEvent::Updated { chain_order_id } => {
                tracing::warn!(
                    order_id = %chain_order_id,
                    "order_updated without indexed spot market"
                );
                if opts.publish_ws {
                    if let Err(error) =
                        hydrate_all_spot_markets(chain, store, query_account, Some(&mut ws)).await
                    {
                        tracing::warn!(
                            error = %error,
                            "failed to rehydrate spot books after order_updated mapping miss"
                        );
                    }
                }
            }
        }
    }

    let market_futs: Vec<_> = markets
        .into_iter()
        .map(|(spot_market, events)| {
            let chain = chain.clone();
            let query_account = query_account.to_string();
            let store = store.clone();
            let user_hub = user_hub.clone();
            async move {
                apply_market_order_events(
                    &chain,
                    &query_account,
                    &store,
                    &user_hub,
                    block_num,
                    &spot_market,
                    events,
                    opts,
                )
                .await
            }
        })
        .collect();

    for set in join_all(market_futs).await {
        ws.merge(set);
    }

    if tx_count > 0 {
        tracing::info!(
            block_num,
            tx_count,
            ok_count,
            publish_ws = opts.publish_ws,
            "indexed block"
        );
    }

    ws.block_num = block_num;
    ws.digest = digest;
    ws
}

async fn apply_global_event(
    chain: &SharedChainClient,
    query_account: &str,
    store: &SharedIndexState,
    action_name: &str,
    event: &TransactionEvent,
    ws: &mut IndexWriteSet,
) {
    let EventData::Bytes(data) = &event.data else {
        return;
    };
    match action_name {
        "event_contract_created" => {
            match bincode::deserialize::<EventContractCreatedEvent>(data) {
                Ok(created) => {
                    index_market_created(chain, query_account, store, created, ws).await;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "failed to decode event_contract_created");
                }
            }
        }
        "event_contract_resolved" => {
            if let Ok(resolved) = bincode::deserialize::<EventContractResolvedEvent>(data) {
                store
                    .update_market_state(
                        &resolved.market_address.to_string(),
                        "Resolved",
                        Some(ws),
                    )
                    .await;
            }
        }
        "vault_created" => match bincode::deserialize::<VaultCreatedEvent>(data) {
            Ok(created) => {
                index_vault_created(store, created, ws).await;
            }
            Err(e) => {
                tracing::warn!(error = %e, "failed to decode vault_created");
            }
        },
        "vault_deposited" => {
            if let Ok(deposited) = bincode::deserialize::<VaultDepositedEvent>(data) {
                store
                    .update_vault_equity(
                        &deposited.vault.to_string(),
                        &format_token_amount(deposited.equity),
                        Some(ws),
                    )
                    .await;
            }
        }
        "vault_withdrawn" => {
            if let Ok(withdrawn) = bincode::deserialize::<VaultWithdrawnEvent>(data) {
                store
                    .update_vault_equity(
                        &withdrawn.vault.to_string(),
                        &format_token_amount(withdrawn.equity),
                        Some(ws),
                    )
                    .await;
            }
        }
        "vault_manager_updated" => {
            if let Ok(updated) = bincode::deserialize::<VaultManagerUpdatedEvent>(data) {
                store
                    .update_vault_manager(
                        &updated.vault.to_string(),
                        &updated.new_manager.to_string(),
                        Some(ws),
                    )
                    .await;
            }
        }
        "vault_deposit_permission_updated" => {
            if let Ok(updated) = bincode::deserialize::<VaultDepositPermissionUpdatedEvent>(data)
            {
                store
                    .update_vault_allow_deposit(
                        &updated.vault.to_string(),
                        updated.allow_deposit,
                        Some(ws),
                    )
                    .await;
            }
        }
        "vault_closed" => {
            if let Ok(closed) = bincode::deserialize::<VaultClosedEvent>(data) {
                store
                    .mark_vault_closed(&closed.vault.to_string(), Some(ws))
                    .await;
            }
        }
        "market_created" => match bincode::deserialize::<MarketCreatedEvent>(data) {
            Ok(created) => {
                index_spot_market_created(store, chain, query_account, created, ws).await;
            }
            Err(e) => {
                tracing::warn!(error = %e, "failed to decode market_created");
            }
        },
        _ => {}
    }
}

async fn apply_market_order_events(
    chain: &SharedChainClient,
    query_account: &str,
    store: &SharedIndexState,
    user_hub: &SharedUserEventHub,
    block_num: u64,
    spot_market: &str,
    events: Vec<MarketOrderEvent>,
    opts: ProcessOpts,
) -> IndexWriteSet {
    let mut ws = IndexWriteSet::default();
    for event in events {
        match event {
            MarketOrderEvent::Created(created) => {
                let chain_order_id = created.order_id.to_string();
                if store.has_chain_order(spot_market, &chain_order_id).await {
                    tracing::warn!(
                        block_num,
                        order_id = chain_order_id,
                        spot_market,
                        "block_sync order_created skipped duplicate"
                    );
                    continue;
                }
                if opts.publish_ws {
                    maybe_warn_order_created_price(store, &created, spot_market).await;
                }
                if let Err(error) = ensure_chain_hydrated(
                    chain,
                    store,
                    query_account,
                    spot_market,
                    DEFAULT_BOOK_DEPTH,
                    Some(&mut ws),
                )
                .await
                {
                    tracing::warn!(
                        spot_market,
                        error = %error,
                        "failed to hydrate book before order_created"
                    );
                }
                apply_order_created_to_book(
                    store,
                    block_num,
                    &created,
                    spot_market,
                    opts.publish_ws,
                    Some(&mut ws),
                )
                .await;
                index_order_created(store, created, spot_market, None, Some(&mut ws)).await;
                if opts.publish_ws {
                    publish_user_order_created(
                        user_hub,
                        store,
                        spot_market,
                        &chain_order_id,
                        block_num,
                    )
                    .await;
                }
            }
            MarketOrderEvent::Cancelled(cancelled) => {
                let chain_order_id = cancelled.order_id.to_string();
                store.books
                    .apply_cancelled(
                        spot_market,
                        cancelled.side,
                        cancelled.price,
                        cancelled.cancelled_amount,
                        block_num,
                        opts.publish_ws,
                        Some(&mut ws),
                    )
                    .await;
                store
                    .update_order_cancelled(spot_market, &chain_order_id, Some(&mut ws))
                    .await;
                if opts.publish_ws {
                    publish_user_order_cancelled(
                        user_hub,
                        store,
                        spot_market,
                        &chain_order_id,
                        block_num,
                    )
                    .await;
                }
            }
            MarketOrderEvent::Updated(updated) => {
                let chain_order_id = updated.order_id.to_string();
                if let Err(error) = ensure_chain_hydrated(
                    chain,
                    store,
                    query_account,
                    spot_market,
                    DEFAULT_BOOK_DEPTH,
                    Some(&mut ws),
                )
                .await
                {
                    tracing::warn!(
                        spot_market,
                        error = %error,
                        "failed to hydrate book before order_updated"
                    );
                }
                store.books
                    .apply_updated(
                        spot_market,
                        updated.side,
                        updated.price,
                        updated.old_amount,
                        updated.new_amount,
                        updated.remaining_amount,
                        block_num,
                        opts.publish_ws,
                        Some(&mut ws),
                    )
                    .await;
                store
                    .update_order_amount(
                        spot_market,
                        &chain_order_id,
                        updated.new_amount,
                        updated.remaining_amount,
                        Some(&mut ws),
                    )
                    .await;
                if opts.publish_ws {
                    publish_user_order_updated(
                        user_hub,
                        store,
                        spot_market,
                        &chain_order_id,
                        block_num,
                    )
                    .await;
                }
            }
            MarketOrderEvent::Filled { filled, tx_sender } => {
                let chain_order_id = filled.order_id.to_string();
                store
                    .record_last_trade_price(spot_market, filled.price, Some(&mut ws))
                    .await;
                if opts.publish_ws {
                    if matches!(filled.side, lightpool_sdk::OrderSide::Buy) {
                        store
                            .bars
                            .on_trade(
                                spot_market,
                                filled.price,
                                filled.fill_amount,
                                crate::bars::Bars::now_ts(),
                            )
                            .await;
                    }
                }
                if let Err(error) = ensure_chain_hydrated(
                    chain,
                    store,
                    query_account,
                    spot_market,
                    DEFAULT_BOOK_DEPTH,
                    Some(&mut ws),
                )
                .await
                {
                    tracing::warn!(
                        spot_market,
                        error = %error,
                        "failed to hydrate book before order_filled"
                    );
                }
                store.books
                    .apply_filled(
                        spot_market,
                        filled.side,
                        filled.price,
                        filled.fill_amount,
                        block_num,
                        filled.price,
                        opts.publish_ws,
                        Some(&mut ws),
                        {
                            let ms = store.indexing_block_timestamp_ms();
                            if ms > 0 {
                                Some(ms)
                            } else {
                                None
                            }
                        },
                    )
                    .await;
                store
                    .update_order_fill(
                        spot_market,
                        &chain_order_id,
                        filled.fill_amount,
                        filled.remaining_amount,
                        filled.is_fully_filled,
                        Some(&mut ws),
                    )
                    .await;
                if let Some((_, user_address, _)) = store
                    .stored_order_by_chain_id(spot_market, &chain_order_id)
                    .await
                {
                    store
                        .apply_vault_fill_to_portfolio(
                            &user_address,
                            spot_market,
                            filled.side,
                            filled.fill_amount,
                            Some(&mut ws),
                        )
                        .await;
                }
                if opts.publish_ws {
                    publish_user_order_filled(
                        user_hub,
                        store,
                        &chain_order_id,
                        spot_market,
                        filled.price,
                        filled.fill_amount,
                        filled.remaining_amount,
                        filled.is_fully_filled,
                        filled.side,
                        block_num,
                        filled.cloid.clone(),
                        tx_sender,
                    )
                    .await;
                }
            }
            MarketOrderEvent::Executed(executed) => {
                let chain_order_id = executed.order_id.to_string();
                // Market orders never emit `order_created`; this event archives the taker.
                if store.has_chain_order(spot_market, &chain_order_id).await {
                    tracing::debug!(
                        block_num,
                        order_id = %chain_order_id,
                        spot_market,
                        "market_order_executed skipped; order already hot-indexed"
                    );
                    continue;
                }
                let filled_amount = executed.filled_amount;
                let remaining = executed.amount.saturating_sub(filled_amount);
                let is_fully_filled = filled_amount > 0 && remaining == 0;
                let Some(order) = index_market_order_executed(
                    store,
                    executed.clone(),
                    spot_market,
                    Some(&mut ws),
                )
                .await
                else {
                    continue;
                };
                if opts.publish_ws {
                    let user_address = executed.creator.to_string();
                    user_hub
                        .publish_order(
                            "update",
                            &user_address,
                            &chain_order_id,
                            spot_market,
                            order.clone(),
                            block_num,
                        )
                        .await;
                    if filled_amount > 0 {
                        let side_str = match executed.side {
                            lightpool_sdk::OrderSide::Buy => "buy",
                            lightpool_sdk::OrderSide::Sell => "sell",
                        };
                        let price_raw = executed.avg_filled_price.unwrap_or(0);
                        user_hub
                            .publish_trade(
                                &user_address,
                                &chain_order_id,
                                order.id,
                                &order.market_slug,
                                &order.outcome,
                                side_str,
                                &format_price_pieces(price_raw),
                                &format_token_amount(filled_amount),
                                &format_token_amount(remaining),
                                is_fully_filled,
                                spot_market,
                                block_num,
                                None,
                            )
                            .await;
                    }
                }
            }
        }
    }
    ws
}

fn log_tx_result(tx_result: &TransactionResult) {
    if !tracing::enabled!(tracing::Level::DEBUG) {
        return;
    }

    let digest = hex::encode(tx_result.transaction_digest().as_bytes());
    let sender = tx_result.sender().to_string();

    match &tx_result.receipt.status {
        ExecutionStatus::Failure(msg) => {
            tracing::debug!(
                tx_digest = %digest,
                sender = %sender,
                success = false,
                error = msg.as_str(),
                "tx failed"
            );
            return;
        }
        ExecutionStatus::Success => {}
    }

    let event_summaries: Vec<String> = tx_result
        .receipt
        .events
        .iter()
        .map(format_event_detail)
        .collect();

    tracing::debug!(
        tx_digest = %digest,
        sender = %sender,
        success = true,
        event_count = event_summaries.len(),
        events = event_summaries.join(" | "),
        "tx committed"
    );
}

fn event_action_name(event_type: &EventType) -> &str {
    match event_type {
        EventType::Call(name) => name.as_str(),
        EventType::Transfer => "transfer",
        EventType::System => "system",
        EventType::Custom(name) => name.as_str(),
    }
}

fn format_event_detail(event: &TransactionEvent) -> String {
    let action = event_action_name(&event.event_type);

    if let Some(data) = parse_event_data(&event.event_type, &event.data) {
        return format!("{action}: {data}");
    }

    if let Some(data) = parse_spot_event_data(&event.event_type, &event.data) {
        return format!("{action}: {data}");
    }

    let EventData::Bytes(bytes) = &event.data else {
        return format!("{action}: (no payload)");
    };

    match action {
        "event_contract_created" => {
            if let Ok(e) = bincode::deserialize::<EventContractCreatedEvent>(bytes) {
                return format!(
                    "event_contract_created: question={} market={} yes={} no={} collateral={} deadline={} state={}",
                    e.question,
                    e.market_address,
                    e.yes_token,
                    e.no_token,
                    e.collateral_token,
                    e.resolution_deadline,
                    e.state,
                );
            }
        }
        "event_contract_minted" => {
            if let Ok(e) = bincode::deserialize::<EventContractMintedEvent>(bytes) {
                return format!(
                    "event_contract_minted: market={} user={} amount={}",
                    e.market_address,
                    e.user,
                    format_token_amount(e.amount),
                );
            }
        }
        "event_contract_burned" => {
            if let Ok(e) = bincode::deserialize::<EventContractBurnedEvent>(bytes) {
                return format!(
                    "event_contract_burned: market={} user={} amount={}",
                    e.market_address,
                    e.user,
                    format_token_amount(e.amount),
                );
            }
        }
        "event_contract_resolved" => {
            if let Ok(e) = bincode::deserialize::<EventContractResolvedEvent>(bytes) {
                return format!(
                    "event_contract_resolved: market={} outcome={}",
                    e.market_address, e.outcome
                );
            }
        }
        "event_contract_redeemed" => {
            if let Ok(e) = bincode::deserialize::<EventContractRedeemedEvent>(bytes) {
                return format!(
                    "event_contract_redeemed: market={} user={} amount={}",
                    e.market_address,
                    e.user,
                    format_token_amount(e.amount),
                );
            }
        }
        "vault_created" => {
            if let Ok(e) = bincode::deserialize::<VaultCreatedEvent>(bytes) {
                return format!(
                    "vault_created: vault={} name={} manager={} quote={} share={}",
                    e.vault, e.name, e.manager, e.quote_token, e.share_token,
                );
            }
        }
        "vault_deposited" => {
            if let Ok(e) = bincode::deserialize::<VaultDepositedEvent>(bytes) {
                return format!(
                    "vault_deposited: vault={} user={} amount={} shares={} equity={}",
                    e.vault,
                    e.user,
                    format_token_amount(e.amount),
                    format_token_amount(e.shares),
                    format_token_amount(e.equity),
                );
            }
        }
        "vault_withdrawn" => {
            if let Ok(e) = bincode::deserialize::<VaultWithdrawnEvent>(bytes) {
                return format!(
                    "vault_withdrawn: vault={} user={} amount={} shares={} equity={}",
                    e.vault,
                    e.user,
                    format_token_amount(e.amount),
                    format_token_amount(e.shares),
                    format_token_amount(e.equity),
                );
            }
        }
        "vault_closed" => {
            if let Ok(e) = bincode::deserialize::<VaultClosedEvent>(bytes) {
                return format!("vault_closed: vault={}", e.vault);
            }
        }
        "token_created" => {
            if let Ok(e) = bincode::deserialize::<TokenCreatedEvent>(bytes) {
                return format!(
                    "token_created: symbol={} name={} supply={} token={} to={} mintable={}",
                    e.symbol,
                    e.name,
                    format_token_amount(e.total_supply),
                    e.token_address,
                    e.to,
                    e.mintable,
                );
            }
        }
        "token_minted" => {
            if let Ok(e) = bincode::deserialize::<TokenMintedEvent>(bytes) {
                return format!(
                    "token_minted: token={} amount={} to={}",
                    e.token_address,
                    format_token_amount(e.amount),
                    e.to,
                );
            }
        }
        "order_created" => {
            if let Ok(e) = bincode::deserialize::<OrderCreatedEvent>(bytes) {
                let side = match e.side {
                    lightpool_sdk::OrderSide::Buy => "buy",
                    lightpool_sdk::OrderSide::Sell => "sell",
                };
                return format!(
                    "order_created: id={} side={} size={} market={} creator={}",
                    e.order_id,
                    side,
                    format_token_amount(e.amount),
                    e.market,
                    e.creator,
                );
            }
        }
        "order_cancelled" => {
            if let Ok(e) = bincode::deserialize::<OrderCancelledEvent>(bytes) {
                return format!(
                    "order_cancelled: id={} side={:?} amount={}",
                    e.order_id,
                    e.side,
                    format_token_amount(e.cancelled_amount),
                );
            }
        }
        "order_updated" => {
            if let Ok(e) = bincode::deserialize::<OrderUpdatedEvent>(bytes) {
                return format!(
                    "order_updated: id={} side={:?} price={} old={} new={} remaining={}",
                    e.order_id,
                    e.side,
                    format_price_pieces(e.price),
                    format_token_amount(e.old_amount),
                    format_token_amount(e.new_amount),
                    format_token_amount(e.remaining_amount),
                );
            }
        }
        "order_filled" => {
            if let Ok(e) = bincode::deserialize::<OrderFilledEvent>(bytes) {
                return format!(
                    "order_filled: id={} price={} fill={} remaining={} market={}",
                    e.order_id,
                    format_price_pieces(e.price),
                    format_token_amount(e.fill_amount),
                    format_token_amount(e.remaining_amount),
                    e.market,
                );
            }
        }
        "market_order_executed" => {
            if let Ok(e) = bincode::deserialize::<MarketOrderExecutedEvent>(bytes) {
                return format!(
                    "market_order_executed: id={} side={:?} amount={} filled={} avg_price={:?} creator={}",
                    e.order_id,
                    e.side,
                    format_token_amount(e.amount),
                    format_token_amount(e.filled_amount),
                    e.avg_filled_price.map(format_price_pieces),
                    e.creator,
                );
            }
        }
        _ => {}
    }

    if let EventType::Transfer = &event.event_type {
        if let Ok(e) = bincode::deserialize::<TransferEvent>(bytes) {
            return format!(
                "transfer: token={} from={} to={} amount={}",
                e.token,
                e.from,
                e.to,
                format_token_amount(e.amount),
            );
        }
    }

    format!("{action}: (undecoded)")
}

async fn maybe_warn_order_created_price(
    store: &SharedIndexState,
    created: &OrderCreatedEvent,
    spot_market: &str,
) {
    let OrderEventType::Limit { price, .. } = &created.order_type else {
        return;
    };

    let outcome = match store.lookup_spot_market(spot_market).await {
        Some((_, outcome)) => outcome,
        None => "unknown".into(),
    };

    warn_outcome_price_mismatch(&outcome, *price, spot_market, &created.order_id.to_string());
}

pub async fn apply_order_created_to_book(
    store: &SharedIndexState,
    block_num: u64,
    created: &OrderCreatedEvent,
    spot_market: &str,
    publish_ws: bool,
    mut ws: Option<&mut IndexWriteSet>,
) {
    let OrderEventType::Limit { price, .. } = &created.order_type else {
        return;
    };

    store
        .books
        .apply_created(
            spot_market,
            created.side,
            *price,
            created.amount,
            block_num,
            publish_ws,
            ws,
        )
        .await;
}

fn warn_outcome_price_mismatch(outcome: &str, price_raw: u64, spot_market: &str, order_id: &str) {
    use lightpool_sdk::TOKEN_SCALE;
    let threshold = TOKEN_SCALE * 55 / 100;
    let price_display = format_price_pieces(price_raw);

    let mismatch = match outcome {
        "yes" if price_raw > threshold => true,
        "no" if price_raw < threshold => true,
        _ => false,
    };

    if mismatch {
        tracing::warn!(
            order_id,
            spot_market,
            outcome,
            price = %price_display,
            "order price looks inconsistent with spot outcome (possible wrong spot or mapping bug)"
        );
    }
}

async fn index_spot_market_created(
    store: &SharedIndexState,
    chain: &SharedChainClient,
    query_account: &str,
    created: MarketCreatedEvent,
    ws: &mut IndexWriteSet,
) {
    let spot_market = created.market_address.to_string();
    let name = created.name.to_string();

    tracing::debug!(
        name = %name,
        spot_market = %spot_market,
        "indexed spot market"
    );

    store
        .register_named_spot_market(&name, &spot_market, &created.creator.to_string())
        .await;

    if let Err(error) = ensure_chain_hydrated(
        chain,
        store,
        query_account,
        &spot_market,
        DEFAULT_BOOK_DEPTH,
        Some(ws),
    )
    .await
    {
        tracing::warn!(
            name = %name,
            spot_market = %spot_market,
            error = %error,
            "failed to hydrate spot book after market_created"
        );
    }
}

async fn index_market_created(
    chain: &SharedChainClient,
    query_account: &str,
    store: &SharedIndexState,
    created: EventContractCreatedEvent,
    ws: &mut IndexWriteSet,
) {
    let market_address = created.market_address.to_string();
    let question = created.question.clone();
    let slug = store.allocate_market_slug(&question).await;
    let icon_url = None;

    let market = Market::Event {
        id: market_uuid(&market_address),
        slug,
        question,
        icon_url,
        market_address,
        collateral_token: created.collateral_token.to_string(),
        yes_token: created.yes_token.to_string(),
        no_token: created.no_token.to_string(),
        yes_spot_market: created.yes_spot_market.to_string(),
        no_spot_market: created.no_spot_market.to_string(),
        state: created.state.to_string(),
        resolution_deadline: created.resolution_deadline,
        deployer: created.creator.to_string(),
    };

    tracing::debug!(
        market_id = %market.id(),
        slug = %market.slug(),
        question = %market.question(),
        market_address = %market.market_address(),
        "indexed event contract market"
    );

    store.upsert_market(market.clone(), Some(ws)).await;
    hydrate_market_spots(
        chain,
        store,
        query_account,
        market.yes_spot_market(),
        market.no_spot_market(),
        Some(ws),
    )
    .await;
}

async fn index_vault_created(
    store: &SharedIndexState,
    created: VaultCreatedEvent,
    ws: &mut IndexWriteSet,
) {
    let vault_address = created.vault.to_string();
    let trading_account = vault_account(created.vault);
    let vault = Vault {
        id: vault_uuid(&vault_address),
        name: created.name.to_string(),
        vault_address: vault_address.clone(),
        vault_account: trading_account.to_string(),
        manager: created.manager.to_string(),
        quote_token: created.quote_token.to_string(),
        share_token: created.share_token.to_string(),
        equity: "0".into(),
        user_deposit: "0.00".into(),
        portfolio: Vec::new(),
        allow_deposit: true,
        is_closed: false,
    };

    tracing::debug!(
        vault_id = %vault.id,
        name = %vault.name,
        vault_address = %vault.vault_address,
        vault_account = %vault.vault_account,
        manager = %vault.manager,
        "indexed vault"
    );

    store.upsert_vault(vault, Some(ws)).await;
}

pub async fn index_order_created(
    store: &SharedIndexState,
    created: OrderCreatedEvent,
    spot_market: &str,
    status_override: Option<(String, u64)>,
    mut ws: Option<&mut IndexWriteSet>,
) -> Option<Order> {
    let (market_id, outcome) = match store.lookup_spot_market(spot_market).await {
        Some(mapped) => mapped,
        None => {
            tracing::debug!(
                spot_market,
                order_id = %created.order_id,
                "order_created for standalone spot market; registering for order index"
            );
            store.ensure_standalone_spot_market(spot_market).await
        }
    };

    let price_raw = match &created.order_type {
        OrderEventType::Limit { price, .. } => *price,
        OrderEventType::Market { .. } => 0,
        OrderEventType::Trigger { limit_price, .. } => *limit_price,
    };

    let side = match created.side {
        lightpool_sdk::OrderSide::Buy => "buy",
        lightpool_sdk::OrderSide::Sell => "sell",
    };

    let chain_order_id = created.order_id.to_string();
    let (question, market_slug) = match store.get_market(market_id).await {
        Some(market) => (
            market.label().to_string(),
            if !market.slug().is_empty() {
                market.slug().to_string()
            } else {
                market.name().to_string()
            },
        ),
        None => (String::new(), String::new()),
    };
    let normalized_spot = crate::spot_market::normalize_spot_market_key(spot_market);
    let (status, filled_raw) = status_override.unwrap_or_else(|| ("open".into(), 0));
    let order = Order {
        id: Uuid::new_v5(
            &Uuid::NAMESPACE_OID,
            format!("{normalized_spot}:{chain_order_id}").as_bytes(),
        ),
        market_id,
        market_slug,
        question,
        outcome,
        side: side.into(),
        price: format_price_pieces(price_raw),
        size: format_token_amount(created.amount),
        status,
        cloid: created.cloid.clone(),
    };

    tracing::debug!(
        order_id = chain_order_id,
        market_id = %market_id,
        user = %created.creator,
        "indexed order"
    );

    store
        .insert_order(
            order.clone(),
            created.creator.to_string(),
            spot_market,
            chain_order_id,
            created.amount,
            filled_raw,
            ws,
        )
        .await;

    Some(order)
}

/// Archive a taker market order from `market_order_executed` (no prior `order_created`).
pub async fn index_market_order_executed(
    store: &SharedIndexState,
    executed: MarketOrderExecutedEvent,
    spot_market: &str,
    ws: Option<&mut IndexWriteSet>,
) -> Option<Order> {
    let (market_id, outcome) = match store.lookup_spot_market(spot_market).await {
        Some(mapped) => mapped,
        None => {
            tracing::debug!(
                spot_market,
                order_id = %executed.order_id,
                "market_order_executed for standalone spot market; registering for order index"
            );
            store.ensure_standalone_spot_market(spot_market).await
        }
    };

    let status = if executed.filled_amount == 0 {
        "cancelled"
    } else if executed.filled_amount >= executed.amount {
        "filled"
    } else {
        // Remaining size is cancelled on chain for market orders.
        "cancelled"
    };

    let side = match executed.side {
        lightpool_sdk::OrderSide::Buy => "buy",
        lightpool_sdk::OrderSide::Sell => "sell",
    };
    let chain_order_id = executed.order_id.to_string();
    let (question, market_slug) = match store.get_market(market_id).await {
        Some(market) => (
            market.label().to_string(),
            if !market.slug().is_empty() {
                market.slug().to_string()
            } else {
                market.name().to_string()
            },
        ),
        None => (String::new(), String::new()),
    };
    let normalized_spot = crate::spot_market::normalize_spot_market_key(spot_market);
    let price_raw = executed.avg_filled_price.unwrap_or(0);
    let order = Order {
        id: Uuid::new_v5(
            &Uuid::NAMESPACE_OID,
            format!("{normalized_spot}:{chain_order_id}").as_bytes(),
        ),
        market_id,
        market_slug,
        question,
        outcome,
        side: side.into(),
        price: format_price_pieces(price_raw),
        size: format_token_amount(executed.amount),
        status: status.into(),
        cloid: None,
    };

    tracing::debug!(
        order_id = chain_order_id,
        market_id = %market_id,
        user = %executed.creator,
        filled = executed.filled_amount,
        "indexed market_order_executed"
    );

    store
        .insert_order(
            order.clone(),
            executed.creator.to_string(),
            spot_market,
            chain_order_id,
            executed.amount,
            executed.filled_amount,
            ws,
        )
        .await;

    Some(order)
}

pub async fn publish_user_order_created(
    user_hub: &SharedUserEventHub,
    store: &SharedIndexState,
    spot_market: &str,
    chain_order_id: &str,
    block_num: u64,
) {
    let Some((order, user_address, _)) =
        store.stored_order_by_chain_id(spot_market, chain_order_id).await
    else {
        return;
    };
    user_hub
        .publish_order(
            "placement",
            &user_address,
            chain_order_id,
            spot_market,
            order,
            block_num,
        )
        .await;
}

async fn publish_user_order_cancelled(
    user_hub: &SharedUserEventHub,
    store: &SharedIndexState,
    spot_market: &str,
    chain_order_id: &str,
    block_num: u64,
) {
    let Some((order, user_address, _)) =
        store.stored_order_by_chain_id(spot_market, chain_order_id).await
    else {
        return;
    };
    user_hub
        .publish_order(
            "cancellation",
            &user_address,
            chain_order_id,
            spot_market,
            order,
            block_num,
        )
        .await;
}

async fn publish_user_order_updated(
    user_hub: &SharedUserEventHub,
    store: &SharedIndexState,
    spot_market: &str,
    chain_order_id: &str,
    block_num: u64,
) {
    let Some((order, user_address, _)) =
        store.stored_order_by_chain_id(spot_market, chain_order_id).await
    else {
        return;
    };
    user_hub
        .publish_order(
            "update",
            &user_address,
            chain_order_id,
            spot_market,
            order,
            block_num,
        )
        .await;
}

async fn publish_user_order_filled(
    user_hub: &SharedUserEventHub,
    store: &SharedIndexState,
    chain_order_id: &str,
    spot_market: &str,
    price_raw: u64,
    fill_amount_raw: u64,
    remaining_amount_raw: u64,
    is_fully_filled: bool,
    side: lightpool_sdk::OrderSide,
    block_num: u64,
    cloid: Option<String>,
    tx_sender: String,
) {
    let stored = store
        .stored_order_by_chain_id(spot_market, chain_order_id)
        .await;
    let (order, user_address, stored_cloid) = if let Some((order, user_address, _)) = stored {
        let stored_cloid = order.cloid.clone();
        (Some(order), user_address, stored_cloid)
    } else if cloid.is_some() {
        (None, tx_sender, None)
    } else {
        return;
    };

    let side_str = match side {
        lightpool_sdk::OrderSide::Buy => "buy",
        lightpool_sdk::OrderSide::Sell => "sell",
    };
    let order_id = order.as_ref().map(|order| order.id).unwrap_or_else(Uuid::nil);
    let market_slug = order.as_ref().map(|order| order.market_slug.as_str()).unwrap_or("");
    let outcome = order.as_ref().map(|order| order.outcome.as_str()).unwrap_or("");

    user_hub
        .publish_trade(
            &user_address,
            chain_order_id,
            order_id,
            market_slug,
            outcome,
            side_str,
            &format_price_pieces(price_raw),
            &format_token_amount(fill_amount_raw),
            &format_token_amount(remaining_amount_raw),
            is_fully_filled,
            spot_market,
            block_num,
            cloid.or(stored_cloid),
        )
        .await;

    let Some(order) = order else {
        return;
    };
    let event = "update";
    user_hub
        .publish_order(event, &user_address, chain_order_id, spot_market, order, block_num)
        .await;
}
