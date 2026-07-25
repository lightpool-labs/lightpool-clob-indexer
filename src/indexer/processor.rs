// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use lightpool_sdk::event_contract_events::{
    EventContractBurnedEvent, EventContractCreatedEvent, EventContractMintedEvent,
    EventContractRedeemedEvent, EventContractResolvedEvent,
};
use lightpool_sdk::spot_events::{
    OrderCancelledEvent, OrderCreatedEvent, OrderEventType, OrderFilledEvent, OrderUpdatedEvent,
    parse_spot_event_data,
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
use crate::submit_wait::SharedSubmitWaitRegistry;
use crate::ws::process::SharedUserEventHub;

use super::book_store::SharedBookStore;
use super::store::{market_uuid, vault_uuid, SharedIndexStore};

fn spot_market_from_event_contract(event: &TransactionEvent) -> Option<String> {
    event
        .contract
        .as_ref()
        .map(|contract| crate::spot_market::normalize_spot_market_key(&contract.to_string()))
}

async fn resolve_spot_market_for_order_event(
    store: &SharedIndexStore,
    event: &TransactionEvent,
    chain_order_id: &str,
) -> Option<String> {
    if let Some(spot_market) = spot_market_from_event_contract(event) {
        return Some(spot_market);
    }
    store.lookup_spot_market_for_chain_order(chain_order_id).await
}

#[derive(Default)]
struct BlockOrderSyncStats {
    created_total: u32,
    created_yes: u32,
    created_no: u32,
    created_unknown: u32,
    skipped_duplicates: u32,
}

pub async fn process_block(
    chain: &SharedChainClient,
    query_account: &str,
    store: &SharedIndexStore,
    book_store: &SharedBookStore,
    user_hub: &SharedUserEventHub,
    submit_wait: &SharedSubmitWaitRegistry,
    block: ReceiptBlock,
) {
    for tx_result in &block.transaction_outputs {
        // Match submit_queue register key: SignedTransaction digest (tx + signature).
        // receipt.transaction_digest is the unsigned Transaction digest from execution.
        let digest = hex::encode(tx_result.transaction.digest().as_bytes());
        if !submit_wait.complete(&digest, tx_result.receipt.clone()) {
            tracing::debug!(
                digest,
                "no pending submit waiter for transaction digest"
            );
        }
    }

    let block_num = block.block_num;
    let mut sync_stats = BlockOrderSyncStats::default();

    for tx_result in block.transaction_outputs {
        log_tx_result(&tx_result);

        if !tx_result.is_success() {
            continue;
        }

        for event in &tx_result.receipt.events {
            let EventType::Call(action_name) = &event.event_type else {
                continue;
            };

            tracing::info!(action = action_name.as_str(), detail = %format_event_detail(event), "processing tx event");

            match action_name.as_str() {
                "event_contract_created" => {
                    if let EventData::Bytes(data) = &event.data {
                        match bincode::deserialize::<EventContractCreatedEvent>(data) {
                            Ok(created) => {
                                index_market_created(
                                    chain,
                                    query_account,
                                    store,
                                    book_store,
                                    created,
                                )
                                .await;
                            }
                            Err(e) => {
                                tracing::warn!(
                                    error = %e,
                                    "failed to decode event_contract_created"
                                );
                            }
                        }
                    }
                }
                "event_contract_resolved" => {
                    if let EventData::Bytes(data) = &event.data {
                        if let Ok(resolved) =
                            bincode::deserialize::<EventContractResolvedEvent>(data)
                        {
                            store
                                .update_market_state(
                                    &resolved.market_address.to_string(),
                                    "Resolved",
                                )
                                .await;
                        }
                    }
                }
                "vault_created" => {
                    if let EventData::Bytes(data) = &event.data {
                        match bincode::deserialize::<VaultCreatedEvent>(data) {
                            Ok(created) => {
                                index_vault_created(store, created).await;
                            }
                            Err(e) => {
                                tracing::warn!(error = %e, "failed to decode vault_created");
                            }
                        }
                    }
                }
                "vault_deposited" => {
                    if let EventData::Bytes(data) = &event.data {
                        if let Ok(deposited) = bincode::deserialize::<VaultDepositedEvent>(data) {
                            store
                                .update_vault_equity(
                                    &deposited.vault.to_string(),
                                    &format_token_amount(deposited.equity),
                                )
                                .await;
                        }
                    }
                }
                "vault_withdrawn" => {
                    if let EventData::Bytes(data) = &event.data {
                        if let Ok(withdrawn) = bincode::deserialize::<VaultWithdrawnEvent>(data) {
                            store
                                .update_vault_equity(
                                    &withdrawn.vault.to_string(),
                                    &format_token_amount(withdrawn.equity),
                                )
                                .await;
                        }
                    }
                }
                "vault_manager_updated" => {
                    if let EventData::Bytes(data) = &event.data {
                        if let Ok(updated) =
                            bincode::deserialize::<VaultManagerUpdatedEvent>(data)
                        {
                            store
                                .update_vault_manager(
                                    &updated.vault.to_string(),
                                    &updated.new_manager.to_string(),
                                )
                                .await;
                        }
                    }
                }
                "vault_deposit_permission_updated" => {
                    if let EventData::Bytes(data) = &event.data {
                        if let Ok(updated) =
                            bincode::deserialize::<VaultDepositPermissionUpdatedEvent>(data)
                        {
                            store
                                .update_vault_allow_deposit(
                                    &updated.vault.to_string(),
                                    updated.allow_deposit,
                                )
                                .await;
                        }
                    }
                }
                "vault_closed" => {
                    if let EventData::Bytes(data) = &event.data {
                        if let Ok(closed) = bincode::deserialize::<VaultClosedEvent>(data) {
                            store.mark_vault_closed(&closed.vault.to_string()).await;
                        }
                    }
                }
                "order_created" => {
                    if let EventData::Bytes(data) = &event.data {
                        if let Ok(created) = bincode::deserialize::<OrderCreatedEvent>(data) {
                            let chain_order_id = created.order_id.to_string();
                            let spot_market = crate::spot_market::normalize_spot_market_key(
                                &created.market.to_string(),
                            );
                            if store
                                .has_chain_order(&spot_market, &chain_order_id)
                                .await
                            {
                                sync_stats.skipped_duplicates += 1;
                                tracing::warn!(
                                    block_num,
                                    order_id = chain_order_id,
                                    spot_market,
                                    "block_sync order_created skipped duplicate"
                                );
                            } else {
                                log_block_sync_order_created(
                                    &mut sync_stats,
                                    store,
                                    block_num,
                                    &created,
                                    &spot_market,
                                )
                                .await;
                                if let Err(error) = ensure_chain_hydrated(
                                    chain,
                                    book_store,
                                    store,
                                    query_account,
                                    &spot_market,
                                    DEFAULT_BOOK_DEPTH,
                                )
                                .await
                                {
                                    tracing::warn!(
                                        spot_market,
                                        error = %error,
                                        "failed to hydrate book before order_created"
                                    );
                                } else {
                                    apply_order_created_to_book(
                                        book_store,
                                        store,
                                        block_num,
                                        &created,
                                        &spot_market,
                                    )
                                    .await;
                                }
                                index_order_created(store, created, &spot_market, None).await;
                                publish_user_order_created(
                                    user_hub,
                                    store,
                                    &spot_market,
                                    &chain_order_id,
                                    block_num,
                                )
                                .await;
                            }
                        }
                    }
                }
                "order_cancelled" => {
                    if let EventData::Bytes(data) = &event.data {
                        if let Ok(cancelled) = bincode::deserialize::<OrderCancelledEvent>(data) {
                            let chain_order_id = cancelled.order_id.to_string();
                            match resolve_spot_market_for_order_event(store, event, &chain_order_id)
                                .await
                            {
                                Some(spot_market) => {
                                    book_store
                                        .apply_cancelled(
                                            &spot_market,
                                            cancelled.side,
                                            cancelled.price,
                                            cancelled.cancelled_amount,
                                            block_num,
                                        )
                                        .await;
                                    store
                                        .update_order_cancelled(&spot_market, &chain_order_id)
                                        .await;
                                    publish_user_order_cancelled(
                                        user_hub,
                                        store,
                                        &spot_market,
                                        &chain_order_id,
                                        block_num,
                                    )
                                    .await;
                                }
                                None => {
                                    tracing::warn!(
                                        order_id = chain_order_id,
                                        "order_cancelled without indexed spot market; rehydrating all spot books from chain"
                                    );
                                    if let Err(error) = hydrate_all_spot_markets(
                                        chain,
                                        book_store,
                                        store,
                                        query_account,
                                    )
                                    .await
                                    {
                                        tracing::warn!(
                                            error = %error,
                                            "failed to rehydrate spot books after order_cancelled mapping miss"
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
                "order_updated" => {
                    if let EventData::Bytes(data) = &event.data {
                        if let Ok(updated) = bincode::deserialize::<OrderUpdatedEvent>(data) {
                            let chain_order_id = updated.order_id.to_string();
                            match resolve_spot_market_for_order_event(store, event, &chain_order_id)
                                .await
                            {
                                Some(spot_market) => {
                                    if let Err(error) = ensure_chain_hydrated(
                                        chain,
                                        book_store,
                                        store,
                                        query_account,
                                        &spot_market,
                                        DEFAULT_BOOK_DEPTH,
                                    )
                                    .await
                                    {
                                        tracing::warn!(
                                            spot_market,
                                            error = %error,
                                            "failed to hydrate book before order_updated"
                                        );
                                    } else {
                                        book_store
                                            .apply_updated(
                                                &spot_market,
                                                updated.side,
                                                updated.price,
                                                updated.old_amount,
                                                updated.new_amount,
                                                updated.remaining_amount,
                                                block_num,
                                            )
                                            .await;
                                    }
                                    store
                                        .update_order_amount(
                                            &spot_market,
                                            &chain_order_id,
                                            updated.new_amount,
                                            updated.remaining_amount,
                                        )
                                        .await;
                                    publish_user_order_updated(
                                        user_hub,
                                        store,
                                        &spot_market,
                                        &chain_order_id,
                                        block_num,
                                    )
                                    .await;
                                }
                                None => {
                                    tracing::warn!(
                                        order_id = chain_order_id,
                                        "order_updated without indexed spot market; rehydrating all spot books from chain"
                                    );
                                    if let Err(error) = hydrate_all_spot_markets(
                                        chain,
                                        book_store,
                                        store,
                                        query_account,
                                    )
                                    .await
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
                }
                "order_filled" => {
                    if let EventData::Bytes(data) = &event.data {
                        if let Ok(filled) = bincode::deserialize::<OrderFilledEvent>(data) {
                            let chain_order_id = filled.order_id.to_string();
                            let spot_market =
                                crate::spot_market::normalize_spot_market_key(&filled.market.to_string());
                            store
                                .record_last_trade_price(&spot_market, filled.price)
                                .await;
                            if let Err(error) = ensure_chain_hydrated(
                                chain,
                                book_store,
                                store,
                                query_account,
                                &spot_market,
                                DEFAULT_BOOK_DEPTH,
                            )
                            .await
                            {
                                tracing::warn!(
                                    spot_market,
                                    error = %error,
                                    "failed to hydrate book before order_filled"
                                );
                            } else {
                                book_store
                                    .apply_filled(
                                        &spot_market,
                                        filled.side,
                                        filled.price,
                                        filled.fill_amount,
                                        block_num,
                                        filled.price,
                                    )
                                    .await;
                            }
                            store
                                .update_order_fill(
                                    &spot_market,
                                    &chain_order_id,
                                    filled.fill_amount,
                                    filled.remaining_amount,
                                    filled.is_fully_filled,
                                )
                                .await;
                            if let Some((_, user_address, _)) = store
                                .stored_order_by_chain_id(&spot_market, &chain_order_id)
                                .await
                            {
                                store
                                    .apply_vault_fill_to_portfolio(
                                        &user_address,
                                        &spot_market,
                                        filled.side,
                                        filled.fill_amount,
                                    )
                                    .await;
                            }
                            publish_user_order_filled(
                                user_hub,
                                store,
                                &chain_order_id,
                                &spot_market,
                                filled.price,
                                filled.fill_amount,
                                filled.remaining_amount,
                                filled.is_fully_filled,
                                filled.side,
                                block_num,
                            )
                            .await;
                        }
                    }
                }
                _ => {}
            }
        }
    }

    if sync_stats.created_total > 0 || sync_stats.skipped_duplicates > 0 {
        tracing::warn!(
            block_num,
            created_total = sync_stats.created_total,
            created_yes = sync_stats.created_yes,
            created_no = sync_stats.created_no,
            created_unknown = sync_stats.created_unknown,
            skipped_duplicates = sync_stats.skipped_duplicates,
            "block_sync order_created summary"
        );
    }
}

fn log_tx_result(tx_result: &TransactionResult) {
    let digest = hex::encode(tx_result.transaction_digest().as_bytes());
    let sender = tx_result.sender().to_string();
    let block_num = tx_result.receipt.block_num;

    match &tx_result.receipt.status {
        ExecutionStatus::Failure(msg) => {
            tracing::info!(
                tx_digest = %digest,
                block_num,
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

    tracing::info!(
        tx_digest = %digest,
        block_num,
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

async fn log_block_sync_order_created(
    stats: &mut BlockOrderSyncStats,
    store: &SharedIndexStore,
    block_num: u64,
    created: &OrderCreatedEvent,
    spot_market: &str,
) {
    let OrderEventType::Limit { price, .. } = &created.order_type else {
        return;
    };

    let (outcome, slug) = match store.lookup_spot_market(spot_market).await {
        Some((market_id, outcome)) => {
            let slug = store
                .get_market(market_id)
                .await
                .map(|market| market.slug);
            (outcome, slug)
        }
        None => ("unknown".into(), None),
    };

    match outcome.as_str() {
        "yes" => stats.created_yes += 1,
        "no" => stats.created_no += 1,
        _ => stats.created_unknown += 1,
    }
    stats.created_total += 1;

    let side = match created.side {
        lightpool_sdk::OrderSide::Buy => "buy",
        lightpool_sdk::OrderSide::Sell => "sell",
    };

    tracing::warn!(
        block_num,
        order_id = %created.order_id,
        slug = slug.as_deref().unwrap_or("-"),
        outcome,
        spot_market,
        side,
        price = %format_price_pieces(*price),
        quantity = %format_token_amount(created.amount),
        creator = %created.creator,
        "block_sync order_created"
    );

    warn_outcome_price_mismatch(&outcome, *price, spot_market, &created.order_id.to_string());
}

pub async fn apply_order_created_to_book(
    book_store: &SharedBookStore,
    _store: &SharedIndexStore,
    block_num: u64,
    created: &OrderCreatedEvent,
    spot_market: &str,
) {
    let OrderEventType::Limit { price, .. } = &created.order_type else {
        return;
    };

    book_store
        .apply_created(
            spot_market,
            created.side,
            *price,
            created.amount,
            block_num,
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

async fn index_market_created(
    chain: &SharedChainClient,
    query_account: &str,
    store: &SharedIndexStore,
    book_store: &SharedBookStore,
    created: EventContractCreatedEvent,
) {
    let market_address = created.market_address.to_string();
    let question = created.question.clone();
    let slug = store.allocate_market_slug(&question).await;
    let icon_url = None;

    let market = Market {
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
    };

    tracing::info!(
        market_id = %market.id,
        slug = %market.slug,
        question = %market.question,
        market_address = %market.market_address,
        "indexed event contract market"
    );

    store.upsert_market(market.clone()).await;
    hydrate_market_spots(
        chain,
        book_store,
        store,
        query_account,
        &market.yes_spot_market,
        &market.no_spot_market,
    )
    .await;
}

async fn index_vault_created(store: &SharedIndexStore, created: VaultCreatedEvent) {
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

    tracing::info!(
        vault_id = %vault.id,
        name = %vault.name,
        vault_address = %vault.vault_address,
        vault_account = %vault.vault_account,
        manager = %vault.manager,
        "indexed vault"
    );

    store.upsert_vault(vault).await;
}

pub async fn index_order_created(
    store: &SharedIndexStore,
    created: OrderCreatedEvent,
    spot_market: &str,
    status_override: Option<(String, u64)>,
) -> Option<Order> {
    let Some((market_id, outcome)) = store.lookup_spot_market(spot_market).await else {
        tracing::warn!(
            spot_market,
            order_id = %created.order_id,
            "order_created for unknown spot market (book updated but order not indexed)"
        );
        return None;
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
    let question = store
        .get_market(market_id)
        .await
        .map(|market| market.question)
        .unwrap_or_default();
    let market_slug = store
        .get_market(market_id)
        .await
        .map(|market| market.slug)
        .unwrap_or_default();
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
    };

    tracing::info!(
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
        )
        .await;

    Some(order)
}

pub async fn publish_user_order_created(
    user_hub: &SharedUserEventHub,
    store: &SharedIndexStore,
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
            order,
            block_num,
        )
        .await;
}

async fn publish_user_order_cancelled(
    user_hub: &SharedUserEventHub,
    store: &SharedIndexStore,
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
            order,
            block_num,
        )
        .await;
}

async fn publish_user_order_updated(
    user_hub: &SharedUserEventHub,
    store: &SharedIndexStore,
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
            order,
            block_num,
        )
        .await;
}

async fn publish_user_order_filled(
    user_hub: &SharedUserEventHub,
    store: &SharedIndexStore,
    chain_order_id: &str,
    spot_market: &str,
    price_raw: u64,
    fill_amount_raw: u64,
    remaining_amount_raw: u64,
    is_fully_filled: bool,
    side: lightpool_sdk::OrderSide,
    block_num: u64,
) {
    let Some((order, user_address, _)) =
        store.stored_order_by_chain_id(spot_market, chain_order_id).await
    else {
        return;
    };

    let side_str = match side {
        lightpool_sdk::OrderSide::Buy => "buy",
        lightpool_sdk::OrderSide::Sell => "sell",
    };

    user_hub
        .publish_trade(
            &user_address,
            chain_order_id,
            order.id,
            &order.market_slug,
            &order.outcome,
            side_str,
            &format_price_pieces(price_raw),
            &format_token_amount(fill_amount_raw),
            &format_token_amount(remaining_amount_raw),
            is_fully_filled,
            spot_market,
            block_num,
        )
        .await;

    let event = "update";
    user_hub
        .publish_order(event, &user_address, chain_order_id, order, block_num)
        .await;
}
