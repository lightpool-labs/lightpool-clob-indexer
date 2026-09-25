// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use dashmap::{DashMap, DashSet};
use tokio::sync::RwLock as TokioRwLock;
use uuid::Uuid;

use crate::domain::{Market, MarketCategory, MarketQuery, MarketSortOrder, Order, Vault, VaultQuery};
use crate::persist::{
    index_write_set::{order_row_from_parts, IndexWriteSet},
    PersistOrderRow, SharedPersist,
};
use crate::spot_market::{
    normalize_spot_market_key, onchain_order_id, split_onchain_order_id, OnchainOrderId,
};
use crate::bars::Bars;

use super::books::Books;

#[derive(Debug, Clone, Default)]
pub struct IndexedBlockHead {
    pub block_num: u64,
    pub digest: String,
    pub tx_count: usize,
    pub connected: bool,
    pub catching_up: bool,
    /// Unix ms when apply last finished a block (0 = never).
    pub last_indexed_at_ms: u64,
}

pub type SharedIndexedBlockHead = Arc<TokioRwLock<IndexedBlockHead>>;

#[derive(Debug, Clone)]
struct SpotMarketRef {
    market_id: Uuid,
    outcome: String,
}

#[derive(Debug, Clone)]
pub(crate) struct StoredOrder {
    pub order: Order,
    pub user_address: String,
    pub chain_order_id: String,
    pub spot_market: String,
    pub filled_raw: u64,
    pub size_raw: u64,
}

#[derive(Debug, Clone)]
pub struct OrderQueryRecord {
    pub order: Order,
    pub chain_order_id: String,
    pub spot_market: String,
    pub user_address: String,
    pub size_raw: u64,
    pub filled_raw: u64,
}

/// Concurrent in-memory index (orders/markets/vaults) plus sharded order books and bars.
pub struct IndexState {
    pub books: Books,
    pub bars: Bars,
    markets: DashMap<Uuid, Market>,
    slug_to_market: DashMap<String, Uuid>,
    spot_to_market: DashMap<String, SpotMarketRef>,
    spot_by_name: DashMap<String, String>,
    last_trade_price: DashMap<String, u64>,
    orders: DashMap<Uuid, StoredOrder>,
    orders_by_id: DashMap<OnchainOrderId, Uuid>,
    orders_by_user: DashMap<String, DashSet<Uuid>>,
    vaults: DashMap<Uuid, Vault>,
    vault_by_address: DashMap<String, Uuid>,
    vault_by_account: DashMap<String, Uuid>,
    vault_portfolio: DashMap<Uuid, HashMap<String, u64>>,
    persist: Option<SharedPersist>,
    /// Block unix-seconds while applying a ReceiptBlock (0 when idle / unknown).
    indexing_block_timestamp_secs: AtomicU64,
}

pub type SharedIndexState = Arc<IndexState>;

fn user_key(user_address: &str) -> String {
    user_address.trim().to_ascii_lowercase()
}

fn addr_key(address: &str) -> String {
    address.trim().to_ascii_lowercase()
}

fn is_hot_order_status(status: &str) -> bool {
    status == "open" || status == "partial_filled"
}

fn stored_to_persist_row(stored: &StoredOrder) -> PersistOrderRow {
    PersistOrderRow {
        order: stored.order.clone(),
        user_address: stored.user_address.clone(),
        chain_order_id: stored.chain_order_id.clone(),
        spot_market: stored.spot_market.clone(),
        size_raw: stored.size_raw,
        filled_raw: stored.filled_raw,
        status_ts_ms: 0,
    }
}

impl IndexState {
    pub fn new(persist: Option<SharedPersist>) -> Self {
        Self {
            books: Books::new(),
            bars: Bars::new(persist.clone()),
            markets: DashMap::new(),
            slug_to_market: DashMap::new(),
            spot_to_market: DashMap::new(),
            spot_by_name: DashMap::new(),
            last_trade_price: DashMap::new(),
            orders: DashMap::new(),
            orders_by_id: DashMap::new(),
            orders_by_user: DashMap::new(),
            vaults: DashMap::new(),
            vault_by_address: DashMap::new(),
            vault_by_account: DashMap::new(),
            vault_portfolio: DashMap::new(),
            persist,
            indexing_block_timestamp_secs: AtomicU64::new(0),
        }
    }

    pub fn set_indexing_block_timestamp(&self, timestamp_secs: u64) {
        self.indexing_block_timestamp_secs
            .store(timestamp_secs, Ordering::Relaxed);
    }

    /// Block wall time in ms for the block currently being applied; 0 if unknown.
    pub fn indexing_block_timestamp_ms(&self) -> u64 {
        let secs = self.indexing_block_timestamp_secs.load(Ordering::Relaxed);
        secs.saturating_mul(1000)
    }


    pub async fn clear_all(&self) {
        self.books.clear();
        self.bars.clear().await;
        self.markets.clear();
        self.slug_to_market.clear();
        self.spot_to_market.clear();
        self.spot_by_name.clear();
        self.last_trade_price.clear();
        self.orders.clear();
        self.orders_by_id.clear();
        self.orders_by_user.clear();
        self.vaults.clear();
        self.vault_by_address.clear();
        self.vault_by_account.clear();
        self.vault_portfolio.clear();
        self.indexing_block_timestamp_secs.store(0, Ordering::Relaxed);
    }

    pub async fn market_count(&self) -> usize {
        self.markets.len()
    }

    pub async fn export_markets_for_persist(&self) -> Vec<Market> {
        self.markets.iter().map(|e| e.value().clone()).collect()
    }

    pub async fn export_orders_for_persist(&self) -> Vec<crate::persist::PersistOrderRow> {
        self.export_orders_for_persist_spots(None).await
    }

    pub async fn export_orders_for_persist_spots(
        &self,
        only_spots: Option<&[String]>,
    ) -> Vec<crate::persist::PersistOrderRow> {
        self.orders
            .iter()
            .filter(|e| {
                let spot = &e.value().spot_market;
                match only_spots {
                    Some(spots) => spots.iter().any(|s| s == spot),
                    None => true,
                }
            })
            .map(|e| {
                let stored = e.value();
                crate::persist::PersistOrderRow {
                    order: stored.order.clone(),
                    user_address: stored.user_address.clone(),
                    chain_order_id: stored.chain_order_id.clone(),
                    spot_market: stored.spot_market.clone(),
                    size_raw: stored.size_raw,
                    filled_raw: stored.filled_raw,
                    status_ts_ms: 0,
                }
            })
            .collect()
    }

    pub async fn export_last_trades_for_persist(&self) -> Vec<(String, u64)> {
        self.export_last_trades_for_persist_spots(None).await
    }

    pub async fn export_last_trades_for_persist_spots(
        &self,
        only_spots: Option<&[String]>,
    ) -> Vec<(String, u64)> {
        self.last_trade_price
            .iter()
            .filter(|e| match only_spots {
                Some(spots) => spots.iter().any(|s| s == e.key()),
                None => true,
            })
            .map(|e| (e.key().clone(), *e.value()))
            .collect()
    }

    pub async fn export_vaults_for_persist(&self) -> Vec<Vault> {
        self.vaults.iter().map(|e| e.value().clone()).collect()
    }

    pub async fn export_vault_portfolio_for_persist(
        &self,
    ) -> Vec<crate::persist::PersistVaultPortfolioRow> {
        let mut out = Vec::new();
        for entry in self.vault_portfolio.iter() {
            let vault_id = entry.key().to_string();
            for (spot_market, amount_raw) in entry.value().iter() {
                if *amount_raw == 0 {
                    continue;
                }
                out.push(crate::persist::PersistVaultPortfolioRow {
                    vault_id: vault_id.clone(),
                    spot_market: spot_market.clone(),
                    amount_raw: *amount_raw,
                });
            }
        }
        out
    }

    pub async fn import_vault_portfolio_for_persist(
        &self,
        rows: Vec<crate::persist::PersistVaultPortfolioRow>,
    ) {
        self.vault_portfolio.clear();
        for row in rows {
            let Ok(vault_id) = Uuid::parse_str(&row.vault_id) else {
                tracing::warn!(vault_id = %row.vault_id, "skip vault portfolio row with bad id");
                continue;
            };
            let spot = normalize_spot_market_key(&row.spot_market);
            if row.amount_raw == 0 {
                continue;
            }
            self.vault_portfolio
                .entry(vault_id)
                .or_default()
                .insert(spot, row.amount_raw);
        }
    }

    pub async fn vault_count(&self) -> usize {
        self.vaults.len()
    }

    pub async fn query_vaults(&self, query: VaultQuery) -> (Vec<Vault>, usize) {
        let mut vaults: Vec<Vault> = self.vaults.iter().map(|e| e.value().clone()).collect();

        if let Some(manager) = query.manager.as_deref() {
            let manager = manager.trim().to_ascii_lowercase();
            vaults.retain(|vault| vault.manager.trim().to_ascii_lowercase() == manager);
        }

        if !query.vault_addresses.is_empty() {
            let allowed: HashSet<String> = query
                .vault_addresses
                .iter()
                .map(|address| address.trim().to_ascii_lowercase())
                .collect();
            vaults.retain(|vault| {
                allowed.contains(&vault.vault_address.trim().to_ascii_lowercase())
            });
        }

        vaults.sort_by(|left, right| left.vault_address.cmp(&right.vault_address));

        let total = vaults.len();
        let offset = query.offset as usize;
        let page = vaults
            .into_iter()
            .skip(offset)
            .take(query.limit as usize)
            .collect();

        (page, total)
    }

    pub async fn get_vault(&self, id: Uuid) -> Option<Vault> {
        self.vaults.get(&id).map(|e| e.value().clone())
    }

    pub async fn get_vault_by_address(&self, vault_address: &str) -> Option<Vault> {
        let key = addr_key(vault_address);
        let id = self.vault_by_address.get(&key)?.clone();
        self.vaults.get(&id).map(|e| e.value().clone())
    }

    pub async fn upsert_vault(&self, vault: Vault, mut ws: Option<&mut IndexWriteSet>) {
        let address_key = addr_key(&vault.vault_address);
        let account_key = addr_key(&vault.vault_account);
        self.vault_by_address.insert(address_key, vault.id);
        self.vault_by_account.insert(account_key, vault.id);
        self.vault_portfolio.entry(vault.id).or_default();
        if let Some(ws) = ws {
            ws.store_vault(vault.clone());
        }
        self.vaults.insert(vault.id, vault);
    }

    pub async fn apply_vault_fill_to_portfolio(
        &self,
        user_address: &str,
        spot_market: &str,
        side: lightpool_sdk::OrderSide,
        fill_amount: u64,
        mut ws: Option<&mut IndexWriteSet>,
    ) {
        if fill_amount == 0 {
            return;
        }
        let account_key = user_key(user_address);
        let market_key = normalize_spot_market_key(spot_market);
        let Some(vault_id) = self.vault_by_account.get(&account_key).map(|e| *e) else {
            return;
        };
        let mut holdings = self.vault_portfolio.entry(vault_id).or_default();
        let current = holdings.get(&market_key).copied().unwrap_or(0);
        let next = match side {
            lightpool_sdk::OrderSide::Buy => current.saturating_add(fill_amount),
            lightpool_sdk::OrderSide::Sell => {
                if fill_amount > current {
                    tracing::warn!(
                        user_address,
                        spot_market = %market_key,
                        current,
                        fill_amount,
                        "vault portfolio sell underflow; clamping to zero"
                    );
                    0
                } else {
                    current - fill_amount
                }
            }
        };
        if next == 0 {
            holdings.remove(&market_key);
        } else {
            holdings.insert(market_key.clone(), next);
        }
        let vault_id_str = vault_id.to_string();
        drop(holdings);
        if let Some(ws) = ws {
            ws.store_vault_portfolio(&vault_id_str, &market_key, next as i64);
        }
    }

    pub async fn vault_portfolio_holdings(&self, vault_address: &str) -> Vec<(String, u64)> {
        let key = addr_key(vault_address);
        let Some(vault_id) = self.vault_by_address.get(&key).map(|e| *e) else {
            return Vec::new();
        };
        let Some(holdings) = self.vault_portfolio.get(&vault_id) else {
            return Vec::new();
        };
        let mut assets: Vec<(String, u64)> = holdings
            .iter()
            .filter(|(_, amount)| **amount > 0)
            .map(|(market, amount)| (market.clone(), *amount))
            .collect();
        assets.sort_by(|left, right| left.0.cmp(&right.0));
        assets
    }

    pub async fn update_vault_equity(&self, vault_address: &str, equity: &str, mut ws: Option<&mut IndexWriteSet>) {
        let key = addr_key(vault_address);
        let Some(id) = self.vault_by_address.get(&key).map(|e| *e) else {
            return;
        };
        if let Some(mut vault) = self.vaults.get_mut(&id) {
            vault.equity = equity.to_string();
            if let Some(ws) = ws { ws.store_vault(vault.clone()); }
        }
    }

    pub async fn update_vault_allow_deposit(&self, vault_address: &str, allow_deposit: bool, mut ws: Option<&mut IndexWriteSet>) {
        let key = addr_key(vault_address);
        let Some(id) = self.vault_by_address.get(&key).map(|e| *e) else {
            return;
        };
        if let Some(mut vault) = self.vaults.get_mut(&id) {
            vault.allow_deposit = allow_deposit;
            if let Some(ws) = ws { ws.store_vault(vault.clone()); }
        }
    }

    pub async fn update_vault_manager(&self, vault_address: &str, manager: &str, mut ws: Option<&mut IndexWriteSet>) {
        let key = addr_key(vault_address);
        let Some(id) = self.vault_by_address.get(&key).map(|e| *e) else {
            return;
        };
        if let Some(mut vault) = self.vaults.get_mut(&id) {
            vault.manager = manager.to_string();
            if let Some(ws) = ws { ws.store_vault(vault.clone()); }
        }
    }

    pub async fn mark_vault_closed(&self, vault_address: &str, mut ws: Option<&mut IndexWriteSet>) {
        let key = addr_key(vault_address);
        let Some(id) = self.vault_by_address.get(&key).map(|e| *e) else {
            return;
        };
        if let Some(mut vault) = self.vaults.get_mut(&id) {
            vault.is_closed = true;
            vault.allow_deposit = false;
            if let Some(ws) = ws { ws.store_vault(vault.clone()); }
        }
    }

    pub async fn query_markets(&self, query: MarketQuery) -> (Vec<Market>, usize) {
        let mut markets: Vec<Market> = self.markets.iter().map(|e| e.value().clone()).collect();

        if let Some(slug) = query.slug.as_deref() {
            markets.retain(|market| market.slug() == slug || market.name() == slug);
        }

        if !query.slugs.is_empty() {
            let allowed: HashSet<&str> = query.slugs.iter().map(String::as_str).collect();
            markets.retain(|market| {
                allowed.contains(market.slug()) || allowed.contains(market.name())
            });
        }

        if !query.market_ids.is_empty() {
            let allowed: HashSet<Uuid> = query.market_ids.iter().copied().collect();
            markets.retain(|market| allowed.contains(&market.id()));
        }

        if !query.market_addresses.is_empty() {
            let allowed: HashSet<String> = query
                .market_addresses
                .iter()
                .map(|address| address.trim().to_ascii_lowercase())
                .collect();
            markets.retain(|market| {
                allowed.contains(&market.market_address().trim().to_ascii_lowercase())
            });
        }

        if let Some(state) = query.state.as_deref() {
            markets.retain(|market| market.state().eq_ignore_ascii_case(state));
        }

        if let Some(category) = query.category {
            markets.retain(|market| market.category() == category);
        }

        if let Some(deployer) = query.deployer.as_deref() {
            let want = deployer.trim().to_ascii_lowercase();
            markets.retain(|market| market.deployer().trim().to_ascii_lowercase() == want);
        }

        match query.order {
            MarketSortOrder::ResolutionDeadline => {
                if query.ascending {
                    markets.sort_by_key(|market| market.resolution_deadline());
                } else {
                    markets.sort_by_key(|market| std::cmp::Reverse(market.resolution_deadline()));
                }
            }
            MarketSortOrder::Slug => {
                if query.ascending {
                    markets.sort_by(|left, right| {
                        left.slug()
                            .cmp(right.slug())
                            .then_with(|| left.name().cmp(right.name()))
                    });
                } else {
                    markets.sort_by(|left, right| {
                        right
                            .slug()
                            .cmp(left.slug())
                            .then_with(|| right.name().cmp(left.name()))
                    });
                }
            }
            MarketSortOrder::Question => {
                if query.ascending {
                    markets.sort_by(|left, right| left.label().cmp(right.label()));
                } else {
                    markets.sort_by(|left, right| right.label().cmp(left.label()));
                }
            }
        }

        let total = markets.len();
        let offset = query.offset as usize;
        let page = markets
            .into_iter()
            .skip(offset)
            .take(query.limit as usize)
            .collect();

        (page, total)
    }

    pub async fn get_market(&self, id: Uuid) -> Option<Market> {
        self.markets.get(&id).map(|e| e.value().clone())
    }

    pub async fn get_event_by_slug(&self, slug: &str) -> Option<Market> {
        let id = self.slug_to_market.get(slug)?.clone();
        self.markets.get(&id).map(|e| e.value().clone())
    }

    pub async fn allocate_market_slug(&self, question: &str) -> String {
        let existing_slugs: Vec<String> = self
            .slug_to_market
            .iter()
            .map(|e| e.key().clone())
            .collect();
        crate::slug::allocate_unique_slug(&existing_slugs, question)
    }

    pub async fn list_orders_for_user(&self, user_address: &str) -> Vec<OrderQueryRecord> {
        let key = user_key(user_address);
        let Some(ids) = self.orders_by_user.get(&key) else {
            return Vec::new();
        };
        let mut out = Vec::new();
        for id in ids.iter() {
            let Some(stored) = self.orders.get(&id) else {
                continue;
            };
            out.push(Self::order_query_record(stored.value()));
        }
        out
    }

    pub async fn position_token_specs(&self) -> Vec<(String, String)> {
        let mut specs = Vec::new();
        for market in self.markets.iter() {
            let yes = market.yes_token();
            let no = market.no_token();
            if !yes.is_empty() {
                specs.push(("YES".into(), yes.to_string()));
            }
            if !no.is_empty() {
                specs.push(("NO".into(), no.to_string()));
            }
        }
        specs
    }

    fn remove_slug_mappings_for_market(&self, market_id: Uuid) {
        self.slug_to_market.retain(|_, id| *id != market_id);
    }

    pub async fn upsert_market(&self, mut market: Market, mut ws: Option<&mut IndexWriteSet>) {
        if let Some(existing) = self.markets.get(&market.id()) {
            market.set_icon_url_if_empty(existing.icon_url().cloned());
            if let (Some(slot), existing_name) = (market.name_mut(), existing.name()) {
                if slot.is_empty() && !existing_name.is_empty() {
                    *slot = existing_name.to_string();
                }
            }
            if let (Some(slot), existing_slug) = (market.slug_mut(), existing.slug()) {
                if slot.is_empty() && !existing_slug.is_empty() {
                    *slot = existing_slug.to_string();
                }
            }
        }

        if !market.slug().is_empty() {
            self.remove_slug_mappings_for_market(market.id());
            self.slug_to_market
                .insert(market.slug().to_string(), market.id());
        }

        let yes_spot = normalize_spot_market_key(market.yes_spot_market());
        let no_spot = normalize_spot_market_key(market.no_spot_market());
        if let Some(slot) = market.yes_spot_market_mut() {
            *slot = yes_spot.clone();
        }
        if let Some(slot) = market.no_spot_market_mut() {
            *slot = no_spot.clone();
        }
        if let Market::Spot {
            market_address, ..
        } = &mut market
        {
            *market_address = yes_spot.clone();
        }
        if let Market::Perp {
            market_address, ..
        } = &mut market
        {
            *market_address = yes_spot.clone();
        }

        if matches!(market, Market::Event { .. }) && yes_spot == no_spot {
            tracing::warn!(
                market_id = %market.id(),
                slug = %market.slug(),
                spot_market = %yes_spot,
                "event market yes and no spot markets share the same address"
            );
        }

        let market_id = market.id();
        let category = market.category();
        if let Some(ws) = ws.as_deref_mut() {
            ws.store_market(market.clone());
        }
        self.markets.insert(market_id, market);

        match category {
            MarketCategory::Event => {
                self.bind_event_spot_legs(market_id, &yes_spot, &no_spot, ws);
            }
            MarketCategory::Spot => {
                self.spot_to_market.insert(
                    yes_spot,
                    SpotMarketRef {
                        market_id,
                        outcome: "spot".into(),
                    },
                );
            }
            MarketCategory::Perp => {
                self.spot_to_market.insert(
                    yes_spot,
                    SpotMarketRef {
                        market_id,
                        outcome: "perp".into(),
                    },
                );
            }
        }
    }

    /// Point yes/no spot venues at an event instrument; drop orphan spot shells for those addresses.
    fn bind_event_spot_legs(&self, event_id: Uuid, yes_spot: &str, no_spot: &str, mut ws: Option<&mut IndexWriteSet>) {
        for (spot, outcome) in [(yes_spot, "yes"), (no_spot, "no")] {
            self.remove_orphan_spot_shell(spot, ws.as_deref_mut());
            self.spot_to_market.insert(
                spot.to_string(),
                SpotMarketRef {
                    market_id: event_id,
                    outcome: outcome.into(),
                },
            );
        }
    }

    fn remove_orphan_spot_shell(&self, spot_market: &str, mut ws: Option<&mut IndexWriteSet>) {
        let orphan_id = market_uuid(spot_market);
        let Some((_, orphan)) = self.markets.remove(&orphan_id) else {
            return;
        };
        if orphan.category() != MarketCategory::Spot {
            self.markets.insert(orphan_id, orphan);
            return;
        }
        // Spot shells are not in slug_to_market; event slug entries are left untouched.
        if let Some(ws) = ws { ws.delete_market(&orphan_id.to_string()); }
    }

    pub async fn update_market_state(&self, market_address: &str, state: &str, mut ws: Option<&mut IndexWriteSet>) {
        for mut market in self.markets.iter_mut() {
            if market.market_address() == market_address {
                *market.state_mut() = state.to_string();
                if let Some(ws) = ws.as_deref_mut() {
                    ws.store_market(market.clone());
                }
            }
        }
    }

    pub async fn lookup_spot_market(&self, spot_market: &str) -> Option<(Uuid, String)> {
        let key = normalize_spot_market_key(spot_market);
        self.spot_to_market
            .get(&key)
            .map(|spot| (spot.market_id, spot.outcome.clone()))
    }

    pub async fn list_spot_markets(&self) -> Vec<String> {
        self.spot_to_market
            .iter()
            .map(|e| e.key().clone())
            .collect()
    }

    pub async fn register_named_spot_market(
        &self,
        name: &str,
        spot_market: &str,
        deployer: &str,
    ) {
        let spot = normalize_spot_market_key(spot_market);
        let name_key = name.trim().to_ascii_uppercase();
        if name_key.is_empty() || spot.is_empty() {
            return;
        }

        self.spot_by_name.insert(name_key, spot.clone());

        // Already an event (or other) leg — keep name indexes only, do not create a spot shell.
        if let Some(existing) = self.spot_to_market.get(&spot) {
            if let Some(market) = self.markets.get(&existing.market_id) {
                if market.category() != MarketCategory::Spot {
                    return;
                }
            }
        }

        let market_id = market_uuid(&spot);
        self.spot_to_market
            .entry(spot.clone())
            .or_insert(SpotMarketRef {
                market_id,
                outcome: "spot".into(),
            });
        self.markets.entry(market_id).or_insert_with(|| Market::Spot {
            id: market_id,
            name: name.trim().to_string(),
            icon_url: None,
            market_address: spot.clone(),
            state: "Active".into(),
            deployer: deployer.trim().to_string(),
            base_token: String::new(),
            quote_token: String::new(),
        });
    }

    pub async fn ensure_standalone_spot_market(&self, spot_market: &str) -> (Uuid, String) {
        let spot = normalize_spot_market_key(spot_market);
        if let Some(existing) = self.lookup_spot_market(&spot).await {
            return existing;
        }
        let name = self
            .spot_by_name
            .iter()
            .find(|e| e.value() == &spot)
            .map(|e| e.key().clone())
            .unwrap_or_else(|| spot.clone());
        self.register_named_spot_market(&name, &spot, "").await;
        self.lookup_spot_market(&spot)
            .await
            .unwrap_or_else(|| (market_uuid(&spot), "spot".into()))
    }

    pub async fn resolve_spot_market_key(&self, id_or_name: &str) -> Option<String> {
        let key = id_or_name.trim();
        if key.is_empty() {
            return None;
        }
        if key.starts_with("0x") || key.starts_with("0X") {
            return Some(normalize_spot_market_key(key));
        }

        let upper = key.to_ascii_uppercase();
        self.spot_by_name.get(&upper).map(|e| e.value().clone())
    }

    pub async fn record_last_trade_price(&self, spot_market: &str, price: u64, mut ws: Option<&mut IndexWriteSet>) {
        let key = normalize_spot_market_key(spot_market);
        self.last_trade_price.insert(key.clone(), price);
        if let Some(ws) = ws { ws.store_last_trade(&key, price as i64); }
    }

    pub async fn last_trade_price(&self, spot_market: &str) -> Option<u64> {
        let key = normalize_spot_market_key(spot_market);
        self.last_trade_price.get(&key).map(|e| *e)
    }

    pub async fn has_chain_order(&self, spot_market: &str, chain_order_id: &str) -> bool {
        let key = onchain_order_id(spot_market, chain_order_id);
        self.orders_by_id.contains_key(&key)
    }

    pub async fn lookup_spot_market_for_chain_order(&self, chain_order_id: &str) -> Option<String> {
        let suffix = format!(":{chain_order_id}");
        let mut matches = self.orders_by_id.iter().filter_map(|e| {
            let key = e.key().as_str();
            if !key.ends_with(&suffix) {
                return None;
            }
            split_onchain_order_id(key).map(|(spot, _)| spot.to_string())
        });

        let spot = matches.next()?;
        if matches.next().is_some() {
            return None;
        }
        Some(spot)
    }

    pub async fn order_cancel_context(
        &self,
        order_id: Uuid,
        user_address: &str,
    ) -> Option<(Order, String, String)> {
        let stored = self.orders.get(&order_id)?;
        if !stored.user_address.eq_ignore_ascii_case(user_address) {
            return None;
        }
        if stored.order.status != "open" && stored.order.status != "partial_filled" {
            return None;
        }
        Some((
            stored.order.clone(),
            stored.chain_order_id.clone(),
            stored.spot_market.clone(),
        ))
    }

    pub async fn stored_order_context_by_id(
        &self,
        order_id: Uuid,
        user_address: &str,
    ) -> Option<(Order, String, String)> {
        let stored = self.orders.get(&order_id)?;
        if !stored.user_address.eq_ignore_ascii_case(user_address) {
            return None;
        }
        Some((
            stored.order.clone(),
            stored.chain_order_id.clone(),
            stored.spot_market.clone(),
        ))
    }

    pub async fn insert_order(
        &self,
        order: Order,
        user_address: String,
        spot_market: &str,
        chain_order_id: String,
        size_raw: u64,
        filled_raw: u64,
        mut ws: Option<&mut IndexWriteSet>,
    ) {
        let spot = normalize_spot_market_key(spot_market);
        let stored = StoredOrder {
            order: order.clone(),
            user_address,
            chain_order_id: chain_order_id.clone(),
            spot_market: spot.clone(),
            filled_raw,
            size_raw,
        };
        if !is_hot_order_status(&stored.order.status) {
            self.write_order_history(&stored_to_persist_row(&stored));
            return;
        }
        let user = user_key(&stored.user_address);
        let order_id = stored.order.id;
        let id = onchain_order_id(&spot, &chain_order_id);
        self.orders_by_id.insert(id, order_id);
        self.orders_by_user
            .entry(user)
            .or_default()
            .insert(order_id);
        if let Some(ws) = ws {
            ws.store_order(order_row_from_parts(
                stored.order.clone(),
                stored.user_address.clone(),
                stored.chain_order_id.clone(),
                stored.spot_market.clone(),
                stored.size_raw,
                stored.filled_raw,
            ));
        }
        self.orders.insert(order_id, stored);
    }

    fn write_order_history(&self, row: &PersistOrderRow) {
        let Some(persist) = &self.persist else {
            return;
        };
        let mut row = row.clone();
        if row.status_ts_ms == 0 {
            let ms = self.indexing_block_timestamp_ms();
            if ms > 0 {
                row.status_ts_ms = ms;
            }
        }
        persist.enqueue_order_history(row);
    }

    fn remove_hot_order(&self, order_id: Uuid, stored: &StoredOrder) {
        let id = onchain_order_id(&stored.spot_market, &stored.chain_order_id);
        self.orders_by_id.remove(&id);
        let user = user_key(&stored.user_address);
        if let Some(set) = self.orders_by_user.get(&user) {
            set.remove(&order_id);
        }
        self.orders.remove(&order_id);
    }

    fn archive_terminal_order(&self, order_id: Uuid, stored: StoredOrder, mut ws: Option<&mut IndexWriteSet>) {
        let row = stored_to_persist_row(&stored);
        self.remove_hot_order(order_id, &stored);
        if let Some(ws) = ws { ws.delete_order(&order_id.to_string()); }
        self.write_order_history(&row);
    }

    pub async fn query_order(
        &self,
        spot_market: &str,
        chain_order_id: &str,
        user_address: Option<&str>,
    ) -> Option<OrderQueryRecord> {
        let key = onchain_order_id(spot_market, chain_order_id);
        let order_id = self.orders_by_id.get(&key)?.clone();
        let stored = self.orders.get(&order_id)?;
        if let Some(user) = user_address {
            if !stored.user_address.eq_ignore_ascii_case(user) {
                return None;
            }
        }
        Some(Self::order_query_record(stored.value()))
    }

    pub async fn find_open_order_match(
        &self,
        spot_market: &str,
        user_address: &str,
        side: &str,
        price: &str,
        size_raw: u64,
    ) -> Option<OrderQueryRecord> {
        let spot = normalize_spot_market_key(spot_market);
        let key = user_key(user_address);
        let ids = self.orders_by_user.get(&key)?;
        for id in ids.iter() {
            let Some(stored) = self.orders.get(&id) else {
                continue;
            };
            if stored.order.status != "open" && stored.order.status != "partial_filled" {
                continue;
            }
            if stored.spot_market != spot {
                continue;
            }
            if stored.order.side != side || stored.order.price != price {
                continue;
            }
            if stored.size_raw != size_raw {
                continue;
            }
            return Some(Self::order_query_record(stored.value()));
        }
        None
    }

    fn order_query_record(stored: &StoredOrder) -> OrderQueryRecord {
        OrderQueryRecord {
            order: stored.order.clone(),
            chain_order_id: stored.chain_order_id.clone(),
            spot_market: stored.spot_market.clone(),
            user_address: stored.user_address.clone(),
            size_raw: stored.size_raw,
            filled_raw: stored.filled_raw,
        }
    }

    pub async fn stored_order_by_chain_id(
        &self,
        spot_market: &str,
        chain_order_id: &str,
    ) -> Option<(Order, String, String)> {
        let key = onchain_order_id(spot_market, chain_order_id);
        let order_id = self.orders_by_id.get(&key)?.clone();
        let stored = self.orders.get(&order_id)?;
        Some((
            stored.order.clone(),
            stored.user_address.clone(),
            stored.spot_market.clone(),
        ))
    }

    pub async fn update_order_cancelled(&self, spot_market: &str, chain_order_id: &str, mut ws: Option<&mut IndexWriteSet>) {
        let key = onchain_order_id(spot_market, chain_order_id);
        let Some(order_id) = self.orders_by_id.get(&key).map(|e| *e) else {
            return;
        };
        let Some(mut stored) = self.orders.get_mut(&order_id) else {
            return;
        };
        stored.order.status = "cancelled".into();
        let snapshot = stored.clone();
        drop(stored);
        self.archive_terminal_order(order_id, snapshot, ws);
    }

    pub async fn update_order_amount(
        &self,
        spot_market: &str,
        chain_order_id: &str,
        new_amount: u64,
        remaining_amount: u64,
        mut ws: Option<&mut IndexWriteSet>,
    ) {
        let key = onchain_order_id(spot_market, chain_order_id);
        let Some(order_id) = self.orders_by_id.get(&key).map(|e| *e) else {
            return;
        };
        let Some(mut stored) = self.orders.get_mut(&order_id) else {
            return;
        };

        stored.size_raw = new_amount;
        stored.order.size = crate::chain::format_token_amount(new_amount);
        stored.order.status = if remaining_amount == 0 {
            "filled".into()
        } else if stored.filled_raw > 0 {
            "partial_filled".into()
        } else {
            "open".into()
        };
        let terminal = !is_hot_order_status(&stored.order.status);
        let snapshot = if terminal {
            Some(stored.clone())
        } else {
            if let Some(ws) = ws.as_deref_mut() {
                ws.store_order(order_row_from_parts(
                    stored.order.clone(),
                    stored.user_address.clone(),
                    stored.chain_order_id.clone(),
                    stored.spot_market.clone(),
                    stored.size_raw,
                    stored.filled_raw,
                ));
            }
            None
        };
        drop(stored);
        if let Some(snapshot) = snapshot {
            self.archive_terminal_order(order_id, snapshot, ws);
        }
    }

    pub async fn update_order_fill(
        &self,
        spot_market: &str,
        chain_order_id: &str,
        fill_amount: u64,
        remaining_amount: u64,
        is_fully_filled: bool,
        mut ws: Option<&mut IndexWriteSet>,
    ) {
        let key = onchain_order_id(spot_market, chain_order_id);
        let Some(order_id) = self.orders_by_id.get(&key).map(|e| *e) else {
            return;
        };
        let Some(mut stored) = self.orders.get_mut(&order_id) else {
            return;
        };

        stored.filled_raw = stored.filled_raw.saturating_add(fill_amount);
        stored.order.status = if is_fully_filled || remaining_amount == 0 {
            "filled".into()
        } else {
            "partial_filled".into()
        };
        let terminal = !is_hot_order_status(&stored.order.status);
        let snapshot = if terminal {
            Some(stored.clone())
        } else {
            if let Some(ws) = ws.as_deref_mut() {
                ws.store_order(order_row_from_parts(
                    stored.order.clone(),
                    stored.user_address.clone(),
                    stored.chain_order_id.clone(),
                    stored.spot_market.clone(),
                    stored.size_raw,
                    stored.filled_raw,
                ));
            }
            None
        };
        drop(stored);
        if let Some(snapshot) = snapshot {
            self.archive_terminal_order(order_id, snapshot, ws);
        }
    }
}

/// Rebuild secondary order indexes after bulk-loading `orders` without `insert_order`.
#[allow(dead_code)]
pub fn rebuild_order_indexes(store: &IndexState) {
    store.orders_by_id.clear();
    store.orders_by_user.clear();
    for entry in store.orders.iter() {
        let order_uuid = *entry.key();
        let stored = entry.value();
        let id = onchain_order_id(&stored.spot_market, &stored.chain_order_id);
        store.orders_by_id.insert(id, order_uuid);
        store
            .orders_by_user
            .entry(user_key(&stored.user_address))
            .or_default()
            .insert(order_uuid);
    }
}

pub fn new_head() -> SharedIndexedBlockHead {
    Arc::new(TokioRwLock::new(IndexedBlockHead::default()))
}

pub fn market_uuid(market_address: &str) -> Uuid {
    Uuid::new_v5(&Uuid::NAMESPACE_OID, market_address.as_bytes())
}

pub fn vault_uuid(vault_address: &str) -> Uuid {
    Uuid::new_v5(
        &Uuid::NAMESPACE_OID,
        format!("vault:{vault_address}").as_bytes(),
    )
}
