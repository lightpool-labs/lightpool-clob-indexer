// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use tokio::sync::RwLock;
use uuid::Uuid;

use crate::domain::{Market, MarketQuery, MarketSortOrder, Order, Vault, VaultQuery};
use crate::spot_market::{chain_order_key, normalize_spot_market_key};

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

pub type SharedIndexedBlockHead = Arc<RwLock<IndexedBlockHead>>;

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

#[derive(Default)]
struct IndexStoreInner {
    markets: HashMap<Uuid, Market>,
    slug_to_market_id: HashMap<String, Uuid>,
    spot_to_market: HashMap<String, SpotMarketRef>,
    /// Spot pair name (e.g. `AAPL/USDT`) → spot `ContractAddress` key.
    spot_by_name: HashMap<String, String>,
    /// Base symbol (e.g. `AAPL`) → spot `ContractAddress` key.
    spot_by_symbol: HashMap<String, String>,
    last_trade_price_by_spot: HashMap<String, u64>,
    orders: HashMap<Uuid, StoredOrder>,
    chain_order_index: HashMap<String, Uuid>,
    vaults: HashMap<Uuid, Vault>,
    vault_address_index: HashMap<String, Uuid>,
    vault_account_index: HashMap<String, Uuid>,
    /// Indexed vault spot holdings: vault_id -> spot_market -> base amount (raw).
    vault_portfolio: HashMap<Uuid, HashMap<String, u64>>,
}

pub struct IndexStore {
    inner: RwLock<IndexStoreInner>,
}

pub type SharedIndexStore = Arc<IndexStore>;

impl IndexStore {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(IndexStoreInner::default()),
        }
    }

    pub async fn clear_all(&self) {
        *self.inner.write().await = IndexStoreInner::default();
    }

    pub async fn market_count(&self) -> usize {
        self.inner.read().await.markets.len()
    }

    pub async fn export_markets_for_persist(&self) -> Vec<Market> {
        self.inner.read().await.markets.values().cloned().collect()
    }

    pub async fn export_orders_for_persist(&self) -> Vec<crate::persist::PersistOrderRow> {
        let inner = self.inner.read().await;
        let mut out = Vec::new();
        for stored in inner.orders.values() {
            let Some(market) = inner.markets.get(&stored.order.market_id) else {
                continue;
            };
            let spot_market = if stored.order.outcome == "yes" {
                market.yes_spot_market.clone()
            } else {
                market.no_spot_market.clone()
            };
            out.push(crate::persist::PersistOrderRow {
                order: stored.order.clone(),
                user_address: stored.user_address.clone(),
                chain_order_id: stored.chain_order_id.clone(),
                spot_market,
                size_raw: stored.size_raw,
                filled_raw: stored.filled_raw,
            });
        }
        out
    }

    pub async fn export_last_trades_for_persist(&self) -> Vec<(String, u64)> {
        self.inner
            .read()
            .await
            .last_trade_price_by_spot
            .iter()
            .map(|(spot, price)| (spot.clone(), *price))
            .collect()
    }

    pub async fn export_vaults_for_persist(&self) -> Vec<Vault> {
        self.inner.read().await.vaults.values().cloned().collect()
    }

    pub async fn export_vault_portfolio_for_persist(
        &self,
    ) -> Vec<crate::persist::PersistVaultPortfolioRow> {
        let inner = self.inner.read().await;
        let mut out = Vec::new();
        for (vault_id, holdings) in &inner.vault_portfolio {
            for (spot_market, amount_raw) in holdings {
                if *amount_raw == 0 {
                    continue;
                }
                out.push(crate::persist::PersistVaultPortfolioRow {
                    vault_id: vault_id.to_string(),
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
        let mut inner = self.inner.write().await;
        inner.vault_portfolio.clear();
        for row in rows {
            let Ok(vault_id) = Uuid::parse_str(&row.vault_id) else {
                tracing::warn!(vault_id = %row.vault_id, "skip vault portfolio row with bad id");
                continue;
            };
            let spot = normalize_spot_market_key(&row.spot_market);
            if row.amount_raw == 0 {
                continue;
            }
            inner
                .vault_portfolio
                .entry(vault_id)
                .or_default()
                .insert(spot, row.amount_raw);
        }
    }

    pub async fn vault_count(&self) -> usize {
        self.inner.read().await.vaults.len()
    }

    pub async fn query_vaults(&self, query: VaultQuery) -> (Vec<Vault>, usize) {
        let inner = self.inner.read().await;
        let mut vaults: Vec<Vault> = inner.vaults.values().cloned().collect();

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
        self.inner.read().await.vaults.get(&id).cloned()
    }

    pub async fn get_vault_by_address(&self, vault_address: &str) -> Option<Vault> {
        let key = vault_address.trim().to_ascii_lowercase();
        let inner = self.inner.read().await;
        inner
            .vault_address_index
            .get(&key)
            .and_then(|id| inner.vaults.get(id).cloned())
    }

    pub async fn upsert_vault(&self, vault: Vault) {
        let mut inner = self.inner.write().await;
        let address_key = vault.vault_address.trim().to_ascii_lowercase();
        let account_key = vault.vault_account.trim().to_ascii_lowercase();
        inner.vault_address_index.insert(address_key, vault.id);
        inner.vault_account_index.insert(account_key, vault.id);
        inner.vault_portfolio.entry(vault.id).or_default();
        inner.vaults.insert(vault.id, vault);
    }

    pub async fn apply_vault_fill_to_portfolio(
        &self,
        user_address: &str,
        spot_market: &str,
        side: lightpool_sdk::OrderSide,
        fill_amount: u64,
    ) {
        if fill_amount == 0 {
            return;
        }
        let account_key = user_address.trim().to_ascii_lowercase();
        let market_key = normalize_spot_market_key(spot_market);
        let mut inner = self.inner.write().await;
        let Some(vault_id) = inner.vault_account_index.get(&account_key).copied() else {
            return;
        };
        let holdings = inner.vault_portfolio.entry(vault_id).or_default();
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
            holdings.insert(market_key, next);
        }
    }

    pub async fn vault_portfolio_holdings(
        &self,
        vault_address: &str,
    ) -> Vec<(String, u64)> {
        let key = vault_address.trim().to_ascii_lowercase();
        let inner = self.inner.read().await;
        let Some(vault_id) = inner.vault_address_index.get(&key).copied() else {
            return Vec::new();
        };
        let Some(holdings) = inner.vault_portfolio.get(&vault_id) else {
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

    pub async fn update_vault_equity(&self, vault_address: &str, equity: &str) {
        let key = vault_address.trim().to_ascii_lowercase();
        let mut inner = self.inner.write().await;
        if let Some(id) = inner.vault_address_index.get(&key).copied() {
            if let Some(vault) = inner.vaults.get_mut(&id) {
                vault.equity = equity.to_string();
            }
        }
    }

    pub async fn update_vault_allow_deposit(&self, vault_address: &str, allow_deposit: bool) {
        let key = vault_address.trim().to_ascii_lowercase();
        let mut inner = self.inner.write().await;
        if let Some(id) = inner.vault_address_index.get(&key).copied() {
            if let Some(vault) = inner.vaults.get_mut(&id) {
                vault.allow_deposit = allow_deposit;
            }
        }
    }

    pub async fn update_vault_manager(&self, vault_address: &str, manager: &str) {
        let key = vault_address.trim().to_ascii_lowercase();
        let mut inner = self.inner.write().await;
        if let Some(id) = inner.vault_address_index.get(&key).copied() {
            if let Some(vault) = inner.vaults.get_mut(&id) {
                vault.manager = manager.to_string();
            }
        }
    }

    pub async fn mark_vault_closed(&self, vault_address: &str) {
        let key = vault_address.trim().to_ascii_lowercase();
        let mut inner = self.inner.write().await;
        if let Some(id) = inner.vault_address_index.get(&key).copied() {
            if let Some(vault) = inner.vaults.get_mut(&id) {
                vault.is_closed = true;
                vault.allow_deposit = false;
            }
        }
    }

    pub async fn query_markets(&self, query: MarketQuery) -> (Vec<Market>, usize) {
        let inner = self.inner.read().await;
        let mut markets: Vec<Market> = inner.markets.values().cloned().collect();

        if let Some(slug) = query.slug.as_deref() {
            markets.retain(|market| market.slug == slug);
        }

        if !query.slugs.is_empty() {
            let allowed: HashSet<&str> = query.slugs.iter().map(String::as_str).collect();
            markets.retain(|market| allowed.contains(market.slug.as_str()));
        }

        if !query.market_ids.is_empty() {
            let allowed: HashSet<Uuid> = query.market_ids.iter().copied().collect();
            markets.retain(|market| allowed.contains(&market.id));
        }

        if !query.market_addresses.is_empty() {
            let allowed: HashSet<String> = query
                .market_addresses
                .iter()
                .map(|address| address.trim().to_ascii_lowercase())
                .collect();
            markets.retain(|market| {
                allowed.contains(&market.market_address.trim().to_ascii_lowercase())
            });
        }

        if let Some(state) = query.state.as_deref() {
            markets.retain(|market| market.state.eq_ignore_ascii_case(state));
        }

        match query.order {
            MarketSortOrder::ResolutionDeadline => {
                if query.ascending {
                    markets.sort_by_key(|market| market.resolution_deadline);
                } else {
                    markets.sort_by_key(|market| std::cmp::Reverse(market.resolution_deadline));
                }
            }
            MarketSortOrder::Slug => {
                if query.ascending {
                    markets.sort_by(|left, right| left.slug.cmp(&right.slug));
                } else {
                    markets.sort_by(|left, right| right.slug.cmp(&left.slug));
                }
            }
            MarketSortOrder::Question => {
                if query.ascending {
                    markets.sort_by(|left, right| left.question.cmp(&right.question));
                } else {
                    markets.sort_by(|left, right| right.question.cmp(&left.question));
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
        self.inner.read().await.markets.get(&id).cloned()
    }

    pub async fn get_event_by_slug(&self, slug: &str) -> Option<Market> {
        let inner = self.inner.read().await;
        inner
            .slug_to_market_id
            .get(slug)
            .and_then(|id| inner.markets.get(id).cloned())
    }

    pub async fn allocate_market_slug(&self, question: &str) -> String {
        let inner = self.inner.read().await;
        let existing_slugs: Vec<String> = inner.slug_to_market_id.keys().cloned().collect();
        crate::slug::allocate_unique_slug(&existing_slugs, question)
    }

    pub async fn list_orders_for_user(&self, user_address: &str) -> Vec<OrderQueryRecord> {
        let inner = self.inner.read().await;
        let mut out = Vec::new();
        for stored in inner.orders.values() {
            if !stored.user_address.eq_ignore_ascii_case(user_address) {
                continue;
            }
            let Some(market) = inner.markets.get(&stored.order.market_id) else {
                continue;
            };
            let spot_market = if stored.order.outcome == "yes" {
                market.yes_spot_market.clone()
            } else {
                market.no_spot_market.clone()
            };
            out.push(Self::order_query_record(
                stored,
                normalize_spot_market_key(&spot_market),
            ));
        }
        out
    }


    pub async fn position_token_specs(&self) -> Vec<(String, String)> {
        let inner = self.inner.read().await;
        let mut specs = Vec::new();

        for market in inner.markets.values() {
            specs.push(("YES".into(), market.yes_token.clone()));
            specs.push(("NO".into(), market.no_token.clone()));
        }

        specs
    }

    fn remove_slug_mappings_for_market(inner: &mut IndexStoreInner, market_id: Uuid) {
        inner.slug_to_market_id.retain(|_, id| *id != market_id);
    }

    pub async fn upsert_market(&self, mut market: Market) {
        let mut inner = self.inner.write().await;
        if let Some(existing) = inner.markets.get(&market.id) {
            if market.icon_url.is_none() {
                market.icon_url = existing.icon_url.clone();
            }
            if market.slug.is_empty() {
                market.slug = existing.slug.clone();
            }
        }

        if !market.slug.is_empty() {
            Self::remove_slug_mappings_for_market(&mut inner, market.id);
            inner
                .slug_to_market_id
                .insert(market.slug.clone(), market.id);
        }

        let yes_spot = normalize_spot_market_key(&market.yes_spot_market);
        let no_spot = normalize_spot_market_key(&market.no_spot_market);
        market.yes_spot_market = yes_spot.clone();
        market.no_spot_market = no_spot.clone();

        if yes_spot == no_spot {
            tracing::warn!(
                market_id = %market.id,
                slug = %market.slug,
                spot_market = %yes_spot,
                "yes and no spot markets share the same address"
            );
        }

        inner.spot_to_market.insert(
            yes_spot,
            SpotMarketRef {
                market_id: market.id,
                outcome: "yes".into(),
            },
        );
        inner.spot_to_market.insert(
            no_spot,
            SpotMarketRef {
                market_id: market.id,
                outcome: "no".into(),
            },
        );
        inner.markets.insert(market.id, market);
    }

    pub async fn update_market_state(&self, market_address: &str, state: &str) {
        let mut inner = self.inner.write().await;
        for market in inner.markets.values_mut() {
            if market.market_address == market_address {
                market.state = state.to_string();
            }
        }
    }

    pub async fn lookup_spot_market(&self, spot_market: &str) -> Option<(Uuid, String)> {
        let key = normalize_spot_market_key(spot_market);
        let inner = self.inner.read().await;
        inner
            .spot_to_market
            .get(&key)
            .map(|spot| (spot.market_id, spot.outcome.clone()))
    }

    pub async fn list_spot_markets(&self) -> Vec<String> {
        let inner = self.inner.read().await;
        inner.spot_to_market.keys().cloned().collect()
    }

    pub async fn register_named_spot_market(&self, name: &str, spot_market: &str) {
        let spot = normalize_spot_market_key(spot_market);
        let name_key = name.trim().to_ascii_uppercase();
        if name_key.is_empty() || spot.is_empty() {
            return;
        }

        let mut inner = self.inner.write().await;
        inner.spot_by_name.insert(name_key.clone(), spot.clone());
        let symbol = if let Some((symbol, _)) = name_key.split_once('/') {
            if !symbol.is_empty() {
                inner.spot_by_symbol.insert(symbol.to_string(), spot.clone());
                symbol.to_string()
            } else {
                inner.spot_by_symbol.insert(name_key.clone(), spot.clone());
                name_key.clone()
            }
        } else {
            inner.spot_by_symbol.insert(name_key.clone(), spot.clone());
            name_key.clone()
        };

        // Equity / standalone spots are not event-contract yes/no markets. Still map them so
        // `GET /api/orders` and the user WS channel can index placements (book alone is not enough).
        let market_id = market_uuid(&spot);
        inner.spot_to_market.entry(spot.clone()).or_insert(SpotMarketRef {
            market_id,
            outcome: "spot".into(),
        });
        inner.markets.entry(market_id).or_insert_with(|| Market {
            id: market_id,
            slug: symbol.to_ascii_lowercase(),
            question: name.trim().to_string(),
            icon_url: None,
            market_address: spot.clone(),
            collateral_token: String::new(),
            yes_token: String::new(),
            no_token: String::new(),
            yes_spot_market: spot.clone(),
            no_spot_market: spot,
            state: "Active".into(),
            resolution_deadline: 0,
        });
        inner
            .slug_to_market_id
            .entry(symbol.to_ascii_lowercase())
            .or_insert(market_id);
    }

    /// Ensure a standalone spot (tokenized equity) is resolvable for order indexing.
    pub async fn ensure_standalone_spot_market(&self, spot_market: &str) -> (Uuid, String) {
        let spot = normalize_spot_market_key(spot_market);
        if let Some(existing) = self.lookup_spot_market(&spot).await {
            return existing;
        }
        let name = {
            let inner = self.inner.read().await;
            inner
                .spot_by_name
                .iter()
                .find(|(_, value)| value == &&spot)
                .map(|(name, _)| name.clone())
                .or_else(|| {
                    inner
                        .spot_by_symbol
                        .iter()
                        .find(|(_, value)| value == &&spot)
                        .map(|(symbol, _)| symbol.clone())
                })
                .unwrap_or_else(|| spot.clone())
        };
        self.register_named_spot_market(&name, &spot).await;
        self.lookup_spot_market(&spot)
            .await
            .unwrap_or_else(|| (market_uuid(&spot), "spot".into()))
    }

    /// Resolve `AAPL`, `AAPL/USDT`, or a spot `ContractAddress` hex to a normalized spot key.
    pub async fn resolve_spot_market_key(&self, id_or_symbol: &str) -> Option<String> {
        let key = id_or_symbol.trim();
        if key.is_empty() {
            return None;
        }
        if key.starts_with("0x") || key.starts_with("0X") {
            return Some(normalize_spot_market_key(key));
        }

        let upper = key.to_ascii_uppercase();
        let inner = self.inner.read().await;
        inner
            .spot_by_symbol
            .get(&upper)
            .or_else(|| inner.spot_by_name.get(&upper))
            .cloned()
    }

    pub async fn record_last_trade_price(&self, spot_market: &str, price: u64) {
        let key = normalize_spot_market_key(spot_market);
        self.inner
            .write()
            .await
            .last_trade_price_by_spot
            .insert(key, price);
    }

    pub async fn last_trade_price(&self, spot_market: &str) -> Option<u64> {
        let key = normalize_spot_market_key(spot_market);
        self.inner
            .read()
            .await
            .last_trade_price_by_spot
            .get(&key)
            .copied()
    }

    pub async fn has_chain_order(&self, spot_market: &str, chain_order_id: &str) -> bool {
        let key = chain_order_key(spot_market, chain_order_id);
        self.inner.read().await.chain_order_index.contains_key(&key)
    }

    pub async fn lookup_spot_market_for_chain_order(&self, chain_order_id: &str) -> Option<String> {
        let inner = self.inner.read().await;
        let suffix = format!(":{chain_order_id}");
        let mut matches = inner
            .chain_order_index
            .keys()
            .filter(|key| key.ends_with(&suffix))
            .filter_map(|key| key.strip_suffix(&suffix))
            .map(str::to_string);

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
        let inner = self.inner.read().await;
        let stored = inner.orders.get(&order_id)?;
        if !stored.user_address.eq_ignore_ascii_case(user_address) {
            return None;
        }
        if stored.order.status != "open" && stored.order.status != "partial_filled" {
            return None;
        }
        let market = inner.markets.get(&stored.order.market_id)?;
        let spot_market = if stored.order.outcome == "yes" {
            market.yes_spot_market.clone()
        } else {
            market.no_spot_market.clone()
        };
        Some((
            stored.order.clone(),
            stored.chain_order_id.clone(),
            spot_market,
        ))
    }

    pub async fn stored_order_context_by_id(
        &self,
        order_id: Uuid,
        user_address: &str,
    ) -> Option<(Order, String, String)> {
        let inner = self.inner.read().await;
        let stored = inner.orders.get(&order_id)?;
        if !stored.user_address.eq_ignore_ascii_case(user_address) {
            return None;
        }
        let market = inner.markets.get(&stored.order.market_id)?;
        let spot_market = if stored.order.outcome == "yes" {
            market.yes_spot_market.clone()
        } else {
            market.no_spot_market.clone()
        };
        Some((
            stored.order.clone(),
            stored.chain_order_id.clone(),
            spot_market,
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
    ) {
        let stored = StoredOrder {
            order: order.clone(),
            user_address,
            chain_order_id: chain_order_id.clone(),
            filled_raw,
            size_raw,
        };
        let key = chain_order_key(spot_market, &chain_order_id);
        let mut inner = self.inner.write().await;
        inner.chain_order_index.insert(key, order.id);
        inner.orders.insert(order.id, stored);
    }

    pub async fn query_order(
        &self,
        spot_market: &str,
        chain_order_id: &str,
        user_address: Option<&str>,
    ) -> Option<OrderQueryRecord> {
        let key = chain_order_key(spot_market, chain_order_id);
        let inner = self.inner.read().await;
        let order_id = inner.chain_order_index.get(&key)?;
        let stored = inner.orders.get(order_id)?;
        if let Some(user) = user_address {
            if !stored.user_address.eq_ignore_ascii_case(user) {
                return None;
            }
        }
        Some(Self::order_query_record(
            stored,
            normalize_spot_market_key(spot_market),
        ))
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
        let inner = self.inner.read().await;
        for stored in inner.orders.values() {
            if !stored.user_address.eq_ignore_ascii_case(user_address) {
                continue;
            }
            if stored.order.status != "open" && stored.order.status != "partial_filled" {
                continue;
            }
            let Some(market) = inner.markets.get(&stored.order.market_id) else {
                continue;
            };
            let order_spot = if stored.order.outcome == "yes" {
                &market.yes_spot_market
            } else {
                &market.no_spot_market
            };
            if normalize_spot_market_key(order_spot) != spot {
                continue;
            }
            if stored.order.side != side || stored.order.price != price {
                continue;
            }
            if stored.size_raw != size_raw {
                continue;
            }
            return Some(Self::order_query_record(stored, spot.clone()));
        }
        None
    }

    fn order_query_record(stored: &StoredOrder, spot_market: String) -> OrderQueryRecord {
        OrderQueryRecord {
            order: stored.order.clone(),
            chain_order_id: stored.chain_order_id.clone(),
            spot_market,
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
        let inner = self.inner.read().await;
        let key = chain_order_key(spot_market, chain_order_id);
        let order_id = inner.chain_order_index.get(&key)?;
        let stored = inner.orders.get(order_id)?;
        Some((
            stored.order.clone(),
            stored.user_address.clone(),
            normalize_spot_market_key(spot_market),
        ))
    }

    pub async fn update_order_cancelled(&self, spot_market: &str, chain_order_id: &str) {
        let mut inner = self.inner.write().await;
        let key = chain_order_key(spot_market, chain_order_id);
        let Some(order_id) = inner.chain_order_index.get(&key).copied() else {
            return;
        };
        if let Some(stored) = inner.orders.get_mut(&order_id) {
            stored.order.status = "cancelled".into();
        }
    }

    pub async fn update_order_amount(
        &self,
        spot_market: &str,
        chain_order_id: &str,
        new_amount: u64,
        remaining_amount: u64,
    ) {
        let mut inner = self.inner.write().await;
        let key = chain_order_key(spot_market, chain_order_id);
        let Some(order_id) = inner.chain_order_index.get(&key).copied() else {
            return;
        };
        let Some(stored) = inner.orders.get_mut(&order_id) else {
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
    }

    pub async fn update_order_fill(
        &self,
        spot_market: &str,
        chain_order_id: &str,
        fill_amount: u64,
        remaining_amount: u64,
        is_fully_filled: bool,
    ) {
        let mut inner = self.inner.write().await;
        let key = chain_order_key(spot_market, chain_order_id);
        let Some(order_id) = inner.chain_order_index.get(&key).copied() else {
            return;
        };
        let Some(stored) = inner.orders.get_mut(&order_id) else {
            return;
        };

        stored.filled_raw = stored.filled_raw.saturating_add(fill_amount);
        stored.order.status = if is_fully_filled || remaining_amount == 0 {
            "filled".into()
        } else {
            "partial_filled".into()
        };
    }
}

pub fn new_head() -> SharedIndexedBlockHead {
    Arc::new(RwLock::new(IndexedBlockHead::default()))
}

pub fn market_uuid(market_address: &str) -> Uuid {
    Uuid::new_v5(&Uuid::NAMESPACE_OID, market_address.as_bytes())
}

pub fn vault_uuid(vault_address: &str) -> Uuid {
    Uuid::new_v5(&Uuid::NAMESPACE_OID, format!("vault:{vault_address}").as_bytes())
}

