// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum MarketCategory {
    #[default]
    Spot,
    Event,
    Perp,
}

impl MarketCategory {
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "spot" => Some(Self::Spot),
            "event" | "event_contract" => Some(Self::Event),
            "perp" | "perpetual" => Some(Self::Perp),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Spot => "spot",
            Self::Event => "event",
            Self::Perp => "perp",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "category", rename_all = "snake_case")]
pub enum Market {
    Spot {
        id: Uuid,
        name: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        icon_url: Option<String>,
        market_address: String,
        state: String,
        #[serde(default)]
        deployer: String,
        #[serde(default)]
        base_token: String,
        #[serde(default)]
        quote_token: String,
    },
    Event {
        id: Uuid,
        slug: String,
        question: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        icon_url: Option<String>,
        market_address: String,
        collateral_token: String,
        yes_token: String,
        no_token: String,
        yes_spot_market: String,
        no_spot_market: String,
        state: String,
        resolution_deadline: u64,
        #[serde(default)]
        deployer: String,
    },
    Perp {
        id: Uuid,
        name: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        icon_url: Option<String>,
        market_address: String,
        state: String,
        #[serde(default)]
        deployer: String,
        #[serde(default)]
        underlying: String,
    },
}

impl Default for Market {
    fn default() -> Self {
        Self::Spot {
            id: Uuid::nil(),
            name: String::new(),
            icon_url: None,
            market_address: String::new(),
            state: String::new(),
            deployer: String::new(),
            base_token: String::new(),
            quote_token: String::new(),
        }
    }
}

impl Market {
    pub fn category(&self) -> MarketCategory {
        match self {
            Self::Spot { .. } => MarketCategory::Spot,
            Self::Event { .. } => MarketCategory::Event,
            Self::Perp { .. } => MarketCategory::Perp,
        }
    }

    pub fn id(&self) -> Uuid {
        match self {
            Self::Spot { id, .. } | Self::Event { id, .. } | Self::Perp { id, .. } => *id,
        }
    }

    /// Spot/perp market name. Empty for event.
    pub fn name(&self) -> &str {
        match self {
            Self::Spot { name, .. } | Self::Perp { name, .. } => name,
            Self::Event { .. } => "",
        }
    }

    pub fn name_mut(&mut self) -> Option<&mut String> {
        match self {
            Self::Spot { name, .. } | Self::Perp { name, .. } => Some(name),
            Self::Event { .. } => None,
        }
    }

    /// Event URL slug. Empty for spot/perp.
    pub fn slug(&self) -> &str {
        match self {
            Self::Event { slug, .. } => slug,
            Self::Spot { .. } | Self::Perp { .. } => "",
        }
    }

    pub fn slug_mut(&mut self) -> Option<&mut String> {
        match self {
            Self::Event { slug, .. } => Some(slug),
            Self::Spot { .. } | Self::Perp { .. } => None,
        }
    }

    /// Event question text. Empty for spot/perp.
    pub fn question(&self) -> &str {
        match self {
            Self::Event { question, .. } => question,
            Self::Spot { .. } | Self::Perp { .. } => "",
        }
    }

    /// Human label: event question, or spot/perp name.
    pub fn label(&self) -> &str {
        match self {
            Self::Event { question, .. } => question,
            Self::Spot { name, .. } | Self::Perp { name, .. } => name,
        }
    }

    pub fn icon_url(&self) -> Option<&String> {
        match self {
            Self::Spot { icon_url, .. }
            | Self::Event { icon_url, .. }
            | Self::Perp { icon_url, .. } => icon_url.as_ref(),
        }
    }

    pub fn set_icon_url_if_empty(&mut self, value: Option<String>) {
        let slot = match self {
            Self::Spot { icon_url, .. }
            | Self::Event { icon_url, .. }
            | Self::Perp { icon_url, .. } => icon_url,
        };
        if slot.is_none() {
            *slot = value;
        }
    }

    pub fn market_address(&self) -> &str {
        match self {
            Self::Spot { market_address, .. }
            | Self::Event { market_address, .. }
            | Self::Perp { market_address, .. } => market_address,
        }
    }

    pub fn state(&self) -> &str {
        match self {
            Self::Spot { state, .. } | Self::Event { state, .. } | Self::Perp { state, .. } => {
                state
            }
        }
    }

    pub fn state_mut(&mut self) -> &mut String {
        match self {
            Self::Spot { state, .. } | Self::Event { state, .. } | Self::Perp { state, .. } => {
                state
            }
        }
    }

    pub fn deployer(&self) -> &str {
        match self {
            Self::Spot { deployer, .. }
            | Self::Event { deployer, .. }
            | Self::Perp { deployer, .. } => deployer,
        }
    }

    pub fn resolution_deadline(&self) -> u64 {
        match self {
            Self::Event {
                resolution_deadline,
                ..
            } => *resolution_deadline,
            Self::Spot { .. } | Self::Perp { .. } => 0,
        }
    }

    pub fn yes_token(&self) -> &str {
        match self {
            Self::Event { yes_token, .. } => yes_token,
            Self::Spot { .. } | Self::Perp { .. } => "",
        }
    }

    pub fn no_token(&self) -> &str {
        match self {
            Self::Event { no_token, .. } => no_token,
            Self::Spot { .. } | Self::Perp { .. } => "",
        }
    }

    pub fn yes_spot_market(&self) -> &str {
        match self {
            Self::Event { yes_spot_market, .. } => yes_spot_market,
            Self::Spot { market_address, .. } | Self::Perp { market_address, .. } => {
                market_address
            }
        }
    }

    pub fn no_spot_market(&self) -> &str {
        match self {
            Self::Event { no_spot_market, .. } => no_spot_market,
            Self::Spot { market_address, .. } | Self::Perp { market_address, .. } => {
                market_address
            }
        }
    }

    pub fn yes_spot_market_mut(&mut self) -> Option<&mut String> {
        match self {
            Self::Event { yes_spot_market, .. } => Some(yes_spot_market),
            Self::Spot { .. } | Self::Perp { .. } => None,
        }
    }

    pub fn no_spot_market_mut(&mut self) -> Option<&mut String> {
        match self {
            Self::Event { no_spot_market, .. } => Some(no_spot_market),
            Self::Spot { .. } | Self::Perp { .. } => None,
        }
    }

    /// Rebuild from pre-enum flat checkpoint JSON (best-effort).
    pub fn from_legacy_flat(value: serde_json::Value) -> Option<Self> {
        #[derive(Deserialize)]
        struct Flat {
            id: Uuid,
            #[serde(default)]
            slug: String,
            #[serde(default)]
            question: String,
            #[serde(default)]
            name: String,
            #[serde(default)]
            icon_url: Option<String>,
            market_address: String,
            #[serde(default)]
            collateral_token: String,
            #[serde(default)]
            yes_token: String,
            #[serde(default)]
            no_token: String,
            #[serde(default)]
            yes_spot_market: String,
            #[serde(default)]
            no_spot_market: String,
            state: String,
            #[serde(default)]
            resolution_deadline: u64,
            #[serde(default)]
            category: Option<String>,
            #[serde(default)]
            deployer: String,
        }

        let flat: Flat = serde_json::from_value(value).ok()?;
        let category = flat
            .category
            .as_deref()
            .and_then(MarketCategory::parse)
            .unwrap_or_else(|| {
                let distinct_legs = flat.yes_spot_market != flat.no_spot_market
                    && !flat.yes_spot_market.is_empty()
                    && !flat.no_spot_market.is_empty();
                let has_outcome_tokens = !flat.yes_token.is_empty() || !flat.no_token.is_empty();
                if distinct_legs || has_outcome_tokens {
                    MarketCategory::Event
                } else {
                    MarketCategory::Spot
                }
            });

        let spot_name = if !flat.name.is_empty() {
            flat.name
        } else if !flat.question.is_empty() {
            flat.question.clone()
        } else {
            flat.slug.clone()
        };

        Some(match category {
            MarketCategory::Event => Self::Event {
                id: flat.id,
                slug: flat.slug,
                question: flat.question,
                icon_url: flat.icon_url,
                market_address: flat.market_address,
                collateral_token: flat.collateral_token,
                yes_token: flat.yes_token,
                no_token: flat.no_token,
                yes_spot_market: flat.yes_spot_market,
                no_spot_market: flat.no_spot_market,
                state: flat.state,
                resolution_deadline: flat.resolution_deadline,
                deployer: flat.deployer,
            },
            MarketCategory::Perp => Self::Perp {
                id: flat.id,
                name: spot_name,
                icon_url: flat.icon_url,
                market_address: flat.market_address,
                state: flat.state,
                deployer: flat.deployer,
                underlying: String::new(),
            },
            MarketCategory::Spot => Self::Spot {
                id: flat.id,
                name: spot_name,
                icon_url: flat.icon_url,
                market_address: flat.market_address,
                state: flat.state,
                deployer: flat.deployer,
                base_token: String::new(),
                quote_token: String::new(),
            },
        })
    }
}

pub const DEFAULT_MARKETS_PAGE_LIMIT: u32 = 100;
pub const MAX_MARKETS_PAGE_LIMIT: u32 = 100;
pub const MAX_MARKETS_SLUG_BATCH: usize = 100;
pub const MAX_MARKETS_ID_BATCH: usize = 100;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarketSortOrder {
    ResolutionDeadline,
    Slug,
    Question,
}

impl MarketSortOrder {
    pub fn parse(value: Option<&str>) -> Self {
        match value.map(str::trim).filter(|v| !v.is_empty()) {
            Some("slug") | Some("name") => Self::Slug,
            Some("question") | Some("label") => Self::Question,
            _ => Self::ResolutionDeadline,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MarketQuery {
    pub limit: u32,
    pub offset: u32,
    pub slug: Option<String>,
    pub slugs: Vec<String>,
    pub market_ids: Vec<Uuid>,
    pub market_addresses: Vec<String>,
    pub state: Option<String>,
    pub category: Option<MarketCategory>,
    pub deployer: Option<String>,
    pub order: MarketSortOrder,
    pub ascending: bool,
}
