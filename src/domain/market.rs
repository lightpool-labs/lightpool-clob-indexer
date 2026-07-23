// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Market {
    pub id: Uuid,
    pub slug: String,
    pub question: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub icon_url: Option<String>,
    pub market_address: String,
    pub collateral_token: String,
    pub yes_token: String,
    pub no_token: String,
    pub yes_spot_market: String,
    pub no_spot_market: String,
    pub state: String,
    pub resolution_deadline: u64,
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
            Some("slug") => Self::Slug,
            Some("question") => Self::Question,
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
    pub order: MarketSortOrder,
    pub ascending: bool,
}
