// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use serde::Deserialize;
use uuid::Uuid;

use crate::domain::{
    MarketQuery, MarketSortOrder, DEFAULT_MARKETS_PAGE_LIMIT, MAX_MARKETS_ID_BATCH,
    MAX_MARKETS_PAGE_LIMIT, MAX_MARKETS_SLUG_BATCH,
};
use crate::error::{AppError, AppResult};

#[derive(Debug, Deserialize)]
pub struct QueryMarketsParams {
    pub limit: Option<u32>,
    pub offset: Option<u32>,
    pub slug: Option<String>,
    pub slugs: Option<String>,
    pub market_ids: Option<String>,
    pub market_addresses: Option<String>,
    pub state: Option<String>,
    pub order: Option<String>,
    pub ascending: Option<bool>,
}

fn parse_csv(value: Option<String>) -> Vec<String> {
    value
        .map(|items| {
            items
                .split(',')
                .map(str::trim)
                .filter(|item| !item.is_empty())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

fn parse_uuid_csv(value: Option<String>) -> AppResult<Vec<Uuid>> {
    let items = parse_csv(value);
    if items.len() > MAX_MARKETS_ID_BATCH {
        return Err(AppError::BadRequest(format!(
            "market_ids accepts at most {MAX_MARKETS_ID_BATCH} ids"
        )));
    }

    items
        .into_iter()
        .map(|item| {
            Uuid::parse_str(&item)
                .map_err(|error| AppError::BadRequest(format!("invalid market id '{item}': {error}")))
        })
        .collect()
}

pub fn build_market_query(params: QueryMarketsParams) -> AppResult<MarketQuery> {
    let slugs = parse_csv(params.slugs);
    if slugs.len() > MAX_MARKETS_SLUG_BATCH {
        return Err(AppError::BadRequest(format!(
            "slugs accepts at most {MAX_MARKETS_SLUG_BATCH} values"
        )));
    }

    let market_addresses = parse_csv(params.market_addresses);
    if market_addresses.len() > MAX_MARKETS_ID_BATCH {
        return Err(AppError::BadRequest(format!(
            "market_addresses accepts at most {MAX_MARKETS_ID_BATCH} values"
        )));
    }

    let limit = params
        .limit
        .unwrap_or(DEFAULT_MARKETS_PAGE_LIMIT)
        .clamp(1, MAX_MARKETS_PAGE_LIMIT);
    let offset = params.offset.unwrap_or(0);

    Ok(MarketQuery {
        limit,
        offset,
        slug: params
            .slug
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty()),
        slugs,
        market_ids: parse_uuid_csv(params.market_ids)?,
        market_addresses,
        state: params
            .state
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty()),
        order: MarketSortOrder::parse(params.order.as_deref()),
        ascending: params.ascending.unwrap_or(true),
    })
}
