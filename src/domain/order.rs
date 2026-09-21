// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    pub id: Uuid,
    pub market_id: Uuid,
    #[serde(default)]
    pub market_slug: String,
    pub question: String,
    pub outcome: String,
    pub side: String,
    pub price: String,
    pub size: String,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cloid: Option<String>,
}
