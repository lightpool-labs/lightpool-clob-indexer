// Copyright (c) LightPool Labs
// Author: xiaoyu1998

pub mod models;
pub mod process;
pub mod routes;

use axum::Router;

use crate::state::AppState;

pub fn router() -> Router<AppState> {
    Router::new()
        .nest("/health", routes::health::router())
        .nest("/markets", routes::markets::router())
        .nest("/spot", routes::spot::router())
        .nest("/accounts", routes::accounts::router())
        .nest("/orders", routes::orders::router())
        .nest("/tx", routes::tx::router())
        .nest("/ws", crate::ws::router())
}
