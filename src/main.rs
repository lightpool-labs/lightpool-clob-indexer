// Copyright (c) LightPool Labs
// Author: xiaoyu1998

mod app;
mod book_hydrate;
mod bars;
mod chain;
mod config;
mod domain;
mod error;
mod http;
mod indexer;
mod mempool_client;
mod peer;
mod persist;
mod slug;
mod spot_market;
mod state;
mod submit_queue;
mod submit_wait;
mod vault_enrich;
mod ws;

use clap::Parser;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::app::App;
use crate::config::Config;

#[derive(Debug, Parser)]
#[command(name = "lightpool-clob-indexer", about = "LightPool CLOB indexer")]
struct Cli {
    /// Disable sqlite persistence (no recover/checkpoint/block/bar/order-history writes).
    /// Equivalent to ENABLE_SQLITE=false or DISABLE_PERSIST=true.
    #[arg(long, default_value_t = false)]
    no_persist: bool,
}

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();

    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(
            |_| "lightpool_clob_indexer=info,tower_http=warn".into(),
        ))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let cli = Cli::parse();
    let config = Config::from_env_with_overrides(cli.no_persist);
    if !config.enable_sqlite {
        tracing::info!("sqlite persistence disabled");
    }

    let mut app = App::build(config);
    app.recover().await;
    app.start_workers();
    app.serve().await;
}
