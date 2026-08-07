// Copyright (c) LightPool Labs

use std::env;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use rusqlite::{params, Connection, Transaction};

fn main() {
    let n_markets = env_usize("MARKETS", 500);
    let orders_per_market = env_usize("ORDERS_PER_MARKET", 50);
    let levels_per_book = env_usize("LEVELS_PER_BOOK", 20);
    let warmup = env_usize("WARMUP", 2);
    let iters = env_usize("ITERS", 10).max(1);

    let books = n_markets * 2;
    let n_levels = books * levels_per_book;
    let n_orders = n_markets * orders_per_market;

    let market_payload = r#"{"id":"00000000-0000-0000-0000-000000000001","slug":"m","question":"q","market_address":"0x1","collateral_token":"0x2","yes_token":"0x3","no_token":"0x4","yes_spot_market":"0x5","no_spot_market":"0x6","state":"Open","resolution_deadline":1}"#;
    let order_payload = r#"{"id":"00000000-0000-0000-0000-000000000002","market_id":"00000000-0000-0000-0000-000000000001","market_slug":"m","question":"q","outcome":"yes","side":"buy","price":"0.5","size":"1","status":"open"}"#;

    println!(
        "scenario: markets={n_markets} books={books} levels={n_levels} orders={n_orders} warmup={warmup} iters={iters}"
    );
    println!("note: measures SQLite write only (no export / serde_json cost from RAM)");

    let disk_path = env::var("BENCH_DB")
        .map(PathBuf::from)
        .unwrap_or_else(|_| std::env::temp_dir().join("clob-index-checkpoint-bench.sqlite3"));

    for (label, disk) in [("in-memory", false), ("disk WAL", true)] {
        let conn = if disk {
            let _ = std::fs::remove_file(&disk_path);
            let _ = std::fs::remove_file(format!("{}-wal", disk_path.display()));
            let _ = std::fs::remove_file(format!("{}-shm", disk_path.display()));
            println!("disk path: {}", disk_path.display());
            Connection::open(&disk_path).expect("open disk sqlite")
        } else {
            Connection::open_in_memory().expect("open memory sqlite")
        };
        setup(&conn);

        for _ in 0..warmup {
            run_checkpoint(
                &conn,
                n_markets,
                n_orders,
                books,
                n_levels,
                levels_per_book,
                market_payload,
                order_payload,
            );
        }

        let mut samples = Vec::with_capacity(iters);
        for _ in 0..iters {
            let started = Instant::now();
            run_checkpoint(
                &conn,
                n_markets,
                n_orders,
                books,
                n_levels,
                levels_per_book,
                market_payload,
                order_payload,
            );
            samples.push(started.elapsed());
        }
        samples.sort();
        println!("\n{label}:");
        print_stats("row-by-row prepared checkpoint", &samples);
    }
}

fn env_usize(key: &str, default: usize) -> usize {
    env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn print_stats(name: &str, samples: &[Duration]) {
    let sum: Duration = samples.iter().copied().sum();
    let avg = sum / samples.len() as u32;
    println!(
        "  {name}: min={:?} p50={:?} max={:?} avg={:?}",
        samples[0],
        samples[samples.len() / 2],
        samples[samples.len() - 1],
        avg
    );
}

fn setup(conn: &Connection) {
    conn.execute_batch(
        "
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = NORMAL;
        CREATE TABLE IF NOT EXISTS markets (
            id TEXT PRIMARY KEY,
            payload TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS orders (
            id TEXT PRIMARY KEY,
            user_address TEXT NOT NULL,
            chain_order_id TEXT NOT NULL,
            spot_market TEXT NOT NULL,
            size_raw INTEGER NOT NULL,
            filled_raw INTEGER NOT NULL,
            status TEXT NOT NULL,
            payload TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS last_trades (
            spot_market TEXT PRIMARY KEY,
            price_raw INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS book_levels (
            spot_market TEXT NOT NULL,
            side TEXT NOT NULL,
            price_raw INTEGER NOT NULL,
            size_raw INTEGER NOT NULL,
            PRIMARY KEY (spot_market, side, price_raw)
        );
        CREATE TABLE IF NOT EXISTS book_meta (
            spot_market TEXT PRIMARY KEY,
            sequence INTEGER NOT NULL,
            last_trade_price INTEGER
        );
        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        ",
    )
    .expect("setup schema");
}

fn clear(tx: &Transaction<'_>) {
    for table in [
        "markets",
        "orders",
        "last_trades",
        "book_levels",
        "book_meta",
    ] {
        tx.execute(&format!("DELETE FROM {table}"), [])
            .unwrap_or_else(|e| panic!("clear {table}: {e}"));
    }
}

fn run_checkpoint(
    conn: &Connection,
    n_markets: usize,
    n_orders: usize,
    books: usize,
    n_levels: usize,
    levels_per_book: usize,
    market_payload: &str,
    order_payload: &str,
) {
    let tx = conn.unchecked_transaction().expect("begin");
    clear(&tx);

    {
        let mut stmt = tx
            .prepare("INSERT INTO markets (id, payload) VALUES (?1, ?2)")
            .expect("prepare markets");
        for i in 0..n_markets {
            stmt.execute(params![format!("{i}"), market_payload])
                .expect("insert market");
        }
    }

    {
        let mut stmt = tx
            .prepare(
                "INSERT INTO orders
                 (id, user_address, chain_order_id, spot_market, size_raw, filled_raw, status, payload)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            )
            .expect("prepare orders");
        for i in 0..n_orders {
            stmt.execute(params![
                format!("o{i}"),
                "0xabc",
                format!("c{i}"),
                format!("s{}", i % books.max(1)),
                1i64,
                0i64,
                "open",
                order_payload,
            ])
            .expect("insert order");
        }
    }

    {
        let mut stmt = tx
            .prepare("INSERT INTO last_trades (spot_market, price_raw) VALUES (?1, ?2)")
            .expect("prepare last_trades");
        for i in 0..books {
            stmt.execute(params![format!("s{i}"), 100i64])
                .expect("insert last_trade");
        }
    }

    {
        let mut stmt = tx
            .prepare(
                "INSERT INTO book_levels (spot_market, side, price_raw, size_raw)
                 VALUES (?1, ?2, ?3, ?4)",
            )
            .expect("prepare book_levels");
        for i in 0..n_levels {
            let side = if i % 2 == 0 { "buy" } else { "sell" };
            stmt.execute(params![
                format!("s{}", i / levels_per_book.max(1)),
                side,
                (i % levels_per_book.max(1)) as i64,
                10i64,
            ])
            .expect("insert book_level");
        }
    }

    {
        let mut stmt = tx
            .prepare(
                "INSERT INTO book_meta (spot_market, sequence, last_trade_price)
                 VALUES (?1, ?2, ?3)",
            )
            .expect("prepare book_meta");
        for i in 0..books {
            stmt.execute(params![format!("s{i}"), 1i64, Some(100i64)])
                .expect("insert book_meta");
        }
    }

    tx.execute(
        "INSERT OR REPLACE INTO meta (key, value) VALUES ('last_block_num', ?1)",
        params!["1"],
    )
    .expect("meta block");
    tx.execute(
        "INSERT OR REPLACE INTO meta (key, value) VALUES ('last_digest', ?1)",
        params!["deadbeef"],
    )
    .expect("meta digest");
    tx.commit().expect("commit");
}
