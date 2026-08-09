# lightpool-clob-index

On-chain orderbook indexer for [LightPool](https://github.com/lightpool-labs/lightpool-node) — a self-deployable L1 with spot matching and settlement on chain (targeting 200k TPS).

This service indexes blockchain CLOB / order book data and exposes **HTTP + WebSocket** market data APIs for apps and trading bots. Apps talk to **clob-index**, not the node RPC/WS directly.

## What it does

- Indexes on-chain spot orderbook state from a LightPool node
- Serves books, markets, orders, bars, and account balances over HTTP
- Streams orderbook / quote / user updates over WebSocket
- Accepts signed txs via `POST /api/tx/submit` (build with [lightpool-sdk](https://github.com/lightpool-labs/lightpool-sdk-rust))

## Requirements

- Rust toolchain
- A running LightPool node (RPC + WS)
- `lightpool-sdk` available to the Cargo workspace (see `Cargo.toml`)

## Quick start

```bash
cp .env.example .env
# edit LIGHTPOOL_RPC_URL / LIGHTPOOL_WS_URL if needed

cargo run --release
```

Default listen: `http://127.0.0.1:3002`  
WebSocket: `ws://127.0.0.1:3002/api/ws`

### Env (see `.env.example`)

| Variable | Example | Meaning |
|----------|---------|---------|
| `HOST` / `PORT` | `0.0.0.0` / `3002` | HTTP listen address |
| `LIGHTPOOL_RPC_URL` | `http://127.0.0.1:26300` | Node RPC |
| `LIGHTPOOL_WS_URL` | `ws://127.0.0.1:26400` | Node WS |
| `ENABLE_INDEXER` | `true` | Index chain events |
| `ENABLE_SQLITE` | `true` | Persist index state |
| `SQLITE_PATH` | `data/clob-index.sqlite3` | SQLite file |

## API sketch

All routes are under `/api`.

| Method | Path | Purpose |
|--------|------|---------|
| `GET` | `/api/health` | Liveness |
| `GET` | `/api/ready` | Readiness |
| `GET` | `/api/spot/:market/book` | Spot orderbook |
| `GET` | `/api/spot/:market/info` | Spot market info |
| `GET` | `/api/spot/:market/bars` | Bars / candles |
| `GET` | `/api/orders` | List orders |
| `POST` | `/api/tx/submit` | Submit signed tx |
| `WS` | `/api/ws` | Real-time feeds |

For full request/response shapes when wiring an app, use the Cursor skill in [lightpool-node](https://github.com/lightpool-labs/lightpool-node) (`plugins/spot-lightpool`).

## Related

- Node / deploy package: https://github.com/lightpool-labs/lightpool-node
- Signing SDK: https://github.com/lightpool-labs/lightpool-sdk-rust
