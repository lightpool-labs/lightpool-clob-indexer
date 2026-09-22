# clob-indexer examples

## `submit_repro`

1. **Bootstrap** (signed): `POST /api/tx/submit` — create USDT, stock, market; **waits for receipt**
2. **Burst** (unsigned, like `burst_transfer`): fire-and-forget places across `--depth` bid/ask levels as mid moves — **does not wait for receipt / 504**

### Default: mempool TCP

```bash
cargo run --example submit_repro -- \
  --depth 20 --mid 100.00 --move-ticks 1 \
  --tasks 1 --rate 10000
```

### Via indexer inject (no receipt wait)

Requires indexer with `/api/tx/inject`:

```bash
cargo run --example submit_repro -- \
  --target index \
  --depth 20 --tasks 1 --rate 10000
```

| Flag | Meaning |
|------|---------|
| `--target mempool` | Raw TCP to mempool (default; same idea as burst_transfer) |
| `--target index` | `POST /api/tx/inject` — mempool ack only |
| `--tasks` | Parallel workers (mempool: one TCP each) |
| `--rate` | Target txs/s **per task** |

`/api/tx/submit` is only used for bootstrap. Flood path never waits on block receipt.
