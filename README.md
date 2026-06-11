# eth-price-poc-sdk

Tiny Python client for the Price-of-Ethereum PoC dataset.

Pulls live or static depth/route snapshots from the public deployment
(or any compatible API base) so you can analyse the data locally —
plot it, write it to a notebook, run your own metrics on top.

The landing page shows the headline charts; everything else
(per-block route metadata, the dense per-block curve, per-target
capping flags, etc.) is intended to be explored through this SDK
against the same API.

## Install

```bash
pip install -e .
# (Until we publish to PyPI; the package name will be `eth-price-poc-sdk`.)
```

## Quickstart

```python
from eth_price_poc import client

# Default base resolves to the public Vercel site, which proxies through
# to the live API. Override with
# EthPricePoCClient(base=...) to point at your own deployment.
c = client()

snap   = c.latest()       # most recent block's full snapshot
status = c.status()       # mode (live/static), blocks_behind, fynd health
cov    = c.coverage()     # indexed protocols, components, last update
hist   = c.history(limit=720)  # rolling window

print(snap["block"], snap["spot_price"])

# Optional pandas integration (install with: pip install -e .[pandas])
df = c.history_as_dataframe(limit=720)
print(df.head())
```

## What you can do with this that the website can't show you

- Plot the **full bookmap** at custom resolution (the site renders 96
  rows; the data lets you pick any number).
- Extract **the per-block route metadata** (which Uniswap V3 pool the
  best $5M route actually used at block N).
- Backtest a fill strategy: "if I'd traded $X at this block, what would
  it have cost vs the next 10 blocks?"
- Cross-reference against your own dataset (CEX prints, on-chain
  events, etc.).
- Pull the dense `curve` array (~110 measured points per side on the hosted deployment; latest block via `latest()`, recent historical blocks via `curve_for_block()` / `/api/curve`)
  and build a custom depth chart.
- Tap into `route_meta_by_level` to compare which protocols win at
  which trade sizes block by block.

## API surface

| Method | Endpoint | Notes |
|---|---|---|
| `client.latest()` | `GET /api/latest` | Single most recent block snapshot |
| `client.history(limit=N)` | `GET /api/history?limit=N` | Rolling window, current schema |
| `client.status()` | `GET /api/status` | Live/static, blocks_behind, fynd health |
| `client.coverage()` | `GET /api/coverage` | Indexed protocols, components |
| `client.history_as_dataframe(limit=N)` | derived | Convenience pandas wrapper |

If the live API is unreachable, `client()` will transparently fall back
to the static `data.json` and `coverage_static.json` snapshots served
from the same origin — same shape, frozen at the last refresh.

## Schema reference

Each block in `history` (and the same shape under `latest`) carries:

```
block:        int          # Ethereum block number
time:         str          # ISO-8601 UTC, when this snapshot was collected
spot_price:   float        # marginal-trade price (~$1K probe)
robust_mid:   float        # manipulation-resistant median mid
duration_ms:  int          # how long this snapshot took to assemble
pair:         "ETH/USDC"
token_in / token_out:      # { address, symbol, decimals }
impact_levels: [float]     # the per-target rows present under `levels`
levels: {                  # per-target depth (anchored or sweep-derived)
  "1.0": {
    "buy":  { amount_usd, price, actual_impact_pct, target_reached,
              bound, amount_in, amount_out, gas_estimate, route,
              derived_from }
    "sell": { ... }
  }, ...
}
curve: {                   # dense measured points (~110 per side hosted)
  "buy":  [ { amount_usd, price, impact_pct, amount_in, amount_out,
              gas_estimate, route }, ... ]
  "sell": [ ... ]
  samples_per_side, search_min_usd, search_max_usd
}
route_meta:           # 1% probe route
route_meta_by_level:  # routes per key impact target
```

Capped values are marked explicitly: `bound:"max"` when the search
ceiling can't reach the target impact. `derived_from` says how a row
was computed (`anchored_bisection` for headline targets,
`sweep_interpolation` elsewhere).

## Roadmap

ETH/USDC is the first pair. The collector, API, and SDK are
pair-agnostic — point them at any token pair Fynd can quote and the
same depth/curve/route data falls out.
