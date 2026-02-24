# collector/

Live data ingestion pipeline. Three collectors run concurrently via `run_collectors.py`.

---

## Collectors

### `hl_collector.py` — HyperliquidCollector

**Source:** Hyperliquid WebSocket + REST

| Loop | Method | Data | Table | Symbols |
|------|--------|------|-------|---------|
| `_ws_loop` | WS subscribe | Trades | `trades` | Top 10 HL symbols |
| `_ws_loop` | WS subscribe | Order book (top 10 levels) | `orderbook_imbalance` | Top 10 HL symbols |
| `_poll_oi_loop` | REST `metaAndAssetCtxs` every 30s | Open interest, mark price, funding rate | `oi_snapshots` | All 29 HL symbols |

**Notes:**
- HL WS is capped to top 10 symbols for trades and orderbook — HL's API drops connections if too many per-coin subscriptions are opened simultaneously.
- `liq=0` in stats is expected: HL has no public liquidation WebSocket feed. The `liquidations` channel subscription is effectively a no-op. HL liquidations are not collected.

---

### `binance_collector.py` — BinanceCollector

**Source:** Binance Futures WebSocket (combined stream)

| Loop | Stream | Data | Table | Symbols |
|------|--------|------|-------|---------|
| `_main_ws_loop` | `{sym}@aggTrade` | Trades | `trades` | Top 20 symbols (USDT pairs) |
| `_main_ws_loop` | `{sym}@markPrice@1s` | Mark price, funding rate | `oi_snapshots` | Top 20 symbols |
| `_liquidation_ws_loop` | `!forceOrder@arr` (global) | Liquidations | `liquidations` | All Binance futures |

**Notes:**
- ⚠️ **OI is always 0 for Binance rows in `oi_snapshots`** — `markPrice` events carry mark price and funding rate but not open interest. Real Binance OI requires a separate REST poll (`/fapi/v1/openInterest`), which is not implemented yet.
- Liquidation coverage is complete: `!forceOrder@arr` is a global stream covering all symbols, not just the top 20.

---

### `candle_collector.py` — CandleCollector

**Source:** Binance Futures WebSocket + Hyperliquid REST

| Loop | Method | Data | Table | Symbols | Intervals |
|------|--------|------|-------|---------|-----------|
| `_binance_ws_loop` | WS `{sym}@kline_{interval}` | OHLCV (on candle close) | `candles` | Top 20 Binance symbols | 1m, 5m |
| `_hl_poll_loop` | REST `candleSnapshot` every 60s | OHLCV (last completed candle) | `candles` | All 29 HL symbols | 1m, 5m |

**Notes:**
- Binance candles are inserted only when `k.x == true` (candle is closed), so no partial bars.
- HL REST requests are staggered ~2s per coin to avoid rate limits. Full cycle fits within the 60s window.
- `ON CONFLICT DO NOTHING` on inserts — safe to overlap with historical backfills.

---

## Overlap / Duplicate Jobs

| Data type | hl_collector | binance_collector | candle_collector |
|-----------|-------------|-------------------|------------------|
| Trades | ✅ HL (top 10) | ✅ Binance (top 20) | — |
| OI snapshots | ✅ HL real OI + mark + funding | ⚠️ Binance mark + funding only (OI=0) | — |
| Orderbook imbalance | ✅ HL (top 10) | — | — |
| Liquidations | ❌ HL feed missing | ✅ Binance (global) | — |
| Candles (1m, 5m) | — | — | ✅ Both exchanges |

**Issues to address:**

1. **Binance OI is always 0** — `binance_collector` writes `oi_snapshots` rows with `oi=0.0`. Either add a REST poll for `/fapi/v1/openInterest` or remove the OI write from the mark price handler to avoid polluting the table with zero-OI rows.

2. **HL liquidations missing** — HL has no WS liquidation feed. Options: poll HL REST for recent liquidations, or accept that only Binance liquidations are collected.

3. **HL trades capped at top 10** — orderbook and trades only cover 10 of 29 symbols due to connection limits. Could open a second WS connection for the remaining 19 symbols.

---

## Symbol Coverage

| Collector | Symbols | Count |
|-----------|---------|-------|
| `HL_SYMBOLS` (hl_collector, candle_collector) | BTC ETH SOL BNB XRP DOGE ADA AVAX LINK DOT UNI ATOM LTC ARB OP WLD INJ SUI PEPE WIF BONK JUP TIA SEI APT NEAR FIL ETC BCH | 29 |
| `TOP_SYMBOLS` (binance_collector, candle_collector) | BTC ETH SOL BNB XRP DOGE ADA AVAX LINK DOT UNI ATOM LTC ARB OP WLD INJ SUI PEPE WIF | 20 |
| HL WS (trades + orderbook) | Top 10 of HL_SYMBOLS only | 10 |

---

## Tables Written

| Table | Writers |
|-------|---------|
| `trades` | hl_collector, binance_collector |
| `oi_snapshots` | hl_collector, binance_collector |
| `orderbook_imbalance` | hl_collector |
| `liquidations` | binance_collector |
| `candles` | candle_collector |
