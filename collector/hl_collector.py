import asyncio
import json
import logging
import os
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aiohttp
import websockets
from dotenv import load_dotenv

from collector.db import db

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("hl_collector")


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def ffloat(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


HL_SYMBOLS = [
    "BTC", "ETH", "SOL", "BNB", "XRP", "DOGE", "ADA", "AVAX", "LINK", "DOT",
    "UNI", "ATOM", "LTC", "ARB", "OP", "WLD", "INJ", "SUI", "PEPE", "WIF",
    "BONK", "JUP", "TIA", "SEI", "APT", "NEAR", "FIL", "ETC", "BCH",
]

# Depth (levels) used to compute orderbook imbalance
OB_DEPTH = 10


class HyperliquidCollector:
    def __init__(self) -> None:
        load_dotenv(Path(__file__).resolve().parent.parent / ".env")
        self.ws_url = os.getenv("HL_WS", "wss://api.hyperliquid.xyz/ws")
        self.rest_url = os.getenv("HL_REST", "https://api.hyperliquid.xyz/info")
        self.stats: dict = defaultdict(int)

    async def setup(self) -> None:
        await db.connect()
        await db.create_tables()

    async def _poll_oi_loop(self) -> None:
        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(self.rest_url, json={"type": "metaAndAssetCtxs"}, timeout=aiohttp.ClientTimeout(total=20)) as resp:
                        resp.raise_for_status()
                        data = await resp.json()

                if not isinstance(data, list) or len(data) < 2:
                    await asyncio.sleep(30)
                    continue

                meta, ctxs = data[0], data[1]
                universe = meta.get("universe", []) if isinstance(meta, dict) else []
                ts = now_utc()

                for i, ctx in enumerate(ctxs if isinstance(ctxs, list) else []):
                    if i >= len(universe):
                        continue
                    symbol = universe[i].get("name")
                    if not symbol:
                        continue
                    await db.insert_oi_snapshot(
                        ts, "hyperliquid", symbol,
                        ffloat(ctx.get("openInterest")),
                        ffloat(ctx.get("markPx")),
                        ffloat(ctx.get("funding")),
                    )
                    self.stats["oi_snapshots"] += 1

            except Exception:
                logger.exception("OI poll failed")

            await asyncio.sleep(30)

    async def _stats_loop(self) -> None:
        while True:
            await asyncio.sleep(60)
            logger.info("stats trades=%d liq=%d oi=%d ob=%d",
                        self.stats["trades"], self.stats["liquidations"],
                        self.stats["oi_snapshots"], self.stats["orderbook"])

    async def _handle_message(self, raw: str) -> None:
        msg = json.loads(raw)
        channel = msg.get("channel")
        data = msg.get("data")
        ts = now_utc()

        if channel == "trades" and isinstance(data, list):
            for t in data:
                symbol = t.get("coin") or t.get("symbol")
                price = ffloat(t.get("px"))
                size = ffloat(t.get("sz"))
                side = "buy" if str(t.get("side", "")).lower() in ("b", "buy", "bid") else "sell"
                if symbol and price > 0 and size > 0:
                    await db.insert_trade(ts, "hyperliquid", symbol, price, size, side)
                    self.stats["trades"] += 1

        elif channel == "liquidations" and isinstance(data, list):
            for liq in data:
                symbol = liq.get("coin") or liq.get("symbol")
                price = ffloat(liq.get("px"))
                size = ffloat(liq.get("sz"))
                side = "buy" if str(liq.get("side", "")).lower() in ("b", "buy", "bid") else "sell"
                if symbol and price > 0 and size > 0:
                    await db.insert_liquidation(ts, "hyperliquid", symbol, side, price, size)
                    self.stats["liquidations"] += 1

        elif channel == "l2Book" and isinstance(data, dict):
            symbol = data.get("coin")
            levels = data.get("levels")
            if not symbol or not isinstance(levels, list) or len(levels) < 2:
                return
            bids, asks = levels[0], levels[1]

            bid_vol = sum(ffloat(l.get("sz")) for l in bids[:OB_DEPTH])
            ask_vol = sum(ffloat(l.get("sz")) for l in asks[:OB_DEPTH])
            best_bid = ffloat(bids[0].get("px")) if bids else 0.0
            best_ask = ffloat(asks[0].get("px")) if asks else 0.0
            spread = best_ask - best_bid if best_bid > 0 and best_ask > 0 else 0.0
            total = bid_vol + ask_vol
            imbalance = (bid_vol - ask_vol) / total if total > 0 else 0.0

            await db.insert_orderbook(ts, "hyperliquid", symbol, bid_vol, ask_vol, spread, imbalance)
            self.stats["orderbook"] += 1

    async def _ws_loop(self) -> None:
        backoff = 1
        while True:
            try:
                async with websockets.connect(self.ws_url, ping_interval=20, ping_timeout=20, max_queue=1000) as ws:
                    logger.info("Connected to Hyperliquid WS")
                    backoff = 1
                    await ws.send(json.dumps({"method": "subscribe", "subscription": {"type": "trades"}}))
                    await ws.send(json.dumps({"method": "subscribe", "subscription": {"type": "liquidations"}}))
                    for sym in HL_SYMBOLS:
                        await ws.send(json.dumps({"method": "subscribe", "subscription": {"type": "l2Book", "coin": sym}}))
                    async for raw in ws:
                        await self._handle_message(raw)
            except Exception:
                logger.exception("HL WS error, reconnect in %ss", backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    async def run(self) -> None:
        await self.setup()
        await asyncio.gather(self._ws_loop(), self._poll_oi_loop(), self._stats_loop())


if __name__ == "__main__":
    asyncio.run(HyperliquidCollector().run())
