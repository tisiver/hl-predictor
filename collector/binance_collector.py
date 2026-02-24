import asyncio
import json
import logging
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

import aiohttp
import websockets

from collector.db import db

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("binance_collector")

LIQUIDATION_WS_URL = "wss://fstream.binance.com/ws/!forceOrder@arr"

TOP_SYMBOLS = [
    "BTC", "ETH", "SOL", "BNB", "XRP", "DOGE", "ADA", "AVAX", "LINK", "DOT",
    "UNI", "ATOM", "LTC", "ARB", "OP", "WLD", "INJ", "SUI", "PEPE", "WIF",
]
SYMBOLS = [f"{s}USDT" for s in TOP_SYMBOLS]


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def ffloat(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


class BinanceCollector:
    def __init__(self) -> None:
        self.stats: dict = defaultdict(int)
        self._mark_cache: dict[str, tuple[float, float]] = {}

    def ws_url(self) -> str:
        streams = []
        for sym in SYMBOLS:
            s = sym.lower()
            streams.append(f"{s}@aggTrade")
            streams.append(f"{s}@markPrice@1s")
        return f"wss://fstream.binance.com/stream?streams={'/'.join(streams)}"

    async def setup(self) -> None:
        await db.connect()
        await db.create_tables()

    async def _handle_agg_trade(self, payload: dict) -> None:
        symbol = payload.get("s")
        price = ffloat(payload.get("p"))
        size = ffloat(payload.get("q"))
        side = "sell" if payload.get("m") else "buy"  # m=True => buyer is market maker
        if symbol and price > 0 and size > 0:
            await db.insert_trade(now_utc(), "binance", symbol, price, size, side)
            self.stats["trades"] += 1

    async def _handle_mark_price(self, payload: dict) -> None:
        symbol = payload.get("s")
        mark_price = ffloat(payload.get("p"))
        funding_rate = ffloat(payload.get("r"))
        if symbol and mark_price > 0:
            self._mark_cache[symbol] = (mark_price, funding_rate)

    async def _poll_oi_loop(self) -> None:
        base_url = "https://fapi.binance.com/fapi/v1/openInterest"
        while True:
            cycle_start = asyncio.get_running_loop().time()
            try:
                async with aiohttp.ClientSession() as session:
                    for symbol in SYMBOLS:
                        cached = self._mark_cache.get(symbol)
                        if not cached:
                            await asyncio.sleep(1)
                            continue

                        mark_price, funding_rate = cached
                        try:
                            async with session.get(
                                base_url,
                                params={"symbol": symbol},
                                timeout=aiohttp.ClientTimeout(total=10),
                            ) as resp:
                                resp.raise_for_status()
                                payload = await resp.json()

                            oi = ffloat(payload.get("openInterest"))
                            await db.insert_oi_snapshot(now_utc(), "binance", symbol, oi, mark_price, funding_rate)
                            self.stats["oi"] += 1
                        except Exception:
                            logger.exception("Binance OI REST poll failed for %s", symbol)

                        await asyncio.sleep(1)
            except Exception:
                logger.exception("Binance OI poll loop error")

            elapsed = asyncio.get_running_loop().time() - cycle_start
            await asyncio.sleep(max(0, 60 - elapsed))

    async def _handle_liquidation(self, payload: dict) -> None:
        # forceOrder payload: {"e":"forceOrder","E":...,"o":{...}}
        order = payload.get("o") if payload.get("e") == "forceOrder" else payload
        symbol = order.get("s")
        side = "buy" if str(order.get("S", "")).upper() == "BUY" else "sell"
        price = ffloat(order.get("ap") or order.get("p"))  # avg fill price, fallback to order price
        size = ffloat(order.get("z") or order.get("q"))    # filled qty, fallback to order qty
        if symbol and price > 0 and size > 0:
            await db.insert_liquidation(now_utc(), "binance", symbol, side, price, size)
            self.stats["liquidations"] += 1

    async def _stats_loop(self) -> None:
        while True:
            await asyncio.sleep(60)
            logger.info("binance stats trades=%d oi=%d liq=%d",
                        self.stats["trades"], self.stats["oi"], self.stats["liquidations"])

    async def _main_ws_loop(self) -> None:
        backoff = 1
        while True:
            try:
                async with websockets.connect(self.ws_url(), ping_interval=20, ping_timeout=20, max_queue=1000) as ws:
                    logger.info("Connected to Binance futures WS (%d streams)", len(SYMBOLS) * 2)
                    backoff = 1
                    async for raw in ws:
                        msg = json.loads(raw)
                        stream = msg.get("stream", "")
                        data = msg.get("data", {})
                        if stream.endswith("@aggTrade"):
                            await self._handle_agg_trade(data)
                        elif "@markPrice" in stream:
                            await self._handle_mark_price(data)
            except Exception:
                logger.exception("Binance WS error, reconnect in %ss", backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    async def _liquidation_ws_loop(self) -> None:
        backoff = 1
        while True:
            try:
                async with websockets.connect(LIQUIDATION_WS_URL, ping_interval=20, ping_timeout=20) as ws:
                    logger.info("Connected to Binance liquidation WS (!forceOrder@arr)")
                    backoff = 1
                    async for raw in ws:
                        payload = json.loads(raw)
                        await self._handle_liquidation(payload)
            except Exception:
                logger.exception("Binance liquidation WS error, reconnect in %ss", backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    async def run(self) -> None:
        await self.setup()
        await asyncio.gather(
            self._main_ws_loop(),
            self._liquidation_ws_loop(),
            self._poll_oi_loop(),
            self._stats_loop(),
        )


if __name__ == "__main__":
    asyncio.run(BinanceCollector().run())
