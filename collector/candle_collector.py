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

from collector.binance_collector import TOP_SYMBOLS
from collector.db import db
from collector.hl_collector import HL_SYMBOLS

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("candle_collector")


class CandleCollector:
    def __init__(self) -> None:
        load_dotenv(Path(__file__).resolve().parent.parent / ".env")
        self.hl_rest_url = os.getenv("HL_REST", "https://api.hyperliquid.xyz/info")
        self.binance_symbols = [f"{s}USDT" for s in TOP_SYMBOLS]
        self.stats: dict[str, int] = defaultdict(int)

    async def setup(self) -> None:
        await db.connect()
        await db.create_tables()

    def _binance_ws_url(self) -> str:
        streams = []
        for sym in self.binance_symbols:
            s = sym.lower()
            streams.append(f"{s}@kline_1m")
            streams.append(f"{s}@kline_5m")
        return f"wss://fstream.binance.com/stream?streams={'/'.join(streams)}"

    @staticmethod
    def _ffloat(v: Any, default: float = 0.0) -> float:
        try:
            return float(v)
        except (TypeError, ValueError):
            return default

    async def _handle_binance_kline(self, payload: dict[str, Any]) -> None:
        kline = payload.get("k", {})
        is_closed = bool(kline.get("x"))
        if not is_closed:
            return

        symbol = kline.get("s")
        interval = kline.get("i")
        open_time_ms = kline.get("t")
        open_ = self._ffloat(kline.get("o"))
        high = self._ffloat(kline.get("h"))
        low = self._ffloat(kline.get("l"))
        close = self._ffloat(kline.get("c"))
        volume = self._ffloat(kline.get("v"))

        if not symbol or interval not in ("1m", "5m") or open_time_ms is None:
            return

        ts = datetime.fromtimestamp(int(open_time_ms) / 1000, tz=timezone.utc)
        await db.insert_candle(ts, "binance", symbol, interval, open_, high, low, close, volume)
        self.stats["binance"] += 1

    async def _binance_ws_loop(self) -> None:
        backoff = 1
        while True:
            try:
                async with websockets.connect(self._binance_ws_url(), ping_interval=20, ping_timeout=20, max_queue=2000) as ws:
                    logger.info("Connected to Binance kline WS (%d streams)", len(self.binance_symbols) * 2)
                    backoff = 1
                    async for raw in ws:
                        msg = json.loads(raw)
                        data = msg.get("data", {})
                        stream = msg.get("stream", "")
                        if "@kline_" in stream:
                            await self._handle_binance_kline(data)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Binance kline WS error, reconnect in %ss", backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    async def _fetch_hl_candle(self, session: aiohttp.ClientSession, coin: str, interval: str) -> None:
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        interval_ms = 60 * 1000 if interval == "1m" else 5 * 60 * 1000
        lookback_ms = 2 * 60 * 1000 if interval == "1m" else 10 * 60 * 1000
        payload = {
            "type": "candleSnapshot",
            "req": {
                "coin": coin,
                "interval": interval,
                "startTime": now_ms - lookback_ms,
                "endTime": now_ms,
            },
        }

        async with session.post(self.hl_rest_url, json=payload, timeout=aiohttp.ClientTimeout(total=20)) as resp:
            resp.raise_for_status()
            data = await resp.json()

        if not isinstance(data, list) or not data:
            return

        completed = []
        for c in data:
            if not isinstance(c, dict) or c.get("t") is None:
                continue
            try:
                open_time_ms = int(c["t"])
            except (TypeError, ValueError):
                continue
            if open_time_ms + interval_ms <= now_ms:
                completed.append(c)
        if not completed:
            return
        candle = completed[-1]
        candle_time_ms = candle.get("t")
        open_ = self._ffloat(candle.get("o"))
        high = self._ffloat(candle.get("h"))
        low = self._ffloat(candle.get("l"))
        close = self._ffloat(candle.get("c"))
        volume = self._ffloat(candle.get("v"))

        if candle_time_ms is None:
            return

        ts = datetime.fromtimestamp(int(candle_time_ms) / 1000, tz=timezone.utc)
        await db.insert_candle(ts, "hyperliquid", coin, interval, open_, high, low, close, volume)
        self.stats["hl"] += 1

    async def _hl_poll_loop(self) -> None:
        while True:
            cycle_start = asyncio.get_running_loop().time()
            try:
                async with aiohttp.ClientSession() as session:
                    for idx, coin in enumerate(HL_SYMBOLS):
                        target_at = cycle_start + (idx * 2)
                        sleep_for = target_at - asyncio.get_running_loop().time()
                        if sleep_for > 0:
                            await asyncio.sleep(sleep_for)
                        try:
                            await asyncio.wait_for(
                                asyncio.gather(
                                    self._fetch_hl_candle(session, coin, "1m"),
                                    self._fetch_hl_candle(session, coin, "5m"),
                                ),
                                timeout=1.9,
                            )
                        except asyncio.CancelledError:
                            raise
                        except asyncio.TimeoutError:
                            logger.warning("HL candle poll timed out for coin=%s", coin)
                        except Exception:
                            logger.exception("HL candle poll failed for coin=%s", coin)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("HL candle polling loop failed")
            sleep_for = (cycle_start + 60) - asyncio.get_running_loop().time()
            if sleep_for > 0:
                await asyncio.sleep(sleep_for)

    async def _stats_loop(self) -> None:
        while True:
            await asyncio.sleep(60)
            logger.info("candle_collector stats binance=%d hl=%d", self.stats["binance"], self.stats["hl"])

    async def run(self) -> None:
        await self.setup()
        await asyncio.gather(
            self._binance_ws_loop(),
            self._hl_poll_loop(),
            self._stats_loop(),
        )


if __name__ == "__main__":
    asyncio.run(CandleCollector().run())
