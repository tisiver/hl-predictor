import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
import random
from typing import Any

import aiohttp
from dotenv import load_dotenv

from collector.db import db

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

HL_URL = "https://api.hyperliquid.xyz/info"
BINANCE_KLINES_URL = "https://fapi.binance.com/fapi/v1/klines"

DAYS_BACK = 30
HL_MAX_CONCURRENCY = 5
BINANCE_MAX_CONCURRENCY = 10

HL_SYMBOLS_RAW = [
    "BTC", "ETH", "SOL", "BNB", "XRP", "DOGE", "ADA", "AVAX", "LINK", "DOT",
    "UNI", "ATOM", "LTC", "ARB", "OP", "WLD", "INJ", "SUI", "PEPE", "WIF",
    "BONK", "JUP", "TIA", "SEI", "APT", "NEAR", "FIL", "ETC", "LTC", "BCH",
]
BINANCE_TOP20 = [
    "BTC", "ETH", "SOL", "BNB", "XRP", "DOGE", "ADA", "AVAX", "LINK", "DOT",
    "UNI", "ATOM", "LTC", "ARB", "OP", "WLD", "INJ", "SUI", "PEPE", "WIF",
]


def unique(seq: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for item in seq:
        if item in seen:
            continue
        out.append(item)
        seen.add(item)
    return out


HL_SYMBOLS = unique(HL_SYMBOLS_RAW)
BINANCE_SYMBOLS = [f"{s}USDT" for s in BINANCE_TOP20]


def dt_to_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def interval_to_ms(interval: str) -> int:
    if not interval.endswith("m"):
        raise ValueError(f"Unsupported interval: {interval}")
    mins = int(interval[:-1])
    return mins * 60_000


class PerKeyRateLimiter:
    def __init__(self, min_interval_sec: float) -> None:
        self.min_interval = min_interval_sec
        self._next_ts: dict[str, float] = {}
        self._lock = asyncio.Lock()

    async def wait(self, key: str) -> None:
        loop = asyncio.get_running_loop()
        async with self._lock:
            now = loop.time()
            next_ts = self._next_ts.get(key, 0.0)
            wait_s = max(0.0, next_ts - now)
            self._next_ts[key] = max(now, next_ts) + self.min_interval
        if wait_s > 0:
            await asyncio.sleep(wait_s)


class GlobalRateLimiter:
    def __init__(self, max_per_sec: float) -> None:
        self.min_interval = 1.0 / max_per_sec
        self._next_ts = 0.0
        self._lock = asyncio.Lock()

    async def wait(self) -> None:
        loop = asyncio.get_running_loop()
        async with self._lock:
            now = loop.time()
            wait_s = max(0.0, self._next_ts - now)
            self._next_ts = max(now, self._next_ts) + self.min_interval
        if wait_s > 0:
            await asyncio.sleep(wait_s)


@dataclass
class Summary:
    hl_candle_rows: int = 0
    hl_candle_days_skipped: int = 0
    hl_funding_rows: int = 0
    hl_funding_symbols_skipped: int = 0
    binance_candle_rows: int = 0
    binance_days_skipped: int = 0
    errors: int = 0
    error_messages: list[str] = field(default_factory=list)

    def add_error(self, msg: str) -> None:
        self.errors += 1
        if len(self.error_messages) < 20:
            self.error_messages.append(msg)


class HistoricalDownloader:
    def __init__(self) -> None:
        self.summary = Summary()
        self.hl_sem = asyncio.Semaphore(HL_MAX_CONCURRENCY)
        self.binance_sem = asyncio.Semaphore(BINANCE_MAX_CONCURRENCY)
        self.hl_rate = PerKeyRateLimiter(min_interval_sec=1.0)
        self.binance_rate = GlobalRateLimiter(max_per_sec=10.0)
        self.timeout = aiohttp.ClientTimeout(total=30)

    async def setup(self) -> None:
        await db.connect()
        await db.create_tables()

    async def _day_has_candles(
        self,
        exchange: str,
        symbol: str,
        interval: str,
        day_start: datetime,
        day_end: datetime,
    ) -> bool:
        """Return True only when the day has >=95% of expected candles.

        Checking for *any* row is insufficient: an interrupted run leaves a
        partial day that is then permanently skipped on rerun.  Instead we
        compare the actual row count against the number of intervals that fit
        in the window and treat the day as complete only when coverage is high.
        """
        interval_ms = interval_to_ms(interval)
        # Use the actual fetch window (already clipped by caller to the real
        # start/end boundary), not a full calendar day, so the first and last
        # partial days are measured against the right expected count.
        window_ms = int((day_end - day_start).total_seconds() * 1000)
        expected = max(1, window_ms // interval_ms)

        pool = await db.connect()
        async with pool.acquire() as conn:
            cnt = await conn.fetchval(
                """
                SELECT COUNT(*)
                FROM candles
                WHERE exchange = $1
                  AND symbol = $2
                  AND interval = $3
                  AND time >= $4
                  AND time < $5
                """,
                exchange,
                symbol,
                interval,
                day_start,
                day_end,
            )
        return bool(cnt and cnt >= expected * 0.95)

    async def _has_funding(self, symbol: str, start: datetime) -> bool:
        """Return True when funding history is current (latest row within 24 h of now).

        Count-based checks fail for newly-listed symbols whose history is
        shorter than the full backfill window.  Using the latest timestamp is
        exchange-agnostic: if we have a recent row the symbol is up to date
        regardless of listing age, and any partial previous run is caught
        because the latest row will lag behind the present time.
        """
        now = datetime.now(timezone.utc)
        pool = await db.connect()
        async with pool.acquire() as conn:
            latest = await conn.fetchval(
                """
                SELECT MAX(time)
                FROM oi_snapshots
                WHERE exchange = 'hyperliquid_funding'
                  AND symbol = $1
                  AND time >= $2
                """,
                symbol,
                start,
            )
        if latest is None:
            return False
        # HL funding is emitted every 8 h; within-24h means we're current.
        return (now - latest).total_seconds() < 86_400

    async def _insert_candles_bulk(self, rows: list[tuple[Any, ...]]) -> None:
        if not rows:
            return
        pool = await db.connect()
        async with pool.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO candles (time, exchange, symbol, interval, open, high, low, close, volume)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
                ON CONFLICT (time, exchange, symbol, interval) DO NOTHING
                """,
                rows,
            )

    async def _insert_funding_bulk(self, rows: list[tuple[Any, ...]]) -> None:
        if not rows:
            return
        pool = await db.connect()
        async with pool.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO oi_snapshots (time, exchange, symbol, oi, mark_price, funding_rate)
                SELECT $1,$2,$3,$4,$5,$6
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM oi_snapshots
                    WHERE time = $1
                      AND exchange = $2
                      AND symbol = $3
                )
                """,
                rows,
            )

    async def _post_hl(self, session: aiohttp.ClientSession, symbol: str, payload: dict[str, Any]) -> Any:
        retries = 3
        for i in range(retries):
            try:
                async with self.hl_sem:
                    await self.hl_rate.wait(symbol)
                    async with session.post(HL_URL, json=payload, timeout=self.timeout) as resp:
                        if resp.status == 429:
                            await asyncio.sleep((2 ** (i + 1)) + random.uniform(0, 1))
                            continue
                        resp.raise_for_status()
                        return await resp.json()
            except Exception as exc:  # noqa: PERF203
                if i == retries - 1:
                    raise exc
                await asyncio.sleep((2 ** (i + 1)) + random.uniform(0, 1))
        return []

    async def _get_binance_klines(
        self,
        session: aiohttp.ClientSession,
        symbol: str,
        interval: str,
        start_ms: int,
        end_ms: int,
        limit: int = 1500,
    ) -> list[Any]:
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": start_ms,
            "endTime": end_ms,
            "limit": limit,
        }
        retries = 3
        for i in range(retries):
            try:
                async with self.binance_sem:
                    await self.binance_rate.wait()
                    async with session.get(BINANCE_KLINES_URL, params=params, timeout=self.timeout) as resp:
                        if resp.status == 429:
                            await asyncio.sleep((2 ** (i + 1)) + random.uniform(0, 1))
                            continue
                        resp.raise_for_status()
                        data = await resp.json()
                        if isinstance(data, list):
                            return data
                        return []
            except Exception as exc:  # noqa: PERF203
                if i == retries - 1:
                    raise exc
                await asyncio.sleep((2 ** (i + 1)) + random.uniform(0, 1))
        return []

    async def _download_hl_candles_for_symbol(
        self,
        session: aiohttp.ClientSession,
        symbol: str,
        interval: str,
        start: datetime,
        end: datetime,
    ) -> None:
        interval_ms = interval_to_ms(interval)
        chunk_ms = interval_ms * 1000
        day = start.replace(hour=0, minute=0, second=0, microsecond=0)
        while day < end:
            day_end = min(day + timedelta(days=1), end)
            # Use the actual fetch boundary (not bare midnight) so the
            # completeness check counts against the same window we fetched.
            actual_day_start = max(day, start)
            if await self._day_has_candles("hyperliquid", symbol, interval, actual_day_start, day_end):
                self.summary.hl_candle_days_skipped += 1
                day = day + timedelta(days=1)
                continue

            chunk_start_ms = max(dt_to_ms(day), dt_to_ms(start))
            day_end_ms = dt_to_ms(day_end)
            while chunk_start_ms < day_end_ms:
                chunk_end_ms = min(chunk_start_ms + chunk_ms, day_end_ms)
                print(
                    f"[HL candles] {symbol} {interval} "
                    f"{datetime.fromtimestamp(chunk_start_ms / 1000, tz=timezone.utc).isoformat()} -> "
                    f"{datetime.fromtimestamp(chunk_end_ms / 1000, tz=timezone.utc).isoformat()}"
                )
                payload = {
                    "type": "candleSnapshot",
                    "req": {
                        "coin": symbol,
                        "interval": interval,
                        "startTime": chunk_start_ms,
                        "endTime": chunk_end_ms,
                    },
                }
                try:
                    rows = await self._post_hl(session, symbol, payload)
                except Exception as exc:
                    self.summary.add_error(f"HL candle request failed for {symbol} {interval}: {exc}")
                    chunk_start_ms = chunk_end_ms
                    continue

                to_insert: list[tuple[Any, ...]] = []
                for row in rows if isinstance(rows, list) else []:
                    try:
                        ts = datetime.fromtimestamp(int(row["t"]) / 1000, tz=timezone.utc)
                        to_insert.append(
                            (
                                ts,
                                "hyperliquid",
                                symbol,
                                interval,
                                float(row["o"]),
                                float(row["h"]),
                                float(row["l"]),
                                float(row["c"]),
                                float(row["v"]),
                            )
                        )
                    except Exception:
                        continue
                await self._insert_candles_bulk(to_insert)
                self.summary.hl_candle_rows += len(to_insert)
                chunk_start_ms = chunk_end_ms

            day = day + timedelta(days=1)

    async def download_hl_candles(self, start: datetime, end: datetime) -> None:
        async with aiohttp.ClientSession() as session:
            tasks = []
            for symbol in HL_SYMBOLS:
                tasks.append(self._download_hl_candles_for_symbol(session, symbol, "1m", start, end))
                tasks.append(self._download_hl_candles_for_symbol(session, symbol, "5m", start, end))
            await asyncio.gather(*tasks)

    async def _download_hl_funding_for_symbol(
        self,
        session: aiohttp.ClientSession,
        symbol: str,
        start: datetime,
    ) -> None:
        if await self._has_funding(symbol, start):
            self.summary.hl_funding_symbols_skipped += 1
            return

        start_ms = dt_to_ms(start)
        print(f"[HL funding] {symbol} from {start.isoformat()}")
        payload = {
            "type": "fundingHistory",
            "req": {
                "coin": symbol,
                "startTime": start_ms,
            },
        }
        try:
            rows = await self._post_hl(session, symbol, payload)
        except Exception as exc:
            self.summary.add_error(f"HL funding request failed for {symbol}: {exc}")
            return

        to_insert: list[tuple[Any, ...]] = []
        for row in rows if isinstance(rows, list) else []:
            try:
                ts = datetime.fromtimestamp(int(row["time"]) / 1000, tz=timezone.utc)
                to_insert.append(
                    (
                        ts,
                        "hyperliquid_funding",
                        str(row.get("coin", symbol)),
                        0.0,
                        0.0,
                        float(row["fundingRate"]),
                    )
                )
            except Exception:
                continue
        await self._insert_funding_bulk(to_insert)
        self.summary.hl_funding_rows += len(to_insert)

    async def download_hl_funding(self, start: datetime) -> None:
        async with aiohttp.ClientSession() as session:
            tasks = [self._download_hl_funding_for_symbol(session, symbol, start) for symbol in HL_SYMBOLS]
            await asyncio.gather(*tasks)

    async def _download_binance_for_symbol(
        self,
        session: aiohttp.ClientSession,
        symbol: str,
        start: datetime,
        end: datetime,
    ) -> None:
        for interval in ("1m", "5m"):
            day = start.replace(hour=0, minute=0, second=0, microsecond=0)
            while day < end:
                day_end = min(day + timedelta(days=1), end)
                actual_day_start = max(day, start)
                if await self._day_has_candles("binance", symbol, interval, actual_day_start, day_end):
                    self.summary.binance_days_skipped += 1
                    day = day + timedelta(days=1)
                    continue

                start_ms = max(dt_to_ms(day), dt_to_ms(start))
                end_ms = dt_to_ms(day_end) - 1  # Binance endTime is inclusive; make range effectively exclusive.
                if end_ms < start_ms:
                    day = day + timedelta(days=1)
                    continue
                print(
                    f"[Binance klines] {symbol} {interval} "
                    f"{datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc).isoformat()} -> "
                    f"{datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc).isoformat()}"
                )
                try:
                    rows = await self._get_binance_klines(
                        session,
                        symbol,
                        interval,
                        start_ms,
                        end_ms,
                        limit=1500,
                    )
                except Exception as exc:
                    self.summary.add_error(
                        f"Binance klines request failed for {symbol} {interval}: {exc}"
                    )
                    day = day + timedelta(days=1)
                    continue

                to_insert: list[tuple[Any, ...]] = []
                for row in rows:
                    try:
                        ts = datetime.fromtimestamp(int(row[0]) / 1000, tz=timezone.utc)
                        to_insert.append(
                            (
                                ts,
                                "binance",
                                symbol,
                                interval,
                                float(row[1]),
                                float(row[2]),
                                float(row[3]),
                                float(row[4]),
                                float(row[5]),
                            )
                        )
                    except Exception:
                        continue

                await self._insert_candles_bulk(to_insert)
                self.summary.binance_candle_rows += len(to_insert)
                day = day + timedelta(days=1)

    async def download_binance_klines(self, start: datetime, end: datetime) -> None:
        async with aiohttp.ClientSession() as session:
            tasks = [self._download_binance_for_symbol(session, symbol, start, end) for symbol in BINANCE_SYMBOLS]
            await asyncio.gather(*tasks)

    async def run(self) -> None:
        await self.setup()
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=DAYS_BACK)

        print(f"Starting historical backfill: {start.isoformat()} -> {end.isoformat()}")
        await self.download_hl_candles(start=start, end=end)
        await self.download_hl_funding(start=start)
        await self.download_binance_klines(start=start, end=end)

        print("\nSummary")
        print(f"HL candle rows inserted: {self.summary.hl_candle_rows}")
        print(f"HL candle days skipped: {self.summary.hl_candle_days_skipped}")
        print(f"HL funding rows inserted: {self.summary.hl_funding_rows}")
        print(f"HL funding symbols skipped: {self.summary.hl_funding_symbols_skipped}")
        print(f"Binance candle rows inserted: {self.summary.binance_candle_rows}")
        print(f"Binance candle days skipped: {self.summary.binance_days_skipped}")
        print(f"Errors: {self.summary.errors}")
        for msg in self.summary.error_messages:
            print(f"- {msg}")


async def main() -> None:
    downloader = HistoricalDownloader()
    await downloader.run()


if __name__ == "__main__":
    asyncio.run(main())
