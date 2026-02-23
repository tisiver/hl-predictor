import os
from pathlib import Path
from typing import Optional
from datetime import datetime

import asyncpg
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

_pool: Optional[asyncpg.Pool] = None


async def connect() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        db_url = os.getenv("DB_URL")
        if not db_url:
            raise RuntimeError("DB_URL is not set")
        _pool = await asyncpg.create_pool(dsn=db_url, min_size=1, max_size=10)
    return _pool


async def create_tables() -> None:
    pool = await connect()
    async with pool.acquire() as conn:
        await conn.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                time        TIMESTAMPTZ NOT NULL,
                exchange    TEXT NOT NULL,
                symbol      TEXT NOT NULL,
                price       FLOAT8 NOT NULL,
                size        FLOAT8 NOT NULL,
                side        TEXT NOT NULL
            );
        """)
        await conn.execute(
            "SELECT create_hypertable('trades', 'time', if_not_exists => TRUE);"
        )

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS oi_snapshots (
                time         TIMESTAMPTZ NOT NULL,
                exchange     TEXT NOT NULL,
                symbol       TEXT NOT NULL,
                oi           FLOAT8 NOT NULL,
                mark_price   FLOAT8 NOT NULL,
                funding_rate FLOAT8 NOT NULL
            );
        """)
        await conn.execute(
            "SELECT create_hypertable('oi_snapshots', 'time', if_not_exists => TRUE);"
        )

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS orderbook_imbalance (
                time       TIMESTAMPTZ NOT NULL,
                exchange   TEXT NOT NULL,
                symbol     TEXT NOT NULL,
                bid_vol    FLOAT8 NOT NULL,
                ask_vol    FLOAT8 NOT NULL,
                spread     FLOAT8 NOT NULL,
                imbalance  FLOAT8 NOT NULL
            );
        """)
        await conn.execute(
            "SELECT create_hypertable('orderbook_imbalance', 'time', if_not_exists => TRUE);"
        )

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS liquidations (
                time      TIMESTAMPTZ NOT NULL,
                exchange  TEXT NOT NULL,
                symbol    TEXT NOT NULL,
                side      TEXT NOT NULL,
                price     FLOAT8 NOT NULL,
                size      FLOAT8 NOT NULL
            );
        """)
        await conn.execute(
            "SELECT create_hypertable('liquidations', 'time', if_not_exists => TRUE);"
        )

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS candles (
                time      TIMESTAMPTZ NOT NULL,
                exchange  TEXT NOT NULL,
                symbol    TEXT NOT NULL,
                interval  TEXT NOT NULL,
                open      FLOAT8 NOT NULL,
                high      FLOAT8 NOT NULL,
                low       FLOAT8 NOT NULL,
                close     FLOAT8 NOT NULL,
                volume    FLOAT8 NOT NULL
            );
        """)
        await conn.execute(
            "SELECT create_hypertable('candles', 'time', if_not_exists => TRUE);"
        )


async def insert_trade(time: datetime, exchange: str, symbol: str, price: float, size: float, side: str) -> None:
    pool = await connect()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO trades (time, exchange, symbol, price, size, side) VALUES ($1,$2,$3,$4,$5,$6)",
            time, exchange, symbol, price, size, side,
        )


async def insert_oi_snapshot(time: datetime, exchange: str, symbol: str, oi: float, mark_price: float, funding_rate: float) -> None:
    pool = await connect()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO oi_snapshots (time, exchange, symbol, oi, mark_price, funding_rate) VALUES ($1,$2,$3,$4,$5,$6)",
            time, exchange, symbol, oi, mark_price, funding_rate,
        )


async def insert_orderbook(time: datetime, exchange: str, symbol: str, bid_vol: float, ask_vol: float, spread: float, imbalance: float) -> None:
    pool = await connect()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO orderbook_imbalance (time, exchange, symbol, bid_vol, ask_vol, spread, imbalance) VALUES ($1,$2,$3,$4,$5,$6,$7)",
            time, exchange, symbol, bid_vol, ask_vol, spread, imbalance,
        )


async def insert_liquidation(time: datetime, exchange: str, symbol: str, side: str, price: float, size: float) -> None:
    pool = await connect()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO liquidations (time, exchange, symbol, side, price, size) VALUES ($1,$2,$3,$4,$5,$6)",
            time, exchange, symbol, side, price, size,
        )


async def insert_candle(
    time: datetime,
    exchange: str,
    symbol: str,
    interval: str,
    open_: float,
    high: float,
    low: float,
    close: float,
    volume: float,
) -> None:
    pool = await connect()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO candles (time, exchange, symbol, interval, open, high, low, close, volume) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)",
            time, exchange, symbol, interval, open_, high, low, close, volume,
        )


class _DB:
    async def connect(self): return await connect()
    async def create_tables(self): return await create_tables()
    async def insert_trade(self, *a, **kw): return await insert_trade(*a, **kw)
    async def insert_oi_snapshot(self, *a, **kw): return await insert_oi_snapshot(*a, **kw)
    async def insert_orderbook(self, *a, **kw): return await insert_orderbook(*a, **kw)
    async def insert_liquidation(self, *a, **kw): return await insert_liquidation(*a, **kw)
    async def insert_candle(self, *a, **kw): return await insert_candle(*a, **kw)


db = _DB()
