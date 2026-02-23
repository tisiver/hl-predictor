"""Launch HL + Binance collectors concurrently."""
import asyncio
import logging
import sys

sys.path.insert(0, ".")

from collector.hl_collector import HyperliquidCollector
from collector.binance_collector import BinanceCollector

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")


async def main():
    hl = HyperliquidCollector()
    bn = BinanceCollector()
    await asyncio.gather(hl.run(), bn.run())


if __name__ == "__main__":
    asyncio.run(main())
