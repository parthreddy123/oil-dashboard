"""Derived benchmark scraper for Murban and OPEC Basket.

These benchmarks are estimated from existing data using typical historical differentials.
- Murban: Dubai/Oman + $1.00 (Murban typically trades $0.50-$1.50 above Dubai)
- OPEC Basket: Brent - $2.00 (OPEC basket typically $1-3 below Brent)
"""

import os
import sys
import logging
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from database.db_manager import get_connection, upsert_crude_prices_bulk, init_db, log_scrape

logger = logging.getLogger(__name__)

DERIVED_BENCHMARKS = {
    "murban": {
        "base": "oman_dubai",
        "spread": 1.00,
        "source": "estimated_from_dubai",
    },
    "arab_light": {
        "base": "oman_dubai",
        "spread": 0.50,
        "source": "estimated_from_dubai",
    },
    "basrah_light": {
        "base": "oman_dubai",
        "spread": -2.50,
        "source": "estimated_from_dubai",
    },
    "tapis": {
        "base": "brent",
        "spread": 1.50,
        "source": "estimated_from_brent",
    },
    "espo": {
        "base": "brent",
        "spread": -5.00,
        "source": "estimated_from_brent",
    },
    "opec_basket": {
        "base": "brent",
        "spread": -2.00,
        "source": "estimated_from_brent",
    },
}


def scrape(days=90):
    """Derive additional benchmarks from existing crude price data."""
    start = datetime.now()
    total = 0

    with get_connection() as conn:
        for benchmark, cfg in DERIVED_BENCHMARKS.items():
            rows = conn.execute(
                """SELECT date, price FROM crude_prices
                   WHERE benchmark = ? ORDER BY date DESC LIMIT ?""",
                (cfg["base"], days),
            ).fetchall()

            if not rows:
                logger.warning(f"No {cfg['base']} data to derive {benchmark}")
                continue

            bulk = []
            prev_price = None
            for r in sorted(rows, key=lambda x: x["date"]):
                derived = round(float(r["price"]) + cfg["spread"], 2)
                change_pct = None
                if prev_price and prev_price != 0:
                    change_pct = round((derived - prev_price) / prev_price * 100, 2)
                prev_price = derived
                bulk.append((r["date"], benchmark, derived, change_pct, cfg["source"]))

            upsert_crude_prices_bulk(bulk)
            total += len(bulk)
            logger.info(f"Derived {len(bulk)} {benchmark} prices from {cfg['base']}")

    duration = (datetime.now() - start).total_seconds()
    log_scrape("benchmarks", "success" if total > 0 else "failed", total, duration=duration)
    return total


def run():
    init_db()
    count = scrape()
    print(f"Benchmark scraper: {count} records")
    return count


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
