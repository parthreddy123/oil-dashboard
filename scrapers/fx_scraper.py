"""FX rate scraper using free APIs (Yahoo Finance + ExchangeRate-API fallback)."""

import os
import sys
import logging
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from scrapers.base_scraper import BaseScraper
from database.db_manager import upsert_fx_rate, log_scrape, init_db

logger = logging.getLogger(__name__)


class FXScraper(BaseScraper):
    def __init__(self):
        super().__init__("fx", cache_expiry_hours=12)

    def scrape(self):
        start = datetime.now()
        count = 0
        today = datetime.now().strftime("%Y-%m-%d")

        # Method 1: ExchangeRate-API (free, no key needed)
        try:
            url = "https://open.er-api.com/v6/latest/USD"
            data = self.fetch(url)
            if data and data.get("result") == "success":
                inr_rate = data["rates"].get("INR")
                if inr_rate and 50 < inr_rate < 120:
                    upsert_fx_rate(today, "USD/INR", float(inr_rate), "exchangerate_api")
                    count += 1
                    logger.info(f"USD/INR rate: {inr_rate}")
        except Exception as e:
            logger.warning(f"ExchangeRate-API FX failed: {e}")

        # Method 2: Fallback to Yahoo Finance
        if count == 0:
            try:
                url = "https://query1.finance.yahoo.com/v8/finance/chart/USDINR=X"
                params = {"range": "5d", "interval": "1d"}
                data = self.fetch(url, params=params)
                if data and "chart" in data:
                    result = data["chart"]["result"][0]
                    rate = result["meta"].get("regularMarketPrice")
                    if rate and 50 < rate < 120:
                        upsert_fx_rate(today, "USD/INR", float(rate), "yahoo_finance")
                        count += 1
                        logger.info(f"USD/INR rate (Yahoo): {rate}")
            except Exception as e:
                logger.warning(f"Yahoo Finance FX failed: {e}")

        duration = (datetime.now() - start).total_seconds()
        log_scrape("fx_rates", "success" if count > 0 else "failed", count, duration=duration)
        return count


def run():
    init_db()
    scraper = FXScraper()
    count = scraper.scrape()
    print(f"FX scraper: {count} rates")
    return count


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
