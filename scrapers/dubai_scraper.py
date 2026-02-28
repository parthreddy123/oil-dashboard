"""Dubai/Oman crude price scraper using free sources."""

import os
import sys
import re
import logging
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from scrapers.base_scraper import BaseScraper
from database.db_manager import upsert_crude_price, get_latest_price, log_scrape, init_db

logger = logging.getLogger(__name__)

# Historical average Brent-Dubai spread (USD/bbl)
DEFAULT_BRENT_DUBAI_SPREAD = 2.50


class DubaiScraper(BaseScraper):
    def __init__(self):
        super().__init__("dubai", cache_expiry_hours=12)

    def scrape(self):
        start = datetime.now()
        count = 0
        today = datetime.now().strftime("%Y-%m-%d")

        # Method 1: Try OilPrice.com commodity pages for Dubai price
        try:
            html = self.fetch(
                "https://oilprice.com/oil-price-charts/",
                response_type="text", use_cache=False,
            )
            if html:
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(html, "lxml")
                for row in soup.select("tr, .commodity-row, [class*='price']"):
                    text = row.get_text(strip=True)
                    match = re.search(
                        r"(?:Dubai|Oman|Murban).*?(\d{2,3}\.\d{1,2})",
                        text, re.IGNORECASE,
                    )
                    if match:
                        price = float(match.group(1))
                        if 20 < price < 200:
                            upsert_crude_price(today, "oman_dubai", price, source="oilprice")
                            count += 1
                            logger.info(f"Dubai/Oman price from OilPrice: ${price}")
                            break
        except Exception as e:
            logger.warning(f"OilPrice Dubai scrape failed: {e}")

        # Method 2: Fallback — derive from Brent using typical spread
        if count == 0:
            brent = get_latest_price("brent")
            if brent:
                brent_price = float(brent["price"])
                dubai_price = round(brent_price - DEFAULT_BRENT_DUBAI_SPREAD, 2)
                upsert_crude_price(today, "oman_dubai", dubai_price, source="derived_from_brent")
                count += 1
                logger.info(
                    f"Dubai/Oman derived from Brent: ${dubai_price} "
                    f"(Brent ${brent_price} - spread ${DEFAULT_BRENT_DUBAI_SPREAD})"
                )

        duration = (datetime.now() - start).total_seconds()
        log_scrape("dubai_crude", "success" if count > 0 else "failed", count, duration=duration)
        return count


def run():
    init_db()
    scraper = DubaiScraper()
    count = scraper.scrape()
    print(f"Dubai scraper: {count} records")
    return count


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
