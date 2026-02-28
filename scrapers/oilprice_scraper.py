"""OilPrice.com scraper for live prices and news RSS."""

import os
import logging
import re
from datetime import datetime

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from scrapers.base_scraper import BaseScraper
from database.db_manager import (
    upsert_crude_price, insert_news_article, log_scrape, init_db
)

logger = logging.getLogger(__name__)


class OilPriceScraper(BaseScraper):
    def __init__(self):
        super().__init__("oilprice", cache_expiry_hours=1)
        self.rss_url = "https://oilprice.com/rss/main"

    def scrape_live_prices(self):
        """Scrape current oil prices from OilPrice.com page."""
        start = datetime.now()
        count = 0
        try:
            from bs4 import BeautifulSoup
            html = self.fetch("https://oilprice.com/", response_type="text", use_cache=False)
            if not html:
                return 0

            soup = BeautifulSoup(html, "lxml")
            today = datetime.now().strftime("%Y-%m-%d")

            # Look for price elements on the page
            for widget in soup.select(".commodity_price, .oil-price-widget, [class*='price']"):
                text = widget.get_text(strip=True)
                # Try to extract benchmark name and price
                for benchmark, pattern in [
                    ("brent", r"Brent.*?(\d+\.?\d*)"),
                    ("wti", r"WTI.*?(\d+\.?\d*)"),
                ]:
                    match = re.search(pattern, text, re.IGNORECASE)
                    if match:
                        price = float(match.group(1))
                        if 20 < price < 200:  # sanity check
                            upsert_crude_price(today, benchmark, price, source="oilprice")
                            count += 1

        except Exception as e:
            logger.error(f"Error scraping live prices: {e}")

        duration = (datetime.now() - start).total_seconds()
        log_scrape("oilprice_prices", "success" if count > 0 else "failed", count, duration=duration)
        return count

    def scrape_news(self):
        """Scrape news from OilPrice.com RSS feed."""
        start = datetime.now()
        count = 0

        try:
            feed = self.fetch_rss(self.rss_url)
            if not feed or not feed.entries:
                logger.warning("No entries in OilPrice RSS feed")
                return 0

            for entry in feed.entries[:30]:
                title = entry.get("title", "").strip()
                summary = entry.get("summary", "").strip()
                link = entry.get("link", "").strip()
                published = entry.get("published", "")

                if not title or not link:
                    continue

                # Parse published date
                pub_date = None
                if published:
                    try:
                        from email.utils import parsedate_to_datetime
                        pub_date = parsedate_to_datetime(published).strftime("%Y-%m-%d %H:%M:%S")
                    except Exception:
                        pub_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                # Remove HTML tags from summary
                if summary:
                    summary = re.sub(r"<[^>]+>", "", summary).strip()

                inserted = insert_news_article(
                    published_date=pub_date,
                    title=title,
                    summary=summary[:500] if summary else None,
                    url=link,
                    source="oilprice",
                    category="oil_gas",
                )
                if inserted:
                    count += 1

        except Exception as e:
            logger.error(f"Error scraping OilPrice news: {e}")

        duration = (datetime.now() - start).total_seconds()
        log_scrape("oilprice_news", "success" if count > 0 else "partial", count, duration=duration)
        return count

    def scrape(self):
        prices = self.scrape_live_prices()
        news = self.scrape_news()
        return prices + news


def run():
    init_db()
    scraper = OilPriceScraper()
    count = scraper.scrape()
    print(f"OilPrice scraper: {count} records")
    return count


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
