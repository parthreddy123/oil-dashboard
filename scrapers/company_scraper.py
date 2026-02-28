"""Indian oil company press release scraper."""

import os
import re
import logging
from datetime import datetime

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from scrapers.base_scraper import BaseScraper
from database.db_manager import insert_news_article, log_scrape, init_db

logger = logging.getLogger(__name__)

COMPANIES = {
    "IOCL": {
        "name": "Indian Oil Corporation",
        "url": "https://iocl.com/NewsDetail.aspx",
    },
    "BPCL": {
        "name": "Bharat Petroleum",
        "url": "https://www.bharatpetroleum.in/media/press-releases.aspx",
    },
    "HPCL": {
        "name": "Hindustan Petroleum",
        "url": "https://www.hindustanpetroleum.com/pressreleases",
    },
    "RIL": {
        "name": "Reliance Industries",
        "url": "https://www.ril.com/news-media/press-releases",
    },
    "ONGC": {
        "name": "ONGC",
        "url": "https://www.ongcindia.com/wps/wcm/connect/en/media/press-release/",
    },
}


class CompanyScraper(BaseScraper):
    def __init__(self):
        super().__init__("company", cache_expiry_hours=24, rate_limit_seconds=5)

    def _scrape_generic_page(self, company_id, company_info):
        """Generic scraper that tries to extract press releases from HTML pages."""
        count = 0
        try:
            html = self.fetch(company_info["url"], response_type="text")
            if not html:
                return 0

            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html, "lxml")

            # Try common patterns for news/press release links
            articles = []

            # Pattern 1: Look for links in common containers
            for container in soup.select(
                ".news-list, .press-release, .media-list, article, "
                ".news-item, .content-list li, .press-list li, table.GridView tr"
            ):
                link = container.find("a", href=True)
                if link:
                    title = link.get_text(strip=True)
                    href = link["href"]
                    if title and len(title) > 10:
                        articles.append((title, href))

            # Pattern 2: Fallback - find all links with oil/gas-related text
            if not articles:
                for link in soup.find_all("a", href=True):
                    text = link.get_text(strip=True)
                    if len(text) > 20 and any(k in text.lower() for k in
                            ["oil", "gas", "petrol", "diesel", "refin", "crude",
                             "production", "capacity", "quarter", "annual"]):
                        articles.append((text, link["href"]))

            for title, href in articles[:10]:
                if href.startswith("/"):
                    # Build full URL from base
                    from urllib.parse import urljoin
                    href = urljoin(company_info["url"], href)

                inserted = insert_news_article(
                    published_date=datetime.now().strftime("%Y-%m-%d"),
                    title=title[:300],
                    summary=f"Press release from {company_info['name']}",
                    url=href,
                    source=company_id,
                    category="company_press_release",
                )
                if inserted:
                    count += 1

        except Exception as e:
            logger.error(f"Error scraping {company_id}: {e}")

        return count

    def scrape(self):
        """Scrape press releases from all Indian oil companies."""
        start = datetime.now()
        total_count = 0

        for company_id, company_info in COMPANIES.items():
            try:
                count = self._scrape_generic_page(company_id, company_info)
                total_count += count
                logger.info(f"Scraped {count} articles from {company_id}")
            except Exception as e:
                logger.error(f"Error with {company_id}: {e}")

        duration = (datetime.now() - start).total_seconds()
        log_scrape("company", "success" if total_count > 0 else "partial",
                   total_count, duration=duration)
        return total_count


def run():
    init_db()
    scraper = CompanyScraper()
    count = scraper.scrape()
    print(f"Company scraper: {count} press releases")
    return count


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
