"""Google News RSS aggregator with deduplication."""

import os
import re
import logging
from datetime import datetime
from urllib.parse import quote_plus

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from scrapers.base_scraper import BaseScraper
from database.db_manager import insert_news_article, log_scrape, init_db

logger = logging.getLogger(__name__)

NEWS_QUERIES = [
    "oil prices today",
    "crude oil market",
    "India refinery",
    "OPEC production",
    "oil and gas India",
    "petroleum India",
    "global oil demand supply",
]


class NewsAggregator(BaseScraper):
    def __init__(self):
        super().__init__("news_aggregator", cache_expiry_hours=3, rate_limit_seconds=3)
        self.rss_base = "https://news.google.com/rss/search?q="

    def _clean_html(self, text):
        if not text:
            return ""
        return re.sub(r"<[^>]+>", "", text).strip()

    def scrape(self):
        start = datetime.now()
        total_count = 0
        seen_urls = set()

        for query in NEWS_QUERIES:
            try:
                url = self.rss_base + quote_plus(query) + "&hl=en-IN&gl=IN&ceid=IN:en"
                feed = self.fetch_rss(url)
                if not feed or not feed.entries:
                    continue

                for entry in feed.entries[:15]:
                    title = entry.get("title", "").strip()
                    link = entry.get("link", "").strip()
                    summary = self._clean_html(entry.get("summary", ""))
                    published = entry.get("published", "")

                    if not title or not link or link in seen_urls:
                        continue
                    seen_urls.add(link)

                    pub_date = None
                    if published:
                        try:
                            from email.utils import parsedate_to_datetime
                            pub_date = parsedate_to_datetime(published).strftime("%Y-%m-%d %H:%M:%S")
                        except Exception:
                            pub_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                    # Extract source from title (Google News format: "Title - Source")
                    source_name = "google_news"
                    if " - " in title:
                        parts = title.rsplit(" - ", 1)
                        if len(parts) == 2:
                            source_name = parts[1].strip()
                            title = parts[0].strip()

                    inserted = insert_news_article(
                        published_date=pub_date,
                        title=title,
                        summary=summary[:500] if summary else None,
                        url=link,
                        source=source_name,
                        category=query.replace(" ", "_"),
                    )
                    if inserted:
                        total_count += 1

            except Exception as e:
                logger.error(f"Error fetching news for '{query}': {e}")

        duration = (datetime.now() - start).total_seconds()
        log_scrape("news_aggregator", "success" if total_count > 0 else "failed",
                   total_count, duration=duration)
        logger.info(f"News aggregator: {total_count} new articles from {len(NEWS_QUERIES)} queries")
        return total_count


def run():
    init_db()
    scraper = NewsAggregator()
    count = scraper.scrape()
    print(f"News aggregator: {count} articles")
    return count


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
