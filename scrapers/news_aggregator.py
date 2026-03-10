"""Google News RSS aggregator with deduplication and direct RSS feeds."""

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

# ~24 Google News queries covering geopolitical, market, and regional angles
NEWS_QUERIES = [
    # Core oil market
    "oil prices today",
    "crude oil market",
    "Brent crude price",
    "WTI crude oil",
    "global oil demand supply",
    "oil futures trading",
    # OPEC and production
    "OPEC production cut",
    "OPEC+ meeting output",
    "Saudi Arabia oil production",
    "Russia oil exports",
    "US shale oil production",
    # India specific
    "India refinery",
    "oil and gas India",
    "petroleum India",
    "Indian oil imports",
    "India fuel prices petrol diesel",
    # Geopolitical
    "Middle East oil conflict",
    "Iran oil sanctions",
    "oil pipeline geopolitics",
    "energy security oil",
    "oil embargo sanctions",
    # Downstream and products
    "refinery crack spread margins",
    "LNG natural gas prices",
    "petrochemical feedstock prices",
]

# Direct RSS feeds from energy and geopolitical sources
# Each entry: (url, source_name, needs_oil_filter)
RSS_FEEDS = [
    # Oil-specific feeds (no keyword filter needed)
    ("https://www.rigzone.com/news/rss/rigzone_latest.aspx", "Rigzone", False),
    ("https://www.hellenicshippingnews.com/category/oil-energy/feed/", "Hellenic Shipping News", False),
    ("https://oilprice.com/rss/main", "OilPrice.com", False),
    ("https://www.spglobal.com/commodityinsights/en/rss/oil", "S&P Global Commodity Insights", False),
    ("https://www.spglobal.com/commodityinsights/en/rss/latest-news", "S&P Global Commodities", False),
    ("https://feeds.reuters.com/reuters/businessNews", "Reuters Business", True),
    ("https://www.reuters.com/markets/commodities/rss", "Reuters Commodities", False),
    # General news with oil keyword filter
    ("https://www.aljazeera.com/xml/rss/all.xml", "Al Jazeera", True),
    ("https://www.middleeasteye.net/rss", "Middle East Eye", True),
]

# Keywords for filtering non-oil-specific feeds
OIL_ENERGY_KEYWORDS = re.compile(
    r"\b("
    r"oil|crude|petroleum|petrol|diesel|gasoline|kerosene|naphtha|fuel\s*oil|"
    r"brent|wti|opec|refiner|lng|lpg|natural\s*gas|barrel|bbl|"
    r"pipeline|tanker|energy|hydrocarbon|shale|fracking|upstream|downstream|"
    r"drill|offshore|onshore|oilfield|petrochemical|"
    r"saudi\s*aramco|exxon|chevron|shell|bp|total\s*energies|reliance|ioc[l]?|"
    r"crack\s*spread|grm|refining\s*margin|"
    r"sanction.*iran|sanction.*russia|embargo|strait\s*of\s*hormuz|"
    r"oil\s*price|fuel\s*price|energy\s*crisis|energy\s*security"
    r")\b",
    re.IGNORECASE,
)


def _matches_oil_keywords(title, summary):
    """Check if title or summary contains oil/energy related keywords."""
    text = f"{title} {summary}"
    return bool(OIL_ENERGY_KEYWORDS.search(text))


class NewsAggregator(BaseScraper):
    def __init__(self):
        super().__init__("news_aggregator", cache_expiry_hours=3, rate_limit_seconds=3)
        self.rss_base = "https://news.google.com/rss/search?q="

    def _clean_html(self, text):
        if not text:
            return ""
        return re.sub(r"<[^>]+>", "", text).strip()

    def _parse_pub_date(self, published):
        """Parse published date string into standardized format."""
        if not published:
            return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            from email.utils import parsedate_to_datetime
            return parsedate_to_datetime(published).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def _process_entry(self, entry, seen_urls, source_name, category, extract_source_from_title=False):
        """Process a single RSS entry. Returns True if inserted, False otherwise."""
        title = entry.get("title", "").strip()
        link = entry.get("link", "").strip()
        summary = self._clean_html(entry.get("summary", ""))
        published = entry.get("published", "")

        if not title or not link or link in seen_urls:
            return False
        seen_urls.add(link)

        pub_date = self._parse_pub_date(published)

        # Extract source from title for Google News (format: "Title - Source")
        if extract_source_from_title:
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
            category=category,
        )
        return inserted

    def scrape(self):
        start = datetime.now()
        total_count = 0
        seen_urls = set()

        # --- Part 1: Google News queries ---
        google_count = 0
        for query in NEWS_QUERIES:
            try:
                url = self.rss_base + quote_plus(query) + "&hl=en-IN&gl=IN&ceid=IN:en"
                feed = self.fetch_rss(url)
                if not feed or not feed.entries:
                    continue

                for entry in feed.entries[:25]:
                    if self._process_entry(entry, seen_urls, "google_news",
                                           query.replace(" ", "_"),
                                           extract_source_from_title=True):
                        google_count += 1

            except Exception as e:
                logger.error(f"Error fetching news for '{query}': {e}")

        total_count += google_count
        logger.info(f"Google News: {google_count} new articles from {len(NEWS_QUERIES)} queries")

        # --- Part 2: Direct RSS feeds ---
        rss_count = 0
        for feed_url, source_name, needs_filter in RSS_FEEDS:
            try:
                feed = self.fetch_rss(feed_url)
                if not feed or not feed.entries:
                    logger.debug(f"No entries from {source_name} ({feed_url})")
                    continue

                feed_articles = 0
                for entry in feed.entries[:25]:
                    title = entry.get("title", "").strip()
                    summary = self._clean_html(entry.get("summary", ""))

                    # Apply oil keyword filter for non-oil-specific feeds
                    if needs_filter and not _matches_oil_keywords(title, summary):
                        continue

                    category = source_name.lower().replace(" ", "_")
                    if self._process_entry(entry, seen_urls, source_name, category,
                                           extract_source_from_title=False):
                        feed_articles += 1
                        rss_count += 1

                logger.debug(f"{source_name}: {feed_articles} new articles")

            except Exception as e:
                logger.error(f"Error fetching RSS from {source_name} ({feed_url}): {e}")

        total_count += rss_count
        logger.info(f"Direct RSS: {rss_count} new articles from {len(RSS_FEEDS)} feeds")

        duration = (datetime.now() - start).total_seconds()
        log_scrape("news_aggregator", "success" if total_count > 0 else "failed",
                   total_count, duration=duration)
        logger.info(f"News aggregator total: {total_count} new articles "
                    f"({google_count} Google + {rss_count} RSS) in {duration:.1f}s")
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
