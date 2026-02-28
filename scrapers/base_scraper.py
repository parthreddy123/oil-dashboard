"""Base scraper with retry, caching, and rate limiting."""

import os
import time
import json
import hashlib
import logging
import requests
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

CACHE_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "cache")


class BaseScraper:
    """Base class for all scrapers with retry, cache, and rate-limit support."""

    def __init__(self, name, cache_expiry_hours=24, max_retries=3, rate_limit_seconds=2):
        self.name = name
        self.cache_expiry = timedelta(hours=cache_expiry_hours)
        self.max_retries = max_retries
        self.rate_limit = rate_limit_seconds
        self._last_request_time = 0
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "OilDashboard/1.0 (Educational Research Tool)"
        })
        os.makedirs(CACHE_DIR, exist_ok=True)

    def _cache_key(self, url, params=None):
        raw = url + json.dumps(params or {}, sort_keys=True)
        return hashlib.md5(raw.encode()).hexdigest()

    def _cache_path(self, key):
        return os.path.join(CACHE_DIR, f"{self.name}_{key}.json")

    def _get_cached(self, key):
        path = self._cache_path(key)
        if not os.path.exists(path):
            return None
        try:
            with open(path, "r") as f:
                cached = json.load(f)
            cached_time = datetime.fromisoformat(cached["timestamp"])
            if datetime.now() - cached_time < self.cache_expiry:
                logger.debug(f"[{self.name}] Cache hit for {key}")
                return cached["data"]
        except (json.JSONDecodeError, KeyError):
            pass
        return None

    def _set_cache(self, key, data):
        path = self._cache_path(key)
        with open(path, "w") as f:
            json.dump({"timestamp": datetime.now().isoformat(), "data": data}, f)

    def _rate_limit_wait(self):
        elapsed = time.time() - self._last_request_time
        if elapsed < self.rate_limit:
            time.sleep(self.rate_limit - elapsed)
        self._last_request_time = time.time()

    def fetch(self, url, params=None, use_cache=True, response_type="json"):
        """Fetch URL with retry, cache, and rate limiting."""
        cache_key = self._cache_key(url, params)

        if use_cache:
            cached = self._get_cached(cache_key)
            if cached is not None:
                return cached

        last_error = None
        for attempt in range(1, self.max_retries + 1):
            try:
                self._rate_limit_wait()
                resp = self.session.get(url, params=params, timeout=30)
                resp.raise_for_status()

                if response_type == "json":
                    data = resp.json()
                elif response_type == "text":
                    data = resp.text
                elif response_type == "bytes":
                    # Don't cache binary in JSON
                    return resp.content
                else:
                    data = resp.text

                if use_cache and response_type != "bytes":
                    self._set_cache(cache_key, data)
                return data

            except requests.RequestException as e:
                last_error = e
                logger.warning(f"[{self.name}] Attempt {attempt}/{self.max_retries} failed: {e}")
                if attempt < self.max_retries:
                    time.sleep(2 ** attempt)

        logger.error(f"[{self.name}] All {self.max_retries} attempts failed for {url}: {last_error}")
        return None

    def fetch_rss(self, url):
        """Fetch and parse RSS feed."""
        import feedparser
        cache_key = self._cache_key(url)
        cached = self._get_cached(cache_key)
        if cached:
            return feedparser.parse(cached)

        self._rate_limit_wait()
        text = self.fetch(url, response_type="text", use_cache=False)
        if text:
            self._set_cache(cache_key, text)
            return feedparser.parse(text)
        return None

    def download_pdf(self, url, filename=None):
        """Download a PDF file to the cache directory."""
        if not filename:
            filename = self._cache_key(url) + ".pdf"
        path = os.path.join(CACHE_DIR, filename)

        if os.path.exists(path):
            age = datetime.now() - datetime.fromtimestamp(os.path.getmtime(path))
            if age < self.cache_expiry:
                return path

        content = self.fetch(url, response_type="bytes", use_cache=False)
        if content:
            with open(path, "wb") as f:
                f.write(content)
            return path
        return None

    def scrape(self):
        """Override in subclasses. Should return number of records scraped."""
        raise NotImplementedError
