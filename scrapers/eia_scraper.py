"""EIA API scraper for crude prices, product spot prices, and international production."""

import os
import logging
from datetime import datetime, timedelta
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from scrapers.base_scraper import BaseScraper
from config.secrets_helper import get_secret
from database.db_manager import (
    upsert_crude_prices_bulk, upsert_product_price,
    insert_global_event, log_scrape, init_db,
)
logger = logging.getLogger(__name__)

# EIA product series for spot prices (USD/gallon)
PRODUCT_SERIES = {
    "EER_EPMRU_PF4_RGC_DPG": ("petrol", "RBOB Gasoline"),
    "EER_EPD2DXL0_PF4_RGC_DPG": ("diesel", "ULSD Diesel"),
    "EER_EPJK_PF4_RGC_DPG": ("atf", "Jet Fuel"),
    "EER_EPLLPA_PF4_Y44MB_DPG": ("lpg", "Propane (LPG proxy)"),
}

# 1 barrel = 42 US gallons
GALLONS_PER_BARREL = 42.0

# OPEC + key producer country codes
OPEC_COUNTRIES = {
    "SAU": "Saudi Arabia",
    "IRQ": "Iraq",
    "IRN": "Iran",
    "ARE": "UAE",
    "KWT": "Kuwait",
    "NGA": "Nigeria",
    "LBY": "Libya",
    "AGO": "Angola",
    "VEN": "Venezuela",
    "DZA": "Algeria",
    "RUS": "Russia",
}


class EIAScraper(BaseScraper):
    def __init__(self):
        super().__init__("eia", cache_expiry_hours=12)
        self.api_key = get_secret("EIA_API_KEY")
        self.base_url = "https://api.eia.gov/v2"

    def _fetch_series(self, series_id, start_date=None, limit=365):
        """Fetch a data series from EIA API v2."""
        url = f"{self.base_url}/petroleum/pri/spt/data/"
        params = {
            "api_key": self.api_key,
            "frequency": "daily",
            "data[0]": "value",
            "sort[0][column]": "period",
            "sort[0][direction]": "desc",
            "length": limit,
        }
        if series_id == "RBRTE":
            params["facets[series][]"] = "RBRTE"
        elif series_id == "RWTC":
            params["facets[series][]"] = "RWTC"
        else:
            params["facets[series][]"] = series_id

        if start_date:
            params["start"] = str(start_date)

        data = self.fetch(url, params=params)
        if data and "response" in data and "data" in data["response"]:
            return data["response"]["data"]
        return None

    def scrape_crude_prices(self, days=365):
        """Scrape Brent and WTI daily prices."""
        start = datetime.now()
        total_records = 0

        for series_id, benchmark in [("RBRTE", "brent"), ("RWTC", "wti")]:
            try:
                records = self._fetch_series(series_id, limit=days)
                if not records:
                    logger.warning(f"No data returned for {benchmark}")
                    continue

                # Records come sorted DESC (newest first); reverse to chronological order
                # so change_pct is calculated as (today - yesterday) / yesterday
                records_chrono = sorted(
                    [r for r in records if r.get("value") is not None and r.get("period") is not None],
                    key=lambda r: r["period"],
                )
                rows = []
                prev_price = None
                for rec in records_chrono:
                    price = float(rec["value"])
                    period = rec["period"]
                    change_pct = None
                    if prev_price and prev_price != 0:
                        change_pct = round((price - prev_price) / prev_price * 100, 2)
                    prev_price = price
                    rows.append((period, benchmark, price, change_pct, "eia"))

                if rows:
                    upsert_crude_prices_bulk(rows)
                    total_records += len(rows)
                    logger.info(f"Scraped {len(rows)} {benchmark} prices from EIA")

            except Exception as e:
                logger.error(f"Error scraping {benchmark}: {e}")

        duration = (datetime.now() - start).total_seconds()
        status = "success" if total_records > 0 else "failed"
        log_scrape("eia_crude_prices", status, total_records, duration=duration)
        return total_records

    def scrape_product_prices(self, days=365):
        """Scrape US product spot prices (gasoline, diesel, jet fuel) in USD/gallon,
        convert to USD/bbl, and store as international product prices."""
        start = datetime.now()
        total_records = 0

        for series_id, (product_key, product_name) in PRODUCT_SERIES.items():
            try:
                records = self._fetch_series(series_id, limit=days)
                if not records:
                    logger.warning(f"No data for {product_name}")
                    continue

                count = 0
                for rec in records:
                    price_gal = rec.get("value")
                    period = rec.get("period")
                    if price_gal is None or period is None:
                        continue
                    price_gal = float(price_gal)
                    # Convert USD/gallon to USD/barrel
                    price_bbl = round(price_gal * GALLONS_PER_BARREL, 2)
                    upsert_product_price(
                        period, product_key, price_bbl,
                        unit="USD/bbl", location="International", source="eia",
                    )
                    count += 1

                total_records += count
                logger.info(f"Scraped {count} {product_name} prices from EIA")

            except Exception as e:
                logger.error(f"Error scraping {product_key}: {e}")

        duration = (datetime.now() - start).total_seconds()
        status = "success" if total_records > 0 else "failed"
        log_scrape("eia_product_prices", status, total_records, duration=duration)
        return total_records

    def scrape_international_production(self, months=12):
        """Scrape OPEC and key producer country petroleum production from EIA international data."""
        start = datetime.now()
        total_records = 0

        url = f"{self.base_url}/international/data/"

        for country_code, country_name in OPEC_COUNTRIES.items():
            try:
                params = {
                    "api_key": self.api_key,
                    "frequency": "monthly",
                    "data[0]": "value",
                    "facets[productId][]": "57",  # Crude oil + condensate
                    "facets[activityId][]": "1",   # Production
                    "facets[countryRegionId][]": country_code,
                    "facets[unit][]": "TBPD",     # Thousand barrels per day
                    "sort[0][column]": "period",
                    "sort[0][direction]": "desc",
                    "length": months,
                }

                data = self.fetch(url, params=params)
                if not data or "response" not in data or "data" not in data["response"]:
                    logger.warning(f"No production data for {country_name}")
                    continue

                records = data["response"]["data"]
                count = 0
                for rec in records:
                    value = rec.get("value")
                    period = rec.get("period")
                    if value is None or period is None:
                        continue

                    # Convert period YYYY-MM to first of month
                    date_str = f"{period}-01"
                    production_tbpd = float(value)
                    # Convert TBPD to mb/d (divide by 1000)
                    production_mbd = round(production_tbpd / 1000, 3)

                    insert_global_event(
                        date_str,
                        event_type="opec_production",
                        region=country_name,
                        description=f"{country_name} crude oil production",
                        value=production_mbd,
                        unit="mb/d",
                        source="eia",
                    )
                    count += 1

                total_records += count
                if count > 0:
                    logger.info(f"Scraped {count} production records for {country_name}")

            except Exception as e:
                logger.error(f"Error scraping production for {country_name}: {e}")

        # Also fetch world demand/supply balance
        try:
            total_records += self._scrape_world_balance()
        except Exception as e:
            logger.warning(f"World balance scrape failed: {e}")

        duration = (datetime.now() - start).total_seconds()
        status = "success" if total_records > 0 else "failed"
        log_scrape("eia_international", status, total_records, duration=duration)
        return total_records

    def _scrape_world_balance(self):
        """Scrape world petroleum production and consumption for supply/demand balance."""
        url = f"{self.base_url}/international/data/"
        count = 0

        for activity_id, activity_name, event_type in [
            ("1", "Production", "demand_supply"),
            ("2", "Consumption", "demand_supply"),
        ]:
            params = {
                "api_key": self.api_key,
                "frequency": "monthly",
                "data[0]": "value",
                "facets[productId][]": "57",
                "facets[activityId][]": activity_id,
                "facets[countryRegionId][]": "WORL",
                "facets[unit][]": "TBPD",
                "sort[0][column]": "period",
                "sort[0][direction]": "desc",
                "length": 3,
            }

            data = self.fetch(url, params=params)
            if data and "response" in data and "data" in data["response"]:
                for rec in data["response"]["data"]:
                    value = rec.get("value")
                    period = rec.get("period")
                    if value is None or period is None:
                        continue
                    date_str = f"{period}-01"
                    mbd = round(float(value) / 1000, 2)
                    region = f"World {activity_name}"
                    insert_global_event(
                        date_str, event_type, region,
                        f"World petroleum {activity_name.lower()}",
                        mbd, "mb/d", "eia",
                    )
                    count += 1

        return count

    def scrape(self):
        """Run all EIA scrapers."""
        total = self.scrape_crude_prices()
        total += self.scrape_product_prices()
        total += self.scrape_international_production()
        return total


def run():
    init_db()
    scraper = EIAScraper()
    count = scraper.scrape()
    print(f"EIA scraper: {count} records")
    return count


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
