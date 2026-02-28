"""PPAC (Petroleum Planning & Analysis Cell) scraper for Indian refinery data."""

import os
import re
import logging
from datetime import datetime

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from scrapers.base_scraper import BaseScraper
from scrapers.pdf_extractor import extract_tables_from_pdf, find_table_with_keyword, clean_numeric
from database.db_manager import (
    upsert_refinery_data, upsert_trade_flow, upsert_product_price, log_scrape, init_db
)

logger = logging.getLogger(__name__)

# Indian refinery data - known refineries and their companies
INDIAN_REFINERIES = {
    "Indian Oil": {
        "company": "IOCL",
        "refineries": ["Digboi", "Guwahati", "Barauni", "Gujarat", "Haldia",
                       "Mathura", "Panipat", "Bongaigaon", "Paradip"],
    },
    "BPCL": {
        "company": "BPCL",
        "refineries": ["Mumbai", "Kochi", "Bina"],
    },
    "HPCL": {
        "company": "HPCL",
        "refineries": ["Mumbai", "Visakh"],
    },
    "MRPL": {"company": "MRPL", "refineries": ["Mangalore"]},
    "RIL": {
        "company": "RIL",
        "refineries": ["Jamnagar DTA", "Jamnagar SEZ"],
    },
    "Nayara": {"company": "Nayara", "refineries": ["Vadinar"]},
    "CPCL": {"company": "CPCL", "refineries": ["Manali", "Nagapattinam"]},
    "NRL": {"company": "NRL", "refineries": ["Numaligarh"]},
    "ONGC": {"company": "ONGC", "refineries": ["Tatipaka"]},
}


class PPACScraper(BaseScraper):
    def __init__(self):
        super().__init__("ppac", cache_expiry_hours=168)  # Weekly cache
        self.base_url = "https://ppac.gov.in"

    def _find_latest_report_url(self):
        """Try to find the latest PPAC ready reckoner or monthly report PDF."""
        pages = [
            f"{self.base_url}/content/152_1_ProductionPetroleum.aspx",
            f"{self.base_url}/content/147_1_ReadyReckoner.aspx",
        ]
        keywords = ["snapshot", "ready reckoner", "production", "refinery",
                     "monthly report", "crude oil", "petroleum product",
                     "consumption report", "indigenous"]
        try:
            for page_url in pages:
                html = self.fetch(page_url, response_type="text")
                if not html:
                    continue

                from bs4 import BeautifulSoup
                soup = BeautifulSoup(html, "lxml")

                for link in soup.find_all("a", href=True):
                    href = link["href"]
                    text = link.get_text(strip=True).lower()
                    if ".pdf" in href.lower() and any(k in text for k in keywords):
                        if href.startswith("/"):
                            return self.base_url + href
                        return href

        except Exception as e:
            logger.error(f"Error finding PPAC report URL: {e}")
        return None

    def scrape(self):
        """Scrape PPAC data: try Claude API extraction first, then text regex, then sample fallback."""
        start = datetime.now()
        total_records = 0

        try:
            report_url = self._find_latest_report_url()
            if report_url:
                pdf_path = self.download_pdf(report_url, "ppac_latest.pdf")
                if pdf_path:
                    # Try Claude API extraction first (most reliable for PDFs)
                    total_records += self._extract_with_claude(pdf_path)

                    # Fallback: text-based regex extraction from PDF
                    if total_records == 0:
                        total_records += self._extract_with_regex(pdf_path)

            if total_records == 0:
                logger.warning("PDF extraction yielded no data, using sample data")
                total_records = self._load_sample_data()

        except Exception as e:
            logger.error(f"PPAC scraping error: {e}")
            if total_records == 0:
                total_records = self._load_sample_data()

        duration = (datetime.now() - start).total_seconds()
        status = "success" if total_records > 0 else "failed"
        log_scrape("ppac", status, total_records, duration=duration)
        return total_records

    def _extract_with_claude(self, pdf_path):
        """Use Claude API to extract structured data from PPAC PDF."""
        count = 0
        today = datetime.now().strftime("%Y-%m-%d")

        try:
            from scrapers.claude_pdf_extractor import (
                extract_refinery_data, extract_trade_flows, extract_product_prices,
            )

            # Refinery data
            refineries = extract_refinery_data(pdf_path)
            for r in refineries:
                upsert_refinery_data(
                    today, r.get("refinery", "Unknown"), r.get("company", "Unknown"),
                    r.get("capacity_mmtpa"), r.get("throughput_tmt"),
                    r.get("utilization_pct"), source="ppac_claude",
                )
                count += 1

            # Trade flows
            trades = extract_trade_flows(pdf_path)
            for t in trades:
                upsert_trade_flow(
                    today, "crude_import", t.get("country", "Unknown"),
                    "crude", t.get("volume_tmt"), t.get("value_musd"),
                    source="ppac_claude",
                )
                count += 1

            # Product prices
            prices = extract_product_prices(pdf_path)
            for p in prices:
                product = p.get("product", "")
                prod_key = self._normalize_product_name(product) or product
                if prod_key and p.get("price"):
                    upsert_product_price(
                        today, prod_key, p["price"],
                        p.get("unit", "INR/L"), "India", "ppac_claude",
                    )
                    count += 1

            logger.info(f"Claude extracted {count} total records from PPAC PDF")

        except Exception as e:
            logger.warning(f"Claude extraction failed: {e}")

        return count

    def _extract_with_regex(self, pdf_path):
        """Extract consumption data from PDF text using regex patterns."""
        count = 0
        today = datetime.now().strftime("%Y-%m-%d")

        try:
            import pdfplumber
            full_text = ""
            with pdfplumber.open(pdf_path) as pdf:
                for page in pdf.pages[:20]:
                    text = page.extract_text()
                    if text:
                        full_text += text + "\n"

            if len(full_text) < 200:
                return 0

            # Extract product consumption volumes from text patterns
            # Pattern: "MS consumption ... volume of X,XXX TMT" or "X.XX MMT"
            product_patterns = {
                "petrol": [r"MS.*?volume\s+of\s+([\d,]+)\s*TMT",
                           r"MS.*?consumption.*?([\d.]+)\s*(?:MMT|million)"],
                "diesel": [r"HSD.*?volume\s+of\s+([\d,]+)\s*TMT",
                           r"HSD.*?consumption.*?([\d.]+)\s*(?:MMT|million)"],
                "lpg": [r"LPG.*?volume\s+of\s+([\d,]+)\s*TMT",
                         r"LPG.*?consumption.*?([\d.]+)\s*(?:MMT|million)"],
                "atf": [r"ATF.*?volume\s+of\s+([\d,]+)\s*TMT",
                         r"ATF.*?consumption.*?([\d.]+)\s*(?:MMT|million)"],
                "naphtha": [r"[Nn]aphtha.*?volume\s+of\s+([\d,]+)\s*TMT",
                             r"[Nn]aphtha.*?([\d,]+)\s*TMT"],
                "kerosene": [r"[Kk]erosene.*?volume\s+of\s+([\d,]+)\s*TMT"],
            }

            for product, patterns in product_patterns.items():
                for pattern in patterns:
                    match = re.search(pattern, full_text, re.IGNORECASE | re.DOTALL)
                    if match:
                        val_str = match.group(1).replace(",", "")
                        try:
                            volume = float(val_str)
                            # Store as consumption volume in TMT
                            upsert_product_price(
                                today, product, volume,
                                "TMT", "India_consumption", "ppac_regex",
                            )
                            count += 1
                            logger.info(f"Regex extracted {product} consumption: {volume}")
                        except ValueError:
                            pass
                        break

            # Extract total POL consumption
            total_match = re.search(
                r"consumption.*?was\s+([\d.]+)\s*million\s*metric\s*tonn",
                full_text, re.IGNORECASE | re.DOTALL
            )
            if total_match:
                total_mmt = float(total_match.group(1))
                upsert_product_price(
                    today, "total_pol", total_mmt,
                    "MMT", "India_consumption", "ppac_regex",
                )
                count += 1
                logger.info(f"Regex extracted total POL consumption: {total_mmt} MMT")

            if count > 0:
                logger.info(f"Regex extracted {count} records from PPAC PDF")

        except Exception as e:
            logger.warning(f"Regex extraction failed: {e}")

        return count

    def _identify_company(self, refinery_name):
        for group_name, info in INDIAN_REFINERIES.items():
            for ref in info["refineries"]:
                if ref.lower() in refinery_name.lower():
                    return info["company"]
        return "Unknown"

    def _normalize_product_name(self, name):
        mapping = {
            "petrol": "petrol", "gasoline": "petrol", "ms": "petrol",
            "diesel": "diesel", "hsd": "diesel",
            "atf": "atf", "aviation": "atf",
            "lpg": "lpg",
            "kerosene": "kerosene", "sko": "kerosene",
            "naphtha": "naphtha",
            "fuel oil": "fuel_oil", "fo": "fuel_oil",
        }
        for key, val in mapping.items():
            if key in name:
                return val
        return None

    def _load_sample_data(self):
        """Load representative sample data when PDF scraping fails."""
        today = datetime.now().strftime("%Y-%m-%d")
        count = 0

        # Sample refinery data (approximate real-world figures)
        sample_refineries = [
            ("Jamnagar DTA", "RIL", 33.0, 2800, 101.8),
            ("Jamnagar SEZ", "RIL", 35.2, 2900, 98.9),
            ("Panipat", "IOCL", 15.0, 1250, 100.0),
            ("Gujarat", "IOCL", 13.7, 1100, 96.4),
            ("Paradip", "IOCL", 15.0, 1280, 102.4),
            ("Kochi", "BPCL", 15.5, 1300, 100.6),
            ("Mumbai (BPCL)", "BPCL", 12.0, 980, 98.0),
            ("Mangalore", "MRPL", 15.0, 1230, 98.4),
            ("Visakh", "HPCL", 8.3, 700, 101.2),
            ("Mumbai (HPCL)", "HPCL", 7.5, 610, 97.6),
            ("Vadinar", "Nayara", 20.0, 1650, 99.0),
            ("Bina", "BPCL", 7.8, 640, 98.5),
            ("Haldia", "IOCL", 8.0, 670, 100.5),
            ("Mathura", "IOCL", 8.0, 660, 99.0),
            ("Barauni", "IOCL", 6.0, 490, 98.0),
            ("Manali", "CPCL", 10.5, 870, 99.4),
        ]

        for name, company, capacity, throughput, utilization in sample_refineries:
            upsert_refinery_data(today, name, company, capacity, throughput, utilization, source="ppac_sample")
            count += 1

        # Sample crude import by source country
        sample_imports = [
            ("Iraq", 1050, 6800),
            ("Saudi Arabia", 850, 5700),
            ("UAE", 470, 3100),
            ("Kuwait", 380, 2500),
            ("Nigeria", 350, 2300),
            ("USA", 280, 1800),
            ("Russia", 750, 4200),
            ("Others", 520, 3400),
        ]
        for country, volume, value in sample_imports:
            upsert_trade_flow(today, "crude_import", country, "crude", volume, value, "ppac_sample")
            count += 1

        # Sample product prices (INR/Litre for Delhi)
        sample_prices = [
            ("petrol", 94.72),
            ("diesel", 87.62),
            ("lpg", 803.0),  # INR per cylinder
        ]
        for product, price in sample_prices:
            unit = "INR/cylinder" if product == "lpg" else "INR/L"
            upsert_product_price(today, product, price, unit, "India", "ppac_sample")
            count += 1

        return count


def run():
    init_db()
    scraper = PPACScraper()
    count = scraper.scrape()
    print(f"PPAC scraper: {count} records")
    return count


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
