"""Calculations for crack spreads, GRM, utilization rates, and weekly snapshots."""

import os
import logging
from datetime import datetime, timedelta

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from database.db_manager import (
    get_connection, get_crude_prices, get_product_prices,
    get_refinery_data, upsert_metric_snapshot, get_fx_rate
)

logger = logging.getLogger(__name__)

# Conversion constants
LITERS_PER_BARREL = 158.987

# Indian retail fuel prices include ~45-55% taxes (excise + VAT + dealer margin).
# To approximate refinery-gate prices, we apply estimated tax deductions.
# These are approximate and should be replaced with actual data if available.
INDIA_TAX_DEDUCTION_INR_PER_L = {
    "petrol": 44.0,   # ~₹20 excise + ~₹20 VAT + ~₹4 dealer margin
    "diesel": 35.0,   # ~₹16 excise + ~₹16 VAT + ~₹3 dealer margin
    "atf": 8.0,       # ATF excise + airport charges
    "naphtha": 3.0,   # Naphtha (industrial) has minimal taxes
    "fuel_oil": 3.0,  # Fuel oil has minimal taxes
    "lpg": 0.0,       # LPG handled separately (per cylinder)
}


def calculate_indian_basket():
    """Calculate Indian crude basket price.

    Official formula: 72% Oman/Dubai + 28% Brent (as per PPAC methodology).
    """
    with get_connection() as conn:
        brent = conn.execute(
            "SELECT price FROM crude_prices WHERE benchmark='brent' ORDER BY date DESC LIMIT 1"
        ).fetchone()
        dubai = conn.execute(
            "SELECT price FROM crude_prices WHERE benchmark='oman_dubai' ORDER BY date DESC LIMIT 1"
        ).fetchone()

        if brent and dubai:
            brent_price = float(brent["price"])
            dubai_price = float(dubai["price"])
            basket_price = round(0.72 * dubai_price + 0.28 * brent_price, 2)
            return basket_price
        elif brent:
            # Fallback: approximate Dubai as Brent minus typical spread
            brent_price = float(brent["price"])
            approx_dubai = brent_price - 2.50
            basket_price = round(0.72 * approx_dubai + 0.28 * brent_price, 2)
            return basket_price
    return None


def calculate_crack_spreads():
    """Calculate crack spreads for key products vs Brent.

    Product prices from PPAC are in INR/L. We convert to USD/bbl using
    the FX rate for comparison with Brent crude price (USD/bbl).

    Conversion: price_usd_bbl = (price_inr_l / fx_rate) * 158.987
    """
    with get_connection() as conn:
        brent = conn.execute(
            "SELECT price FROM crude_prices WHERE benchmark='brent' ORDER BY date DESC LIMIT 1"
        ).fetchone()
        if not brent:
            return {}

        brent_price = float(brent["price"])

        # Get FX rate for conversion
        fx_row = get_fx_rate("USD/INR")
        if not fx_row:
            logger.warning("No FX rate available, cannot compute crack spreads")
            return {}
        fx_rate = float(fx_row["rate"])

        # Get the latest date for each product (USD/bbl), limit to recent data
        products_usd = conn.execute(
            """SELECT product, price FROM product_prices
               WHERE unit='USD/bbl'
               GROUP BY product HAVING date = MAX(date)
               ORDER BY date DESC"""
        ).fetchall()

        # Also get INR/L prices (Indian retail/wholesale), latest per product
        products_inr = conn.execute(
            """SELECT product, price FROM product_prices
               WHERE unit='INR/L'
               GROUP BY product HAVING date = MAX(date)
               ORDER BY date DESC"""
        ).fetchall()

        spreads = {}
        seen = set()

        # Process USD/bbl prices directly
        petrol_usd = None
        for p in products_usd:
            product = p["product"]
            if product in seen:
                continue
            seen.add(product)
            product_price = float(p["price"])
            spreads[product] = round(product_price - brent_price, 2)
            if product == "petrol":
                petrol_usd = product_price

        # Estimate missing products from available data
        # Naphtha: ~90% of gasoline price (it's a feedstock, lower value)
        if "naphtha" not in seen and petrol_usd:
            naphtha_price = petrol_usd * 0.90
            spreads["naphtha"] = round(naphtha_price - brent_price, 2)
            seen.add("naphtha")
        # Fuel oil: ~65% of crude price (heavy, low-value residual)
        if "fuel_oil" not in seen:
            fuel_oil_price = brent_price * 0.65
            spreads["fuel_oil"] = round(fuel_oil_price - brent_price, 2)
            seen.add("fuel_oil")

        # Convert INR/L to USD/bbl for products not already covered
        # Deduct estimated taxes to approximate refinery-gate prices
        for p in products_inr:
            product = p["product"]
            if product in seen:
                continue
            seen.add(product)
            price_inr_l = float(p["price"])
            # Deduct estimated taxes to get refinery-gate price
            tax_deduction = INDIA_TAX_DEDUCTION_INR_PER_L.get(product, 0)
            refinery_gate_inr = max(price_inr_l - tax_deduction, price_inr_l * 0.5)
            price_usd_bbl = round((refinery_gate_inr / fx_rate) * LITERS_PER_BARREL, 2)
            spread = round(price_usd_bbl - brent_price, 2)
            spreads[product] = spread
            logger.debug(
                f"{product}: {price_inr_l} INR/L = {price_usd_bbl:.2f} USD/bbl, "
                f"crack spread = {spread:+.2f}"
            )

        return spreads


def estimate_grm(crack_spreads=None):
    """Estimate Gross Refining Margin from crack spreads.

    GRM = weighted average of product crack spreads.
    Yield weights approximate an Indian complex refinery (Reliance-type).
    """
    if not crack_spreads:
        crack_spreads = calculate_crack_spreads()

    if not crack_spreads:
        return None

    weights = {
        "diesel": 0.42,
        "petrol": 0.22,
        "naphtha": 0.12,
        "atf": 0.10,
        "fuel_oil": 0.08,
        "lpg": 0.06,
    }

    weighted_crack = 0.0
    total_weight = 0.0
    for product, weight in weights.items():
        if product in crack_spreads:
            weighted_crack += crack_spreads[product] * weight
            total_weight += weight

    # Require at least 50% of yield weights covered to report a GRM
    if total_weight >= 0.5:
        grm = weighted_crack / total_weight
        return round(grm, 2)
    elif total_weight > 0:
        logger.warning(f"GRM coverage too low ({total_weight:.0%}), need >=50% of yield weights")
    return None


def store_crack_spreads():
    """Calculate and persist crack spreads + GRM to the database."""
    spreads = calculate_crack_spreads()
    grm = estimate_grm(spreads)

    if not spreads:
        return 0

    today = datetime.now().strftime("%Y-%m-%d")
    count = 0

    with get_connection() as conn:
        for product, spread in spreads.items():
            conn.execute(
                """INSERT INTO crack_spreads (date, product, spread, crude_benchmark,
                   estimated_grm, source)
                   VALUES (?, ?, ?, 'brent', ?, 'calculated')
                   ON CONFLICT(date, product, crude_benchmark) DO UPDATE SET
                     spread=excluded.spread, estimated_grm=excluded.estimated_grm,
                     source=excluded.source""",
                (today, product, spread, grm),
            )
            count += 1

    logger.info(f"Stored {count} crack spreads, GRM=${grm}")
    return count


def calculate_utilization_stats():
    """Calculate aggregate refinery utilization statistics."""
    with get_connection() as conn:
        rows = conn.execute(
            """SELECT company, AVG(utilization_pct) as avg_util,
               SUM(throughput_tmt) as total_throughput,
               SUM(capacity_mmtpa) as total_capacity
               FROM refinery_data
               WHERE date = (SELECT MAX(date) FROM refinery_data)
               GROUP BY company"""
        ).fetchall()

        if not rows:
            return {}

        stats = {}
        for row in rows:
            stats[row["company"]] = {
                "avg_utilization": round(float(row["avg_util"]), 1) if row["avg_util"] else None,
                "total_throughput": float(row["total_throughput"]) if row["total_throughput"] else None,
                "total_capacity": float(row["total_capacity"]) if row["total_capacity"] else None,
            }

        # Overall: capacity-weighted average utilization
        total = conn.execute(
            """SELECT SUM(utilization_pct * capacity_mmtpa) / SUM(capacity_mmtpa) as weighted_util,
               SUM(throughput_tmt) as total_throughput
               FROM refinery_data
               WHERE date = (SELECT MAX(date) FROM refinery_data)
               AND capacity_mmtpa > 0"""
        ).fetchone()
        if total:
            stats["_overall"] = {
                "avg_utilization": round(float(total["weighted_util"]), 1) if total["weighted_util"] else None,
                "total_throughput": float(total["total_throughput"]) if total["total_throughput"] else None,
            }

        return stats


def build_weekly_snapshot():
    """Build a weekly metrics snapshot for KPI cards."""
    today = datetime.now().strftime("%Y-%m-%d")
    week_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    month_ago = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    count = 0

    with get_connection() as conn:
        # Brent price + changes
        brent_now = conn.execute(
            "SELECT price FROM crude_prices WHERE benchmark='brent' ORDER BY date DESC LIMIT 1"
        ).fetchone()
        brent_week = conn.execute(
            "SELECT price FROM crude_prices WHERE benchmark='brent' AND date <= ? ORDER BY date DESC LIMIT 1",
            (week_ago,)
        ).fetchone()
        brent_month = conn.execute(
            "SELECT price FROM crude_prices WHERE benchmark='brent' AND date <= ? ORDER BY date DESC LIMIT 1",
            (month_ago,)
        ).fetchone()

        if brent_now:
            price = float(brent_now["price"])
            wow = None
            mom = None
            if brent_week and float(brent_week["price"]) != 0:
                wow = round((price - float(brent_week["price"])) / float(brent_week["price"]) * 100, 2)
            if brent_month and float(brent_month["price"]) != 0:
                mom = round((price - float(brent_month["price"])) / float(brent_month["price"]) * 100, 2)
            upsert_metric_snapshot(today, "brent_price", price, wow, mom, "USD/bbl")
            count += 1

        # WTI price
        wti_now = conn.execute(
            "SELECT price FROM crude_prices WHERE benchmark='wti' ORDER BY date DESC LIMIT 1"
        ).fetchone()
        if wti_now:
            upsert_metric_snapshot(today, "wti_price", float(wti_now["price"]), unit="USD/bbl")
            count += 1

        # Dubai/Oman price
        dubai_now = conn.execute(
            "SELECT price FROM crude_prices WHERE benchmark='oman_dubai' ORDER BY date DESC LIMIT 1"
        ).fetchone()
        if dubai_now:
            upsert_metric_snapshot(today, "dubai_oman_price", float(dubai_now["price"]), unit="USD/bbl")
            count += 1

        # Brent-Dubai spread
        if brent_now and dubai_now:
            spread = round(float(brent_now["price"]) - float(dubai_now["price"]), 2)
            upsert_metric_snapshot(today, "brent_dubai_spread", spread, unit="USD/bbl")
            count += 1

        # Indian basket
        basket = calculate_indian_basket()
        if basket:
            upsert_metric_snapshot(today, "indian_basket", basket, unit="USD/bbl")
            count += 1

        # Utilization
        util_stats = calculate_utilization_stats()
        if "_overall" in util_stats:
            upsert_metric_snapshot(today, "refinery_utilization",
                                  util_stats["_overall"]["avg_utilization"], unit="%")
            count += 1

        # News sentiment
        bullish = conn.execute(
            "SELECT COUNT(*) as c FROM news_articles WHERE impact_tag='bullish' AND published_date >= ?",
            (week_ago,)
        ).fetchone()
        bearish = conn.execute(
            "SELECT COUNT(*) as c FROM news_articles WHERE impact_tag='bearish' AND published_date >= ?",
            (week_ago,)
        ).fetchone()
        if bullish and bearish:
            total_tagged = (bullish["c"] or 0) + (bearish["c"] or 0)
            if total_tagged > 0:
                sentiment = round((bullish["c"] or 0) / total_tagged * 100, 1)
                upsert_metric_snapshot(today, "bullish_sentiment_pct", sentiment, unit="%")
                count += 1

    # Store crack spreads + GRM (compute once, reuse)
    try:
        spreads = calculate_crack_spreads()
        grm = estimate_grm(spreads)
        if spreads:
            cs_today = datetime.now().strftime("%Y-%m-%d")
            with get_connection() as conn2:
                for product, spread in spreads.items():
                    conn2.execute(
                        """INSERT INTO crack_spreads (date, product, spread, crude_benchmark,
                           estimated_grm, source)
                           VALUES (?, ?, ?, 'brent', ?, 'calculated')
                           ON CONFLICT(date, product, crude_benchmark) DO UPDATE SET
                             spread=excluded.spread, estimated_grm=excluded.estimated_grm,
                             source=excluded.source""",
                        (cs_today, product, spread, grm),
                    )
            count += 1
        if grm is not None:
            upsert_metric_snapshot(today, "estimated_grm", grm, unit="USD/bbl")
            count += 1
    except Exception as e:
        logger.warning(f"Crack spread storage failed: {e}")

    logger.info(f"Built weekly snapshot with {count} metrics")
    return count


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    build_weekly_snapshot()
