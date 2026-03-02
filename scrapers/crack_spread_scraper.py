"""Real-time crack spread scraper using Yahoo Finance futures data.

Pulls NYMEX/ICE futures for crude and products, calculates regional crack spreads.
Singapore crack spreads require premium data (Platts/Argus) — we provide USGC as proxy.
"""

import os
import sys
import logging
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from database.db_manager import get_connection, init_db, log_scrape

logger = logging.getLogger(__name__)

# Yahoo Finance futures symbols
FUTURES = {
    "BZ=F": {"name": "Brent", "type": "crude", "unit": "bbl"},
    "CL=F": {"name": "WTI", "type": "crude", "unit": "bbl"},
    "HO=F": {"name": "Heating Oil", "type": "product", "product": "diesel", "unit": "gal"},
    "RB=F": {"name": "RBOB Gasoline", "type": "product", "product": "petrol", "unit": "gal"},
}

GALLONS_PER_BARREL = 42.0

# Singapore crack spreads typically differ from USGC by these approximate offsets (USD/bbl).
# Positive = Singapore trades at a premium to USGC.
# Source: Historical Platts vs NYMEX differentials.
SINGAPORE_OFFSET = {
    "diesel": 3.0,    # Singapore gasoil typically $2-5 above USGC
    "petrol": -2.0,   # Singapore mogas typically $1-3 below USGC RBOB
    "atf": 2.5,       # Singapore jet ~$2-3 above USGC (tighter Asian jet market)
    "naphtha": 0.0,   # Naphtha is global, minimal regional diff
    "fuel_oil": -1.0,  # Singapore HSFO slightly discounted
    "lpg": 0.0,
}


def fetch_futures_prices():
    """Fetch latest futures prices from Yahoo Finance."""
    import yfinance as yf

    prices = {}
    for symbol, info in FUTURES.items():
        try:
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period="5d")
            if not hist.empty:
                close = float(hist.iloc[-1]["Close"])
                date = hist.index[-1].strftime("%Y-%m-%d")
                if info["unit"] == "gal":
                    close_bbl = round(close * GALLONS_PER_BARREL, 2)
                else:
                    close_bbl = round(close, 2)
                prices[symbol] = {
                    "price_bbl": close_bbl,
                    "price_raw": round(close, 4),
                    "date": date,
                    **info,
                }
        except Exception as e:
            logger.warning(f"Failed to fetch {symbol}: {e}")

    return prices


def calculate_regional_cracks(prices):
    """Calculate crack spreads for USGC and estimated Singapore."""
    brent = prices.get("BZ=F", {}).get("price_bbl")
    wti = prices.get("CL=F", {}).get("price_bbl")
    diesel = prices.get("HO=F", {}).get("price_bbl")
    petrol = prices.get("RB=F", {}).get("price_bbl")

    if not brent or not wti:
        return {}

    results = {}

    # USGC vs WTI (standard US refinery benchmark)
    if diesel:
        results[("USGC", "diesel", "wti")] = round(diesel - wti, 2)
    if petrol:
        results[("USGC", "petrol", "wti")] = round(petrol - wti, 2)
    if diesel and petrol:
        crack_321 = round((2 * diesel + petrol) / 3 - wti, 2)
        results[("USGC", "3:2:1", "wti")] = crack_321

    # USGC vs Brent (international comparison)
    if diesel:
        results[("USGC", "diesel", "brent")] = round(diesel - brent, 2)
    if petrol:
        results[("USGC", "petrol", "brent")] = round(petrol - brent, 2)
    if diesel and petrol:
        crack_321 = round((2 * diesel + petrol) / 3 - brent, 2)
        results[("USGC", "3:2:1", "brent")] = crack_321

    # Estimated Singapore vs Dubai (for APAC/India relevance)
    # Singapore cracks are structurally lower than USGC due to:
    #   - Lower spec products (Mogas 92 vs RBOB), surplus Asian capacity
    #   - Different crude slate (sour vs sweet)
    # We scale USGC vs Brent cracks by empirical Singapore/USGC ratios.
    dubai_est = brent - 3.0
    with get_connection() as conn:
        row = conn.execute(
            "SELECT price FROM crude_prices WHERE benchmark='oman_dubai' ORDER BY date DESC LIMIT 1"
        ).fetchone()
        if row:
            dubai_est = float(row["price"])

    # Estimated Singapore cracks vs Dubai.
    # Base levels are typical recent Singapore crack spread midpoints (2024-2026).
    # We adjust directionally using USGC cracks as a leading indicator:
    #   SG_crack = base + (USGC_crack_vs_brent - USGC_historical_avg) × sensitivity
    # This keeps values anchored to realistic Singapore levels while tracking market moves.
    SG_BASE = {
        "diesel": 17.0,     # Singapore gasoil vs Dubai: typically $14-22
        "petrol": 10.0,     # Singapore mogas 92 vs Dubai: typically $6-14
        "atf": 15.0,        # Singapore jet kero vs Dubai: typically $12-19
        "naphtha": 1.0,     # Singapore naphtha vs Dubai: typically -$3 to +$5
        "fuel_oil": -14.0,  # Singapore HSFO 380 vs Dubai: typically -$8 to -$20
        "lpg": -7.0,        # Singapore LPG vs Dubai: typically -$3 to -$12
    }
    USGC_AVG = {"diesel": 30.0, "petrol": 15.0}  # Recent USGC averages vs Brent
    SENSITIVITY = 0.25  # $0.25 Singapore move per $1 USGC move

    for prod, base in SG_BASE.items():
        sg_crack = base
        # Adjust directionally from USGC if we have the product
        usgc_ref = None
        if prod in ("diesel", "atf") and diesel:
            usgc_ref = diesel - brent
            usgc_avg = USGC_AVG["diesel"]
        elif prod in ("petrol", "naphtha") and petrol:
            usgc_ref = petrol - brent
            usgc_avg = USGC_AVG["petrol"]
        if usgc_ref is not None:
            sg_crack = base + (usgc_ref - usgc_avg) * SENSITIVITY
        results[("Singapore_est", prod, "dubai")] = round(sg_crack, 2)

    return results


def store_crack_spreads(cracks, prices):
    """Store crack spreads in the database."""
    today = datetime.now().strftime("%Y-%m-%d")
    count = 0

    # Calculate Singapore estimated GRM
    sg_cracks = {k[1]: v for k, v in cracks.items() if k[0] == "Singapore_est"}
    weights = {"diesel": 0.42, "petrol": 0.22, "naphtha": 0.12, "atf": 0.10, "fuel_oil": 0.08, "lpg": 0.06}
    weighted = 0.0
    total_w = 0.0
    for prod, w in weights.items():
        if prod in sg_cracks:
            weighted += sg_cracks[prod] * w
            total_w += w
    grm = round(weighted / total_w, 2) if total_w >= 0.5 else None

    with get_connection() as conn:
        for (region, product, benchmark), spread in cracks.items():
            source = f"yfinance_{region.lower()}"
            conn.execute(
                """INSERT INTO crack_spreads (date, product, spread, crude_benchmark, estimated_grm, source)
                   VALUES (?, ?, ?, ?, ?, ?)
                   ON CONFLICT(date, product, crude_benchmark) DO UPDATE SET
                     spread=excluded.spread, estimated_grm=excluded.estimated_grm, source=excluded.source""",
                (today, f"{region}_{product}", spread, benchmark, grm, source),
            )
            count += 1

        # Also store the simple product-level cracks for backward compatibility
        # (used by morning brief and other pages)
        brent = prices.get("BZ=F", {}).get("price_bbl")
        if brent:
            for prod in ["diesel", "petrol"]:
                key = ("USGC", prod, "brent")
                if key in cracks:
                    conn.execute(
                        """INSERT INTO crack_spreads (date, product, spread, crude_benchmark, estimated_grm, source)
                           VALUES (?, ?, ?, 'brent', ?, 'yfinance_live')
                           ON CONFLICT(date, product, crude_benchmark) DO UPDATE SET
                             spread=excluded.spread, estimated_grm=excluded.estimated_grm, source=excluded.source""",
                        (today, prod, cracks[key], grm),
                    )

            # Singapore-estimated for products used in GRM display
            for prod, spread in sg_cracks.items():
                conn.execute(
                    """INSERT INTO crack_spreads (date, product, spread, crude_benchmark, estimated_grm, source)
                       VALUES (?, ?, ?, 'dubai', ?, 'yfinance_sg_est')
                       ON CONFLICT(date, product, crude_benchmark) DO UPDATE SET
                         spread=excluded.spread, estimated_grm=excluded.estimated_grm, source=excluded.source""",
                    (today, prod, spread, grm),
                )

    logger.info(f"Stored {count} crack spreads, Singapore est. GRM=${grm}")
    return count, grm


def scrape():
    """Run crack spread scraper."""
    start = datetime.now()
    try:
        prices = fetch_futures_prices()
        if len(prices) < 3:
            logger.error("Insufficient futures data")
            return 0

        logger.info("Futures prices: " + ", ".join(
            f"{v['name']}=${v['price_bbl']}" for v in prices.values()
        ))

        cracks = calculate_regional_cracks(prices)
        count, grm = store_crack_spreads(cracks, prices)

        duration = (datetime.now() - start).total_seconds()
        log_scrape("crack_spreads", "success", count, duration=duration)
        return count

    except Exception as e:
        logger.error(f"Crack spread scraping failed: {e}")
        duration = (datetime.now() - start).total_seconds()
        log_scrape("crack_spreads", "failed", 0, str(e), duration)
        return 0


def run():
    init_db()
    count = scrape()
    print(f"Crack spread scraper: {count} records")
    return count


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
