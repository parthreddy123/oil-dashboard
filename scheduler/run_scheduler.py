"""APScheduler-based job scheduler for periodic data refresh."""

import os
import sys
import logging
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger
from database.db_manager import init_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("scheduler")


def job_daily_prices():
    """Daily: EIA crude prices + OilPrice live prices + FX rates + Dubai/Oman."""
    logger.info("Running daily price scrape...")
    try:
        from scrapers.eia_scraper import run as run_eia
        from scrapers.oilprice_scraper import OilPriceScraper
        from scrapers.fx_scraper import run as run_fx
        from scrapers.dubai_scraper import run as run_dubai
        run_eia()
        OilPriceScraper().scrape_live_prices()
        run_fx()
        run_dubai()
    except Exception as e:
        logger.error(f"Daily prices job failed: {e}")


def job_news():
    """Every 6 hours: Google News + OilPrice RSS + impact tagging."""
    logger.info("Running news scrape...")
    try:
        from scrapers.news_aggregator import run as run_news
        from scrapers.oilprice_scraper import OilPriceScraper
        from processing.impact_tagger import tag_all_untagged
        run_news()
        OilPriceScraper().scrape_news()
        tag_all_untagged()
    except Exception as e:
        logger.error(f"News job failed: {e}")


def job_weekly_gov():
    """Weekly: PPAC + company press releases + snapshot."""
    logger.info("Running weekly government data scrape...")
    try:
        from scrapers.ppac_scraper import run as run_ppac
        from scrapers.company_scraper import run as run_company
        from processing.calculations import build_weekly_snapshot
        run_ppac()
        run_company()
        build_weekly_snapshot()
    except Exception as e:
        logger.error(f"Weekly gov data job failed: {e}")


def job_monthly_opec():
    """Monthly: OPEC MOMR data."""
    logger.info("Running monthly OPEC scrape...")
    try:
        from scrapers.opec_scraper import run as run_opec
        run_opec()
    except Exception as e:
        logger.error(f"Monthly OPEC job failed: {e}")


def main():
    init_db()
    scheduler = BlockingScheduler()

    # Daily price update at 8:00 AM
    scheduler.add_job(job_daily_prices, IntervalTrigger(hours=24),
                      id="daily_prices", next_run_time=datetime.now())

    # News every 6 hours
    scheduler.add_job(job_news, IntervalTrigger(hours=6),
                      id="news_6h", next_run_time=datetime.now())

    # Weekly government data on Mondays at 9:00 AM
    scheduler.add_job(job_weekly_gov, IntervalTrigger(weeks=1),
                      id="weekly_gov", next_run_time=datetime.now())

    # Monthly OPEC on the 15th
    scheduler.add_job(job_monthly_opec, IntervalTrigger(days=30),
                      id="monthly_opec", next_run_time=datetime.now())

    logger.info("Scheduler started. Press Ctrl+C to exit.")
    logger.info("Jobs: daily_prices (24h), news_6h (6h), weekly_gov (7d), monthly_opec (30d)")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped.")


if __name__ == "__main__":
    main()
