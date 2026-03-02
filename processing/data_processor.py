"""Data processor - coordinates scraping, tagging, and snapshot building."""

import os
import sys
import logging
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from database.db_manager import init_db, log_scrape
from processing.impact_tagger import tag_all_untagged
from processing.calculations import build_weekly_snapshot
from processing.narrative_generator import generate_narrative

logger = logging.getLogger(__name__)


def run_all_scrapers():
    """Run all scrapers and return results summary."""
    init_db()
    results = {}

    scrapers = [
        ("eia", "scrapers.eia_scraper"),
        ("oilprice", "scrapers.oilprice_scraper"),
        ("fx", "scrapers.fx_scraper"),
        ("dubai", "scrapers.dubai_scraper"),
        ("benchmarks", "scrapers.benchmark_scraper"),
        ("news", "scrapers.news_aggregator"),
        ("ppac", "scrapers.ppac_scraper"),
        ("opec", "scrapers.opec_scraper"),
        ("company", "scrapers.company_scraper"),
    ]

    for name, module_path in scrapers:
        try:
            logger.info(f"Running {name} scraper...")
            import importlib
            module = importlib.import_module(module_path)
            count = module.run()
            results[name] = {"status": "success", "count": count}
            logger.info(f"{name}: {count} records")
        except Exception as e:
            results[name] = {"status": "failed", "error": str(e)}
            logger.error(f"{name} failed: {e}")

    return results


def run_post_processing():
    """Run impact tagging and snapshot building."""
    results = {}

    try:
        tagged = tag_all_untagged()
        results["impact_tagger"] = {"status": "success", "count": tagged}
    except Exception as e:
        results["impact_tagger"] = {"status": "failed", "error": str(e)}

    try:
        snapshot_count = build_weekly_snapshot()
        results["snapshot"] = {"status": "success", "count": snapshot_count}
    except Exception as e:
        results["snapshot"] = {"status": "failed", "error": str(e)}

    # Generate LLM strategic narrative (runs after snapshots so all data is available)
    try:
        html = generate_narrative()
        if html:
            results["narrative"] = {"status": "success", "count": 1}
        else:
            results["narrative"] = {"status": "skipped", "count": 0}
    except Exception as e:
        results["narrative"] = {"status": "failed", "error": str(e)}
        logger.warning(f"Narrative generation failed (non-fatal): {e}")

    return results


def full_refresh():
    """Run complete data refresh: scrape + tag + snapshot."""
    start = datetime.now()
    logger.info("Starting full data refresh...")

    scrape_results = run_all_scrapers()
    process_results = run_post_processing()

    duration = (datetime.now() - start).total_seconds()

    all_results = {**scrape_results, **process_results}
    total_records = sum(r.get("count", 0) for r in all_results.values() if r.get("status") == "success")
    failed = [k for k, v in all_results.items() if v.get("status") == "failed"]

    status = "success" if not failed else "partial"
    error_msg = f"Failed: {', '.join(failed)}" if failed else None
    log_scrape("full_refresh", status, total_records, error_msg, duration)

    logger.info(f"Full refresh complete: {total_records} records in {duration:.1f}s")
    return all_results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    results = full_refresh()
    for name, result in results.items():
        print(f"  {name}: {result}")
