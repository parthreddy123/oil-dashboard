"""Manual data refresh script with CLI options."""

import os
import sys
import argparse
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.db_manager import init_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("manual_refresh")


def main():
    parser = argparse.ArgumentParser(description="Manual data refresh for Oil Dashboard")
    parser.add_argument("--all", action="store_true", help="Run all scrapers")
    parser.add_argument("--eia", action="store_true", help="Run EIA scraper")
    parser.add_argument("--oilprice", action="store_true", help="Run OilPrice scraper")
    parser.add_argument("--news", action="store_true", help="Run News aggregator")
    parser.add_argument("--ppac", action="store_true", help="Run PPAC scraper")
    parser.add_argument("--opec", action="store_true", help="Run OPEC scraper")
    parser.add_argument("--company", action="store_true", help="Run Company scraper")
    parser.add_argument("--fx", action="store_true", help="Run FX rate scraper")
    parser.add_argument("--dubai", action="store_true", help="Run Dubai/Oman scraper")
    parser.add_argument("--benchmarks", action="store_true", help="Run Murban/OPEC Basket benchmark scraper")
    parser.add_argument("--tag", action="store_true", help="Run impact tagger")
    parser.add_argument("--retag", action="store_true", help="Re-tag ALL articles (after keyword changes)")
    parser.add_argument("--snapshot", action="store_true", help="Build weekly snapshot")

    args = parser.parse_args()

    init_db()

    if args.all:
        from processing.data_processor import full_refresh
        results = full_refresh()
        print("\n=== Refresh Results ===")
        for name, result in results.items():
            status = result.get("status", "unknown")
            count = result.get("count", 0)
            error = result.get("error", "")
            print(f"  {name:20s} {status:10s} {count:5d} records  {error}")
        return

    ran_something = False

    if args.eia:
        from scrapers.eia_scraper import run
        run()
        ran_something = True

    if args.oilprice:
        from scrapers.oilprice_scraper import run
        run()
        ran_something = True

    if args.news:
        from scrapers.news_aggregator import run
        run()
        ran_something = True

    if args.ppac:
        from scrapers.ppac_scraper import run
        run()
        ran_something = True

    if args.opec:
        from scrapers.opec_scraper import run
        run()
        ran_something = True

    if args.company:
        from scrapers.company_scraper import run
        run()
        ran_something = True

    if args.fx:
        from scrapers.fx_scraper import run
        run()
        ran_something = True

    if args.dubai:
        from scrapers.dubai_scraper import run
        run()
        ran_something = True

    if args.benchmarks:
        from scrapers.benchmark_scraper import run
        run()
        ran_something = True

    if args.tag:
        from processing.impact_tagger import tag_all_untagged
        count = tag_all_untagged()
        print(f"Tagged {count} articles")
        ran_something = True

    if args.retag:
        from processing.impact_tagger import retag_all
        count = retag_all()
        print(f"Re-tagged {count} articles")
        ran_something = True

    if args.snapshot:
        from processing.calculations import build_weekly_snapshot
        count = build_weekly_snapshot()
        print(f"Built snapshot with {count} metrics")
        ran_something = True

    if not ran_something:
        parser.print_help()
        print("\nExamples:")
        print("  python scheduler/manual_refresh.py --all")
        print("  python scheduler/manual_refresh.py --news --tag")
        print("  python scheduler/manual_refresh.py --eia --snapshot")


if __name__ == "__main__":
    main()
