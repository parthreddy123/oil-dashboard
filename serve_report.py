"""Local server for Scenario Engine report with live refresh."""

import os
import sys
import json
import logging
import threading
from http.server import HTTPServer, SimpleHTTPRequestHandler
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PORT = 8050
REPORT_FILE = "scenario_report.html"

# Track refresh state
refresh_state = {"running": False, "last_status": None, "last_time": None}


def run_refresh():
    """Run full pipeline: scrape → tag → score → narratives → export HTML."""
    from database.db_manager import init_db
    init_db()

    steps = []

    # 1. Scrape fresh data
    try:
        from processing.data_processor import run_all_scrapers, run_post_processing
        scrape = run_all_scrapers()
        ok = sum(1 for r in scrape.values() if r.get("status") == "success")
        steps.append(f"Scraped: {ok}/{len(scrape)} sources OK")
    except Exception as e:
        steps.append(f"Scrape error: {e}")

    # 2. Post-processing (tagging, snapshots, narratives, scenario engine)
    try:
        post = run_post_processing()
        steps.append(f"Post-processing: {len(post)} steps")
    except Exception as e:
        steps.append(f"Post-processing error: {e}")

    # 3. Export HTML
    try:
        from export_html import generate_html
        generate_html(REPORT_FILE)
        steps.append("HTML exported")
    except Exception as e:
        steps.append(f"Export error: {e}")

    return steps


class ReportHandler(SimpleHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/" or self.path == "/index.html":
            self.path = f"/{REPORT_FILE}"
            return super().do_GET()
        elif self.path == "/status":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps(refresh_state).encode())
            return
        return super().do_GET()

    def do_POST(self):
        if self.path == "/refresh":
            if refresh_state["running"]:
                self.send_response(409)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Refresh already running"}).encode())
                return

            refresh_state["running"] = True
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"status": "started"}).encode())

            def _bg_refresh():
                try:
                    steps = run_refresh()
                    refresh_state["last_status"] = "success"
                    refresh_state["steps"] = steps
                    logger.info(f"Refresh done: {steps}")
                except Exception as e:
                    refresh_state["last_status"] = f"error: {e}"
                    logger.error(f"Refresh failed: {e}")
                finally:
                    refresh_state["running"] = False
                    refresh_state["last_time"] = datetime.now().strftime("%H:%M:%S")

            threading.Thread(target=_bg_refresh, daemon=True).start()
            return

        self.send_response(404)
        self.end_headers()

    def log_message(self, format, *args):
        if "/status" not in str(args):
            logger.info(format % args)


if __name__ == "__main__":
    # Generate initial report if missing
    if not os.path.exists(REPORT_FILE):
        logger.info("Generating initial report...")
        from export_html import generate_html
        generate_html(REPORT_FILE)

    server = HTTPServer(("0.0.0.0", PORT), ReportHandler)
    logger.info(f"Serving at http://localhost:{PORT}")
    logger.info("Press Ctrl+C to stop")

    try:
        import webbrowser
        webbrowser.open(f"http://localhost:{PORT}")
    except Exception:
        pass

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Stopped.")
