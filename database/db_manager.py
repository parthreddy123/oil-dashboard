"""SQLite database manager for Oil & Gas Intelligence Dashboard."""

import sqlite3
import os
from datetime import datetime, date
from contextlib import contextmanager

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "oil_dashboard.db")


def get_db_path():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    return DB_PATH


@contextmanager
def get_connection(readonly=False):
    db_path = get_db_path()
    if readonly and os.path.exists(db_path):
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    else:
        conn = sqlite3.connect(db_path)
        readonly = False  # fall back to read-write if DB doesn't exist yet
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    if readonly:
        conn.execute("PRAGMA query_only=ON")
    try:
        yield conn
        if not readonly:
            conn.commit()
    except Exception:
        if not readonly:
            conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    """Create all tables if they don't exist."""
    with get_connection() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS crude_prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            benchmark TEXT NOT NULL,  -- 'brent', 'wti', 'indian_basket', 'oman_dubai'
            price REAL,
            change_pct REAL,
            source TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            UNIQUE(date, benchmark)
        );

        CREATE TABLE IF NOT EXISTS product_prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            product TEXT NOT NULL,  -- 'petrol', 'diesel', 'atf', 'lpg', 'naphtha', 'fuel_oil'
            price REAL,
            unit TEXT DEFAULT 'USD/bbl',
            location TEXT DEFAULT 'India',
            source TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            UNIQUE(date, product, location)
        );

        CREATE TABLE IF NOT EXISTS refinery_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            refinery TEXT NOT NULL,
            company TEXT,
            capacity_mmtpa REAL,
            throughput_tmt REAL,  -- thousand metric tonnes
            utilization_pct REAL,
            crude_type TEXT,  -- 'indigenous', 'imported'
            source TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            UNIQUE(date, refinery)
        );

        CREATE TABLE IF NOT EXISTS trade_flows (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            flow_type TEXT NOT NULL,  -- 'crude_import', 'product_export', 'product_import'
            country TEXT,
            product TEXT,
            volume_tmt REAL,
            value_musd REAL,
            source TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            UNIQUE(date, flow_type, country, product)
        );

        CREATE TABLE IF NOT EXISTS crack_spreads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            product TEXT NOT NULL,
            spread REAL,  -- product price - crude price
            crude_benchmark TEXT DEFAULT 'brent',
            estimated_grm REAL,
            source TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            UNIQUE(date, product, crude_benchmark)
        );

        CREATE TABLE IF NOT EXISTS global_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            event_type TEXT,  -- 'opec_production', 'turnaround', 'demand_supply'
            region TEXT,
            description TEXT,
            value REAL,
            unit TEXT,
            source TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            UNIQUE(date, event_type, region)
        );

        CREATE TABLE IF NOT EXISTS news_articles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            published_date TEXT,
            title TEXT NOT NULL,
            summary TEXT,
            url TEXT UNIQUE,
            source TEXT,
            category TEXT,
            impact_tag TEXT,  -- 'bullish', 'bearish', 'neutral'
            impact_score REAL DEFAULT 0,
            impact_category TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS scrape_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scraper TEXT NOT NULL,
            status TEXT NOT NULL,  -- 'success', 'failed', 'partial'
            records_count INTEGER DEFAULT 0,
            error_message TEXT,
            duration_seconds REAL,
            started_at TEXT DEFAULT (datetime('now')),
            completed_at TEXT
        );

        CREATE TABLE IF NOT EXISTS key_metrics_snapshot (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_date TEXT NOT NULL,
            metric_name TEXT NOT NULL,
            metric_value REAL,
            change_wow REAL,  -- week over week
            change_mom REAL,  -- month over month
            unit TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            UNIQUE(snapshot_date, metric_name)
        );

        CREATE TABLE IF NOT EXISTS fx_rates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            pair TEXT NOT NULL DEFAULT 'USD/INR',
            rate REAL NOT NULL,
            source TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            UNIQUE(date, pair)
        );

        CREATE INDEX IF NOT EXISTS idx_crude_prices_benchmark_date ON crude_prices(benchmark, date);
        CREATE INDEX IF NOT EXISTS idx_product_prices_product_date ON product_prices(product, date);
        CREATE INDEX IF NOT EXISTS idx_news_articles_impact_date ON news_articles(impact_tag, published_date);
        CREATE INDEX IF NOT EXISTS idx_refinery_data_date ON refinery_data(date);
        CREATE INDEX IF NOT EXISTS idx_global_events_type_date ON global_events(event_type, date);
        CREATE INDEX IF NOT EXISTS idx_trade_flows_date ON trade_flows(date);
        CREATE INDEX IF NOT EXISTS idx_fx_rates_pair_date ON fx_rates(pair, date);
        CREATE INDEX IF NOT EXISTS idx_crack_spreads_date ON crack_spreads(date, source);
        CREATE INDEX IF NOT EXISTS idx_snapshots_date ON key_metrics_snapshot(snapshot_date DESC);
        """)


# --- CRUD Methods ---

def upsert_crude_price(dt, benchmark, price, change_pct=None, source=None):
    with get_connection() as conn:
        conn.execute(
            """INSERT INTO crude_prices (date, benchmark, price, change_pct, source)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(date, benchmark) DO UPDATE SET
                 price=excluded.price, change_pct=excluded.change_pct, source=excluded.source""",
            (str(dt), benchmark, price, change_pct, source),
        )


def upsert_crude_prices_bulk(rows):
    """rows: list of (date, benchmark, price, change_pct, source)"""
    with get_connection() as conn:
        conn.executemany(
            """INSERT INTO crude_prices (date, benchmark, price, change_pct, source)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(date, benchmark) DO UPDATE SET
                 price=excluded.price, change_pct=excluded.change_pct, source=excluded.source""",
            rows,
        )


def upsert_product_price(dt, product, price, unit="USD/bbl", location="India", source=None):
    with get_connection() as conn:
        conn.execute(
            """INSERT INTO product_prices (date, product, price, unit, location, source)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(date, product, location) DO UPDATE SET
                 price=excluded.price, unit=excluded.unit, source=excluded.source""",
            (str(dt), product, price, unit, location, source),
        )


def upsert_refinery_data(dt, refinery, company, capacity, throughput, utilization, crude_type=None, source=None):
    with get_connection() as conn:
        conn.execute(
            """INSERT INTO refinery_data (date, refinery, company, capacity_mmtpa, throughput_tmt,
               utilization_pct, crude_type, source) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(date, refinery) DO UPDATE SET
                 company=excluded.company, capacity_mmtpa=excluded.capacity_mmtpa,
                 throughput_tmt=excluded.throughput_tmt, utilization_pct=excluded.utilization_pct,
                 crude_type=excluded.crude_type, source=excluded.source""",
            (str(dt), refinery, company, capacity, throughput, utilization, crude_type, source),
        )


def upsert_trade_flow(dt, flow_type, country, product, volume, value=None, source=None):
    with get_connection() as conn:
        conn.execute(
            """INSERT INTO trade_flows (date, flow_type, country, product, volume_tmt, value_musd, source)
               VALUES (?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(date, flow_type, country, product) DO UPDATE SET
                 volume_tmt=excluded.volume_tmt, value_musd=excluded.value_musd, source=excluded.source""",
            (str(dt), flow_type, country, product, volume, value, source),
        )


def insert_news_article(published_date, title, summary, url, source, category=None,
                        impact_tag="neutral", impact_score=0.0, impact_category=None):
    with get_connection() as conn:
        try:
            conn.execute(
                """INSERT INTO news_articles (published_date, title, summary, url, source,
                   category, impact_tag, impact_score, impact_category)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (str(published_date) if published_date else None, title, summary, url,
                 source, category, impact_tag, impact_score, impact_category),
            )
            return True
        except sqlite3.IntegrityError:
            return False  # duplicate URL


def insert_global_event(dt, event_type, region, description, value=None, unit=None, source=None):
    with get_connection() as conn:
        conn.execute(
            """INSERT INTO global_events (date, event_type, region, description, value, unit, source)
               VALUES (?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(date, event_type, region) DO UPDATE SET
                 description=excluded.description, value=excluded.value,
                 unit=excluded.unit, source=excluded.source""",
            (str(dt), event_type, region, description, value, unit, source),
        )


def log_scrape(scraper, status, records_count=0, error_message=None, duration=None):
    with get_connection() as conn:
        conn.execute(
            """INSERT INTO scrape_log (scraper, status, records_count, error_message, duration_seconds, completed_at)
               VALUES (?, ?, ?, ?, ?, datetime('now'))""",
            (scraper, status, records_count, error_message, duration),
        )


def upsert_metric_snapshot(snapshot_date, metric_name, value, change_wow=None, change_mom=None, unit=None):
    with get_connection() as conn:
        conn.execute(
            """INSERT INTO key_metrics_snapshot (snapshot_date, metric_name, metric_value,
               change_wow, change_mom, unit) VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(snapshot_date, metric_name) DO UPDATE SET
                 metric_value=excluded.metric_value, change_wow=excluded.change_wow,
                 change_mom=excluded.change_mom, unit=excluded.unit""",
            (str(snapshot_date), metric_name, value, change_wow, change_mom, unit),
        )


# --- Query Methods ---

def get_crude_prices(benchmark=None, start_date=None, end_date=None, limit=365):
    """Get crude prices with optional filters."""
    query = "SELECT * FROM crude_prices WHERE 1=1"
    params = []
    if benchmark:
        query += " AND benchmark = ?"
        params.append(benchmark)
    if start_date:
        query += " AND date >= ?"
        params.append(str(start_date))
    if end_date:
        query += " AND date <= ?"
        params.append(str(end_date))
    query += " ORDER BY date DESC LIMIT ?"
    params.append(limit)
    with get_connection(readonly=True) as conn:
        return conn.execute(query, params).fetchall()


def get_product_prices(product=None, start_date=None, end_date=None, limit=365):
    query = "SELECT * FROM product_prices WHERE 1=1"
    params = []
    if product:
        query += " AND product = ?"
        params.append(product)
    if start_date:
        query += " AND date >= ?"
        params.append(str(start_date))
    if end_date:
        query += " AND date <= ?"
        params.append(str(end_date))
    query += " ORDER BY date DESC LIMIT ?"
    params.append(limit)
    with get_connection(readonly=True) as conn:
        return conn.execute(query, params).fetchall()


def get_refinery_data(company=None, start_date=None, end_date=None, limit=500):
    query = "SELECT * FROM refinery_data WHERE 1=1"
    params = []
    if company:
        query += " AND company = ?"
        params.append(company)
    if start_date:
        query += " AND date >= ?"
        params.append(str(start_date))
    if end_date:
        query += " AND date <= ?"
        params.append(str(end_date))
    query += " ORDER BY date DESC LIMIT ?"
    params.append(limit)
    with get_connection(readonly=True) as conn:
        return conn.execute(query, params).fetchall()


def get_trade_flows(flow_type=None, start_date=None, end_date=None, limit=500):
    query = "SELECT * FROM trade_flows WHERE 1=1"
    params = []
    if flow_type:
        query += " AND flow_type = ?"
        params.append(flow_type)
    if start_date:
        query += " AND date >= ?"
        params.append(str(start_date))
    if end_date:
        query += " AND date <= ?"
        params.append(str(end_date))
    query += " ORDER BY date DESC LIMIT ?"
    params.append(limit)
    with get_connection(readonly=True) as conn:
        return conn.execute(query, params).fetchall()


def get_crack_spreads(product=None, start_date=None, end_date=None, limit=365):
    query = "SELECT * FROM crack_spreads WHERE 1=1"
    params = []
    if product:
        query += " AND product = ?"
        params.append(product)
    if start_date:
        query += " AND date >= ?"
        params.append(str(start_date))
    if end_date:
        query += " AND date <= ?"
        params.append(str(end_date))
    query += " ORDER BY date DESC LIMIT ?"
    params.append(limit)
    with get_connection(readonly=True) as conn:
        return conn.execute(query, params).fetchall()


def get_news_articles(impact_tag=None, source=None, start_date=None, end_date=None, limit=100):
    query = "SELECT * FROM news_articles WHERE 1=1"
    params = []
    if impact_tag:
        query += " AND impact_tag = ?"
        params.append(impact_tag)
    if source:
        query += " AND source = ?"
        params.append(source)
    if start_date:
        query += " AND published_date >= ?"
        params.append(str(start_date))
    if end_date:
        query += " AND published_date <= ?"
        params.append(str(end_date))
    query += " ORDER BY published_date DESC LIMIT ?"
    params.append(limit)
    with get_connection(readonly=True) as conn:
        return conn.execute(query, params).fetchall()


def get_global_events(event_type=None, start_date=None, end_date=None, limit=200):
    query = "SELECT * FROM global_events WHERE 1=1"
    params = []
    if event_type:
        query += " AND event_type = ?"
        params.append(event_type)
    if start_date:
        query += " AND date >= ?"
        params.append(str(start_date))
    if end_date:
        query += " AND date <= ?"
        params.append(str(end_date))
    query += " ORDER BY date DESC LIMIT ?"
    params.append(limit)
    with get_connection(readonly=True) as conn:
        return conn.execute(query, params).fetchall()


def get_latest_scrape(scraper=None):
    query = "SELECT * FROM scrape_log"
    params = []
    if scraper:
        query += " WHERE scraper = ?"
        params.append(scraper)
    query += " ORDER BY started_at DESC LIMIT 1"
    with get_connection(readonly=True) as conn:
        return conn.execute(query, params).fetchone()


def get_metric_snapshots(metric_name=None, limit=52):
    query = "SELECT * FROM key_metrics_snapshot WHERE 1=1"
    params = []
    if metric_name:
        query += " AND metric_name = ?"
        params.append(metric_name)
    query += " ORDER BY snapshot_date DESC LIMIT ?"
    params.append(limit)
    with get_connection(readonly=True) as conn:
        return conn.execute(query, params).fetchall()


def get_latest_price(benchmark):
    """Get the most recent price for a benchmark."""
    with get_connection(readonly=True) as conn:
        row = conn.execute(
            "SELECT * FROM crude_prices WHERE benchmark = ? ORDER BY date DESC LIMIT 1",
            (benchmark,)
        ).fetchone()
        return row


def upsert_fx_rate(dt, pair, rate, source=None):
    with get_connection() as conn:
        conn.execute(
            """INSERT INTO fx_rates (date, pair, rate, source)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(date, pair) DO UPDATE SET
                 rate=excluded.rate, source=excluded.source""",
            (str(dt), pair, rate, source),
        )


def get_fx_rate(pair="USD/INR", date=None):
    """Get the latest FX rate, or rate for a specific date."""
    with get_connection(readonly=True) as conn:
        if date:
            row = conn.execute(
                "SELECT * FROM fx_rates WHERE pair=? AND date<=? ORDER BY date DESC LIMIT 1",
                (pair, str(date))
            ).fetchone()
        else:
            row = conn.execute(
                "SELECT * FROM fx_rates WHERE pair=? ORDER BY date DESC LIMIT 1",
                (pair,)
            ).fetchone()
        return row


def get_all_benchmarks():
    """Get list of distinct benchmarks in the database."""
    with get_connection(readonly=True) as conn:
        rows = conn.execute("SELECT DISTINCT benchmark FROM crude_prices").fetchall()
        return [r["benchmark"] for r in rows]


if __name__ == "__main__":
    init_db()
    print(f"Database initialized at {get_db_path()}")
