"""Cached data access layer for Streamlit dashboard pages."""

import streamlit as st
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.db_manager import (
    get_crude_prices, get_product_prices, get_refinery_data,
    get_trade_flows, get_crack_spreads, get_news_articles,
    get_global_events, get_metric_snapshots, get_latest_scrape, get_latest_price,
    get_fx_rate as _get_fx_rate, get_latest_narrative as _get_latest_narrative,
    get_connection,
)


@st.cache_data(ttl=300)
def cached_crude_prices(benchmark=None, start_date=None, end_date=None, limit=180):
    return [dict(r) for r in get_crude_prices(benchmark, start_date, end_date, limit)]


@st.cache_data(ttl=300)
def cached_product_prices(product=None, start_date=None, end_date=None, limit=180):
    return [dict(r) for r in get_product_prices(product, start_date, end_date, limit)]


@st.cache_data(ttl=600)
def cached_refinery_data(company=None, start_date=None, end_date=None, limit=200):
    return [dict(r) for r in get_refinery_data(company, start_date, end_date, limit)]


@st.cache_data(ttl=600)
def cached_trade_flows(flow_type=None, start_date=None, end_date=None, limit=200):
    return [dict(r) for r in get_trade_flows(flow_type, start_date, end_date, limit)]


@st.cache_data(ttl=300)
def cached_crack_spreads(product=None, start_date=None, end_date=None, limit=180):
    return [dict(r) for r in get_crack_spreads(product, start_date, end_date, limit)]


@st.cache_data(ttl=300)
def cached_news_articles(impact_tag=None, source=None, start_date=None, end_date=None, limit=100):
    return [dict(r) for r in get_news_articles(impact_tag, source, start_date, end_date, limit)]


@st.cache_data(ttl=600)
def cached_global_events(event_type=None, start_date=None, end_date=None, limit=100):
    return [dict(r) for r in get_global_events(event_type, start_date, end_date, limit)]


@st.cache_data(ttl=300)
def cached_metric_snapshots(metric_name=None, limit=52):
    return [dict(r) for r in get_metric_snapshots(metric_name, limit)]


@st.cache_data(ttl=60)
def cached_latest_scrape(scraper=None):
    row = get_latest_scrape(scraper)
    return dict(row) if row else None


@st.cache_data(ttl=300)
def cached_fx_rate(pair="USD/INR"):
    row = _get_fx_rate(pair)
    return dict(row) if row else None


@st.cache_data(ttl=300)
def cached_strategic_narrative():
    return _get_latest_narrative()


@st.cache_data(ttl=300)
def cached_crack_spreads_brief():
    """Get latest crack spreads + GRM for Morning Brief (single cached query)."""
    cracks = {}
    grm = None
    with get_connection(readonly=True) as conn:
        rows = conn.execute(
            """SELECT product, spread, estimated_grm FROM crack_spreads
               WHERE date=(SELECT MAX(date) FROM crack_spreads)
               AND source = 'yfinance_sg_est'"""
        ).fetchall()
        if not rows:
            rows = conn.execute(
                """SELECT product, spread, estimated_grm FROM crack_spreads
                   WHERE date=(SELECT MAX(date) FROM crack_spreads)
                   AND source = 'calculated'"""
            ).fetchall()
        for r in rows:
            cracks[r["product"]] = float(r["spread"])
            if grm is None and r["estimated_grm"] is not None:
                grm = float(r["estimated_grm"])
    return {"cracks": cracks, "grm": grm}
