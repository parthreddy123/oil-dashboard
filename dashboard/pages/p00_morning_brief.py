"""Page 0: Morning Brief - Single-page CEO summary with alerts."""

import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import streamlit as st
import pandas as pd
from datetime import datetime
from dashboard.data_access import (
    cached_crude_prices, cached_news_articles, cached_metric_snapshots,
)
from dashboard.components.kpi_card import kpi_card
from dashboard.components.news_card import news_card
from dashboard.components.price_chart import line_chart
from dashboard.components.theme import (
    CYAN, TEAL, GOLD, AMBER, EMERALD, CORAL,
    TEXT_PRIMARY, TEXT_SECONDARY, TEXT_MUTED, TEXT_DIM,
    BG_CARD, BG_ELEVATED, BORDER_SUBTLE, PLOTLY_CONFIG,
)
from processing.calculations import calculate_crack_spreads, estimate_grm
from database.db_manager import get_connection

# Alert thresholds
THRESHOLDS = {
    "brent_high": 95.0,
    "brent_low": 60.0,
    "brent_dubai_spread": 5.0,
    "grm_low": 2.0,
    "grm_high": 15.0,
}


def _check_alerts(snapshots, grm):
    """Generate alert messages based on threshold breaches."""
    alerts = []

    brent = snapshots.get("brent_price")
    if brent:
        val = float(brent["metric_value"])
        if val > THRESHOLDS["brent_high"]:
            alerts.append(("warning", f"Brent at ${val:.2f}/bbl - above ${THRESHOLDS['brent_high']} threshold"))
        elif val < THRESHOLDS["brent_low"]:
            alerts.append(("warning", f"Brent at ${val:.2f}/bbl - below ${THRESHOLDS['brent_low']} threshold"))

    spread = snapshots.get("brent_dubai_spread")
    if spread:
        val = float(spread["metric_value"])
        if abs(val) > THRESHOLDS["brent_dubai_spread"]:
            alerts.append(("info", f"Brent-Dubai spread at ${val:.2f}/bbl - widened beyond normal"))

    if grm is not None:
        if grm < THRESHOLDS["grm_low"]:
            alerts.append(("error", f"GRM at ${grm:.2f}/bbl - below minimum threshold"))
        elif grm > THRESHOLDS["grm_high"]:
            alerts.append(("success", f"GRM at ${grm:.2f}/bbl - exceptionally strong"))

    brent_snap = snapshots.get("brent_price")
    if brent_snap and brent_snap.get("change_wow"):
        wow = float(brent_snap["change_wow"])
        if abs(wow) > 5:
            direction = "surged" if wow > 0 else "crashed"
            alerts.append(("warning", f"Brent {direction} {abs(wow):.1f}% week-over-week"))

    return alerts


def render():
    now = datetime.now()
    st.markdown(f"""
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:0.5rem;">
        <div>
            <h1 style="margin:0;font-size:1.8rem;font-weight:800;color:{TEXT_PRIMARY};
                letter-spacing:-0.02em;">Morning Brief</h1>
            <div style="font-size:0.75rem;color:{TEXT_DIM};margin-top:2px;">
                Executive summary for {now.strftime("%A, %B %d, %Y")}</div>
        </div>
        <div style="text-align:right;">
            <div style="font-size:0.68rem;color:{TEXT_MUTED};text-transform:uppercase;
                letter-spacing:0.08em;">Generated at {now.strftime("%H:%M")}</div>
        </div>
    </div>""", unsafe_allow_html=True)

    # Build snapshot dict keeping only the newest (first) entry per metric
    snapshots = {}
    for row in cached_metric_snapshots(limit=20):
        if row["metric_name"] not in snapshots:
            snapshots[row["metric_name"]] = row

    # Use cached crack spreads and GRM from DB (computed during snapshot build)
    cracks = {}
    grm = None
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT product, spread, estimated_grm FROM crack_spreads WHERE date=(SELECT MAX(date) FROM crack_spreads)"
        ).fetchall()
        for r in rows:
            cracks[r["product"]] = float(r["spread"])
            if grm is None and r["estimated_grm"] is not None:
                grm = float(r["estimated_grm"])

    # --- Alerts Section ---
    alerts = _check_alerts(snapshots, grm)
    if alerts:
        st.markdown(f"""
        <div style="background:rgba(245,158,11,0.08);border:1px solid rgba(245,158,11,0.3);
            border-radius:10px;padding:12px 16px;margin-bottom:1rem;">
            <div style="font-size:0.72rem;font-weight:700;color:{AMBER};text-transform:uppercase;
                letter-spacing:0.08em;margin-bottom:8px;">Alerts & Threshold Breaches</div>
        """, unsafe_allow_html=True)
        for level, msg in alerts:
            icon = {"warning": "&#9888;", "error": "&#9940;", "info": "&#8505;",
                    "success": "&#9989;"}.get(level, "&#9679;")
            color = {"warning": AMBER, "error": CORAL, "info": CYAN,
                     "success": EMERALD}.get(level, TEXT_MUTED)
            st.markdown(f"""
            <div style="font-size:0.82rem;color:{color};padding:3px 0;">
                {icon} {msg}</div>""", unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)

    # --- Price KPIs ---
    col1, col2, col3, col4, col5 = st.columns(5)

    brent_hist = cached_crude_prices(benchmark="brent", limit=14)
    brent_sparkline = [r["price"] for r in reversed(brent_hist)] if brent_hist else None

    with col1:
        b = snapshots.get("brent_price")
        kpi_card("Brent Crude", float(b["metric_value"]) if b else None, "USD/bbl",
                 float(b["change_wow"]) if b and b.get("change_wow") else None,
                 sparkline_data=brent_sparkline, accent_color=CYAN)
    with col2:
        d = snapshots.get("dubai_oman_price")
        dubai_hist = cached_crude_prices(benchmark="oman_dubai", limit=14)
        dubai_sparkline = [r["price"] for r in reversed(dubai_hist)] if dubai_hist else None
        kpi_card("Dubai/Oman", float(d["metric_value"]) if d else None, "USD/bbl",
                 sparkline_data=dubai_sparkline, accent_color=TEAL)
    with col3:
        sp = snapshots.get("brent_dubai_spread")
        kpi_card("Brent-Dubai Spread", float(sp["metric_value"]) if sp else None, "USD/bbl",
                 accent_color=AMBER)
    with col4:
        ib = snapshots.get("indian_basket")
        kpi_card("Indian Basket", float(ib["metric_value"]) if ib else None, "USD/bbl",
                 accent_color=GOLD)
    with col5:
        grm_color = EMERALD if grm and grm > 5 else (AMBER if grm and grm > 0 else CORAL)
        kpi_card("Estimated GRM", grm, "USD/bbl", accent_color=grm_color)

    st.divider()

    # --- Chart + Crack Spreads ---
    col_chart, col_cracks = st.columns([3, 2])

    with col_chart:
        st.markdown(f'<div style="font-size:0.9rem;font-weight:700;color:{TEXT_PRIMARY};">'
                    f'7-Day Brent & Dubai/Oman</div>', unsafe_allow_html=True)
        brent_7d = cached_crude_prices(benchmark="brent", limit=7)
        dubai_7d = cached_crude_prices(benchmark="oman_dubai", limit=7)
        if brent_7d:
            df_b = pd.DataFrame(brent_7d)[["date", "price"]].rename(columns={"price": "Brent"})
            frames = [df_b]
            y_cols = {"Brent": "Brent"}
            if dubai_7d:
                df_d = pd.DataFrame(dubai_7d)[["date", "price"]].rename(columns={"price": "Dubai"})
                frames.append(df_d)
                y_cols["Dubai"] = "Dubai/Oman"
            df = frames[0]
            for f in frames[1:]:
                df = df.merge(f, on="date", how="outer")
            df = df.sort_values("date")
            fig = line_chart(df, "date", y_cols, "", height=300,
                             fill_area=True, show_range_selector=False)
            st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CONFIG)
        else:
            st.info("No price data available. Click Refresh Data in sidebar.")

    with col_cracks:
        st.markdown(f'<div style="font-size:0.9rem;font-weight:700;color:{TEXT_PRIMARY};">'
                    f'Product Crack Spreads</div>', unsafe_allow_html=True)
        if cracks:
            for product, spread in sorted(cracks.items(), key=lambda x: x[1], reverse=True):
                c = EMERALD if spread > 0 else CORAL
                bar_width = min(abs(spread) * 3, 100)
                st.markdown(f"""
                <div style="display:flex;align-items:center;gap:8px;margin:6px 0;">
                    <div style="width:70px;font-size:0.75rem;color:{TEXT_SECONDARY};font-weight:600;">
                        {product.title()}</div>
                    <div style="flex:1;background:rgba(255,255,255,0.04);border-radius:4px;height:18px;">
                        <div style="width:{bar_width}%;height:100%;background:{c}40;
                            border-radius:4px;"></div>
                    </div>
                    <div style="width:80px;text-align:right;font-size:0.82rem;font-weight:700;
                        color:{c};font-variant-numeric:tabular-nums;">${spread:+.2f}</div>
                </div>""", unsafe_allow_html=True)

            if grm is not None:
                grm_c = EMERALD if grm > 5 else (AMBER if grm > 0 else CORAL)
                st.markdown(f"""
                <div style="margin-top:12px;padding:10px 14px;background:rgba(255,255,255,0.03);
                    border-radius:8px;border:1px solid {grm_c}30;">
                    <div style="font-size:0.65rem;color:{TEXT_MUTED};text-transform:uppercase;
                        letter-spacing:0.08em;">Estimated GRM</div>
                    <div style="font-size:1.3rem;font-weight:700;color:{grm_c};
                        font-variant-numeric:tabular-nums;">${grm:.2f}/bbl</div>
                </div>""", unsafe_allow_html=True)
        else:
            st.caption("Crack spread data unavailable. Run a refresh to populate FX rates and product prices.")

    st.divider()

    # --- Top 5 News ---
    st.markdown(f'<div style="font-size:0.9rem;font-weight:700;color:{TEXT_PRIMARY};margin-bottom:0.5rem;">'
                f'Top Market-Moving News</div>', unsafe_allow_html=True)

    recent_articles = cached_news_articles(limit=50)
    if recent_articles:
        scored = sorted(recent_articles,
                        key=lambda a: abs(a.get("impact_score", 0)), reverse=True)
        top_5 = [a for a in scored if a.get("impact_score", 0) != 0][:5]
        if not top_5:
            top_5 = scored[:5]

        for article in top_5:
            news_card(
                article["title"], article.get("summary", ""),
                article.get("source", ""), article.get("published_date"),
                article.get("impact_tag", "neutral"),
                article.get("impact_score", 0), article.get("url"),
            )
    else:
        st.info("No news articles. Run a data refresh.")
