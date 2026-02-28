"""Page 1: Overview - Executive KPIs, trend chart, top news."""

import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import streamlit as st
import pandas as pd
from datetime import datetime
from dashboard.data_access import cached_crude_prices, cached_news_articles, cached_metric_snapshots, cached_refinery_data
from dashboard.components.kpi_card import kpi_card
from dashboard.components.price_chart import line_chart
from dashboard.components.news_card import news_card
from dashboard.components.theme import CYAN, TEAL, GOLD, AMBER, EMERALD, CORAL, PURPLE, TEXT_PRIMARY, TEXT_SECONDARY, TEXT_MUTED, TEXT_DIM, BG_CARD, BG_ELEVATED, PLOTLY_CONFIG


def render():
    # Branded header
    st.markdown(f"""
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:1rem;">
        <div>
            <h1 style="margin:0;font-size:1.6rem;font-weight:800;color:{TEXT_PRIMARY};letter-spacing:-0.02em;">
                Market Overview</h1>
            <div style="font-size:0.72rem;color:{TEXT_DIM};margin-top:2px;">
                Global oil & gas intelligence at a glance</div>
        </div>
        <div style="text-align:right;">
            <div style="font-size:0.7rem;color:{TEXT_MUTED};text-transform:uppercase;letter-spacing:0.08em;">
                {datetime.now().strftime("%A, %B %d, %Y")}</div>
        </div>
    </div>""", unsafe_allow_html=True)

    # --- KPI Row ---
    snapshots = {row["metric_name"]: row for row in cached_metric_snapshots(limit=20)}

    # Get sparkline data
    brent_hist = cached_crude_prices(benchmark="brent", limit=14)
    brent_sparkline = [r["price"] for r in reversed(brent_hist)] if brent_hist else None

    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        b = snapshots.get("brent_price")
        kpi_card("Brent Crude", float(b["metric_value"]) if b else None, "USD/bbl",
                 float(b["change_wow"]) if b and b["change_wow"] else None,
                 sparkline_data=brent_sparkline, accent_color=CYAN)
    with col2:
        d = snapshots.get("dubai_oman_price")
        dubai_hist = cached_crude_prices(benchmark="oman_dubai", limit=14)
        dubai_sparkline = [r["price"] for r in reversed(dubai_hist)] if dubai_hist else None
        kpi_card("Dubai/Oman", float(d["metric_value"]) if d else None, "USD/bbl",
                 sparkline_data=dubai_sparkline, accent_color=TEAL)
    with col3:
        ib = snapshots.get("indian_basket")
        kpi_card("Indian Basket", float(ib["metric_value"]) if ib else None, "USD/bbl",
                 accent_color=GOLD)
    with col4:
        u = snapshots.get("refinery_utilization")
        kpi_card("Refinery Util.", float(u["metric_value"]) if u else None, "%",
                 accent_color=AMBER)
    with col5:
        s = snapshots.get("bullish_sentiment_pct")
        kpi_card("Bullish Sentiment", float(s["metric_value"]) if s else None, "%",
                 accent_color=EMERALD)

    st.divider()

    # --- Price Chart + News ---
    col_chart, col_news = st.columns([3, 2])

    with col_chart:
        st.subheader("Crude Price Trends")
        brent_prices = cached_crude_prices(benchmark="brent", limit=90)
        dubai_prices = cached_crude_prices(benchmark="oman_dubai", limit=90)
        wti_prices = cached_crude_prices(benchmark="wti", limit=90)

        if brent_prices or dubai_prices:
            df_b = pd.DataFrame(brent_prices)[["date", "price"]].rename(columns={"price": "brent"}) if brent_prices else pd.DataFrame(columns=["date", "brent"])
            df_d = pd.DataFrame(dubai_prices)[["date", "price"]].rename(columns={"price": "dubai"}) if dubai_prices else pd.DataFrame(columns=["date", "dubai"])
            df_w = pd.DataFrame(wti_prices)[["date", "price"]].rename(columns={"price": "wti"}) if wti_prices else pd.DataFrame(columns=["date", "wti"])
            df = df_b.merge(df_d, on="date", how="outer").merge(df_w, on="date", how="outer").sort_values("date")
            y_cols = {"brent": "Brent", "dubai": "Dubai/Oman", "wti": "WTI"}
            fig = line_chart(df, "date", y_cols,
                            "Crude Oil Prices (90 Days)", height=440, fill_area=True, show_range_selector=True)
            st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CONFIG)
        else:
            st.info("No price data. Click 'Refresh Data' in the sidebar.")

    with col_news:
        st.subheader("Top Market Signals")

        # Bullish
        bullish = cached_news_articles(impact_tag="bullish", limit=3)
        if bullish:
            st.markdown(f'<div style="display:flex;align-items:center;gap:6px;margin:0.3rem 0;">'
                        f'<span style="color:{EMERALD};font-size:0.9rem;">&#9650;</span>'
                        f'<span style="font-size:0.78rem;font-weight:700;color:{EMERALD};'
                        f'text-transform:uppercase;letter-spacing:0.05em;">Bullish</span></div>',
                        unsafe_allow_html=True)
            for a in bullish:
                news_card(a["title"], a.get("summary", ""), a.get("source", ""),
                         a.get("published_date"), "bullish", a.get("impact_score", 0), a.get("url"))

        # Bearish
        bearish = cached_news_articles(impact_tag="bearish", limit=3)
        if bearish:
            st.markdown(f'<div style="display:flex;align-items:center;gap:6px;margin:0.3rem 0;">'
                        f'<span style="color:{CORAL};font-size:0.9rem;">&#9660;</span>'
                        f'<span style="font-size:0.78rem;font-weight:700;color:{CORAL};'
                        f'text-transform:uppercase;letter-spacing:0.05em;">Bearish</span></div>',
                        unsafe_allow_html=True)
            for a in bearish:
                news_card(a["title"], a.get("summary", ""), a.get("source", ""),
                         a.get("published_date"), "bearish", a.get("impact_score", 0), a.get("url"))

        if not bullish and not bearish:
            st.info("No tagged news yet. Refresh data to fetch news.")

    # --- Refinery Snapshot ---
    st.divider()
    st.subheader("Indian Refinery Snapshot")
    refinery_data = cached_refinery_data(limit=50)
    if refinery_data:
        df_ref = pd.DataFrame(refinery_data)
        if "company" in df_ref.columns and "throughput_tmt" in df_ref.columns:
            summary = df_ref.groupby("company").agg({
                "throughput_tmt": "sum", "capacity_mmtpa": "sum", "utilization_pct": "mean",
            }).round(1).reset_index()
            summary.columns = ["Company", "Throughput (TMT)", "Capacity (MMTPA)", "Avg Utilization (%)"]
            summary = summary.sort_values("Throughput (TMT)", ascending=False)
            st.dataframe(summary, use_container_width=True, hide_index=True)
    else:
        st.info("No refinery data. Run a refresh.")
