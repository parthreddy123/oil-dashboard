"""Page 2: Crude Prices - Multi-line chart, statistics grid, Brent-WTI spread."""

import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import streamlit as st
import pandas as pd
from dashboard.data_access import cached_crude_prices
from dashboard.components.price_chart import line_chart
from dashboard.components.filters import date_range_filter, benchmark_filter
from dashboard.components.theme import CYAN, TEAL, EMERALD, CORAL, TEXT_PRIMARY, TEXT_SECONDARY, TEXT_MUTED, TEXT_DIM, BG_CARD, PLOTLY_CONFIG


def render():
    st.header("Crude Oil Prices")

    with st.sidebar:
        st.subheader("Filters")
        start_date, end_date = date_range_filter("crude")
        benchmarks = benchmark_filter("crude")

    all_data = []
    for bm in benchmarks:
        rows = cached_crude_prices(benchmark=bm, start_date=str(start_date), end_date=str(end_date), limit=1000)
        for r in rows:
            all_data.append({"date": r["date"], "price": r["price"], "benchmark": r["benchmark"]})

    if not all_data:
        st.info("No crude price data. Click 'Refresh Data' in the sidebar.")
        return

    df = pd.DataFrame(all_data)
    df["date"] = pd.to_datetime(df["date"])

    # --- Price Chart ---
    st.subheader("Price Trends")
    pivot = df.pivot_table(index="date", columns="benchmark", values="price").reset_index()
    y_cols = {bm: bm.upper() for bm in benchmarks if bm in pivot.columns}
    fig = line_chart(pivot, "date", y_cols, "Crude Oil Benchmark Prices", height=460,
                    show_range_selector=True)
    st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CONFIG)

    # --- Brent-Dubai Spread ---
    if "brent" in benchmarks and "oman_dubai" in benchmarks and "brent" in pivot.columns and "oman_dubai" in pivot.columns:
        st.subheader("Brent-Dubai Spread")
        spread_bd = pivot[["date", "brent", "oman_dubai"]].copy()
        spread_bd = spread_bd.sort_values("date")
        spread_bd["brent"] = spread_bd["brent"].ffill()
        spread_bd["oman_dubai"] = spread_bd["oman_dubai"].ffill()
        spread_bd = spread_bd.dropna(subset=["brent", "oman_dubai"])
        spread_bd["spread"] = spread_bd["brent"] - spread_bd["oman_dubai"]
        fig_bd = line_chart(spread_bd, "date", {"spread": "Brent-Dubai Spread"},
                           "Brent-Dubai Price Differential", "Spread (USD/bbl)")
        st.plotly_chart(fig_bd, use_container_width=True, config=PLOTLY_CONFIG)

    # --- Brent-WTI Spread ---
    if "brent" in benchmarks and "wti" in benchmarks and "brent" in pivot.columns and "wti" in pivot.columns:
        st.subheader("Brent-WTI Spread")
        spread_df = pivot[["date", "brent", "wti"]].copy()
        spread_df = spread_df.sort_values("date")
        spread_df["brent"] = spread_df["brent"].ffill()
        spread_df["wti"] = spread_df["wti"].ffill()
        spread_df = spread_df.dropna(subset=["brent", "wti"])
        spread_df["spread"] = spread_df["brent"] - spread_df["wti"]
        fig_s = line_chart(spread_df, "date", {"spread": "Brent-WTI Spread"},
                          "Brent-WTI Price Differential", "Spread (USD/bbl)")
        st.plotly_chart(fig_s, use_container_width=True, config=PLOTLY_CONFIG)

    # --- Statistics Grid ---
    st.subheader("Price Statistics")
    for bm in benchmarks:
        bm_df = df[df["benchmark"] == bm]["price"]
        if bm_df.empty:
            continue
        latest = bm_df.iloc[0]
        avg, high, low, std, rng = bm_df.mean(), bm_df.max(), bm_df.min(), bm_df.std(), bm_df.max() - bm_df.min()

        st.markdown(f"""
        <div style="margin-bottom:1rem;">
            <div style="font-size:0.78rem;font-weight:700;color:{CYAN};text-transform:uppercase;
                letter-spacing:0.08em;margin-bottom:8px;padding-bottom:4px;
                border-bottom:1px solid rgba(0,212,170,0.2);">{bm.upper()}</div>
            <div style="display:grid;grid-template-columns:repeat(6,1fr);gap:12px;">
                <div><div style="font-size:0.62rem;color:{TEXT_MUTED};text-transform:uppercase;letter-spacing:0.08em;">Latest</div>
                    <div style="font-size:1.05rem;font-weight:700;color:{TEXT_PRIMARY};font-variant-numeric:tabular-nums;">${latest:.2f}</div></div>
                <div><div style="font-size:0.62rem;color:{TEXT_MUTED};text-transform:uppercase;">Average</div>
                    <div style="font-size:1.05rem;font-weight:600;color:{TEXT_SECONDARY};">${avg:.2f}</div></div>
                <div><div style="font-size:0.62rem;color:{TEXT_MUTED};text-transform:uppercase;">High</div>
                    <div style="font-size:1.05rem;font-weight:600;color:{EMERALD};">${high:.2f}</div></div>
                <div><div style="font-size:0.62rem;color:{TEXT_MUTED};text-transform:uppercase;">Low</div>
                    <div style="font-size:1.05rem;font-weight:600;color:{CORAL};">${low:.2f}</div></div>
                <div><div style="font-size:0.62rem;color:{TEXT_MUTED};text-transform:uppercase;">Std Dev</div>
                    <div style="font-size:1.05rem;font-weight:600;color:{TEXT_SECONDARY};">${std:.2f}</div></div>
                <div><div style="font-size:0.62rem;color:{TEXT_MUTED};text-transform:uppercase;">Range</div>
                    <div style="font-size:1.05rem;font-weight:600;color:{TEXT_SECONDARY};">${rng:.2f}</div></div>
            </div>
        </div>""", unsafe_allow_html=True)

