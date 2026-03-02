"""Page 2: Crude Prices - Multi-benchmark chart with global grades."""

import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import streamlit as st
import pandas as pd
from dashboard.data_access import cached_crude_prices
from dashboard.components.price_chart import line_chart
from dashboard.components.filters import date_range_filter, benchmark_filter
from dashboard.components.theme import CYAN, TEAL, EMERALD, CORAL, TEXT_PRIMARY, TEXT_SECONDARY, TEXT_MUTED, TEXT_DIM, BG_CARD, PLOTLY_CONFIG

BM_LABELS = {
    "brent": "Brent", "wti": "WTI", "oman_dubai": "Dubai/Oman",
    "murban": "Murban", "arab_light": "Arab Light", "basrah_light": "Basrah Light",
    "tapis": "Tapis", "espo": "ESPO", "opec_basket": "OPEC Basket",
    "indian_basket": "Indian Basket",
}

ESTIMATED_BENCHMARKS = {"murban", "arab_light", "basrah_light", "tapis", "espo", "opec_basket"}


def render():
    st.header("Crude Oil Prices")

    with st.sidebar:
        st.subheader("Filters")
        start_date, end_date = date_range_filter("crude", default_preset_index=1)
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
    y_cols = {bm: BM_LABELS.get(bm, bm.upper()) for bm in benchmarks if bm in pivot.columns}
    fig = line_chart(pivot, "date", y_cols, "Crude Oil Benchmark Prices", height=500,
                    show_range_selector=True)
    st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CONFIG)

    # --- Statistics Grid ---
    st.subheader("Price Statistics")
    for bm in benchmarks:
        bm_df = df[df["benchmark"] == bm]["price"]
        if bm_df.empty:
            continue
        latest = bm_df.iloc[0]
        avg, high, low, std, rng = bm_df.mean(), bm_df.max(), bm_df.min(), bm_df.std(), bm_df.max() - bm_df.min()
        label = BM_LABELS.get(bm, bm.upper())

        st.markdown(f"""
        <div style="margin-bottom:1rem;">
            <div style="font-size:0.78rem;font-weight:700;color:{CYAN};text-transform:uppercase;
                letter-spacing:0.08em;margin-bottom:8px;padding-bottom:4px;
                border-bottom:1px solid rgba(0,212,170,0.2);">{label}</div>
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

    if any(bm in benchmarks for bm in ESTIMATED_BENCHMARKS):
        st.caption("Estimated grades (Murban, Arab Light, Basrah Light, Tapis, ESPO, OPEC Basket) "
                   "are derived from Brent/Dubai using typical historical differentials. "
                   "For authoritative prices, consult Platts/Argus.")
