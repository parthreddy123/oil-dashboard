"""Page 4: Products & Cracks - Product prices, crack spreads, estimated GRM."""

import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import streamlit as st
import pandas as pd
import plotly.express as px
from dashboard.data_access import cached_product_prices, cached_crack_spreads
from dashboard.components.price_chart import line_chart, bar_chart, waterfall_chart
from dashboard.components.filters import date_range_filter
from dashboard.components.theme import (EMERALD, CORAL, GOLD, CYAN, TEXT_PRIMARY, TEXT_SECONDARY,
                                         TEXT_MUTED, BG_CARD, BG_ELEVATED, PLOTLY_CONFIG, apply_theme)
from processing.calculations import calculate_crack_spreads, estimate_grm


def render():
    st.header("Products & Crack Spreads")

    with st.sidebar:
        st.subheader("Filters")
        start_date, end_date = date_range_filter("products")

    # --- Product Prices ---
    st.subheader("Product Prices")
    products = cached_product_prices(start_date=str(start_date), end_date=str(end_date), limit=500)

    if products:
        df = pd.DataFrame(products)
        latest_date = df["date"].max()
        df_latest = df[df["date"] == latest_date]

        cols = st.columns(min(len(df_latest), 6))
        for i, (_, row) in enumerate(df_latest.iterrows()):
            if i < len(cols):
                with cols[i]:
                    unit = row.get("unit", "")
                    st.metric(row["product"].title(), f"{row['price']:.2f} {unit}")

        # Line chart: only include USD/bbl products (exclude INR/cylinder LPG etc.)
        df_chart = df[df["unit"] == "USD/bbl"].copy() if "unit" in df.columns else df.copy()
        if df_chart["date"].nunique() > 1 and not df_chart.empty:
            pivot = df_chart.pivot_table(index="date", columns="product", values="price").reset_index()
            pivot["date"] = pd.to_datetime(pivot["date"])
            y_cols = {col: col.title() for col in pivot.columns if col != "date"}
            fig = line_chart(pivot, "date", y_cols, "Product Price Trends (USD/bbl)", y_title="Price (USD/bbl)")
            st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CONFIG)
    else:
        st.info("No product price data available.")

    st.divider()

    # --- Crack Spreads + GRM ---
    # Prefer Singapore estimated cracks from DB (anchored to realistic market levels)
    from database.db_manager import get_connection
    cracks = {}
    grm = None
    with get_connection() as conn:
        rows = conn.execute(
            """SELECT product, spread, estimated_grm FROM crack_spreads
               WHERE date=(SELECT MAX(date) FROM crack_spreads)
               AND source = 'yfinance_sg_est'"""
        ).fetchall()
        if rows:
            for r in rows:
                cracks[r["product"]] = float(r["spread"])
                if grm is None and r["estimated_grm"] is not None:
                    grm = float(r["estimated_grm"])
    # Fallback to calculated if no yfinance data
    if not cracks:
        cracks = calculate_crack_spreads()
        grm = estimate_grm(cracks)

    benchmark_label = "Dubai" if any(True for _ in cracks) and grm else "Brent"
    st.subheader(f"Crack Spreads vs {benchmark_label}")

    if cracks:
        col1, col2 = st.columns([2, 1])

        with col1:
            crack_df = pd.DataFrame([{"Product": k.title(), "Crack Spread": v} for k, v in cracks.items()])
            fig = px.bar(crack_df, x="Product", y="Crack Spread", title="Current Crack Spreads vs Brent",
                        color="Crack Spread", color_continuous_scale=["#EF4444", "#F59E0B", "#10B981"])
            fig.add_hline(y=0, line_dash="dash", line_color="rgba(255,255,255,0.2)")
            apply_theme(fig)
            st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CONFIG)

        with col2:
            if grm is not None:
                grm_color = EMERALD if grm > 0 else CORAL
                st.markdown(f"""
                <div style="background:linear-gradient(135deg,{BG_CARD},{BG_ELEVATED});border-radius:12px;
                    padding:1.5rem;border:1px solid {grm_color}30;text-align:center;margin-bottom:1rem;">
                    <div style="font-size:0.7rem;color:{TEXT_MUTED};text-transform:uppercase;
                        letter-spacing:0.1em;margin-bottom:8px;">Gross Refining Margin</div>
                    <div style="font-size:2.2rem;font-weight:800;color:{grm_color};
                        font-variant-numeric:tabular-nums;">${grm:.2f}
                        <span style="font-size:0.8rem;color:{TEXT_MUTED};font-weight:400;">/bbl</span></div>
                </div>""", unsafe_allow_html=True)

                # Waterfall breakdown — weighted by refinery yield
                GRM_WEIGHTS = {
                    "diesel": 0.42, "petrol": 0.22, "naphtha": 0.12,
                    "atf": 0.10, "fuel_oil": 0.08, "lpg": 0.06,
                }
                weighted_items = []
                for prod, spread in cracks.items():
                    w = GRM_WEIGHTS.get(prod, 0)
                    if w > 0:
                        weighted_items.append((prod, round(spread * w, 2), w))
                weighted_items.sort(key=lambda x: abs(x[1]), reverse=True)
                categories = [f"{p.title()} ({w:.0%})" for p, _, w in weighted_items] + ["Net GRM"]
                values = [v for _, v, _ in weighted_items] + [grm]
                fig_wf = waterfall_chart(categories, values, f"GRM Build-up: ${grm:.2f}/bbl", height=350)
                st.plotly_chart(fig_wf, use_container_width=True, config=PLOTLY_CONFIG)
            else:
                st.info("Insufficient data for GRM calculation")

            st.markdown("**Spread Details:**")
            for p, s in sorted(cracks.items(), key=lambda x: x[1], reverse=True):
                c = EMERALD if s > 0 else CORAL
                st.markdown(f'<span style="color:{c};font-weight:600;font-variant-numeric:tabular-nums;">'
                           f'{p.title():12s} {s:+.2f} USD/bbl</span>', unsafe_allow_html=True)
    else:
        st.info("No crack spread data. Need both crude and product prices.")

    # --- Historical ---
    st.divider()
    st.subheader("Historical Crack Spreads")
    hist = cached_crack_spreads(start_date=str(start_date), end_date=str(end_date), limit=500)
    if hist:
        df_c = pd.DataFrame(hist)
        df_c["date"] = pd.to_datetime(df_c["date"])
        pivot = df_c.pivot_table(index="date", columns="product", values="spread").reset_index()
        y_cols = {col: col.title() for col in pivot.columns if col != "date"}
        if y_cols:
            fig = line_chart(pivot, "date", y_cols, "Historical Crack Spreads", "Spread (USD/bbl)")
            st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CONFIG)
    else:
        st.caption("Historical data builds over multiple refreshes.")
