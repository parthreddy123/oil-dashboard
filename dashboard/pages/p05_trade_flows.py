"""Page 5: Trade Flows - Crude imports by source, treemap, donut chart."""

import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import streamlit as st
import pandas as pd
import plotly.express as px
from dashboard.data_access import cached_trade_flows
from dashboard.components.price_chart import treemap_chart, bar_chart, donut_chart
from dashboard.components.filters import date_range_filter
from dashboard.components.theme import (SERIES_COLORS, PLOTLY_CONFIG, apply_theme,
                                         TEXT_PRIMARY, TEXT_SECONDARY, TEXT_MUTED, TEXT_DIM,
                                         BG_ELEVATED, BORDER_SUBTLE)


def render():
    st.header("Trade Flows")

    with st.sidebar:
        st.subheader("Filters")
        start_date, end_date = date_range_filter("trade")
        flow_type = st.selectbox("Flow Type", ["crude_import", "product_export", "product_import"],
                                 format_func=lambda x: x.replace("_", " ").title(), key="trade_flow_type")

    flows = cached_trade_flows(flow_type=flow_type, start_date=str(start_date), end_date=str(end_date), limit=500)
    if not flows:
        st.info("No trade flow data. Run a data refresh.")
        return

    df = pd.DataFrame(flows)
    latest_date = df["date"].max()
    df_latest = df[df["date"] == latest_date].copy()
    st.caption(f"Data as of: {latest_date}")

    # KPIs
    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("Total Volume", f"{df_latest['volume_tmt'].sum():,.0f} TMT")
    with c2:
        if "value_musd" in df_latest.columns and df_latest["value_musd"].notna().any():
            total_val = df_latest["value_musd"].sum()
            st.metric("Total Value", f"${total_val:,.0f}M")
            st.markdown(f"""
            <style>.tf-tooltip-wrap:hover .tf-tooltip-text {{visibility:visible !important;opacity:1 !important;}}</style>
            <div class="tf-tooltip-wrap" style="position:relative;display:inline-block;cursor:help;">
                <span style="font-size:0.65rem;color:{TEXT_DIM};border:1px solid {TEXT_DIM};
                    border-radius:50%;width:14px;height:14px;display:inline-flex;align-items:center;
                    justify-content:center;">i</span>
                <span class="tf-tooltip-text" style="visibility:hidden;opacity:0;position:absolute;
                    z-index:999;bottom:calc(100% + 8px);left:50%;transform:translateX(-50%);
                    width:240px;background:{BG_ELEVATED};color:{TEXT_SECONDARY};font-size:0.7rem;
                    line-height:1.5;padding:10px 12px;border-radius:8px;border:1px solid {BORDER_SUBTLE};
                    box-shadow:0 4px 16px rgba(0,0,0,0.4);transition:opacity 0.2s;pointer-events:none;">
                    <b>Total Value</b> = sum of value_musd across all source countries for the selected
                    flow type on the latest date. Values are in millions of USD (FOB basis where available).
                </span>
            </div>""", unsafe_allow_html=True)
    with c3:
        st.metric("Source Countries", df_latest["country"].nunique())

    st.divider()

    # Treemap
    st.subheader(f"{flow_type.replace('_', ' ').title()} by Country")
    if not df_latest.empty:
        df_latest["root"] = flow_type.replace("_", " ").title()
        fig = treemap_chart(df_latest, ["root", "country"], "volume_tmt",
                           f"India {flow_type.replace('_', ' ').title()} by Source (TMT)", height=480)
        st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CONFIG)

    # Bar + Donut
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Volume by Country")
        sorted_df = df_latest.sort_values("volume_tmt", ascending=True)
        fig = px.bar(sorted_df, x="volume_tmt", y="country", orientation="h",
                     title="Volume (TMT)", labels={"volume_tmt": "TMT", "country": ""})
        fig.update_traces(marker_color=SERIES_COLORS[0])
        apply_theme(fig, height=400)
        st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CONFIG)

    with c2:
        st.subheader("Market Share")
        total = df_latest["volume_tmt"].sum()
        fig = donut_chart(
            df_latest["country"].tolist(), df_latest["volume_tmt"].tolist(),
            f"Share of {flow_type.replace('_', ' ').title()}",
            center_text=f"<b>{total:,.0f}</b><br>TMT", height=400,
        )
        st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CONFIG)

    # Table
    st.subheader("Detailed Data")
    display = df_latest[["country", "volume_tmt"]].copy()
    if "value_musd" in df_latest.columns:
        display["value_musd"] = df_latest["value_musd"]
        display["avg_price"] = (display["value_musd"] / display["volume_tmt"]).round(2)
        display.columns = ["Country", "Volume (TMT)", "Value (M USD)", "Avg Price (USD/T)"]
    else:
        display.columns = ["Country", "Volume (TMT)"]
    display = display.sort_values(display.columns[1], ascending=False)
    csv = display.to_csv(index=False)
    st.download_button("Download CSV", csv, "trade_flows.csv", "text/csv")
    st.dataframe(display, use_container_width=True, hide_index=True)
