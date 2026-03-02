"""Page 3: Indian Refineries - Throughput, utilization, company breakdown."""

import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import streamlit as st
import pandas as pd
import plotly.express as px
from dashboard.data_access import cached_refinery_data
from dashboard.components.price_chart import bar_chart, gauge_chart
from dashboard.components.filters import company_filter
from dashboard.components.theme import (SERIES_COLORS, BG_ELEVATED, TEXT_PRIMARY, TEXT_SECONDARY,
                                         TEXT_MUTED, EMERALD, CORAL, AMBER, PLOTLY_CONFIG, apply_theme)


def render():
    st.header("Indian Refineries")
    with st.sidebar:
        st.subheader("Filters")
        company = company_filter("refinery")

    refinery_data = cached_refinery_data(company=company, limit=500)
    if not refinery_data:
        st.info("No refinery data. Click 'Refresh Data' to load PPAC data.")
        return

    df = pd.DataFrame(refinery_data)
    latest_date = df["date"].max()
    df_latest = df[df["date"] == latest_date].copy()
    st.caption(f"Data as of: {latest_date}")

    # --- KPIs ---
    avg_util = df_latest["utilization_pct"].mean() if not df_latest.empty else 0
    total_tp = df_latest["throughput_tmt"].sum() if not df_latest.empty else 0
    total_cap = df_latest["capacity_mmtpa"].sum() if not df_latest.empty else 0

    c1, c2, c3 = st.columns(3)
    with c1:
        fig = gauge_chart(round(avg_util, 1), "Avg Utilization Rate", threshold=95)
        st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CONFIG)
    with c2:
        st.metric("Total Throughput", f"{total_tp:,.0f} TMT")
        st.metric("Total Capacity", f"{total_cap:,.1f} MMTPA")
    with c3:
        st.metric("Refineries", len(df_latest))
        st.metric("Companies", df_latest["company"].nunique() if "company" in df_latest.columns else "N/A")

    st.divider()

    # --- Company Summary Snapshot ---
    st.subheader("Company Snapshot")
    if not df_latest.empty and "company" in df_latest.columns:
        summary = df_latest.groupby("company").agg({
            "throughput_tmt": "sum", "capacity_mmtpa": "sum", "utilization_pct": "mean",
        }).round(1).reset_index()
        summary.columns = ["Company", "Throughput (TMT)", "Capacity (MMTPA)", "Avg Utilization (%)"]
        summary = summary.sort_values("Throughput (TMT)", ascending=False)
        st.dataframe(summary, use_container_width=True, hide_index=True)

    st.divider()

    # --- Throughput by Refinery ---
    st.subheader("Throughput by Refinery")
    if not df_latest.empty:
        df_sorted = df_latest.sort_values("throughput_tmt", ascending=True)
        fig = px.bar(df_sorted, x="throughput_tmt", y="refinery", color="company",
                     orientation="h", title="Refinery Throughput (TMT)",
                     labels={"throughput_tmt": "Throughput (TMT)", "refinery": ""},
                     height=max(400, len(df_sorted) * 30), color_discrete_sequence=SERIES_COLORS)
        apply_theme(fig, height=max(400, len(df_sorted) * 30))
        fig.update_layout(margin=dict(l=150, r=20, t=55, b=45))
        st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CONFIG)

    # --- Company Breakdown ---
    st.subheader("By Company")
    if not df_latest.empty and "company" in df_latest.columns:
        cutil = df_latest.groupby("company").agg({
            "utilization_pct": "mean", "throughput_tmt": "sum", "capacity_mmtpa": "sum",
        }).round(1).reset_index()
        cutil.columns = ["Company", "Avg Utilization (%)", "Total Throughput (TMT)", "Total Capacity (MMTPA)"]
        cutil = cutil.sort_values("Total Throughput (TMT)", ascending=False)

        c1, c2 = st.columns(2)
        with c1:
            fig = px.bar(cutil, x="Company", y="Total Throughput (TMT)", color="Avg Utilization (%)",
                        title="Company Throughput", color_continuous_scale="Viridis")
            apply_theme(fig)
            st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CONFIG)
        with c2:
            fig = px.bar(cutil, x="Company", y="Avg Utilization (%)", title="Utilization Rate",
                        color="Avg Utilization (%)", color_continuous_scale=["#EF4444", "#F59E0B", "#10B981"])
            fig.add_hline(y=95, line_dash="dash", line_color=CORAL, annotation_text="95%")
            apply_theme(fig)
            st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CONFIG)

    # --- Table ---
    st.subheader("Detailed Data")
    if not df_latest.empty:
        display = df_latest[["refinery", "company", "capacity_mmtpa", "throughput_tmt", "utilization_pct"]].copy()
        display.columns = ["Refinery", "Company", "Capacity (MMTPA)", "Throughput (TMT)", "Utilization (%)"]
        display = display.sort_values("Throughput (TMT)", ascending=False)
        st.dataframe(display, use_container_width=True, hide_index=True)
