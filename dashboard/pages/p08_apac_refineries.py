"""Page 8: APAC Refineries - Capacity, throughput, and regional refining landscape."""

import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import streamlit as st
import pandas as pd
import plotly.express as px
from dashboard.components.price_chart import bar_chart, gauge_chart
from dashboard.components.theme import (
    SERIES_COLORS, EMERALD, CORAL, AMBER, CYAN,
    TEXT_PRIMARY, TEXT_SECONDARY, TEXT_MUTED, TEXT_DIM,
    BG_ELEVATED, PLOTLY_CONFIG, apply_theme,
)

# Reference data: Major APAC refineries (public sources: company reports, IEA, OPEC)
# Capacity in kb/d (thousand barrels per day), utilization estimated from recent public data
SE_ASIAN_REFINERIES = [
    # Singapore
    {"refinery": "Jurong Island (ExxonMobil)", "company": "ExxonMobil", "country": "Singapore",
     "capacity_kbd": 592, "utilization_pct": 92, "complexity": "High", "crude_type": "Light/Medium Sour"},
    {"refinery": "Pulau Bukom (Shell)", "company": "Shell", "country": "Singapore",
     "capacity_kbd": 237, "utilization_pct": 88, "complexity": "High", "crude_type": "Mixed"},
    # South Korea
    {"refinery": "Ulsan (SK Energy)", "company": "SK Energy", "country": "South Korea",
     "capacity_kbd": 840, "utilization_pct": 90, "complexity": "Very High", "crude_type": "Medium Sour"},
    {"refinery": "Yeosu (GS Caltex)", "company": "GS Caltex", "country": "South Korea",
     "capacity_kbd": 800, "utilization_pct": 89, "complexity": "Very High", "crude_type": "Medium Sour"},
    {"refinery": "Daesan (Hyundai Oilbank)", "company": "Hyundai Oilbank", "country": "South Korea",
     "capacity_kbd": 520, "utilization_pct": 87, "complexity": "High", "crude_type": "Medium Sour"},
    {"refinery": "Incheon (S-Oil)", "company": "S-Oil (Aramco)", "country": "South Korea",
     "capacity_kbd": 669, "utilization_pct": 91, "complexity": "Very High", "crude_type": "Arab Light/Medium"},
    # Japan
    {"refinery": "Negishi (ENEOS)", "company": "ENEOS", "country": "Japan",
     "capacity_kbd": 270, "utilization_pct": 82, "complexity": "High", "crude_type": "Medium Sour"},
    {"refinery": "Mizushima (ENEOS)", "company": "ENEOS", "country": "Japan",
     "capacity_kbd": 205, "utilization_pct": 78, "complexity": "High", "crude_type": "Mixed"},
    {"refinery": "Chiba (Idemitsu)", "company": "Idemitsu", "country": "Japan",
     "capacity_kbd": 190, "utilization_pct": 80, "complexity": "High", "crude_type": "Mixed"},
    {"refinery": "Sakai (Cosmo Energy)", "company": "Cosmo Energy", "country": "Japan",
     "capacity_kbd": 100, "utilization_pct": 75, "complexity": "Medium", "crude_type": "Mixed"},
    # China (coastal export-oriented)
    {"refinery": "Zhenhai (Sinopec)", "company": "Sinopec", "country": "China",
     "capacity_kbd": 920, "utilization_pct": 93, "complexity": "Very High", "crude_type": "Mixed"},
    {"refinery": "Dalian (PetroChina)", "company": "PetroChina", "country": "China",
     "capacity_kbd": 410, "utilization_pct": 88, "complexity": "High", "crude_type": "Mixed/Russian"},
    {"refinery": "Quanzhou (Sinochem)", "company": "Sinochem", "country": "China",
     "capacity_kbd": 240, "utilization_pct": 85, "complexity": "High", "crude_type": "Mixed"},
    {"refinery": "Rongsheng (Zhejiang Petro)", "company": "Rongsheng", "country": "China",
     "capacity_kbd": 800, "utilization_pct": 90, "complexity": "Very High", "crude_type": "Mixed"},
    # Thailand
    {"refinery": "Map Ta Phut (IRPC)", "company": "IRPC", "country": "Thailand",
     "capacity_kbd": 215, "utilization_pct": 86, "complexity": "High", "crude_type": "Mixed"},
    {"refinery": "Sriracha (Thai Oil)", "company": "Thai Oil", "country": "Thailand",
     "capacity_kbd": 275, "utilization_pct": 88, "complexity": "High", "crude_type": "Medium Sour"},
    {"refinery": "Rayong (Star Petroleum)", "company": "Star Petroleum", "country": "Thailand",
     "capacity_kbd": 165, "utilization_pct": 84, "complexity": "Medium", "crude_type": "Mixed"},
    # Taiwan
    {"refinery": "Taoyuan (CPC)", "company": "CPC Corp", "country": "Taiwan",
     "capacity_kbd": 540, "utilization_pct": 85, "complexity": "High", "crude_type": "Medium Sour"},
    {"refinery": "Mailiao (Formosa)", "company": "Formosa Plastics", "country": "Taiwan",
     "capacity_kbd": 540, "utilization_pct": 87, "complexity": "Very High", "crude_type": "Mixed"},
    # Malaysia
    {"refinery": "Melaka (Petronas)", "company": "Petronas", "country": "Malaysia",
     "capacity_kbd": 270, "utilization_pct": 83, "complexity": "High", "crude_type": "Tapis/Mixed"},
    {"refinery": "Pengerang (RAPID)", "company": "Petronas/Aramco", "country": "Malaysia",
     "capacity_kbd": 300, "utilization_pct": 80, "complexity": "Very High", "crude_type": "Arab Light"},
    # Indonesia
    {"refinery": "Cilacap (Pertamina)", "company": "Pertamina", "country": "Indonesia",
     "capacity_kbd": 348, "utilization_pct": 75, "complexity": "Medium", "crude_type": "Mixed"},
    {"refinery": "Balikpapan (Pertamina)", "company": "Pertamina", "country": "Indonesia",
     "capacity_kbd": 260, "utilization_pct": 72, "complexity": "Medium", "crude_type": "Local/Mixed"},
    # Vietnam
    {"refinery": "Nghi Son", "company": "NSRP (PVN/Idmitsu/KPI)", "country": "Vietnam",
     "capacity_kbd": 200, "utilization_pct": 78, "complexity": "High", "crude_type": "Kuwait/Mixed"},
    {"refinery": "Dung Quat", "company": "BSR (PVN)", "country": "Vietnam",
     "capacity_kbd": 148, "utilization_pct": 82, "complexity": "Medium", "crude_type": "Bach Ho/Mixed"},
    # Philippines
    {"refinery": "Bataan (Petron)", "company": "Petron Corp", "country": "Philippines",
     "capacity_kbd": 180, "utilization_pct": 80, "complexity": "Medium", "crude_type": "Mixed"},
]


def render():
    st.header("APAC Refineries")
    st.caption("Reference data from company reports, IEA, and OPEC. Utilization rates are estimates.")

    df = pd.DataFrame(SE_ASIAN_REFINERIES)
    df["throughput_kbd"] = (df["capacity_kbd"] * df["utilization_pct"] / 100).round(0).astype(int)

    # --- Filters ---
    with st.sidebar:
        st.subheader("Filters")
        countries = ["All"] + sorted(df["country"].unique().tolist())
        selected_country = st.selectbox("Country", countries, key="apac_ref_country")
        if selected_country != "All":
            df = df[df["country"] == selected_country]

    # --- KPIs ---
    total_cap = df["capacity_kbd"].sum()
    total_tp = df["throughput_kbd"].sum()
    avg_util = (df["capacity_kbd"] * df["utilization_pct"]).sum() / df["capacity_kbd"].sum()

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        fig = gauge_chart(round(avg_util, 1), "Avg Utilization", threshold=95)
        st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CONFIG)
    with c2:
        st.metric("Total Capacity", f"{total_cap:,} kb/d")
        st.metric("Total Throughput", f"{total_tp:,} kb/d")
    with c3:
        st.metric("Refineries", len(df))
        st.metric("Countries", df["country"].nunique())
    with c4:
        st.metric("Companies", df["company"].nunique())
        complexity_counts = df["complexity"].value_counts()
        top_cx = complexity_counts.index[0] if not complexity_counts.empty else "N/A"
        st.metric("Most Common Complexity", top_cx)

    st.divider()

    # --- Capacity by Country ---
    st.subheader("Refining Capacity by Country")
    country_agg = df.groupby("country").agg(
        capacity=("capacity_kbd", "sum"),
        throughput=("throughput_kbd", "sum"),
        refineries=("refinery", "count"),
        avg_util=("utilization_pct", "mean"),
    ).round(1).reset_index().sort_values("capacity", ascending=False)

    c1, c2 = st.columns(2)
    with c1:
        fig = px.bar(country_agg.sort_values("capacity", ascending=True),
                     x="capacity", y="country", orientation="h",
                     title="Total Capacity (kb/d)", color="avg_util",
                     color_continuous_scale="Viridis",
                     labels={"capacity": "kb/d", "country": "", "avg_util": "Avg Util %"})
        apply_theme(fig, height=450)
        fig.update_layout(margin=dict(l=100, r=20, t=55, b=45))
        st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CONFIG)

    with c2:
        from dashboard.components.price_chart import donut_chart
        fig = donut_chart(
            country_agg["country"].tolist(), country_agg["capacity"].tolist(),
            "Capacity Share by Country",
            center_text=f"<b>{total_cap:,}</b><br>kb/d", height=450,
        )
        st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CONFIG)

    st.divider()

    # --- Individual Refineries ---
    st.subheader("Refinery Details")
    df_sorted = df.sort_values("capacity_kbd", ascending=True)
    fig = px.bar(df_sorted, x="capacity_kbd", y="refinery", color="country",
                 orientation="h", title="Refinery Capacity (kb/d)",
                 labels={"capacity_kbd": "kb/d", "refinery": ""},
                 height=max(500, len(df_sorted) * 25),
                 color_discrete_sequence=SERIES_COLORS)
    apply_theme(fig, height=max(500, len(df_sorted) * 25))
    fig.update_layout(margin=dict(l=220, r=20, t=55, b=45))
    st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CONFIG)

    # --- Utilization by Company ---
    st.subheader("Utilization by Company")
    company_agg = df.groupby("company").agg(
        capacity=("capacity_kbd", "sum"),
        throughput=("throughput_kbd", "sum"),
        avg_util=("utilization_pct", "mean"),
    ).round(1).reset_index().sort_values("capacity", ascending=False)

    fig = px.bar(company_agg, x="company", y="avg_util",
                 title="Average Utilization by Company (%)",
                 color="avg_util",
                 color_continuous_scale=[CORAL, AMBER, EMERALD])
    fig.add_hline(y=90, line_dash="dash", line_color=AMBER,
                  annotation_text="90% benchmark")
    apply_theme(fig)
    st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CONFIG)

    # --- Data Table ---
    st.subheader("Full Data")
    display = df[["refinery", "company", "country", "capacity_kbd", "throughput_kbd",
                   "utilization_pct", "complexity", "crude_type"]].copy()
    display.columns = ["Refinery", "Company", "Country", "Capacity (kb/d)", "Throughput (kb/d)",
                       "Utilization (%)", "Complexity", "Crude Slate"]
    display = display.sort_values("Capacity (kb/d)", ascending=False)
    csv = display.to_csv(index=False)
    st.download_button("Download CSV", csv, "se_asian_refineries.csv", "text/csv")
    st.dataframe(display, use_container_width=True, hide_index=True)
