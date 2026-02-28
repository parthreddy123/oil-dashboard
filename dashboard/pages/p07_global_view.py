"""Page 7: Global View - OPEC production, demand/supply balance."""

import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dashboard.data_access import cached_global_events
from dashboard.components.price_chart import bar_chart, donut_chart, waterfall_chart
from dashboard.components.filters import date_range_filter
from dashboard.components.theme import (SERIES_COLORS, EMERALD, CORAL, GOLD, AMBER, TEXT_PRIMARY,
                                         TEXT_SECONDARY, TEXT_MUTED, BG_ELEVATED, PLOTLY_CONFIG, apply_theme)


def render():
    st.header("Global Oil & Gas View")

    with st.sidebar:
        st.subheader("Filters")
        start_date, end_date = date_range_filter("global")

    # --- OPEC Production ---
    st.subheader("OPEC Crude Production")
    opec_data = cached_global_events(event_type="opec_production", start_date=str(start_date),
                                     end_date=str(end_date), limit=200)
    if opec_data:
        df_opec = pd.DataFrame(opec_data)
        latest_date = df_opec["date"].max()
        df_latest = df_opec[df_opec["date"] == latest_date].copy()
        st.caption(f"Data as of: {latest_date}")

        total_prod = df_latest["value"].sum()
        st.metric("Total OPEC Production", f"{total_prod:.2f} mb/d")

        c1, c2 = st.columns(2)
        with c1:
            df_sorted = df_latest.sort_values("value", ascending=True)
            fig = px.bar(df_sorted, x="value", y="region", orientation="h",
                        title="OPEC Production by Country (mb/d)",
                        labels={"value": "mb/d", "region": ""}, color="value",
                        color_continuous_scale="Viridis", height=500)
            apply_theme(fig, height=500)
            fig.update_layout(margin=dict(l=120, r=20, t=55, b=45))
            st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CONFIG)

        with c2:
            fig = donut_chart(
                df_latest["region"].tolist(), df_latest["value"].tolist(),
                "OPEC Production Share",
                center_text=f"<b>{total_prod:.1f}</b><br>mb/d", height=500,
            )
            st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CONFIG)

        # Table
        display = df_latest[["region", "value"]].copy()
        display.columns = ["Country", "Production (mb/d)"]
        display = display.sort_values("Production (mb/d)", ascending=False)
        csv = display.to_csv(index=False)
        st.download_button("Download CSV", csv, "opec_production.csv", "text/csv")
        st.dataframe(display, use_container_width=True, hide_index=True)
    else:
        st.info("No OPEC data. Run a data refresh.")

    st.divider()

    # --- Demand/Supply Balance ---
    st.subheader("Global Demand/Supply Balance")
    ds_data = cached_global_events(event_type="demand_supply", start_date=str(start_date),
                                    end_date=str(end_date), limit=100)
    if ds_data:
        df_ds = pd.DataFrame(ds_data)
        latest_date = df_ds["date"].max()
        df_latest_ds = df_ds[df_ds["date"] == latest_date]

        metrics = {}
        for _, row in df_latest_ds.iterrows():
            desc = str(row["description"]).lower()
            if "world" in desc and "demand" in desc:
                metrics["World Demand"] = row["value"]
            elif "non-opec" in desc:
                metrics["Non-OPEC Supply"] = row["value"]
            elif "opec" in desc and "production" in desc:
                metrics["OPEC Production"] = row["value"]
            elif "call" in desc:
                metrics["Call on OPEC"] = row["value"]

        if metrics:
            cols = st.columns(len(metrics))
            for i, (name, val) in enumerate(metrics.items()):
                with cols[i]:
                    st.metric(name, f"{val:.1f} mb/d")

            if "World Demand" in metrics and "Non-OPEC Supply" in metrics:
                demand = metrics["World Demand"]
                non_opec = metrics.get("Non-OPEC Supply", 0)
                opec = metrics.get("OPEC Production", 0)
                total_supply = non_opec + opec
                balance = total_supply - demand

                # Waterfall visualization
                fig = go.Figure(go.Waterfall(
                    x=["Non-OPEC Supply", "OPEC Production", "Total Supply", "World Demand", "Balance"],
                    y=[non_opec, opec, 0, -demand, 0],
                    measure=["relative", "relative", "total", "relative", "total"],
                    text=[f"{non_opec:.1f}", f"{opec:.1f}", f"{total_supply:.1f}",
                          f"{demand:.1f}", f"{balance:+.1f}"],
                    textposition="outside", textfont=dict(size=12, color=TEXT_PRIMARY),
                    increasing=dict(marker=dict(color=EMERALD)),
                    decreasing=dict(marker=dict(color=CORAL)),
                    totals=dict(marker=dict(color=GOLD)),
                    connector=dict(line=dict(color="rgba(255,255,255,0.1)", dash="dot")),
                ))
                surplus_label = "Surplus" if balance > 0 else "Deficit"
                fig.update_layout(title=f"Global Oil Balance: {balance:+.1f} mb/d ({surplus_label})",
                                 yaxis_title="Million barrels/day", showlegend=False, height=420)
                apply_theme(fig, height=420)
                st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CONFIG)
    else:
        st.info("No demand/supply data. Run a refresh to load OPEC data.")

    st.divider()

    # --- All Events ---
    st.subheader("Recent Global Events")
    all_events = cached_global_events(start_date=str(start_date), end_date=str(end_date), limit=50)
    if all_events:
        df_ev = pd.DataFrame(all_events)
        display = df_ev[["date", "event_type", "region", "description", "value", "unit"]].copy()
        display.columns = ["Date", "Type", "Region", "Description", "Value", "Unit"]
        st.dataframe(display, use_container_width=True, hide_index=True)
