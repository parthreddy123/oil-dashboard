"""Page 9: APAC Trade Flows - Regional crude imports, product exports, and trade patterns."""

import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dashboard.components.price_chart import bar_chart, donut_chart, treemap_chart
from dashboard.components.theme import (
    SERIES_COLORS, EMERALD, CORAL, AMBER, CYAN, GOLD,
    TEXT_PRIMARY, TEXT_SECONDARY, TEXT_MUTED,
    BG_ELEVATED, PLOTLY_CONFIG, apply_theme,
)

# Reference data: APAC crude oil trade flows (IEA, OPEC MOMR, country customs data)
# Volumes in kb/d (thousand barrels per day), 2024-2025 estimates

CRUDE_IMPORTS = [
    # China — world's largest crude importer
    {"importer": "China", "supplier": "Saudi Arabia", "volume_kbd": 1780, "grade": "Arab Light/Medium"},
    {"importer": "China", "supplier": "Russia", "volume_kbd": 2100, "grade": "ESPO/Urals"},
    {"importer": "China", "supplier": "Iraq", "volume_kbd": 1200, "grade": "Basrah Light/Medium"},
    {"importer": "China", "supplier": "UAE", "volume_kbd": 680, "grade": "Murban/Upper Zakum"},
    {"importer": "China", "supplier": "Oman", "volume_kbd": 820, "grade": "Oman Blend"},
    {"importer": "China", "supplier": "Kuwait", "volume_kbd": 650, "grade": "KEC"},
    {"importer": "China", "supplier": "Brazil", "volume_kbd": 950, "grade": "Tupi/Buzios"},
    {"importer": "China", "supplier": "Angola", "volume_kbd": 450, "grade": "Cabinda/Girassol"},
    {"importer": "China", "supplier": "Malaysia", "volume_kbd": 280, "grade": "Tapis/Labuan"},
    {"importer": "China", "supplier": "Iran", "volume_kbd": 1400, "grade": "Iranian Light/Heavy"},
    # South Korea
    {"importer": "South Korea", "supplier": "Saudi Arabia", "volume_kbd": 850, "grade": "Arab Light/Extra Light"},
    {"importer": "South Korea", "supplier": "Kuwait", "volume_kbd": 380, "grade": "KEC"},
    {"importer": "South Korea", "supplier": "Iraq", "volume_kbd": 350, "grade": "Basrah Light"},
    {"importer": "South Korea", "supplier": "UAE", "volume_kbd": 340, "grade": "Murban"},
    {"importer": "South Korea", "supplier": "USA", "volume_kbd": 250, "grade": "WTI/Mars"},
    {"importer": "South Korea", "supplier": "Russia", "volume_kbd": 100, "grade": "ESPO"},
    # Japan
    {"importer": "Japan", "supplier": "Saudi Arabia", "volume_kbd": 1050, "grade": "Arab Light/Extra Light"},
    {"importer": "Japan", "supplier": "UAE", "volume_kbd": 750, "grade": "Murban/Upper Zakum"},
    {"importer": "Japan", "supplier": "Kuwait", "volume_kbd": 280, "grade": "KEC"},
    {"importer": "Japan", "supplier": "Qatar", "volume_kbd": 200, "grade": "Qatar Marine"},
    {"importer": "Japan", "supplier": "Russia", "volume_kbd": 70, "grade": "Sakhalin Blend"},
    # Singapore (as hub — imports for re-export + refining)
    {"importer": "Singapore", "supplier": "Middle East", "volume_kbd": 550, "grade": "Mixed AG grades"},
    {"importer": "Singapore", "supplier": "West Africa", "volume_kbd": 180, "grade": "Bonny/Agbami"},
    {"importer": "Singapore", "supplier": "APAC", "volume_kbd": 120, "grade": "Tapis/Minas"},
    # Thailand
    {"importer": "Thailand", "supplier": "Saudi Arabia", "volume_kbd": 220, "grade": "Arab Light"},
    {"importer": "Thailand", "supplier": "UAE", "volume_kbd": 180, "grade": "Murban"},
    {"importer": "Thailand", "supplier": "Oman", "volume_kbd": 100, "grade": "Oman Blend"},
    {"importer": "Thailand", "supplier": "Kuwait", "volume_kbd": 80, "grade": "KEC"},
    # Taiwan
    {"importer": "Taiwan", "supplier": "Saudi Arabia", "volume_kbd": 350, "grade": "Arab Light"},
    {"importer": "Taiwan", "supplier": "Kuwait", "volume_kbd": 220, "grade": "KEC"},
    {"importer": "Taiwan", "supplier": "UAE", "volume_kbd": 100, "grade": "Murban"},
    {"importer": "Taiwan", "supplier": "USA", "volume_kbd": 80, "grade": "WTI"},
    # Malaysia
    {"importer": "Malaysia", "supplier": "Saudi Arabia", "volume_kbd": 120, "grade": "Arab Light"},
    {"importer": "Malaysia", "supplier": "UAE", "volume_kbd": 80, "grade": "Murban"},
    {"importer": "Malaysia", "supplier": "Russia", "volume_kbd": 60, "grade": "ESPO"},
    # Indonesia
    {"importer": "Indonesia", "supplier": "Saudi Arabia", "volume_kbd": 130, "grade": "Arab Light"},
    {"importer": "Indonesia", "supplier": "Nigeria", "volume_kbd": 80, "grade": "Bonny Light"},
    {"importer": "Indonesia", "supplier": "Iraq", "volume_kbd": 70, "grade": "Basrah Light"},
]

PRODUCT_EXPORTS = [
    # Singapore — world's largest product export hub
    {"exporter": "Singapore", "product": "Diesel/Gasoil", "volume_kbd": 450, "destination": "Australia/APAC"},
    {"exporter": "Singapore", "product": "Jet Fuel", "volume_kbd": 280, "destination": "Regional"},
    {"exporter": "Singapore", "product": "Naphtha", "volume_kbd": 200, "destination": "NE Asia petrochems"},
    {"exporter": "Singapore", "product": "Fuel Oil", "volume_kbd": 350, "destination": "Bunker/Power gen"},
    {"exporter": "Singapore", "product": "Gasoline", "volume_kbd": 180, "destination": "Indonesia/Vietnam"},
    # South Korea
    {"exporter": "South Korea", "product": "Diesel/Gasoil", "volume_kbd": 480, "destination": "APAC/Australia"},
    {"exporter": "South Korea", "product": "Gasoline", "volume_kbd": 220, "destination": "APAC/Pacific"},
    {"exporter": "South Korea", "product": "Jet Fuel", "volume_kbd": 150, "destination": "Japan/Pacific"},
    {"exporter": "South Korea", "product": "Naphtha", "volume_kbd": 120, "destination": "Japan/Taiwan"},
    {"exporter": "South Korea", "product": "LPG", "volume_kbd": 80, "destination": "APAC"},
    # China
    {"exporter": "China", "product": "Diesel/Gasoil", "volume_kbd": 550, "destination": "APAC/Australia"},
    {"exporter": "China", "product": "Gasoline", "volume_kbd": 380, "destination": "APAC/Africa"},
    {"exporter": "China", "product": "Jet Fuel", "volume_kbd": 280, "destination": "Regional"},
    {"exporter": "China", "product": "Naphtha", "volume_kbd": 150, "destination": "NE Asia"},
    # Japan
    {"exporter": "Japan", "product": "Diesel/Gasoil", "volume_kbd": 120, "destination": "APAC/Pacific"},
    {"exporter": "Japan", "product": "Fuel Oil", "volume_kbd": 80, "destination": "Singapore/bunker"},
    # Taiwan
    {"exporter": "Taiwan", "product": "Gasoline", "volume_kbd": 100, "destination": "APAC"},
    {"exporter": "Taiwan", "product": "Diesel/Gasoil", "volume_kbd": 80, "destination": "APAC"},
    # Thailand
    {"exporter": "Thailand", "product": "Diesel/Gasoil", "volume_kbd": 60, "destination": "Mekong countries"},
    {"exporter": "Thailand", "product": "Gasoline", "volume_kbd": 40, "destination": "Cambodia/Laos/Myanmar"},
]


def render():
    st.header("APAC Trade Flows")
    st.caption("Reference data from IEA, OPEC MOMR, and national customs agencies. Volumes are 2024-2025 estimates.")

    with st.sidebar:
        st.subheader("Filters")
        view = st.radio("View", ["Crude Imports", "Product Exports"], key="apac_tf_view")

    if view == "Crude Imports":
        _render_crude_imports()
    else:
        _render_product_exports()


def _render_crude_imports():
    df = pd.DataFrame(CRUDE_IMPORTS)

    # Filter
    with st.sidebar:
        importers = ["All"] + sorted(df["importer"].unique().tolist())
        selected = st.selectbox("Importer", importers, key="apac_tf_importer")
        if selected != "All":
            df = df[df["importer"] == selected]

    # --- KPIs ---
    total_vol = df["volume_kbd"].sum()
    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("Total Crude Imports", f"{total_vol:,} kb/d")
    with c2:
        st.metric("Supplier Countries", df["supplier"].nunique())
    with c3:
        st.metric("Importing Countries", df["importer"].nunique())

    st.divider()

    # --- By Importer ---
    st.subheader("Crude Imports by Country")
    importer_agg = df.groupby("importer")["volume_kbd"].sum().reset_index()
    importer_agg = importer_agg.sort_values("volume_kbd", ascending=False)

    c1, c2 = st.columns(2)
    with c1:
        fig = px.bar(importer_agg.sort_values("volume_kbd", ascending=True),
                     x="volume_kbd", y="importer", orientation="h",
                     title="Total Crude Imports (kb/d)",
                     labels={"volume_kbd": "kb/d", "importer": ""},
                     color="volume_kbd", color_continuous_scale="Viridis")
        apply_theme(fig, height=400)
        fig.update_layout(margin=dict(l=100, r=20, t=55, b=45))
        st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CONFIG)

    with c2:
        fig = donut_chart(
            importer_agg["importer"].tolist(), importer_agg["volume_kbd"].tolist(),
            "Import Share",
            center_text=f"<b>{total_vol:,}</b><br>kb/d", height=400,
        )
        st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CONFIG)

    st.divider()

    # --- By Supplier (who feeds Asia) ---
    st.subheader("Crude Suppliers to APAC")
    supplier_agg = df.groupby("supplier")["volume_kbd"].sum().reset_index()
    supplier_agg = supplier_agg.sort_values("volume_kbd", ascending=False)

    fig = px.bar(supplier_agg.head(15).sort_values("volume_kbd", ascending=True),
                 x="volume_kbd", y="supplier", orientation="h",
                 title="Top Suppliers (kb/d)",
                 labels={"volume_kbd": "kb/d", "supplier": ""})
    fig.update_traces(marker_color=CYAN)
    apply_theme(fig, height=450)
    fig.update_layout(margin=dict(l=120, r=20, t=55, b=45))
    st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CONFIG)

    # --- Treemap ---
    st.subheader("Import Flow Map")
    df_tree = df.copy()
    df_tree["root"] = "APAC Crude Imports"
    fig = treemap_chart(df_tree, ["root", "importer", "supplier"], "volume_kbd",
                        "Crude Import Flows (kb/d)", height=550)
    st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CONFIG)

    # --- Data Table ---
    st.subheader("Detailed Data")
    display = df[["importer", "supplier", "volume_kbd", "grade"]].copy()
    display.columns = ["Importer", "Supplier", "Volume (kb/d)", "Crude Grade"]
    display = display.sort_values("Volume (kb/d)", ascending=False)
    csv = display.to_csv(index=False)
    st.download_button("Download CSV", csv, "apac_crude_imports.csv", "text/csv")
    st.dataframe(display, use_container_width=True, hide_index=True)


def _render_product_exports():
    df = pd.DataFrame(PRODUCT_EXPORTS)

    # Filter
    with st.sidebar:
        exporters = ["All"] + sorted(df["exporter"].unique().tolist())
        selected = st.selectbox("Exporter", exporters, key="apac_tf_exporter")
        if selected != "All":
            df = df[df["exporter"] == selected]

    # --- KPIs ---
    total_vol = df["volume_kbd"].sum()
    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("Total Product Exports", f"{total_vol:,} kb/d")
    with c2:
        st.metric("Products", df["product"].nunique())
    with c3:
        st.metric("Exporting Countries", df["exporter"].nunique())

    st.divider()

    # --- By Exporter ---
    st.subheader("Product Exports by Country")
    exporter_agg = df.groupby("exporter")["volume_kbd"].sum().reset_index()
    exporter_agg = exporter_agg.sort_values("volume_kbd", ascending=False)

    c1, c2 = st.columns(2)
    with c1:
        fig = px.bar(exporter_agg.sort_values("volume_kbd", ascending=True),
                     x="volume_kbd", y="exporter", orientation="h",
                     title="Total Product Exports (kb/d)",
                     labels={"volume_kbd": "kb/d", "exporter": ""},
                     color="volume_kbd", color_continuous_scale="Viridis")
        apply_theme(fig, height=350)
        fig.update_layout(margin=dict(l=100, r=20, t=55, b=45))
        st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CONFIG)

    with c2:
        fig = donut_chart(
            exporter_agg["exporter"].tolist(), exporter_agg["volume_kbd"].tolist(),
            "Export Share",
            center_text=f"<b>{total_vol:,}</b><br>kb/d", height=350,
        )
        st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CONFIG)

    st.divider()

    # --- By Product ---
    st.subheader("Exports by Product")
    product_agg = df.groupby("product")["volume_kbd"].sum().reset_index()
    product_agg = product_agg.sort_values("volume_kbd", ascending=False)

    fig = px.bar(product_agg.sort_values("volume_kbd", ascending=True),
                 x="volume_kbd", y="product", orientation="h",
                 title="Product Export Volumes (kb/d)",
                 labels={"volume_kbd": "kb/d", "product": ""})
    fig.update_traces(marker_color=EMERALD)
    apply_theme(fig, height=350)
    fig.update_layout(margin=dict(l=120, r=20, t=55, b=45))
    st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CONFIG)

    # --- Treemap ---
    st.subheader("Export Flow Map")
    df_tree = df.copy()
    df_tree["root"] = "APAC Product Exports"
    fig = treemap_chart(df_tree, ["root", "exporter", "product"], "volume_kbd",
                        "Product Export Flows (kb/d)", height=500)
    st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CONFIG)

    # --- Data Table ---
    st.subheader("Detailed Data")
    display = df[["exporter", "product", "volume_kbd", "destination"]].copy()
    display.columns = ["Exporter", "Product", "Volume (kb/d)", "Destination"]
    display = display.sort_values("Volume (kb/d)", ascending=False)
    csv = display.to_csv(index=False)
    st.download_button("Download CSV", csv, "apac_product_exports.csv", "text/csv")
    st.dataframe(display, use_container_width=True, hide_index=True)
