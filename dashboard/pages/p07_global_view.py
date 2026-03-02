"""Page 7: Geopolitical Risk & Global View - World map, conflict zones, OPEC data."""

import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dashboard.data_access import cached_global_events, cached_news_articles
from dashboard.components.price_chart import donut_chart
from dashboard.components.filters import date_range_filter
from dashboard.components.theme import (
    SERIES_COLORS, EMERALD, CORAL, GOLD, AMBER, CYAN,
    TEXT_PRIMARY, TEXT_SECONDARY, TEXT_MUTED,
    BG_PRIMARY, BG_ELEVATED, PLOTLY_CONFIG, apply_theme,
)

# --- Curated Geopolitical Risk Zones ---
RISK_ZONES = [
    {
        "name": "Strait of Hormuz",
        "lat": 26.56, "lon": 56.25,
        "risk_level": "high", "risk_type": "Chokepoint / Military",
        "transit": "~20 million bbl/d (20% of world oil)",
        "description": "Critical chokepoint between Persian Gulf and Gulf of Oman. Iranian military presence creates persistent risk to tanker traffic.",
        "keywords": ["hormuz", "iran", "persian gulf", "iranian navy", "irgc"],
    },
    {
        "name": "Red Sea / Bab el-Mandeb",
        "lat": 12.58, "lon": 43.33,
        "risk_level": "high", "risk_type": "Active Conflict",
        "transit": "~9% of seaborne oil",
        "description": "Houthi attacks on commercial shipping have forced rerouting via Cape of Good Hope, adding 10-14 days transit and $1M+ per voyage.",
        "keywords": ["red sea", "houthi", "bab el-mandeb", "yemen", "aden"],
    },
    {
        "name": "Russia-Ukraine",
        "lat": 48.38, "lon": 35.00,
        "risk_level": "high", "risk_type": "Active Conflict / Sanctions",
        "transit": "Russia: ~5 mb/d exports",
        "description": "Western sanctions on Russian oil with $60/bbl price cap. Shadow fleet tanker operations. Reduced pipeline flows to Europe.",
        "keywords": ["russia", "ukraine", "russian oil", "urals", "price cap", "shadow fleet", "nord stream", "druzhba"],
    },
    {
        "name": "Suez Canal",
        "lat": 30.58, "lon": 32.34,
        "risk_level": "medium", "risk_type": "Chokepoint",
        "transit": "~7% of seaborne oil",
        "description": "Connects Mediterranean to Red Sea. Traffic reduced due to Red Sea diversions. Single point of failure for Europe-Asia trade.",
        "keywords": ["suez", "canal", "egypt"],
    },
    {
        "name": "Strait of Malacca",
        "lat": 2.50, "lon": 101.20,
        "risk_level": "medium", "risk_type": "Chokepoint",
        "transit": "~16 million bbl/d",
        "description": "Busiest oil trade chokepoint. Connects Indian Ocean to Pacific. Critical for China, Japan, South Korea oil imports.",
        "keywords": ["malacca", "strait of malacca", "singapore strait"],
    },
    {
        "name": "Libya",
        "lat": 31.20, "lon": 16.60,
        "risk_level": "medium", "risk_type": "Political Instability",
        "transit": "~1.2 mb/d capacity",
        "description": "Chronic instability with rival governments. Production swings from 0.3 to 1.2 mb/d depending on political situation.",
        "keywords": ["libya", "libyan", "tripoli", "benghazi", "noc libya"],
    },
    {
        "name": "Nigeria - Niger Delta",
        "lat": 5.00, "lon": 6.50,
        "risk_level": "medium", "risk_type": "Political / Militant",
        "transit": "~1.3 mb/d production",
        "description": "Pipeline vandalism, oil theft, and militant activity in Niger Delta. Production persistently below capacity.",
        "keywords": ["nigeria", "niger delta", "nigerian", "bonny light"],
    },
    {
        "name": "Venezuela",
        "lat": 8.00, "lon": -66.00,
        "risk_level": "medium", "risk_type": "Sanctions",
        "transit": "~0.8 mb/d (sanctions-limited)",
        "description": "US sanctions limit Venezuelan oil exports. Largest proven reserves globally but production collapsed from 3.0 to <1.0 mb/d.",
        "keywords": ["venezuela", "venezuelan", "pdvsa", "maduro"],
    },
    {
        "name": "South China Sea",
        "lat": 14.00, "lon": 114.00,
        "risk_level": "low", "risk_type": "Territorial Dispute",
        "transit": "~11 million bbl/d",
        "description": "Territorial disputes between China and neighboring nations. Major oil transit route to East Asian refineries.",
        "keywords": ["south china sea", "spratly", "paracel", "nine-dash"],
    },
    {
        "name": "Turkish Straits",
        "lat": 41.10, "lon": 29.00,
        "risk_level": "low", "risk_type": "Chokepoint",
        "transit": "~3 million bbl/d",
        "description": "Bosphorus and Dardanelles connect Black Sea to Mediterranean. Critical for CPC Blend and Kazakh oil exports.",
        "keywords": ["bosphorus", "turkish straits", "dardanelles", "cpc blend"],
    },
]

RISK_COLORS = {"high": CORAL, "medium": AMBER, "low": CYAN}
RISK_SIZES = {"high": 22, "medium": 16, "low": 12}


def _match_news_to_zones(articles):
    """Match news articles to geopolitical zones by keyword."""
    matches = []
    for article in articles:
        text = f"{(article.get('title') or '').lower()} {(article.get('summary') or '').lower()}"
        for zone in RISK_ZONES:
            for kw in zone["keywords"]:
                if kw in text:
                    # Slight jitter to prevent marker stacking
                    jitter_lat = (hash(article.get("title", "")) % 7 - 3) * 0.2
                    jitter_lon = (hash(article.get("url", "")) % 7 - 3) * 0.2
                    matches.append({
                        "date": article.get("published_date", ""),
                        "title": article.get("title", ""),
                        "impact_tag": article.get("impact_tag", "neutral"),
                        "zone": zone["name"],
                        "lat": zone["lat"] + jitter_lat,
                        "lon": zone["lon"] + jitter_lon,
                        "source": article.get("source", ""),
                    })
                    break
    return pd.DataFrame(matches) if matches else pd.DataFrame()


def _build_map(news_df):
    """Build Plotly scattergeo world map with risk zones and news overlay."""
    fig = go.Figure()

    # Layer 1: Static risk zones
    for zone in RISK_ZONES:
        color = RISK_COLORS[zone["risk_level"]]
        size = RISK_SIZES[zone["risk_level"]]
        fig.add_trace(go.Scattergeo(
            lat=[zone["lat"]], lon=[zone["lon"]],
            text=[zone["name"]],
            customdata=[[zone["risk_type"], zone["transit"], zone["description"]]],
            hovertemplate=(
                "<b>%{text}</b><br>"
                "Risk: %{customdata[0]}<br>"
                "Transit: %{customdata[1]}<br><br>"
                "%{customdata[2]}<extra></extra>"
            ),
            marker=dict(
                size=size, color=color, opacity=0.85,
                line=dict(width=1.5, color="rgba(255,255,255,0.3)"),
            ),
            name=zone["name"],
            showlegend=False,
        ))

    # Layer 2: News overlay (smaller diamonds)
    if news_df is not None and not news_df.empty:
        tag_colors = {"bullish": EMERALD, "bearish": CORAL, "neutral": AMBER}
        for tag in ["bearish", "bullish", "neutral"]:
            subset = news_df[news_df["impact_tag"] == tag]
            if subset.empty:
                continue
            fig.add_trace(go.Scattergeo(
                lat=subset["lat"], lon=subset["lon"],
                text=subset["title"],
                customdata=subset[["zone", "date", "source"]].values,
                hovertemplate=(
                    "<b>%{text}</b><br>"
                    "Zone: %{customdata[0]}<br>"
                    "Date: %{customdata[1]} | %{customdata[2]}<extra></extra>"
                ),
                marker=dict(
                    size=8, color=tag_colors.get(tag, AMBER),
                    opacity=0.6, symbol="diamond",
                ),
                name=f"News ({tag.title()})",
                showlegend=True,
            ))

    fig.update_geos(
        projection_type="natural earth",
        showcoastlines=True, coastlinecolor="rgba(255,255,255,0.15)",
        showland=True, landcolor="#1A1F2E",
        showocean=True, oceancolor=BG_PRIMARY,
        showlakes=False,
        showcountries=True, countrycolor="rgba(255,255,255,0.08)",
        showframe=False,
        bgcolor=BG_PRIMARY,
        lonaxis_range=[-30, 140],
        lataxis_range=[-10, 65],
    )

    fig.update_layout(
        height=550, margin=dict(l=0, r=0, t=10, b=0),
        hovermode="closest",
        legend=dict(
            orientation="h", yanchor="bottom", y=-0.05,
            font=dict(color=TEXT_SECONDARY, size=11),
            bgcolor="rgba(0,0,0,0)",
        ),
    )
    apply_theme(fig, height=550)
    fig.update_layout(geo=dict(bgcolor=BG_PRIMARY))

    return fig


def render():
    st.header("Geopolitical Risk & Global View")

    with st.sidebar:
        st.subheader("Filters")
        start_date, end_date = date_range_filter("global")

    # --- Legend ---
    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown(f'<span style="color:{CORAL};font-size:1.2rem;">&#9679;</span> '
                    f'<span style="color:{TEXT_SECONDARY};font-size:0.82rem;font-weight:600;">'
                    f'Active Conflict / High Risk</span>', unsafe_allow_html=True)
    with c2:
        st.markdown(f'<span style="color:{AMBER};font-size:1.2rem;">&#9679;</span> '
                    f'<span style="color:{TEXT_SECONDARY};font-size:0.82rem;font-weight:600;">'
                    f'Sanctions / Political Risk</span>', unsafe_allow_html=True)
    with c3:
        st.markdown(f'<span style="color:{CYAN};font-size:1.2rem;">&#9679;</span> '
                    f'<span style="color:{TEXT_SECONDARY};font-size:0.82rem;font-weight:600;">'
                    f'Chokepoint (Stable)</span>', unsafe_allow_html=True)

    # --- Fetch news for overlay ---
    news_articles = cached_news_articles(
        start_date=str(start_date), end_date=str(end_date), limit=200,
    )
    news_df = _match_news_to_zones(news_articles)

    # --- Map ---
    fig = _build_map(news_df)
    st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CONFIG)

    # --- Risk Zone Details ---
    st.subheader("Risk Zone Details")
    zone_rows = []
    for z in RISK_ZONES:
        color = RISK_COLORS[z["risk_level"]]
        zone_rows.append({
            "Zone": z["name"],
            "Risk Type": z["risk_type"],
            "Level": z["risk_level"].upper(),
            "Oil Transit": z["transit"],
            "Description": z["description"],
        })
    st.dataframe(pd.DataFrame(zone_rows), use_container_width=True, hide_index=True)

    # --- Recent Geopolitical Developments ---
    if not news_df.empty:
        st.subheader("Recent Geopolitical Developments")
        recent = news_df.sort_values("date", ascending=False).head(20)
        display_news = recent[["date", "zone", "title", "impact_tag", "source"]].copy()
        display_news.columns = ["Date", "Zone", "Headline", "Impact", "Source"]
        st.dataframe(display_news, use_container_width=True, hide_index=True)

    st.divider()

    # --- OPEC Production (collapsible) ---
    with st.expander("OPEC Production Data", expanded=False):
        opec_data = cached_global_events(
            event_type="opec_production", start_date=str(start_date),
            end_date=str(end_date), limit=200,
        )
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

            display = df_latest[["region", "value"]].copy()
            display.columns = ["Country", "Production (mb/d)"]
            display = display.sort_values("Production (mb/d)", ascending=False)
            csv = display.to_csv(index=False)
            st.download_button("Download CSV", csv, "opec_production.csv", "text/csv")
            st.dataframe(display, use_container_width=True, hide_index=True)
        else:
            st.info("No OPEC data. Run a data refresh.")

    # --- Demand/Supply Balance (collapsible) ---
    with st.expander("Global Demand/Supply Balance", expanded=False):
        ds_data = cached_global_events(
            event_type="demand_supply", start_date=str(start_date),
            end_date=str(end_date), limit=100,
        )
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

                    fig = go.Figure(go.Waterfall(
                        x=["Non-OPEC Supply", "OPEC Production", "Total Supply",
                           "World Demand", "Balance"],
                        y=[non_opec, opec, 0, -demand, 0],
                        measure=["relative", "relative", "total", "relative", "total"],
                        text=[f"{non_opec:.1f}", f"{opec:.1f}", f"{total_supply:.1f}",
                              f"{demand:.1f}", f"{balance:+.1f}"],
                        textposition="outside",
                        textfont=dict(size=12, color=TEXT_PRIMARY),
                        increasing=dict(marker=dict(color=EMERALD)),
                        decreasing=dict(marker=dict(color=CORAL)),
                        totals=dict(marker=dict(color=GOLD)),
                        connector=dict(line=dict(color="rgba(255,255,255,0.1)", dash="dot")),
                    ))
                    surplus_label = "Surplus" if balance > 0 else "Deficit"
                    fig.update_layout(
                        title=f"Global Oil Balance: {balance:+.1f} mb/d ({surplus_label})",
                        yaxis_title="Million barrels/day", showlegend=False, height=420,
                    )
                    apply_theme(fig, height=420)
                    st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CONFIG)
        else:
            st.info("No demand/supply data. Run a refresh to load OPEC data.")
