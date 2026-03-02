"""Page 6: News & Impact - Scrollable feed, impact timeline, sentiment analysis."""

import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import streamlit as st
import pandas as pd
from dashboard.data_access import cached_news_articles
from dashboard.components.news_card import news_feed
from dashboard.components.price_chart import scatter_chart
from dashboard.components.filters import date_range_filter, impact_filter
from dashboard.components.theme import (EMERALD, CORAL, TEXT_PRIMARY, TEXT_SECONDARY, TEXT_MUTED, TEXT_DIM,
                                         BG_ELEVATED, BORDER_SUBTLE, PLOTLY_CONFIG, apply_theme, SERIES_COLORS)


def render():
    st.header("News & Impact Analysis")

    with st.sidebar:
        st.subheader("Filters")
        start_date, end_date = date_range_filter("news")
        impact_tags = impact_filter("news")
        source_filter = st.text_input("Source (optional)", key="news_source")
        search_term = st.text_input("Search headlines", key="news_search")

    all_articles = []
    for tag in impact_tags:
        articles = cached_news_articles(
            impact_tag=tag, source=source_filter if source_filter else None,
            start_date=str(start_date), end_date=str(end_date), limit=50,
        )
        all_articles.extend(articles)

    # Search filter
    if search_term:
        all_articles = [a for a in all_articles if search_term.lower() in a.get("title", "").lower()]

    all_articles.sort(key=lambda x: x.get("published_date", "") or "", reverse=True)

    # Impact Summary
    bullish_count = sum(1 for a in all_articles if a.get("impact_tag") == "bullish")
    bearish_count = sum(1 for a in all_articles if a.get("impact_tag") == "bearish")
    neutral_count = sum(1 for a in all_articles if a.get("impact_tag") == "neutral")
    total = len(all_articles)

    SENTIMENT_TOOLTIP = (
        "<b>How are articles tagged?</b><br><br>"
        "<b>Bullish</b> = news likely to push oil prices UP "
        "(supply cuts, demand growth, geopolitical risk).<br><br>"
        "<b>Bearish</b> = news likely to push prices DOWN "
        "(demand weakness, supply increases, economic slowdown).<br><br>"
        "<b>Impact Score</b> (-3 to +3) reflects estimated magnitude. "
        "Scored by keyword analysis of headlines and summaries."
    )

    c1, c2, c3, c4 = st.columns(4)
    with c1: st.metric("Total Articles", total)
    with c2: st.metric("Bullish", bullish_count)
    with c3: st.metric("Bearish", bearish_count)
    with c4:
        st.metric("Neutral", neutral_count)
        st.markdown(f"""
        <style>.news-tooltip-wrap:hover .news-tooltip-text {{visibility:visible !important;opacity:1 !important;}}</style>
        <div class="news-tooltip-wrap" style="position:relative;display:inline-block;cursor:help;">
            <span style="font-size:0.65rem;color:{TEXT_DIM};border:1px solid {TEXT_DIM};
                border-radius:50%;width:14px;height:14px;display:inline-flex;align-items:center;
                justify-content:center;">i</span>
            <span class="news-tooltip-text" style="visibility:hidden;opacity:0;position:absolute;
                z-index:999;bottom:calc(100% + 8px);right:0;
                width:280px;background:{BG_ELEVATED};color:{TEXT_SECONDARY};font-size:0.7rem;
                line-height:1.5;padding:10px 12px;border-radius:8px;border:1px solid {BORDER_SUBTLE};
                box-shadow:0 4px 16px rgba(0,0,0,0.4);transition:opacity 0.2s;pointer-events:none;">
                {SENTIMENT_TOOLTIP}
            </span>
        </div>""", unsafe_allow_html=True)

    st.divider()

    # --- Impact Timeline ---
    st.subheader("Impact Timeline")
    if all_articles:
        df = pd.DataFrame(all_articles)
        df["published_date"] = pd.to_datetime(df["published_date"], errors="coerce")
        df_valid = df.dropna(subset=["published_date", "impact_score"]).copy()

        if not df_valid.empty:
            df_valid["abs_score"] = df_valid["impact_score"].abs() * 20 + 5
            fig = scatter_chart(df_valid, "published_date", "impact_score", color_col="impact_tag",
                               size_col="abs_score", title="News Impact Over Time",
                               hover_cols=["title", "source"], height=400)
            fig.add_hline(y=0, line_dash="dash", line_color="rgba(255,255,255,0.15)")
            st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CONFIG)

    st.divider()

    # --- Sentiment Bar + Sources ---
    c1, c2 = st.columns(2)

    with c1:
        st.subheader("Sentiment Breakdown")
        if total > 0:
            import plotly.graph_objects as go
            fig = go.Figure()
            fig.add_trace(go.Bar(y=["Sentiment"], x=[bullish_count], name="Bullish",
                                orientation="h", marker_color=EMERALD,
                                text=[f"{bullish_count} ({bullish_count/total*100:.0f}%)"], textposition="inside"))
            fig.add_trace(go.Bar(y=["Sentiment"], x=[neutral_count], name="Neutral",
                                orientation="h", marker_color=TEXT_MUTED,
                                text=[f"{neutral_count}"], textposition="inside"))
            fig.add_trace(go.Bar(y=["Sentiment"], x=[bearish_count], name="Bearish",
                                orientation="h", marker_color=CORAL,
                                text=[f"{bearish_count} ({bearish_count/total*100:.0f}%)"], textposition="inside"))
            fig.update_layout(barmode="stack", height=140, showlegend=True, yaxis_visible=False)
            apply_theme(fig, height=140)
            fig.update_layout(margin=dict(l=10, r=10, t=30, b=10))
            st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CONFIG)

    with c2:
        st.subheader("Top Sources")
        if all_articles:
            import plotly.express as px
            source_counts = pd.DataFrame(all_articles)["source"].value_counts().head(10)
            fig = px.bar(x=source_counts.values, y=source_counts.index, orientation="h",
                        title="", labels={"x": "Count", "y": ""})
            fig.update_traces(marker_color=SERIES_COLORS[1])
            apply_theme(fig, height=300)
            st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CONFIG)

    st.divider()

    # --- News Feed ---
    st.subheader("News Feed")
    with st.container(height=700):
        news_feed(all_articles, max_items=30)
