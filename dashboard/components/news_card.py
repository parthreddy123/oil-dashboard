"""Professional news card with impact badges and relative timestamps."""

import streamlit as st
from datetime import datetime, timezone
from dashboard.components.theme import TEXT_PRIMARY, TEXT_MUTED, TEXT_DIM, BG_CARD, BG_ELEVATED, BORDER_SUBTLE, EMERALD, CORAL

IMPACT_CONFIG = {
    "bullish": {"color": EMERALD, "icon": "&#9650;", "label": "BULLISH"},
    "bearish": {"color": CORAL, "icon": "&#9660;", "label": "BEARISH"},
    "neutral": {"color": "#6B7280", "icon": "&#9644;", "label": "NEUTRAL"},
}


def _relative_time(dt_str):
    if not dt_str:
        return ""
    try:
        dt = datetime.fromisoformat(str(dt_str).replace("Z", "+00:00")) if isinstance(dt_str, str) else dt_str
        now = datetime.now(timezone.utc)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        hours = (now - dt).total_seconds() / 3600
        if hours < 1:
            return f"{int((now - dt).total_seconds() / 60)}m ago"
        if hours < 24:
            return f"{int(hours)}h ago"
        if hours < 48:
            return "Yesterday"
        if hours < 168:
            return f"{int(hours / 24)}d ago"
        return dt.strftime("%b %d")
    except Exception:
        return str(dt_str)[:10]


def news_card(title, summary, source, published_date, impact_tag, impact_score, url=None):
    cfg = IMPACT_CONFIG.get(impact_tag, IMPACT_CONFIG["neutral"])
    c = cfg["color"]
    score_display = f"{impact_score:+.2f}" if impact_score else "0.00"
    time_str = _relative_time(published_date)
    title_html = f'<a href="{url}" target="_blank" style="color:{TEXT_PRIMARY};text-decoration:none;">{title}</a>' if url else title
    summary_text = (summary[:180] + "...") if summary and len(summary) > 180 else (summary or "")

    st.markdown(f"""
    <div style="background:linear-gradient(135deg,{BG_CARD},{BG_ELEVATED});
        border:1px solid rgba(255,255,255,0.05);border-left:3px solid {c};
        border-radius:8px;padding:12px 14px;margin-bottom:8px;display:flex;
        justify-content:space-between;align-items:flex-start;gap:12px;">
        <div style="flex:1;min-width:0;">
            <div style="font-size:0.85rem;font-weight:600;color:{TEXT_PRIMARY};line-height:1.35;margin-bottom:4px;">
                {title_html}</div>
            {"<div style='font-size:0.73rem;color:" + TEXT_MUTED + ";line-height:1.4;margin-bottom:5px;'>" + summary_text + "</div>" if summary_text else ""}
            <div style="font-size:0.66rem;color:{TEXT_DIM};">
                <span style="font-weight:600;color:{TEXT_MUTED};">{source}</span>
                {"<span> &middot; " + time_str + "</span>" if time_str else ""}
            </div>
        </div>
        <div style="flex-shrink:0;text-align:center;min-width:50px;">
            <div style="background:rgba({int(c[1:3],16)},{int(c[3:5],16)},{int(c[5:7],16)},0.1);
                border:1px solid rgba({int(c[1:3],16)},{int(c[3:5],16)},{int(c[5:7],16)},0.25);
                border-radius:6px;padding:5px 7px;">
                <div style="color:{c};font-size:0.9rem;line-height:1;">{cfg['icon']}</div>
                <div style="color:{c};font-size:0.58rem;font-weight:700;letter-spacing:0.04em;margin-top:2px;">
                    {cfg['label']}</div>
            </div>
            <div style="font-size:0.68rem;color:{c};font-weight:600;margin-top:3px;
                font-variant-numeric:tabular-nums;">{score_display}</div>
        </div>
    </div>""", unsafe_allow_html=True)


def news_feed(articles, max_items=20):
    if not articles:
        st.markdown(f"""
        <div style="text-align:center;padding:3rem 1rem;color:{TEXT_DIM};
            background:{BG_ELEVATED};border-radius:12px;border:1px solid rgba(255,255,255,0.04);">
            <div style="font-size:1.5rem;margin-bottom:0.5rem;">&#128240;</div>
            <div style="font-size:0.85rem;font-weight:600;color:{TEXT_MUTED};">No articles available</div>
            <div style="font-size:0.75rem;margin-top:4px;">Run a data refresh to fetch the latest news</div>
        </div>""", unsafe_allow_html=True)
        return

    counts = {"bullish": 0, "bearish": 0, "neutral": 0}
    for a in articles[:max_items]:
        counts[a.get("impact_tag", "neutral")] = counts.get(a.get("impact_tag", "neutral"), 0) + 1

    st.markdown(f"""
    <div style="display:flex;gap:12px;margin-bottom:0.8rem;font-size:0.72rem;font-weight:600;">
        <span style="color:{TEXT_MUTED};">Showing {min(max_items, len(articles))} of {len(articles)}</span>
        <span style="color:{EMERALD};">&#9650; {counts.get('bullish',0)}</span>
        <span style="color:{CORAL};">&#9660; {counts.get('bearish',0)}</span>
        <span style="color:#6B7280;">&#9644; {counts.get('neutral',0)}</span>
    </div>""", unsafe_allow_html=True)

    for article in articles[:max_items]:
        news_card(
            title=article["title"], summary=article.get("summary", ""),
            source=article.get("source", "Unknown"), published_date=article.get("published_date"),
            impact_tag=article.get("impact_tag", "neutral"),
            impact_score=article.get("impact_score", 0), url=article.get("url"),
        )
