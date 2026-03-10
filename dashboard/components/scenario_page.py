"""Scenario Engine page — strategic geopolitical scenario analysis with LLM depth."""

import streamlit as st
import os, sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from dashboard.components.theme import (
    BG_CARD, BG_ELEVATED, BORDER_SUBTLE, TEXT_PRIMARY, TEXT_SECONDARY,
    TEXT_MUTED, TEXT_DIM, TEAL, CYAN, EMERALD, CORAL, AMBER, PURPLE, GOLD,
)
from processing.scenario_analyzer import (
    SCENARIOS, SCENARIO_IDS, HORIZONS, DEFAULT_HORIZON,
    compute_weights, compute_momentum, _compute_ev, _compute_ranges,
)
from database.db_manager import (
    get_latest_scenario_narrative, get_recent_articles_with_signals,
)

# Scenario accent colors
SCENARIO_COLORS = {
    "quick_resolution": EMERALD,
    "prolonged_standoff": AMBER,
    "conflagration": CORAL,
    "ceasefire": CYAN,
    "regime_change": PURPLE,
}


def _relative_time(dt_str):
    if not dt_str:
        return ""
    try:
        dt = datetime.fromisoformat(str(dt_str).replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        hours = (now - dt).total_seconds() / 3600
        if hours < 1:
            return f"{max(1, int((now - dt).total_seconds() / 60))}m ago"
        if hours < 24:
            return f"{int(hours)}h ago"
        return dt.strftime("%b %d %H:%M")
    except Exception:
        return str(dt_str)[:16]


def _format_time_short(dt_str):
    """Format as HH:MM for the breaking feed."""
    if not dt_str:
        return ""
    try:
        dt = datetime.fromisoformat(str(dt_str).replace("Z", "+00:00"))
        return dt.strftime("%H:%M")
    except Exception:
        return str(dt_str)[:5]


def _render_narrative_box(narrative_data):
    """Render the strategic assessment green box."""
    if not narrative_data or not narrative_data.get("narrative"):
        return
    st.markdown(f"""
    <div class="narrative-box">
        <div style="font-size:0.68rem;font-weight:700;color:{EMERALD};text-transform:uppercase;
            letter-spacing:0.1em;margin-bottom:8px;">Strategic Assessment</div>
        <div>{narrative_data['narrative']}</div>
    </div>""", unsafe_allow_html=True)


def _render_kpi_with_explanation(label, value_str, range_str, explanation, accent_color=TEAL, subtitle=None):
    """Render a KPI box with explanation text below."""
    sub_html = f'<div style="font-size:0.72rem;color:{accent_color};margin-top:4px;">{subtitle}</div>' if subtitle else ''
    st.markdown(f"""
    <div class="kpi-box" style="position:relative;background:linear-gradient(135deg,{BG_CARD},{BG_ELEVATED});
        border:1px solid {BORDER_SUBTLE};border-left:3px solid {accent_color};
        border-radius:10px;padding:14px 16px 12px;margin-bottom:6px;">
        <div class="kpi-title" style="color:{TEXT_SECONDARY};font-size:0.68rem;font-weight:600;
            text-transform:uppercase;letter-spacing:0.08em;margin-bottom:6px;">{label}</div>
        <div class="kpi-value" style="color:{TEXT_PRIMARY};font-size:1.5rem;font-weight:700;
            letter-spacing:-0.02em;line-height:1.1;font-variant-numeric:tabular-nums;">{value_str}</div>
        <div style="font-size:0.68rem;color:{TEXT_DIM};margin-top:4px;">range: {range_str}</div>
        {sub_html}
        {f'<div class="kpi-explanation">{explanation}</div>' if explanation else ''}
    </div>""", unsafe_allow_html=True)


def _render_momentum_row(momentum):
    """Render the 5-box momentum row."""
    if not momentum:
        return

    boxes_html = ""
    for sid in SCENARIO_IDS:
        m = momentum.get(sid, {"direction": "stable", "delta": 0.0})
        color = SCENARIO_COLORS.get(sid, TEAL)
        arrow_map = {"rising": "&#9650;", "falling": "&#9660;", "stable": "&#8212;"}
        delta_color = EMERALD if m["direction"] == "rising" else (CORAL if m["direction"] == "falling" else TEXT_DIM)
        delta_str = f"{m['delta']:+.2f}" if m["delta"] != 0 else "0.00"

        boxes_html += f"""
        <div class="momentum-box" style="border-top:2px solid {color};">
            <div class="scenario-name">{SCENARIOS[sid]['short']}</div>
            <div class="delta" style="color:{delta_color};">
                {arrow_map[m['direction']]} {delta_str}
            </div>
        </div>"""

    st.markdown(f'<div class="momentum-row">{boxes_html}</div>', unsafe_allow_html=True)


def _render_breaking_feed(articles):
    """Render the recent articles chronological feed."""
    if not articles:
        st.markdown(f"""
        <div style="text-align:center;padding:1.5rem;color:{TEXT_DIM};font-size:0.82rem;">
            No recent articles in the last 12 hours
        </div>""", unsafe_allow_html=True)
        return

    for art in articles:
        time_str = _format_time_short(art.get("published_date") or art.get("created_at"))
        title = art.get("title", "")
        url = art.get("url", "")
        source = art.get("source", "")
        signals = art.get("signals", [])

        # Strongest signal for this article
        tag_html = ""
        if signals:
            top_sig = max(signals, key=lambda s: abs(s.get("signal", 0)))
            sig_val = top_sig.get("signal", 0)
            sig_sid = top_sig.get("scenario_id", "")
            sig_name = SCENARIOS.get(sig_sid, {}).get("short", sig_sid[:8])
            sig_color = EMERALD if sig_val > 0 else (CORAL if sig_val < 0 else TEXT_DIM)
            tag_html = (
                f'<span class="signal-tag" style="color:{sig_color};'
                f'background:rgba({int(sig_color[1:3],16)},{int(sig_color[3:5],16)},{int(sig_color[5:7],16)},0.1);'
                f'border:1px solid rgba({int(sig_color[1:3],16)},{int(sig_color[3:5],16)},{int(sig_color[5:7],16)},0.25);">'
                f'{sig_val:+.2f} {sig_name}</span>'
            )

        title_html = f'<a href="{url}" target="_blank">{title}</a>' if url else title

        st.markdown(f"""
        <div class="breaking-article">
            <span class="timestamp">{time_str}</span>
            {tag_html}
            <span class="title">{title_html}</span>
        </div>""", unsafe_allow_html=True)


def _render_scenario_card(sid, scenario, weight, kpis, articles, assessment=None):
    """Render a single scenario card with articles, reasoning, and assessment."""
    s = scenario
    color = SCENARIO_COLORS.get(sid, TEAL)
    pct = weight * 100
    bar_width = max(5, min(100, pct * 2.5))

    # Left column: articles with clickable links + reasoning
    articles_html = ""
    relevant = [a for a in articles if any(
        sig.get("scenario_id") == sid for sig in a.get("signals", [])
    )]
    # Sort by signal strength for this scenario
    def _signal_for(art):
        for sig in art.get("signals", []):
            if sig.get("scenario_id") == sid:
                return abs(sig.get("signal", 0))
        return 0
    relevant.sort(key=_signal_for, reverse=True)

    for art in relevant[:5]:
        title = art.get("title", "")
        url = art.get("url", "")
        title_html = f'<a href="{url}" target="_blank" style="color:{TEXT_PRIMARY};text-decoration:none;">{title}</a>' if url else title

        sig_html = ""
        reasoning_html = ""
        for sig in art.get("signals", []):
            if sig.get("scenario_id") == sid:
                sv = sig.get("signal", 0)
                sc = EMERALD if sv > 0 else (CORAL if sv < 0 else TEXT_DIM)
                sig_html = f'<span style="color:{sc};font-weight:700;font-size:0.78rem;margin-right:6px;">[{sv:+.2f}]</span>'
                if sig.get("reasoning"):
                    reasoning_html = f'<div class="art-reasoning">{sig["reasoning"]}</div>'
                break

        articles_html += f"""
        <div style="margin-bottom:8px;">
            <div style="font-size:0.82rem;line-height:1.4;">{sig_html}{title_html}</div>
            {reasoning_html}
        </div>"""

    if not articles_html:
        articles_html = f'<div style="color:{TEXT_DIM};font-size:0.8rem;padding:8px 0;">No articles scored for this scenario</div>'

    # Right column: KPIs + assessment
    assessment_html = ""
    if assessment:
        assessment_html = f"""
        <div class="scenario-assessment">
            <div class="label">LLM Assessment</div>
            <div>{assessment}</div>
        </div>"""

    st.markdown(f"""
    <div style="background:linear-gradient(135deg,{BG_CARD},{BG_ELEVATED});border:1px solid {BORDER_SUBTLE};
        border-radius:12px;margin-bottom:12px;overflow:hidden;">
        <div style="padding:14px 18px 10px;display:flex;justify-content:space-between;align-items:center;
            border-bottom:1px solid {BORDER_SUBTLE};">
            <div>
                <span style="font-size:1rem;font-weight:700;color:{TEXT_PRIMARY};">{s['name']}</span>
                <span style="font-size:0.72rem;color:{TEXT_MUTED};margin-left:8px;">{s['description'][:80]}</span>
            </div>
            <div style="text-align:right;">
                <span style="font-size:1.3rem;font-weight:800;color:{color};font-variant-numeric:tabular-nums;">
                    {pct:.0f}%</span>
            </div>
        </div>
        <div style="height:4px;background:rgba(255,255,255,0.04);">
            <div style="height:100%;width:{bar_width}%;background:{color};border-radius:0 2px 2px 0;"></div>
        </div>
        <div style="display:flex;gap:0;">
            <div style="flex:1;padding:14px 18px;border-right:1px solid {BORDER_SUBTLE};min-width:0;">
                {articles_html}
            </div>
            <div style="width:280px;flex-shrink:0;padding:14px 18px;">
                <div style="display:flex;gap:12px;margin-bottom:8px;">
                    <div style="text-align:center;">
                        <div style="font-size:0.6rem;color:{TEXT_DIM};text-transform:uppercase;">Brent</div>
                        <div style="font-size:0.95rem;font-weight:700;color:{TEXT_PRIMARY};">${kpis['oil']}</div>
                    </div>
                    <div style="text-align:center;">
                        <div style="font-size:0.6rem;color:{TEXT_DIM};text-transform:uppercase;">GRM</div>
                        <div style="font-size:0.95rem;font-weight:700;color:{TEXT_PRIMARY};">${kpis['grm']}</div>
                    </div>
                    <div style="text-align:center;">
                        <div style="font-size:0.6rem;color:{TEXT_DIM};text-transform:uppercase;">Stock</div>
                        <div style="font-size:0.95rem;font-weight:700;color:{TEXT_PRIMARY};">{kpis['stock']:+.0f}%</div>
                    </div>
                </div>
                {assessment_html}
            </div>
        </div>
    </div>""", unsafe_allow_html=True)


def render():
    """Main render function for Scenario Engine page."""
    st.markdown(f"""
    <div style="display:flex;align-items:center;gap:12px;margin-bottom:0.5rem;">
        <span style="font-size:1.8rem;">&#127919;</span>
        <div>
            <h1 style="margin:0 !important;padding:0 !important;font-size:1.6rem !important;">Scenario Engine</h1>
            <div style="font-size:0.75rem;color:{TEXT_MUTED};">Geopolitical scenario analysis for Indian refinery strategy</div>
        </div>
    </div>""", unsafe_allow_html=True)

    # Horizon selector
    col_hz, col_run, _ = st.columns([2, 1, 5])
    with col_hz:
        horizon = st.selectbox("Horizon", HORIZONS, index=0, format_func=lambda h: {
            "1m": "1 Month", "3m": "3 Months", "6m": "6 Months"
        }.get(h, h))
    with col_run:
        st.markdown("<div style='height:28px'></div>", unsafe_allow_html=True)
        run_analysis = st.button("Run Analysis", type="primary", use_container_width=True)

    if run_analysis:
        with st.spinner("Scoring articles & generating narratives..."):
            try:
                from processing.scenario_analyzer import analyze_articles
                scored = analyze_articles(horizon)
                st.toast(f"Scored {scored} articles")
                weights = compute_weights(horizon, generate_narratives=True)
                st.cache_data.clear()
                st.success(f"Analysis complete. {scored} articles scored.")
                st.rerun()
            except Exception as e:
                st.error(f"Analysis failed: {e}")

    # Load data
    narrative_data = get_latest_scenario_narrative(horizon)
    weights = narrative_data.get("weight_snapshot", {}) if narrative_data else {}
    if not weights:
        weights = {sid: 1.0 / len(SCENARIOS) for sid in SCENARIOS}

    ev = _compute_ev(SCENARIOS, weights, horizon)
    ranges = _compute_ranges(SCENARIOS, horizon)

    # 1. Strategic Narrative Box
    _render_narrative_box(narrative_data)

    # 2. KPI boxes with explanations
    oil_expl = narrative_data.get("oil_explanation", "") if narrative_data else ""
    grm_expl = narrative_data.get("grm_explanation", "") if narrative_data else ""
    stock_expl = narrative_data.get("stock_explanation", "") if narrative_data else ""

    kpi_cols = st.columns(3)
    # Get current Brent price
    from database.db_manager import get_latest_price
    brent_row = get_latest_price("brent")
    current_brent = float(brent_row["price"]) if brent_row else None
    brent_sub = f" (now ${current_brent:.2f})" if current_brent else ""

    with kpi_cols[0]:
        _render_kpi_with_explanation(
            "Brent Price (EV)", f"${ev['oil']:.0f}/bbl",
            f"${ranges['oil'][0]}-{ranges['oil'][1]}", oil_expl, accent_color=AMBER,
            subtitle=f"Current: ${current_brent:.2f}/bbl" if current_brent else None)
    with kpi_cols[1]:
        _render_kpi_with_explanation(
            "GRM (EV)", f"${ev['grm']:.1f}/bbl",
            f"${ranges['grm'][0]}-{ranges['grm'][1]}", grm_expl, accent_color=TEAL)
    with kpi_cols[2]:
        _render_kpi_with_explanation(
            "Stock Impact (EV)", f"{ev['stock']:+.0f}%",
            f"{ranges['stock'][0]:+.0f}% to {ranges['stock'][1]:+.0f}%", stock_expl, accent_color=CYAN)

    # 3. Last 12 Hours section
    st.markdown(f"""
    <h2 style="margin-top:1.5rem !important;">Last 12 Hours</h2>""", unsafe_allow_html=True)

    momentum = compute_momentum(horizon)
    _render_momentum_row(momentum)

    recent_articles = get_recent_articles_with_signals(hours=36, limit=30)
    _render_breaking_feed(recent_articles)

    # 4. Scenario Cards
    st.markdown(f"""
    <h2 style="margin-top:1.5rem !important;">Scenario Analysis</h2>""", unsafe_allow_html=True)

    assessments = narrative_data.get("scenario_assessments", {}) if narrative_data else {}
    top_articles = []
    try:
        from database.db_manager import get_top_articles_across_scenarios
        top_articles = get_top_articles_across_scenarios(limit=30)
    except Exception:
        pass

    # Sort scenarios by weight descending
    sorted_scenarios = sorted(SCENARIOS.items(), key=lambda x: weights.get(x[0], 0), reverse=True)

    for sid, scenario in sorted_scenarios:
        w = weights.get(sid, 0.2)
        kpis = scenario["horizons"].get(horizon, scenario["horizons"]["3m"])
        assessment = assessments.get(sid, "")
        _render_scenario_card(sid, scenario, w, kpis, top_articles, assessment)

    # 5. Footer
    gen_time = narrative_data.get("generated_at", "") if narrative_data else ""
    article_count = narrative_data.get("article_count", 0) if narrative_data else 0
    model_used = narrative_data.get("model_used", "") if narrative_data else ""

    st.markdown(f"""
    <div style="margin-top:2rem;padding:12px 16px;background:{BG_ELEVATED};border-radius:8px;
        border:1px solid {BORDER_SUBTLE};font-size:0.7rem;color:{TEXT_DIM};
        display:flex;justify-content:space-between;flex-wrap:wrap;gap:8px;">
        <span>Generated: {gen_time[:16] if gen_time else 'Not yet'}</span>
        <span>Articles analyzed: {article_count}</span>
        <span>Model: {model_used or 'N/A'}</span>
        <span>Horizon: {horizon}</span>
    </div>""", unsafe_allow_html=True)
