"""Export Scenario Engine report as standalone HTML file."""

import os
import sys
import json
import logging
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database.db_manager import (
    get_latest_scenario_narrative, get_recent_articles_with_signals,
    get_top_articles_across_scenarios, init_db,
)
from processing.scenario_analyzer import (
    SCENARIOS, SCENARIO_IDS, HORIZONS, DEFAULT_HORIZON,
    compute_momentum, _compute_ev, _compute_ranges,
    compute_scenario_products, compute_ev_products,
    get_current_product_prices, get_indian_basket_price,
    PRODUCT_NAMES, GRM_WEIGHTS,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# CSS
# ---------------------------------------------------------------------------

EXPORT_CSS = """
* { margin: 0; padding: 0; box-sizing: border-box; }
body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    background: #0E1117; color: #E5E7EB; line-height: 1.6; padding: 2rem;
    max-width: 1100px; margin: 0 auto;
}
h1 { font-size: 1.6rem; font-weight: 800; color: #F9FAFB; margin-bottom: 0.3rem; }
h2 {
    color: #E5E7EB; font-weight: 700; font-size: 1.15rem;
    border-bottom: 2px solid rgba(0, 212, 170, 0.3); padding-bottom: 0.4rem;
    margin: 1.8rem 0 1rem;
}
.subtitle { font-size: 0.75rem; color: #6B7280; margin-bottom: 1.5rem; }
a { color: #00D4FF; text-decoration: none; }
a:hover { text-decoration: underline; }

.narrative-box {
    background: linear-gradient(135deg, #0D2818 0%, #1A1F2E 100%);
    border: 1px solid rgba(16, 185, 129, 0.3); border-left: 4px solid #10B981;
    border-radius: 12px; padding: 1.2rem 1.5rem; margin-bottom: 1.5rem;
    line-height: 1.7; font-size: 0.88rem;
}
.narrative-box strong { color: #F9FAFB; }
.narrative-label {
    font-size: 0.68rem; font-weight: 700; color: #10B981;
    text-transform: uppercase; letter-spacing: 0.1em; margin-bottom: 8px;
}

.kpi-row { display: flex; gap: 16px; margin-bottom: 1.5rem; }
.kpi-card {
    flex: 1; background: linear-gradient(135deg, #1A1F2E, #151A28);
    border: 1px solid rgba(255,255,255,0.06); border-radius: 10px;
    padding: 14px 16px 12px; position: relative;
}
.kpi-card .label {
    font-size: 0.68rem; font-weight: 600; color: #9CA3AF;
    text-transform: uppercase; letter-spacing: 0.08em; margin-bottom: 6px;
}
.kpi-card .value {
    font-size: 1.5rem; font-weight: 700; color: #F9FAFB;
    font-variant-numeric: tabular-nums;
}
.kpi-card .range { font-size: 0.68rem; color: #4B5563; margin-top: 4px; }
.kpi-explanation {
    font-size: 0.72rem; color: #9CA3AF; line-height: 1.5; margin-top: 8px;
    padding-top: 6px; border-top: 1px solid rgba(255,255,255,0.06); font-style: italic;
}

.momentum-row { display: flex; gap: 10px; flex-wrap: wrap; margin-bottom: 1rem; }
.momentum-box {
    background: linear-gradient(135deg, #1A1F2E, #151A28);
    border: 1px solid rgba(255,255,255,0.06); border-radius: 8px;
    padding: 8px 14px; min-width: 120px; flex: 1; text-align: center;
}
.momentum-box .name {
    font-size: 0.65rem; color: #9CA3AF; text-transform: uppercase;
    letter-spacing: 0.06em; font-weight: 600;
}
.momentum-box .delta { font-size: 1rem; font-weight: 700; margin-top: 2px; }

.breaking-article {
    display: flex; align-items: flex-start; gap: 10px;
    padding: 8px 12px; border-bottom: 1px solid rgba(255,255,255,0.04);
    font-size: 0.82rem;
}
.breaking-article:hover { background: rgba(255,255,255,0.02); }
.breaking-article .ts {
    font-size: 0.7rem; color: #6B7280; white-space: nowrap;
    font-variant-numeric: tabular-nums; min-width: 42px;
}
.signal-tag {
    font-size: 0.65rem; font-weight: 700; padding: 2px 6px;
    border-radius: 4px; white-space: nowrap; display: inline-block;
}

.scenario-card {
    background: linear-gradient(135deg, #1A1F2E, #151A28);
    border: 1px solid rgba(255,255,255,0.06); border-radius: 12px;
    margin-bottom: 14px; overflow: hidden;
}
.scenario-header {
    padding: 14px 18px 10px; display: flex; justify-content: space-between;
    align-items: center; border-bottom: 1px solid rgba(255,255,255,0.06);
}
.scenario-header .name { font-size: 1rem; font-weight: 700; color: #F9FAFB; }
.scenario-header .desc { font-size: 0.72rem; color: #6B7280; margin-left: 8px; }
.scenario-header .pct { font-size: 1.3rem; font-weight: 800; font-variant-numeric: tabular-nums; }
.scenario-bar { height: 4px; background: rgba(255,255,255,0.04); }
.scenario-body { display: flex; }
.scenario-articles {
    flex: 1; padding: 14px 18px; border-right: 1px solid rgba(255,255,255,0.06);
    min-width: 0;
}
.scenario-kpis { width: 280px; flex-shrink: 0; padding: 14px 18px; }
.scenario-kpis .row { display: flex; gap: 12px; margin-bottom: 8px; }
.scenario-kpis .item { text-align: center; }
.scenario-kpis .item .lbl { font-size: 0.6rem; color: #4B5563; text-transform: uppercase; }
.scenario-kpis .item .val { font-size: 0.95rem; font-weight: 700; color: #F9FAFB; }

.art-reasoning {
    font-size: 0.7rem; color: #6B7280; line-height: 1.4; margin-top: 3px;
    padding-left: 8px; border-left: 2px solid rgba(255,255,255,0.06); font-style: italic;
}
.scenario-assessment {
    background: rgba(0, 212, 170, 0.06); border: 1px solid rgba(0, 212, 170, 0.2);
    border-left: 3px solid #00D4AA; border-radius: 8px; padding: 10px 12px;
    margin-top: 10px; font-size: 0.78rem; color: #D1D5DB; line-height: 1.5;
}
.scenario-assessment .lbl {
    font-size: 0.65rem; font-weight: 700; color: #00D4AA;
    text-transform: uppercase; letter-spacing: 0.08em; margin-bottom: 4px;
}

.footer {
    margin-top: 2rem; padding: 12px 16px; background: #151A28;
    border-radius: 8px; border: 1px solid rgba(255,255,255,0.06);
    font-size: 0.7rem; color: #4B5563; display: flex;
    justify-content: space-between; flex-wrap: wrap; gap: 8px;
}

table tbody tr { border-bottom: 1px solid rgba(255,255,255,0.04); }
table td { padding: 5px 10px; }
.narrative-box ul { margin: 0; padding-left: 1.2em; }
.narrative-box li { margin-bottom: 4px; }

/* Tabs */
.tab-bar {
    display: flex; gap: 0; margin-bottom: 1.5rem; border-bottom: 2px solid rgba(255,255,255,0.08);
    overflow-x: auto; -webkit-overflow-scrolling: touch;
}
.tab-btn {
    background: none; border: none; color: #6B7280; padding: 10px 20px;
    font-size: 0.82rem; font-weight: 600; cursor: pointer; white-space: nowrap;
    border-bottom: 2px solid transparent; margin-bottom: -2px; transition: all 0.2s;
    font-family: inherit;
}
.tab-btn:hover { color: #D1D5DB; }
.tab-btn.active { color: #00D4AA; border-bottom-color: #00D4AA; }
.tab-panel { display: none; }
.tab-panel.active { display: block; }

/* Build story */
.phase-card {
    background: linear-gradient(135deg, #1A1F2E, #151A28);
    border: 1px solid rgba(255,255,255,0.06); border-radius: 12px;
    padding: 20px 24px; margin-bottom: 16px; position: relative;
    border-left: 3px solid #00D4AA;
}
.phase-card.user-phase { border-left-color: #F59E0B; }
.phase-num {
    font-size: 0.62rem; font-weight: 700; text-transform: uppercase;
    letter-spacing: 0.1em; color: #00D4AA; margin-bottom: 6px;
}
.phase-card.user-phase .phase-num { color: #F59E0B; }
.phase-title { font-size: 1rem; font-weight: 700; color: #F9FAFB; margin-bottom: 10px; }
.user-prompt {
    background: rgba(245, 158, 11, 0.08); border: 1px solid rgba(245, 158, 11, 0.2);
    border-radius: 8px; padding: 10px 14px; margin-bottom: 12px;
    font-size: 0.82rem; color: #FCD34D; font-style: italic;
}
.user-prompt::before { content: '\201C'; font-size: 1.2rem; color: #F59E0B; margin-right: 4px; }
.user-prompt::after { content: '\201D'; font-size: 1.2rem; color: #F59E0B; margin-left: 4px; }
.build-detail { font-size: 0.82rem; color: #D1D5DB; line-height: 1.6; }
.build-detail ul { padding-left: 1.2em; margin-top: 6px; }
.build-detail li { margin-bottom: 4px; }
.tech-tag {
    display: inline-block; font-size: 0.65rem; font-weight: 600; padding: 2px 8px;
    border-radius: 4px; background: rgba(0, 212, 170, 0.1); color: #00D4AA;
    border: 1px solid rgba(0, 212, 170, 0.2); margin: 2px 2px 2px 0;
}

/* Architecture */
.arch-row { display: flex; gap: 16px; margin-bottom: 20px; align-items: stretch; }
.arch-col { flex: 1; min-width: 0; }
.arch-box {
    background: linear-gradient(135deg, #1A1F2E, #151A28);
    border: 1px solid rgba(255,255,255,0.06); border-radius: 10px;
    padding: 16px; height: 100%;
}
.arch-box .arch-label {
    font-size: 0.62rem; font-weight: 700; text-transform: uppercase;
    letter-spacing: 0.1em; margin-bottom: 10px;
}
.arch-item {
    font-size: 0.78rem; color: #D1D5DB; padding: 6px 0;
    border-bottom: 1px solid rgba(255,255,255,0.04);
}
.arch-item:last-child { border-bottom: none; }
.arch-item .src { color: #F9FAFB; font-weight: 600; }
.arch-item .note { color: #6B7280; font-size: 0.7rem; }
.arch-flow {
    display: flex; align-items: center; gap: 12px; margin: 24px 0;
    flex-wrap: wrap; justify-content: center;
}
.arch-step {
    background: linear-gradient(135deg, #1A1F2E, #151A28);
    border: 1px solid rgba(0, 212, 170, 0.2); border-radius: 10px;
    padding: 12px 16px; text-align: center; min-width: 120px;
}
.arch-step .step-name { font-size: 0.75rem; font-weight: 700; color: #F9FAFB; }
.arch-step .step-detail { font-size: 0.65rem; color: #6B7280; margin-top: 2px; }
.arch-connector { color: #00D4AA; font-size: 1.2rem; font-weight: 700; }

@media (max-width: 768px) {
    body { padding: 0.7rem; font-size: 0.82rem; }
    h1 { font-size: 1.1rem; }
    h2 { font-size: 1rem; margin: 1.2rem 0 0.6rem; }
    .kpi-row { flex-direction: column; gap: 8px; }
    .kpi-card { padding: 10px 12px; }
    .kpi-card .value { font-size: 1.2rem; }
    .kpi-card .label { font-size: 0.62rem; }
    .scenario-body { flex-direction: column; }
    .scenario-kpis { width: 100%; border-right: none; border-top: 1px solid rgba(255,255,255,0.06); padding: 10px 14px; }
    .scenario-articles { padding: 10px 14px; }
    .scenario-header { padding: 10px 14px 8px; flex-wrap: wrap; gap: 4px; }
    .scenario-header .desc { display: none; }
    .scenario-header .pct { font-size: 1.1rem; }
    .momentum-row { gap: 6px; }
    .momentum-box { min-width: 0; padding: 6px 8px; }
    .narrative-box { padding: 10px 12px; font-size: 0.78rem; line-height: 1.5; }
    .narrative-box li { margin-bottom: 2px; }
    .art-reasoning { display: none; }
    .breaking-article { font-size: 0.72rem; padding: 6px 8px; }
    .scenario-assessment { font-size: 0.72rem; padding: 8px 10px; }
    .footer { font-size: 0.62rem; flex-direction: column; gap: 4px; }
    table { font-size: 0.7rem !important; }
    table th, table td { padding: 4px 6px !important; }
    .tab-btn { padding: 8px 14px; font-size: 0.75rem; }
    .phase-card { padding: 14px 16px; }
    .arch-row { flex-direction: column; }
    .arch-flow { flex-direction: column; }
    .arch-connector { transform: rotate(90deg); display: block; text-align: center; }
}
"""

SCENARIO_COLORS = {
    "quick_resolution": "#10B981",
    "prolonged_standoff": "#F59E0B",
    "conflagration": "#EF4444",
    "ceasefire": "#00D4FF",
    "regime_change": "#8B5CF6",
}


def _format_time_short(dt_str):
    if not dt_str:
        return ""
    try:
        dt = datetime.fromisoformat(str(dt_str).replace("Z", "+00:00"))
        return dt.strftime("%H:%M")
    except Exception:
        return str(dt_str)[:5]


def _color_rgba(hex_color, alpha):
    r, g, b = int(hex_color[1:3], 16), int(hex_color[3:5], 16), int(hex_color[5:7], 16)
    return f"rgba({r},{g},{b},{alpha})"


def render_horizon(horizon, narrative_data, momentum, recent_articles, top_articles,
                   current_basket=None, basket_detail=None, compact=False):
    """Render a single horizon section."""
    weights = narrative_data.get("weight_snapshot", {}) if narrative_data else {}
    if not weights:
        weights = {sid: 1.0 / len(SCENARIOS) for sid in SCENARIOS}

    ev = _compute_ev(SCENARIOS, weights, horizon)
    ranges = _compute_ranges(SCENARIOS, horizon)
    assessments = narrative_data.get("scenario_assessments", {}) if narrative_data else {}

    parts = []

    # Strategic narrative
    if narrative_data and narrative_data.get("narrative"):
        parts.append(f"""
        <div class="narrative-box">
            <div class="narrative-label">Strategic Assessment</div>
            <div>{narrative_data['narrative']}</div>
        </div>""")

    # KPIs
    oil_expl = narrative_data.get("oil_explanation", "") if narrative_data else ""
    grm_expl = narrative_data.get("grm_explanation", "") if narrative_data else ""

    brent_html = ''
    basket_ref = ''
    if current_basket and basket_detail:
        dubai, brent = basket_detail
        if brent:
            brent_html = f'<div style="font-size:0.72rem;color:#F59E0B;margin-top:4px;">Current Brent: ${brent:.2f}/bbl</div>'
        detail = ''
        if dubai:
            detail = f' (Dubai ${dubai:.2f} &times; 72% + Brent ${brent:.2f} &times; 28%)'
        basket_ref = f'<div style="font-size:0.72rem;color:#00D4AA;margin-top:4px;">Indian Basket: ${current_basket:.2f}/bbl{detail}</div>'

    parts.append(f"""
    <div class="kpi-row">
        <div class="kpi-card" style="border-left:3px solid #F59E0B;">
            <div class="label">Brent Price (Expected Value)</div>
            <div class="value">${ev['oil']:.0f}/bbl</div>
            <div class="range">range: ${ranges['oil'][0]}-{ranges['oil'][1]}</div>
            {brent_html}
            {f'<div class="kpi-explanation">{oil_expl}</div>' if oil_expl else ''}
        </div>
        <div class="kpi-card" style="border-left:3px solid #00D4AA;">
            <div class="label">Typical Indian Refinery GRM (EV)</div>
            <div class="value">${ev['grm']:.1f}/bbl</div>
            <div class="range">range: ${ranges['grm'][0]}-{ranges['grm'][1]}</div>
            {basket_ref}
            {f'<div class="kpi-explanation">{grm_expl}</div>' if grm_expl else ''}
        </div>
    </div>""")

    # Product price table
    current_products = get_current_product_prices()
    ev_products = compute_ev_products(weights, horizon)
    prod_order = ["diesel", "petrol", "atf", "naphtha", "fuel_oil", "lpg"]
    prod_rows = ""
    for p in prod_order:
        cur = current_products.get(p)
        ev_p = ev_products.get(p)
        cur_str = f"${cur:.0f}" if cur else "—"
        ev_str = f"${ev_p:.0f}" if ev_p else "—"
        chg = ""
        if cur and ev_p:
            delta = ((ev_p - cur) / cur) * 100
            chg_color = "#EF4444" if delta > 0 else "#10B981" if delta < 0 else "#4B5563"
            chg = f'<span style="color:{chg_color};font-weight:600;">{delta:+.0f}%</span>'
        wt = GRM_WEIGHTS.get(p, 0)
        prod_rows += f'<tr><td>{PRODUCT_NAMES.get(p, p)}</td><td>{cur_str}</td><td>{ev_str}</td><td>{chg}</td><td style="color:#4B5563;">{wt:.0%}</td></tr>'

    parts.append(f"""
    <div style="overflow-x:auto;margin-bottom:1.5rem;">
        <table style="width:100%;border-collapse:collapse;font-size:0.78rem;">
            <thead><tr style="border-bottom:2px solid rgba(255,255,255,0.1);text-align:left;">
                <th style="padding:6px 10px;color:#9CA3AF;font-weight:600;font-size:0.68rem;text-transform:uppercase;">Product</th>
                <th style="padding:6px 10px;color:#9CA3AF;font-weight:600;font-size:0.68rem;text-transform:uppercase;">Current (SG)</th>
                <th style="padding:6px 10px;color:#9CA3AF;font-weight:600;font-size:0.68rem;text-transform:uppercase;">EV ({horizon})</th>
                <th style="padding:6px 10px;color:#9CA3AF;font-weight:600;font-size:0.68rem;text-transform:uppercase;">Chg</th>
                <th style="padding:6px 10px;color:#9CA3AF;font-weight:600;font-size:0.68rem;text-transform:uppercase;">GRM Wt</th>
            </tr></thead>
            <tbody style="color:#E5E7EB;">{prod_rows}</tbody>
        </table>
    </div>""")

    if compact:
        return "\n".join(parts)

    # Momentum row
    if momentum:
        boxes = ""
        for sid in SCENARIO_IDS:
            m = momentum.get(sid, {"direction": "stable", "delta": 0.0})
            color = SCENARIO_COLORS.get(sid, "#00D4AA")
            arrows = {"rising": "&#9650;", "falling": "&#9660;", "stable": "&#8212;"}
            delta_color = "#10B981" if m["direction"] == "rising" else ("#EF4444" if m["direction"] == "falling" else "#4B5563")
            boxes += f"""
            <div class="momentum-box" style="border-top:2px solid {color};">
                <div class="name">{SCENARIOS[sid]['short']}</div>
                <div class="delta" style="color:{delta_color};">{arrows[m['direction']]} {m['delta']:+.2f}</div>
            </div>"""
        parts.append(f"""<h2>Last 12 Hours</h2><div class="momentum-row">{boxes}</div>""")

    # Breaking feed
    if recent_articles:
        feed_html = ""
        for art in recent_articles:
            ts = _format_time_short(art.get("published_date") or art.get("created_at"))
            title = art.get("title", "")
            url = art.get("url", "")
            signals = art.get("signals", [])
            tag_html = ""
            if signals:
                top_sig = max(signals, key=lambda s: abs(s.get("signal", 0)))
                sv = top_sig.get("signal", 0)
                sn = SCENARIOS.get(top_sig.get("scenario_id", ""), {}).get("short", "")
                sc = "#10B981" if sv > 0 else ("#EF4444" if sv < 0 else "#4B5563")
                tag_html = f'<span class="signal-tag" style="color:{sc};background:{_color_rgba(sc, 0.1)};border:1px solid {_color_rgba(sc, 0.25)};">{sv:+.2f} {sn}</span>'
            title_html = f'<a href="{url}" target="_blank">{title}</a>' if url else title
            feed_html += f'<div class="breaking-article"><span class="ts">{ts}</span>{tag_html}<span>{title_html}</span></div>'
        parts.append(feed_html)

    # Scenario cards
    parts.append("<h2>Scenario Analysis</h2>")
    sorted_scenarios = sorted(SCENARIOS.items(), key=lambda x: weights.get(x[0], 0), reverse=True)

    for sid, scenario in sorted_scenarios:
        w = weights.get(sid, 0.2)
        color = SCENARIO_COLORS.get(sid, "#00D4AA")
        kpis = scenario["horizons"].get(horizon, scenario["horizons"]["3m"])
        pct = w * 100
        bar_w = max(5, min(100, pct * 2.5))

        articles_html = ""
        relevant = [a for a in top_articles if any(
            sig.get("scenario_id") == sid for sig in a.get("signals", [])
        )]
        relevant.sort(
            key=lambda a: max(
                (abs(s.get("signal", 0)) for s in a.get("signals", []) if s.get("scenario_id") == sid),
                default=0,
            ),
            reverse=True,
        )

        for art in relevant[:5]:
            title = art.get("title", "")
            url = art.get("url", "")
            title_link = f'<a href="{url}" target="_blank" style="color:#F9FAFB;text-decoration:none;">{title}</a>' if url else title
            sig_span = ""
            reasoning_div = ""
            for sig in art.get("signals", []):
                if sig.get("scenario_id") == sid:
                    sv = sig.get("signal", 0)
                    sc = "#10B981" if sv > 0 else ("#EF4444" if sv < 0 else "#4B5563")
                    sig_span = f'<span style="color:{sc};font-weight:700;font-size:0.78rem;margin-right:6px;">[{sv:+.2f}]</span>'
                    if sig.get("reasoning"):
                        reasoning_div = f'<div class="art-reasoning">{sig["reasoning"]}</div>'
                    break
            articles_html += f'<div style="margin-bottom:8px;"><div style="font-size:0.82rem;line-height:1.4;">{sig_span}{title_link}</div>{reasoning_div}</div>'

        if not articles_html:
            articles_html = '<div style="color:#4B5563;font-size:0.8rem;padding:8px 0;">No articles scored for this scenario</div>'

        assessment_html = ""
        assessment = assessments.get(sid, "")
        if assessment:
            assessment_html = f'<div class="scenario-assessment"><div class="lbl">LLM Assessment</div><div>{assessment}</div></div>'

        sc_products = compute_scenario_products(sid, horizon)
        prod_mini = " | ".join(
            f'<span style="white-space:nowrap;">{PRODUCT_NAMES.get(p,p)[:3]} ${sc_products[p]:.0f}</span>'
            for p in ["diesel", "petrol", "atf"]
        )

        parts.append(f"""
        <div class="scenario-card">
            <div class="scenario-header">
                <div>
                    <span class="name">{scenario['name']}</span>
                    <span class="desc">{scenario['description']}</span>
                </div>
                <span class="pct" style="color:{color};">{pct:.0f}%</span>
            </div>
            <div class="scenario-bar"><div style="height:100%;width:{bar_w}%;background:{color};border-radius:0 2px 2px 0;"></div></div>
            <div class="scenario-body">
                <div class="scenario-articles">{articles_html}</div>
                <div class="scenario-kpis">
                    <div class="row">
                        <div class="item"><div class="lbl">Brent</div><div class="val">${kpis['oil']}</div></div>
                        <div class="item"><div class="lbl">GRM</div><div class="val">${kpis['grm']}</div></div>
                    </div>
                    <div style="font-size:0.65rem;color:#6B7280;margin-top:6px;line-height:1.6;">{prod_mini}</div>
                    {assessment_html}
                </div>
            </div>
        </div>""")

    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Static tab content
# ---------------------------------------------------------------------------

BUILD_STORY = """
<div style="max-width:800px;">
<p style="font-size:0.88rem;color:#D1D5DB;margin-bottom:1.5rem;line-height:1.6;">
    Built in a single sprint through iterative collaboration between a strategist and Claude Code (AI pair-programmer).
    Each phase shows the human direction and what shipped. From first prompt to live deployment &mdash; one working session.
</p>

<div class="phase-card user-phase">
    <div class="phase-num">Phase 1 &mdash; The Brief</div>
    <div class="phase-title">Scenario Engine for Oil Geopolitics</div>
    <div class="user-prompt">Build a scenario engine that analyzes Middle East crisis scenarios and estimates impact on crude oil prices and Indian refinery margins. Score real news articles against scenarios using AI.</div>
    <div class="build-detail">
        <ul>
            <li>Designed 5 geopolitical scenarios: Ceasefire, Quick Resolution, Regime Change, Prolonged Standoff, Conflagration</li>
            <li>Built LLM pipeline scoring each news article against all 5 scenarios with signal strength [-1, +1] and reasoning</li>
            <li>Implemented softmax probability computation from average signal strengths</li>
            <li>Created Expected Value (EV) framework: probability-weighted Brent price and GRM across scenarios</li>
            <li>Added strategic narrative generation &mdash; LLM synthesizes top signals into a concise briefing</li>
        </ul>
        <div style="margin-top:8px;">
            <span class="tech-tag">Claude Haiku 4.5</span>
            <span class="tech-tag">SQLite</span>
            <span class="tech-tag">Python</span>
            <span class="tech-tag">Softmax</span>
        </div>
    </div>
</div>

<div class="phase-card user-phase">
    <div class="phase-num">Phase 2 &mdash; Public-Ready</div>
    <div class="phase-title">Security, Transparency, and Access</div>
    <div class="user-prompt">Remove stock impact &mdash; this will go public. Add methodology transparency. The refresh button shouldn't expose any tokens or open GitHub.</div>
    <div class="build-detail">
        <ul>
            <li>Removed all stock references (speculative for public consumption)</li>
            <li>Deployed a <strong>Cloudflare Worker</strong> as secure API proxy &mdash; public users trigger refresh without seeing GitHub tokens</li>
            <li>Added methodology callouts: what the tool does, how GRM is calculated, how prices are estimated</li>
            <li>Auto-reload: page polls for updates after refresh, reloads when new data deploys</li>
        </ul>
        <div style="margin-top:8px;">
            <span class="tech-tag">Cloudflare Workers</span>
            <span class="tech-tag">GitHub Actions</span>
            <span class="tech-tag">GitHub Pages</span>
        </div>
    </div>
</div>

<div class="phase-card user-phase">
    <div class="phase-num">Phase 3 &mdash; Price Grounding</div>
    <div class="phase-title">From Ratios to Analyst Consensus</div>
    <div class="user-prompt">The Brent ratios don&rsquo;t work &mdash; no way we&rsquo;ll have $160. Why can&rsquo;t we find internet consensus per scenario and hard-code per scenario? Also the product prices?</div>
    <div class="build-detail">
        <ul>
            <li>Initial approach used multiplicative ratios &mdash; produced unrealistic extremes ($160+ Brent)</li>
            <li>Pivoted to additive premiums &mdash; still too crude for a strategy tool</li>
            <li><strong>Researched analyst consensus</strong> from Goldman Sachs, JPMorgan, Morgan Stanley, Citi, Wood Mackenzie, Rystad Energy, Deutsche Bank, ING, CSIS, and Oxford Economics (March 2026)</li>
            <li>Hardcoded scenario-conditional prices grounded in historical parallels (post-JCPOA, Abqaiq, Arab Spring, Gulf War)</li>
            <li>Added per-scenario product prices from observed crisis crack spread data</li>
        </ul>
        <div style="margin-top:8px;">
            <span class="tech-tag">Analyst Consensus</span>
            <span class="tech-tag">Historical Parallels</span>
            <span class="tech-tag">Crisis Crack Spreads</span>
        </div>
    </div>
</div>

<div class="phase-card user-phase">
    <div class="phase-num">Phase 4 &mdash; Indian Refinery Context</div>
    <div class="phase-title">Indian Basket + Singapore Benchmarks</div>
    <div class="user-prompt">Are you calculating GRM based on India prices or global prices? Switch to Indian basket and offset by Singapore product price differential.</div>
    <div class="build-detail">
        <ul>
            <li>Switched crude reference from Brent to <strong>Indian basket</strong> (72% Dubai/Oman + 28% Brent) &mdash; the actual import benchmark for Indian refiners</li>
            <li>Applied <strong>Singapore product differentials</strong> vs US Gulf Coast: diesel &minus;$3, petrol &minus;$2.5, ATF &minus;$2, fuel oil +$10</li>
            <li>GRM now reflects what an actual Indian refinery would realize, not a US Gulf Coast proxy</li>
        </ul>
        <div style="margin-top:8px;">
            <span class="tech-tag">Indian Crude Basket</span>
            <span class="tech-tag">Platts Singapore</span>
            <span class="tech-tag">Asian Refining</span>
        </div>
    </div>
</div>

<div class="phase-card user-phase">
    <div class="phase-num">Phase 5 &mdash; Polish</div>
    <div class="phase-title">MBB-Quality Output, Mobile-First</div>
    <div class="user-prompt">The narrative is still too long. Sharpen it. Final touches &mdash; the page should look great on mobile.</div>
    <div class="build-detail">
        <ul>
            <li>Rewrote LLM prompts to enforce <strong>McKinsey partner voice</strong>: 3 bullets max, 15 words each, no filler</li>
            <li>Collapsed methodology into expandable details &mdash; clean on mobile, full info on tap</li>
            <li>Responsive CSS: stacked KPIs, collapsible scenario cards, hidden verbose elements on small screens</li>
            <li>One-click deploy via GitHub Pages &mdash; static HTML, no server required</li>
        </ul>
        <div style="margin-top:8px;">
            <span class="tech-tag">Responsive CSS</span>
            <span class="tech-tag">MBB Voice</span>
            <span class="tech-tag">Static HTML</span>
        </div>
    </div>
</div>

<div style="background:linear-gradient(135deg,#0D2818,#1A1F2E);border:1px solid rgba(16,185,129,0.3);border-radius:10px;padding:16px 20px;margin-top:24px;font-size:0.82rem;color:#D1D5DB;line-height:1.6;">
    <strong style="color:#10B981;">Key takeaway:</strong> The human set the strategic direction at every turn &mdash; what to show, what to remove, which benchmarks matter, how prices should be grounded. The AI handled research, implementation, and deployment. The result: a production-quality scenario analysis tool built and deployed in a single session.
</div>
</div>"""

ARCHITECTURE = """
<div style="max-width:900px;">
<p style="font-size:0.88rem;color:#D1D5DB;margin-bottom:1.5rem;line-height:1.6;">
    End-to-end system architecture: from data ingestion to the live page you&rsquo;re reading now.
</p>

<h2 style="font-size:1.1rem;">Data Pipeline</h2>
<div class="arch-flow">
    <div class="arch-step" style="border-color:rgba(245,158,11,0.4);">
        <div class="step-name" style="color:#F59E0B;">Data Sources</div>
        <div class="step-detail">EIA, yfinance, OilPrice<br>RSS, NewsAPI</div>
    </div>
    <span class="arch-connector">&rarr;</span>
    <div class="arch-step" style="border-color:rgba(139,92,246,0.4);">
        <div class="step-name" style="color:#8B5CF6;">Scrapers</div>
        <div class="step-detail">9 parallel scrapers<br>ThreadPoolExecutor</div>
    </div>
    <span class="arch-connector">&rarr;</span>
    <div class="arch-step" style="border-color:rgba(0,212,255,0.4);">
        <div class="step-name" style="color:#00D4FF;">SQLite DB</div>
        <div class="step-detail">11 tables, WAL mode<br>Prices, articles, signals</div>
    </div>
    <span class="arch-connector">&rarr;</span>
    <div class="arch-step" style="border-color:rgba(0,212,170,0.4);">
        <div class="step-name" style="color:#00D4AA;">Scenario Engine</div>
        <div class="step-detail">LLM scoring + softmax<br>Narratives + assessments</div>
    </div>
    <span class="arch-connector">&rarr;</span>
    <div class="arch-step" style="border-color:rgba(16,185,129,0.4);">
        <div class="step-name" style="color:#10B981;">Static HTML</div>
        <div class="step-detail">GitHub Pages<br>Auto-deploy via Actions</div>
    </div>
</div>

<h2 style="font-size:1.1rem;">Inputs</h2>
<div class="arch-row">
    <div class="arch-col">
        <div class="arch-box" style="border-top:3px solid #F59E0B;">
            <div class="arch-label" style="color:#F59E0B;">Market Data</div>
            <div class="arch-item"><span class="src">EIA API</span> &mdash; Brent, WTI, product spot prices<br><span class="note">Series: RBRTE, RWTC, EER_EPMRU, EER_EPD2DXL0, EER_EPJK, EER_EPLLPA</span></div>
            <div class="arch-item"><span class="src">yfinance</span> &mdash; Live Brent (BZ=F ticker)</div>
            <div class="arch-item"><span class="src">OilPrice.com</span> &mdash; Dubai/Oman crude (Indian basket component)</div>
            <div class="arch-item"><span class="src">ExchangeRate API</span> &mdash; USD/INR</div>
        </div>
    </div>
    <div class="arch-col">
        <div class="arch-box" style="border-top:3px solid #00D4FF;">
            <div class="arch-label" style="color:#00D4FF;">News &amp; Intelligence</div>
            <div class="arch-item"><span class="src">RSS Feeds</span> &mdash; Reuters, BBC, Al Jazeera, OilPrice, Rigzone</div>
            <div class="arch-item"><span class="src">NewsAPI</span> &mdash; Keyword search (oil, Iran, Hormuz, OPEC)</div>
            <div class="arch-item"><span class="src">~150 articles</span> in database, continuously refreshed</div>
        </div>
    </div>
    <div class="arch-col">
        <div class="arch-box" style="border-top:3px solid #8B5CF6;">
            <div class="arch-label" style="color:#8B5CF6;">Reference Data</div>
            <div class="arch-item"><span class="src">Analyst Consensus</span> &mdash; GS, JPM, MS, Citi, WoodMac, Rystad per-scenario Brent estimates</div>
            <div class="arch-item"><span class="src">Crisis Crack Spreads</span> &mdash; Historical product prices in crisis scenarios</div>
            <div class="arch-item"><span class="src">Singapore Differentials</span> &mdash; Platts SG vs USGC offsets per product</div>
        </div>
    </div>
</div>

<h2 style="font-size:1.1rem;">Engine</h2>
<div class="arch-row">
    <div class="arch-col">
        <div class="arch-box" style="border-top:3px solid #00D4AA;">
            <div class="arch-label" style="color:#00D4AA;">LLM Scoring (Claude Haiku 4.5)</div>
            <div class="arch-item"><span class="src">Article &rarr; 5 signals</span><br>Each article scored [-1, +1] against each scenario with reasoning</div>
            <div class="arch-item"><span class="src">Softmax probabilities</span><br>avg_signal &rarr; exp(avg &times; temp) &rarr; normalize. Temperature=3</div>
            <div class="arch-item"><span class="src">Expected Value</span><br>EV = &Sigma;(prob<sub>i</sub> &times; scenario_price<sub>i</sub>) for oil, GRM, products</div>
        </div>
    </div>
    <div class="arch-col">
        <div class="arch-box" style="border-top:3px solid #10B981;">
            <div class="arch-label" style="color:#10B981;">GRM Computation</div>
            <div class="arch-item"><span class="src">Crude cost</span> = Indian basket (72% Dubai + 28% Brent)</div>
            <div class="arch-item"><span class="src">Product revenue</span> = &Sigma;(price<sub>i</sub> &times; weight<sub>i</sub>)<br><span class="note">Die 42%, Pet 22%, Nap 12%, ATF 10%, FO 8%, LPG 6%</span></div>
            <div class="arch-item"><span class="src">Singapore adj.</span><br><span class="note">Die &minus;$3, Pet &minus;$2.5, ATF &minus;$2, FO +$10, LPG +$5</span></div>
            <div class="arch-item"><span class="src">GRM</span> = Product revenue &minus; Crude cost</div>
        </div>
    </div>
</div>

<h2 style="font-size:1.1rem;">LLM Narrative Layer</h2>
<div class="arch-row">
    <div class="arch-col">
        <div class="arch-box" style="border-top:3px solid #EF4444;">
            <div class="arch-label" style="color:#EF4444;">Strategic Narrative</div>
            <div class="arch-item">Input: top 20 scored articles + scenario probabilities + EV metrics</div>
            <div class="arch-item">Output: 3 bullet strategic assessment (MBB partner voice, max 15 words/bullet)</div>
            <div class="arch-item">+ 1-sentence KPI explanations (oil mechanism, GRM mechanism)</div>
        </div>
    </div>
    <div class="arch-col">
        <div class="arch-box" style="border-top:3px solid #F59E0B;">
            <div class="arch-label" style="color:#F59E0B;">Scenario Assessments</div>
            <div class="arch-item">Input: 5 scenarios with probabilities + article evidence</div>
            <div class="arch-item">Output: 1 sharp sentence per scenario explaining the probability</div>
            <div class="arch-item">Cites specific signals/events driving each assessment</div>
        </div>
    </div>
</div>

<h2 style="font-size:1.1rem;">Deployment</h2>
<div class="arch-row">
    <div class="arch-col">
        <div class="arch-box" style="border-top:3px solid #00D4FF;">
            <div class="arch-label" style="color:#00D4FF;">Refresh Flow</div>
            <div class="arch-item"><span class="src">1.</span> User clicks &ldquo;Refresh Data&rdquo; on this page</div>
            <div class="arch-item"><span class="src">2.</span> POST &rarr; Cloudflare Worker (token-secured proxy)</div>
            <div class="arch-item"><span class="src">3.</span> Worker triggers GitHub Actions via repository_dispatch</div>
            <div class="arch-item"><span class="src">4.</span> Actions: scrape &rarr; score &rarr; compute &rarr; generate HTML &rarr; deploy</div>
            <div class="arch-item"><span class="src">5.</span> Page auto-detects new deploy and reloads</div>
        </div>
    </div>
</div>

<div style="background:linear-gradient(135deg,#151A28,#1A1F2E);border:1px solid rgba(255,255,255,0.08);border-radius:10px;padding:14px 18px;margin-top:24px;font-size:0.78rem;color:#6B7280;line-height:1.5;">
    <strong style="color:#9CA3AF;">Stack:</strong> Python 3 &middot; SQLite (WAL) &middot; Claude Haiku 4.5 (Anthropic API) &middot; GitHub Actions &middot; GitHub Pages &middot; Cloudflare Workers &middot; EIA API &middot; yfinance &middot; NewsAPI
</div>
</div>"""


def generate_html(output_path="scenario_report.html"):
    """Generate the full standalone HTML report with all horizons."""
    init_db()

    # Get current Indian basket price (72% Dubai + 28% Brent)
    current_basket, dubai_price, brent_price = get_indian_basket_price()
    basket_detail = (dubai_price, brent_price)

    # Shared data
    momentum = compute_momentum(DEFAULT_HORIZON)
    recent_articles = get_recent_articles_with_signals(hours=12, limit=20)
    top_articles = get_top_articles_across_scenarios(limit=30)

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    horizon_labels = {"3m": "3-Month", "6m": "6-Month", "12m": "12-Month"}

    # Render each horizon
    horizon_sections = ""
    total_articles = 0
    last_gen_time = ""
    last_model = ""

    for h in HORIZONS:
        narrative_data = get_latest_scenario_narrative(h)
        if narrative_data:
            if narrative_data.get("article_count", 0) > total_articles:
                total_articles = narrative_data["article_count"]
            if narrative_data.get("generated_at", ""):
                last_gen_time = narrative_data["generated_at"]
            if narrative_data.get("model_used", ""):
                last_model = narrative_data["model_used"]

        is_compact = (h != HORIZONS[0])
        body = render_horizon(h, narrative_data, momentum, recent_articles, top_articles,
                              current_basket, basket_detail, compact=is_compact)
        horizon_sections += f"""
        <div class="horizon-section">
            <h2 style="font-size:1.3rem;margin-top:2.5rem;">{horizon_labels.get(h, h)} Horizon</h2>
            {body}
        </div>
        <hr style="border-color:rgba(255,255,255,0.08);margin:2rem 0;">
        """

    price_banner = ""
    if current_basket:
        price_banner = f'<div style="font-size:0.85rem;margin-bottom:1rem;"><span style="color:#F59E0B;">Brent: <strong>${brent_price:.2f}/bbl</strong></span>'
        if dubai_price:
            price_banner += f' &nbsp;&middot;&nbsp; <span style="color:#00D4AA;">Indian Basket: <strong>${current_basket:.2f}/bbl</strong> <span style="font-size:0.75rem;color:#6B7280;">(72% Dubai + 28% Brent)</span></span>'
        price_banner += '</div>'

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Scenario Engine &mdash; {now_str}</title>
<style>{EXPORT_CSS}
.refresh-btn {{
    background: linear-gradient(135deg, #00D4AA 0%, #00B894 100%);
    color: #0E1117; border: none; padding: 8px 20px; border-radius: 8px;
    font-weight: 700; font-size: 0.82rem; cursor: pointer; letter-spacing: 0.02em;
}}
.refresh-btn:hover {{ box-shadow: 0 4px 16px rgba(0,212,170,0.3); }}
.refresh-btn:disabled {{ opacity: 0.5; cursor: not-allowed; }}
</style>
</head>
<body>
<div style="display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:10px;">
    <div>
        <h1 style="margin-bottom:0.2rem;">&#127919; Scenario Engine</h1>
        <div class="subtitle" style="margin-bottom:0;">Geopolitical scenario analysis for Indian refinery strategy &middot; {now_str}</div>
    </div>
    <div style="display:flex;align-items:center;gap:10px;">
        <button class="refresh-btn" id="refreshBtn" onclick="doRefresh()">&#8635; Refresh Data</button>
        <span id="refreshStatus" style="font-size:0.75rem;color:#9CA3AF;"></span>
    </div>
</div>

<div class="tab-bar">
    <button class="tab-btn active" onclick="switchTab('analysis')">Analysis</button>
    <button class="tab-btn" onclick="switchTab('build')">How We Built This</button>
    <button class="tab-btn" onclick="switchTab('arch')">Architecture</button>
</div>

<div id="tab-analysis" class="tab-panel active">
{price_banner}

<div style="background:linear-gradient(135deg,#151A28,#1A1F2E);border:1px solid rgba(255,255,255,0.08);border-radius:10px;padding:12px 18px;margin-bottom:1.2rem;font-size:0.8rem;color:#9CA3AF;line-height:1.5;">
    AI-powered geopolitical scenario analysis for Indian refinery strategy. LLM scores news articles against 5 scenarios; <strong style="color:#F9FAFB;">EV</strong> = probability-weighted expectation, not a forecast.
    <details style="margin-top:8px;"><summary style="cursor:pointer;color:#00D4AA;font-weight:600;font-size:0.75rem;">Methodology details</summary>
    <div style="margin-top:8px;font-size:0.75rem;line-height:1.5;">
    <strong style="color:#00D4AA;">GRM</strong> = (weighted product revenue) &minus; crude cost. Crude = <strong>Indian basket</strong> (72% Dubai + 28% Brent). Products = Singapore-benchmarked (Platts SG vs US Gulf differentials). Slate: diesel 42%, petrol 22%, naphtha 12%, ATF 10%, fuel oil 8%, LPG 6%.<br><br>
    <strong style="color:#F59E0B;">Scenario prices</strong> from analyst consensus (GS, JPM, MS, Citi, WoodMac, Rystad, Mar 2026). Product prices from observed crisis crack spreads. Singapore adjustments: diesel &minus;$3, petrol &minus;$2.5, ATF &minus;$2, fuel oil +$10 vs USGC.
    </div></details>
</div>

{horizon_sections}

<div class="footer">
    <span>Generated: {last_gen_time[:16] if last_gen_time else now_str}</span>
    <span>Articles analyzed: {total_articles}</span>
    <span>Model: {last_model or 'N/A'}</span>
    <span>Horizons: {', '.join(HORIZONS)}</span>
</div>
</div>

<div id="tab-build" class="tab-panel">
{BUILD_STORY}
</div>

<div id="tab-arch" class="tab-panel">
{ARCHITECTURE}
</div>

<script>
function switchTab(id) {{
    document.querySelectorAll('.tab-panel').forEach(function(p) {{ p.classList.remove('active'); }});
    document.querySelectorAll('.tab-btn').forEach(function(b) {{ b.classList.remove('active'); }});
    document.getElementById('tab-' + id).classList.add('active');
    var btns = document.querySelectorAll('.tab-btn');
    var map = {{'analysis': 0, 'build': 1, 'arch': 2}};
    if (map[id] !== undefined) btns[map[id]].classList.add('active');
}}

function doRefresh() {{
    var btn = document.getElementById('refreshBtn');
    var st = document.getElementById('refreshStatus');
    btn.disabled = true;
    btn.textContent = 'Triggering...';
    st.textContent = '';
    fetch('https://oil-dashboard-refresh.parthreddy.workers.dev', {{
        method: 'POST',
        headers: {{'Content-Type': 'application/json'}}
    }}).then(function(r) {{ return r.json(); }}).then(function(data) {{
        if (data.status === 'triggered') {{
            btn.textContent = 'Triggered!';
            st.textContent = 'Pipeline running (~90s). Page will auto-reload.';
            startPolling();
        }} else {{
            btn.disabled = false;
            btn.textContent = '\\u21BB Refresh Data';
            st.textContent = 'Refresh failed. Try again.';
        }}
    }}).catch(function() {{
        btn.disabled = false;
        btn.textContent = '\\u21BB Refresh Data';
        st.textContent = 'Network error. Try again.';
    }});
}}

var _initialEtag = null;
function checkForUpdate() {{
    fetch(window.location.href, {{method: 'HEAD', cache: 'no-cache'}})
        .then(function(r) {{
            var etag = r.headers.get('etag') || r.headers.get('last-modified') || '';
            if (_initialEtag === null) {{ _initialEtag = etag; return; }}
            if (etag && etag !== _initialEtag) {{ location.reload(); }}
        }}).catch(function() {{}});
}}
function startPolling() {{ setInterval(checkForUpdate, 15000); }}
checkForUpdate();
setInterval(checkForUpdate, 60000);
</script>
</body>
</html>"""

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

    logger.info(f"Report exported to {output_path}")
    return output_path


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    path = generate_html()
    print(f"Report saved to {path}")
