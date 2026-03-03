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

@media (max-width: 768px) {
    body { padding: 1rem; }
    .kpi-row { flex-direction: column; }
    .scenario-body { flex-direction: column; }
    .scenario-kpis { width: 100%; border-right: none; border-top: 1px solid rgba(255,255,255,0.06); }
    .momentum-row { flex-direction: column; }
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


def render_horizon(horizon, narrative_data, momentum, recent_articles, top_articles, current_brent=None, compact=False):
    """Render a single horizon section. compact=True shows only narrative + KPIs."""
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
    stock_expl = narrative_data.get("stock_explanation", "") if narrative_data else ""

    brent_now_html = f'<div style="font-size:0.72rem;color:#F59E0B;margin-top:4px;">Current Brent: ${current_brent:.2f}/bbl</div>' if current_brent else ''

    parts.append(f"""
    <div class="kpi-row">
        <div class="kpi-card" style="border-left:3px solid #F59E0B;">
            <div class="label">Brent Price (EV)</div>
            <div class="value">${ev['oil']:.0f}/bbl</div>
            <div class="range">range: ${ranges['oil'][0]}-{ranges['oil'][1]}</div>
            {brent_now_html}
            {f'<div class="kpi-explanation">{oil_expl}</div>' if oil_expl else ''}
        </div>
        <div class="kpi-card" style="border-left:3px solid #00D4AA;">
            <div class="label">GRM (EV)</div>
            <div class="value">${ev['grm']:.1f}/bbl</div>
            <div class="range">range: ${ranges['grm'][0]}-{ranges['grm'][1]}</div>
            {f'<div class="kpi-explanation">{grm_expl}</div>' if grm_expl else ''}
        </div>
        <div class="kpi-card" style="border-left:3px solid #00D4FF;">
            <div class="label">Stock Impact (EV)</div>
            <div class="value">{ev['stock']:+.0f}%</div>
            <div class="range">range: {ranges['stock'][0]:+.0f}% to {ranges['stock'][1]:+.0f}%</div>
            {f'<div class="kpi-explanation">{stock_expl}</div>' if stock_expl else ''}
        </div>
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

        # Articles for this scenario
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

        # Assessment
        assessment_html = ""
        assessment = assessments.get(sid, "")
        if assessment:
            assessment_html = f'<div class="scenario-assessment"><div class="lbl">LLM Assessment</div><div>{assessment}</div></div>'

        parts.append(f"""
        <div class="scenario-card">
            <div class="scenario-header">
                <div>
                    <span class="name">{scenario['name']}</span>
                    <span class="desc">{scenario['description'][:80]}</span>
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
                        <div class="item"><div class="lbl">Stock</div><div class="val">{kpis['stock']:+.0f}%</div></div>
                    </div>
                    {assessment_html}
                </div>
            </div>
        </div>""")

    return "\n".join(parts)


def generate_html(output_path="scenario_report.html"):
    """Generate the full standalone HTML report with all horizons."""
    from database.db_manager import get_latest_price
    init_db()

    # Get current Brent price
    brent_row = get_latest_price("brent")
    current_brent = float(brent_row["price"]) if brent_row else None

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

        is_compact = (h != HORIZONS[0])  # Only first horizon gets full detail
        body = render_horizon(h, narrative_data, momentum, recent_articles, top_articles, current_brent, compact=is_compact)
        horizon_sections += f"""
        <div class="horizon-section">
            <h2 style="font-size:1.3rem;margin-top:2.5rem;">{horizon_labels.get(h, h)} Horizon</h2>
            {body}
        </div>
        <hr style="border-color:rgba(255,255,255,0.08);margin:2rem 0;">
        """

    brent_banner = ""
    if current_brent:
        brent_banner = f'<div style="font-size:0.85rem;color:#F59E0B;margin-bottom:1rem;">Current Brent: <strong>${current_brent:.2f}/bbl</strong></div>'

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Scenario Engine Report — {now_str}</title>
<style>{EXPORT_CSS}
.refresh-btn {{
    background: linear-gradient(135deg, #00D4AA 0%, #00B894 100%);
    color: #0E1117; border: none; padding: 8px 20px; border-radius: 8px;
    font-weight: 700; font-size: 0.82rem; cursor: pointer; letter-spacing: 0.02em;
}}
.refresh-btn:hover {{ box-shadow: 0 4px 16px rgba(0,212,170,0.3); }}
.refresh-btn:disabled {{ opacity: 0.5; cursor: not-allowed; }}
.refresh-status {{ font-size: 0.75rem; color: #9CA3AF; margin-left: 12px; }}
</style>
</head>
<body>
<div style="display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:10px;">
    <div>
        <h1 style="margin-bottom:0.2rem;">&#127919; Scenario Engine</h1>
        <div class="subtitle" style="margin-bottom:0;">Geopolitical scenario analysis for Indian refinery strategy &middot; {now_str}</div>
    </div>
    <div style="display:flex;align-items:center;">
        <button class="refresh-btn" id="refreshBtn" onclick="doRefresh()">&#8635; Refresh Data</button>
        <span class="refresh-status" id="refreshStatus"></span>
    </div>
</div>
{brent_banner}

{horizon_sections}

<div class="footer">
    <span>Generated: {last_gen_time[:16] if last_gen_time else now_str}</span>
    <span>Articles analyzed: {total_articles}</span>
    <span>Model: {last_model or 'N/A'}</span>
    <span>Horizons: {', '.join(HORIZONS)}</span>
</div>

<script>
function doRefresh() {{
    var btn = document.getElementById('refreshBtn');
    var status = document.getElementById('refreshStatus');
    btn.disabled = true;
    btn.textContent = 'Refreshing...';
    status.textContent = 'Scraping + scoring + generating...';

    fetch('/refresh', {{method: 'POST'}})
        .then(r => r.json())
        .then(d => {{
            if (d.error) {{ status.textContent = d.error; btn.disabled = false; btn.textContent = '\\u21BB Refresh Data'; return; }}
            pollStatus();
        }})
        .catch(e => {{
            status.textContent = 'Server not running. Start with: python serve_report.py';
            btn.disabled = false; btn.textContent = '\\u21BB Refresh Data';
        }});
}}

function pollStatus() {{
    var status = document.getElementById('refreshStatus');
    var btn = document.getElementById('refreshBtn');
    fetch('/status').then(r => r.json()).then(d => {{
        if (d.running) {{
            status.textContent = 'Processing...';
            setTimeout(pollStatus, 2000);
        }} else {{
            status.textContent = 'Done! Reloading...';
            setTimeout(() => location.reload(), 500);
        }}
    }}).catch(() => {{ setTimeout(pollStatus, 3000); }});
}}
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
