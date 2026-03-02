"""Executive-grade KPI card with sparkline, delta arrows, and threshold awareness."""

import streamlit as st
from dashboard.components.theme import EMERALD, CORAL, TEAL, TEXT_PRIMARY, TEXT_SECONDARY, TEXT_MUTED, TEXT_DIM, BG_CARD, BG_ELEVATED, BORDER_SUBTLE


def _mini_sparkline_svg(values, width=72, height=24, color=TEAL):
    if not values or len(values) < 2:
        return ""
    mn, mx = min(values), max(values)
    rng = mx - mn if mx != mn else 1
    points = []
    for i, v in enumerate(values):
        x = (i / (len(values) - 1)) * width
        y = height - ((v - mn) / rng) * (height - 4) - 2
        points.append(f"{x:.1f},{y:.1f}")
    polyline = " ".join(points)
    fill_points = f"0,{height} " + polyline + f" {width},{height}"
    uid = abs(hash(tuple(values))) % 100000
    return (
        f'<svg width="{width}" height="{height}" viewBox="0 0 {width} {height}" xmlns="http://www.w3.org/2000/svg">'
        f'<defs><linearGradient id="sg{uid}" x1="0" y1="0" x2="0" y2="1">'
        f'<stop offset="0%" stop-color="{color}" stop-opacity="0.3"/>'
        f'<stop offset="100%" stop-color="{color}" stop-opacity="0.02"/>'
        f'</linearGradient></defs>'
        f'<polygon points="{fill_points}" fill="url(#sg{uid})"/>'
        f'<polyline points="{polyline}" fill="none" stroke="{color}" stroke-width="1.5" '
        f'stroke-linecap="round" stroke-linejoin="round"/></svg>'
    )


def kpi_card(title, value, unit="", change=None, change_label="vs last week",
             sparkline_data=None, accent_color=TEAL, tooltip=None):
    """Render a premium KPI card with gradient background, delta, and sparkline."""
    if value is None:
        display_value = "N/A"
    elif isinstance(value, float):
        if unit in ("%",):
            display_value = f"{value:,.1f}"
        elif "USD" in unit or "$" in unit:
            display_value = f"${value:,.2f}"
        else:
            display_value = f"{value:,.2f}"
    else:
        display_value = str(value)

    unit_html = f'<span style="font-size:0.65rem;color:{TEXT_MUTED};font-weight:400;margin-left:3px;">{unit}</span>' if unit and "$" not in display_value else ""

    # Delta
    delta_html = ""
    if change is not None:
        d_color = EMERALD if change >= 0 else CORAL
        arrow = "&#9650;" if change > 0 else ("&#9660;" if change < 0 else "&#8594;")
        delta_html = (
            f'<div style="display:flex;align-items:center;gap:4px;margin-top:6px;">'
            f'<span style="color:{d_color};font-size:0.78rem;font-weight:600;">{arrow} {abs(change):.1f}%</span>'
            f'<span style="color:{TEXT_DIM};font-size:0.62rem;">{change_label}</span></div>'
        )

    # Sparkline
    spark_html = ""
    if sparkline_data and len(sparkline_data) >= 3:
        s_color = EMERALD if sparkline_data[-1] >= sparkline_data[0] else CORAL
        spark_html = (
            f'<div style="position:absolute;bottom:10px;right:14px;opacity:0.7;">'
            f'{_mini_sparkline_svg(sparkline_data, color=s_color)}</div>'
        )

    # Tooltip icon + hover popup
    tooltip_html = ""
    if tooltip:
        tooltip_html = (
            f'<span class="kpi-tooltip-wrap" style="position:relative;display:inline-block;margin-left:5px;cursor:help;">'
            f'<span style="font-size:0.65rem;color:{TEXT_DIM};border:1px solid {TEXT_DIM};'
            f'border-radius:50%;width:14px;height:14px;display:inline-flex;align-items:center;'
            f'justify-content:center;vertical-align:middle;">i</span>'
            f'<span class="kpi-tooltip-text" style="visibility:hidden;opacity:0;position:absolute;'
            f'z-index:999;bottom:calc(100% + 8px);left:50%;transform:translateX(-50%);'
            f'width:260px;background:{BG_ELEVATED};color:{TEXT_SECONDARY};font-size:0.7rem;'
            f'font-weight:400;text-transform:none;letter-spacing:normal;line-height:1.5;'
            f'padding:12px 14px;border-radius:8px;border:1px solid {BORDER_SUBTLE};'
            f'box-shadow:0 4px 16px rgba(0,0,0,0.4);transition:opacity 0.2s;pointer-events:none;">'
            f'{tooltip}</span></span>'
        )

    st.markdown(f"""
    <style>
    .kpi-tooltip-wrap:hover .kpi-tooltip-text {{
        visibility: visible !important;
        opacity: 1 !important;
    }}
    @media (max-width: 768px) {{
        .kpi-box {{ padding: 10px 12px 8px !important; min-height: 70px !important; }}
        .kpi-title {{ font-size: 0.58rem !important; margin-bottom: 4px !important; }}
        .kpi-value {{ font-size: 1.1rem !important; }}
    }}
    </style>
    <div class="kpi-box" style="position:relative;background:linear-gradient(135deg,{BG_CARD},{BG_ELEVATED});
        border:1px solid {BORDER_SUBTLE};border-left:3px solid {accent_color};
        border-radius:10px;padding:16px 18px 14px;margin-bottom:6px;min-height:100px;">
        <div class="kpi-title" style="color:{TEXT_SECONDARY};font-size:0.7rem;font-weight:600;
            text-transform:uppercase;letter-spacing:0.08em;margin-bottom:8px;">{title}{tooltip_html}</div>
        <div class="kpi-value" style="color:{TEXT_PRIMARY};font-size:1.6rem;font-weight:700;
            letter-spacing:-0.02em;line-height:1.1;font-variant-numeric:tabular-nums;">
            {display_value}{unit_html}</div>
        {delta_html}{spark_html}
    </div>""", unsafe_allow_html=True)
