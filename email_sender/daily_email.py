"""Daily email digest — scenario shifts + top articles."""

import os
import sys
import smtplib
import logging
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from config.secrets_helper import get_secret
from database.db_manager import (
    get_latest_scenario_narrative, get_top_articles_across_scenarios,
    get_previous_day_weights, get_accuracy_history,
)
from processing.scenario_analyzer import (
    SCENARIOS, HORIZONS, DEFAULT_HORIZON,
    _compute_ev, _compute_ranges, compute_momentum,
    get_indian_basket_price,
)

logger = logging.getLogger(__name__)

RECIPIENTS = ["parth.reddy@live.com"]


def _build_html(horizon="3m"):
    """Build HTML email body with scenario analysis."""
    narrative = get_latest_scenario_narrative(horizon)
    if not narrative:
        return None

    weights = narrative.get("weight_snapshot", {})
    ev = _compute_ev(SCENARIOS, weights, horizon)
    ranges = _compute_ranges(SCENARIOS, horizon)
    momentum = compute_momentum(horizon)
    basket_price, dubai, brent = get_indian_basket_price()

    # Get previous weights for delta
    prev = get_previous_day_weights(horizon)
    prev_weights = prev.get("weight_snapshot", {}) if prev else {}

    # Build scenario rows
    scenario_rows = ""
    for sid, s in SCENARIOS.items():
        w = weights.get(sid, 0.2)
        pw = prev_weights.get(sid, w)
        delta = w - pw
        arrow = "&#9650;" if delta > 0.01 else ("&#9660;" if delta < -0.01 else "&#9644;")
        color = "#10B981" if delta > 0.01 else ("#EF4444" if delta < -0.01 else "#6B7280")
        kpis = s["horizons"].get(horizon, s["horizons"]["3m"])
        mom = momentum.get(sid, {})
        mom_arrow = {"rising": "&#9650;", "falling": "&#9660;", "stable": "&#9644;"}.get(mom.get("direction", "stable"), "&#9644;")

        scenario_rows += f"""
        <tr style="border-bottom:1px solid #2a2a2a;">
            <td style="padding:10px 12px;font-weight:600;color:#F9FAFB;">{s['name']}</td>
            <td style="padding:10px 12px;text-align:center;font-size:1.1em;font-weight:700;color:#00D4AA;">{w:.0%}</td>
            <td style="padding:10px 12px;text-align:center;color:{color};font-size:0.85em;">
                {arrow} {delta:+.1%}
            </td>
            <td style="padding:10px 12px;text-align:center;color:#E5E7EB;">${kpis['oil']}</td>
            <td style="padding:10px 12px;text-align:center;color:#E5E7EB;">${kpis['grm']}</td>
            <td style="padding:10px 12px;text-align:center;font-size:0.85em;">{mom_arrow}</td>
        </tr>"""

    # Top articles
    top_articles = get_top_articles_across_scenarios(limit=5)
    article_rows = ""
    for a in top_articles:
        signals = a.get("signals", [])
        top_sig = max(signals, key=lambda s: abs(s["signal"])) if signals else None
        sig_text = f'{top_sig["scenario_id"]}: {top_sig["signal"]:+.2f}' if top_sig else ""
        article_rows += f"""
        <tr style="border-bottom:1px solid #2a2a2a;">
            <td style="padding:8px 12px;">
                <a href="{a.get('url','#')}" style="color:#00D4FF;text-decoration:none;">{a['title'][:80]}</a>
            </td>
            <td style="padding:8px 12px;color:#9CA3AF;font-size:0.8em;white-space:nowrap;">{sig_text}</td>
        </tr>"""

    # Narrative
    narr_html = narrative.get("narrative", "<em>No narrative available</em>")
    oil_expl = narrative.get("oil_explanation", "")
    grm_expl = narrative.get("grm_explanation", "")

    # Accuracy stats
    accuracy = get_accuracy_history(limit=10)
    accuracy_section = ""
    if accuracy:
        avg_error = sum(abs(a.get("oil_error", 0)) for a in accuracy) / len(accuracy)
        accuracy_section = f"""
        <div style="background:#1A1F2E;border:1px solid #2a2a2a;border-radius:8px;padding:14px;margin-top:16px;">
            <h3 style="color:#F59E0B;font-size:0.85rem;margin:0 0 8px;">Prediction Accuracy (last {len(accuracy)} scored)</h3>
            <p style="color:#9CA3AF;font-size:0.8rem;margin:0;">Avg absolute Brent error: <strong style="color:#F9FAFB;">${avg_error:.1f}/bbl</strong></p>
        </div>"""

    html = f"""
    <html>
    <body style="background:#0E1117;color:#E5E7EB;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;padding:20px;max-width:700px;margin:0 auto;">

    <h1 style="color:#F9FAFB;font-size:1.3rem;margin-bottom:4px;">Oil Scenario Engine — Daily Brief</h1>
    <p style="color:#6B7280;font-size:0.75rem;margin:0 0 16px;">
        {datetime.utcnow().strftime("%B %d, %Y %H:%M UTC")} | Brent ${brent:.2f} | Indian Basket ${basket_price:.2f}
    </p>

    <div style="background:linear-gradient(135deg,#0D2818,#1A1F2E);border:1px solid rgba(0,212,170,0.3);border-radius:8px;padding:14px;margin-bottom:16px;">
        <h3 style="color:#00D4AA;font-size:0.85rem;margin:0 0 8px;">Strategic Narrative</h3>
        <div style="font-size:0.82rem;line-height:1.6;">{narr_html}</div>
        <p style="color:#9CA3AF;font-size:0.75rem;margin:8px 0 0;">{oil_expl}</p>
    </div>

    <h2 style="color:#E5E7EB;font-size:0.95rem;border-bottom:2px solid rgba(0,212,170,0.3);padding-bottom:6px;">
        Scenario Probabilities ({horizon})
    </h2>
    <p style="font-size:0.8rem;color:#9CA3AF;margin:0 0 8px;">
        EV Brent: <strong style="color:#00D4AA;">${ev['oil']:.0f}</strong> (${ranges['oil'][0]}-${ranges['oil'][1]}) |
        EV GRM: <strong style="color:#F59E0B;">${ev['grm']:.1f}</strong> (${ranges['grm'][0]}-${ranges['grm'][1]})
    </p>
    <table style="width:100%;border-collapse:collapse;font-size:0.82rem;background:#151A28;border-radius:8px;">
        <tr style="border-bottom:2px solid #2a2a2a;">
            <th style="padding:10px 12px;text-align:left;color:#6B7280;">Scenario</th>
            <th style="padding:10px 12px;text-align:center;color:#6B7280;">Weight</th>
            <th style="padding:10px 12px;text-align:center;color:#6B7280;">Delta</th>
            <th style="padding:10px 12px;text-align:center;color:#6B7280;">Brent</th>
            <th style="padding:10px 12px;text-align:center;color:#6B7280;">GRM</th>
            <th style="padding:10px 12px;text-align:center;color:#6B7280;">Mom</th>
        </tr>
        {scenario_rows}
    </table>

    <h2 style="color:#E5E7EB;font-size:0.95rem;border-bottom:2px solid rgba(0,212,170,0.3);padding-bottom:6px;margin-top:20px;">
        Top Signal Articles
    </h2>
    <table style="width:100%;border-collapse:collapse;font-size:0.82rem;background:#151A28;border-radius:8px;">
        {article_rows}
    </table>

    {accuracy_section}

    <p style="color:#4B5563;font-size:0.7rem;margin-top:20px;text-align:center;">
        Reddy Research — Oil Scenario Engine | Articles analyzed: {narrative.get('article_count', 'N/A')}
    </p>
    </body>
    </html>
    """
    return html


def send_daily_digest():
    """Build and send the daily email digest."""
    smtp_server = get_secret("EMAIL_SMTP_SERVER") or "smtp.gmail.com"
    smtp_port = int(get_secret("EMAIL_SMTP_PORT") or "587")
    sender = get_secret("EMAIL_SENDER")
    password = get_secret("EMAIL_APP_PASSWORD")

    if not sender or not password:
        logger.warning("Email credentials not configured, skipping digest")
        return False

    recipients = [r.strip() for r in (get_secret("EMAIL_RECIPIENTS") or ",".join(RECIPIENTS)).split(",")]

    html = _build_html("3m")
    if not html:
        logger.warning("No narrative data available, skipping email")
        return False

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"Oil Scenario Brief — {datetime.utcnow().strftime('%b %d, %Y')}"
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)
    msg.attach(MIMEText(html, "html"))

    try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(sender, password)
            server.sendmail(sender, recipients, msg.as_string())
        logger.info(f"Daily digest sent to {recipients}")
        return True
    except Exception as e:
        logger.error(f"Email send failed: {e}")
        return False


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    send_daily_digest()
