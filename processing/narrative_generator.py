"""LLM-powered strategic narrative generator for Morning Brief."""

import os
import sys
import logging
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from config.secrets_helper import get_secret
from database.db_manager import (
    get_connection, upsert_narrative, get_latest_narrative,
)

logger = logging.getLogger(__name__)

MODEL = "claude-haiku-4-5-20251001"


def _get_client():
    """Lazy-load Anthropic client."""
    import anthropic
    api_key = get_secret("ANTHROPIC_API_KEY")
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY not set")
    return anthropic.Anthropic(api_key=api_key)


def _collect_context():
    """Collect all dashboard data into a structured text block for the LLM."""
    sections = []

    with get_connection(readonly=True) as conn:
        # --- Crude prices + changes ---
        benchmarks = conn.execute(
            """SELECT benchmark, price, date FROM crude_prices
               WHERE date = (SELECT MAX(date) FROM crude_prices)"""
        ).fetchall()
        if benchmarks:
            lines = []
            for b in benchmarks:
                name = b["benchmark"].replace("_", " ").title()
                lines.append(f"  {name}: ${b['price']:.2f}/bbl (as of {b['date']})")
            sections.append("CRUDE PRICES (latest):\n" + "\n".join(lines))

        # WoW / MoM from snapshots
        snaps = conn.execute(
            """SELECT metric_name, metric_value, change_wow, change_mom, unit
               FROM key_metrics_snapshot
               WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM key_metrics_snapshot)"""
        ).fetchall()
        if snaps:
            lines = []
            for s in snaps:
                parts = [f"  {s['metric_name']}: {s['metric_value']:.2f} {s['unit'] or ''}"]
                if s["change_wow"] is not None:
                    parts.append(f"WoW {s['change_wow']:+.1f}%")
                if s["change_mom"] is not None:
                    parts.append(f"MoM {s['change_mom']:+.1f}%")
                lines.append(" | ".join(parts))
            sections.append("KEY METRICS SNAPSHOT:\n" + "\n".join(lines))

        # --- Crack spreads + GRM ---
        cracks = conn.execute(
            """SELECT product, spread, estimated_grm, source FROM crack_spreads
               WHERE date = (SELECT MAX(date) FROM crack_spreads)
               ORDER BY spread DESC"""
        ).fetchall()
        if cracks:
            lines = []
            grm_val = None
            for c in cracks:
                lines.append(f"  {c['product'].title()}: {c['spread']:+.2f} $/bbl")
                if grm_val is None and c["estimated_grm"] is not None:
                    grm_val = c["estimated_grm"]
            if grm_val is not None:
                lines.append(f"  Estimated GRM: ${grm_val:.2f}/bbl")
            sections.append("CRACK SPREADS (vs Brent):\n" + "\n".join(lines))

        # --- Refinery utilization ---
        util = conn.execute(
            """SELECT company, AVG(utilization_pct) as avg_util
               FROM refinery_data
               WHERE date = (SELECT MAX(date) FROM refinery_data)
               GROUP BY company"""
        ).fetchall()
        if util:
            lines = [f"  {u['company']}: {u['avg_util']:.1f}%" for u in util]
            sections.append("REFINERY UTILIZATION:\n" + "\n".join(lines))

        # --- Top 15 news headlines ---
        news = conn.execute(
            """SELECT title, impact_tag, impact_score, source
               FROM news_articles
               ORDER BY published_date DESC LIMIT 15"""
        ).fetchall()
        if news:
            lines = []
            for n in news:
                tag = n["impact_tag"] or "neutral"
                lines.append(f"  [{tag.upper()}] {n['title']} ({n['source']})")
            sections.append("RECENT NEWS HEADLINES:\n" + "\n".join(lines))

        # --- OPEC production ---
        opec = conn.execute(
            """SELECT region, value, unit FROM global_events
               WHERE event_type = 'opec_production'
               AND date = (SELECT MAX(date) FROM global_events WHERE event_type = 'opec_production')
               ORDER BY value DESC LIMIT 15"""
        ).fetchall()
        if opec:
            lines = [f"  {o['region']}: {o['value']:.0f} {o['unit'] or 'kb/d'}" for o in opec]
            sections.append("OPEC PRODUCTION:\n" + "\n".join(lines))

        # --- Trade flows ---
        flows = conn.execute(
            """SELECT flow_type, country, product, volume_tmt
               FROM trade_flows
               WHERE date = (SELECT MAX(date) FROM trade_flows)
               ORDER BY volume_tmt DESC LIMIT 10"""
        ).fetchall()
        if flows:
            lines = [f"  {f['flow_type']}: {f['country']} — {f['product']} {f['volume_tmt']:.0f} TMT" for f in flows]
            sections.append("TRADE FLOWS:\n" + "\n".join(lines))

        # --- FX ---
        fx = conn.execute(
            "SELECT pair, rate, date FROM fx_rates ORDER BY date DESC LIMIT 1"
        ).fetchone()
        if fx:
            sections.append(f"FX RATE: {fx['pair']} = {fx['rate']:.2f} (as of {fx['date']})")

    return "\n\n".join(sections)


SYSTEM_PROMPT = """You are a senior oil & gas market analyst writing a concise morning brief for an Indian refinery executive. You have deep expertise in crude oil markets, refining economics, OPEC dynamics, and India's energy sector."""

USER_PROMPT_TEMPLATE = """Based on the following dashboard data, write a strategic morning brief as **5-7 bullet points**.

Each bullet must include:
1. **What** is happening (cite specific numbers from the data)
2. **Why** it is happening (drivers, root causes, market dynamics)
3. **Near-term outlook** or what to watch next

Format rules:
- Return ONLY an HTML unordered list (<ul>) with <li> items
- Each <li> should have a bold lead-in phrase, then the explanation
- Use <b> tags for emphasis on key numbers and phrases
- Do NOT include any text outside the <ul>...</ul>
- Keep the total length to roughly 150-250 words
- Cover: crude prices, refining margins, news sentiment, and one forward-looking insight

Dashboard data:
{context}"""


def generate_narrative():
    """Generate an LLM-powered strategic narrative from current dashboard data.

    Returns the narrative HTML string, or None on failure.
    """
    context = _collect_context()
    if not context or len(context) < 50:
        logger.warning("Insufficient data to generate narrative")
        return None

    try:
        client = _get_client()
        response = client.messages.create(
            model=MODEL,
            max_tokens=1024,
            messages=[
                {"role": "user", "content": USER_PROMPT_TEMPLATE.format(context=context)},
            ],
            system=SYSTEM_PROMPT,
        )

        html = response.content[0].text.strip()

        # Strip markdown code fences if present (```html ... ```)
        if "```" in html:
            html = html.split("```")[1]
            if html.lower().startswith("html"):
                html = html[4:]
            html = html.strip()

        # Ensure we got valid HTML list
        if "<ul>" not in html.lower():
            logger.warning("LLM response missing <ul> tag, wrapping")
            html = f"<ul><li>{html}</li></ul>"

        # Store in DB
        today = datetime.now().strftime("%Y-%m-%d")
        upsert_narrative(today, html, MODEL)
        logger.info(f"Strategic narrative generated and stored ({len(html)} chars)")

        return html

    except Exception as e:
        logger.error(f"Narrative generation failed: {e}")
        return None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    result = generate_narrative()
    if result:
        print(result)
    else:
        print("No narrative generated (check API key and data)")
