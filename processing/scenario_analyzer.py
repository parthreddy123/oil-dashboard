"""Scenario Engine — geopolitical scenario analysis with LLM-powered signals and narratives."""

import os
import sys
import json
import logging
from datetime import datetime, timedelta
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from config.secrets_helper import get_secret
from database.db_manager import (
    get_connection, get_unscored_articles, insert_article_signals_bulk,
    insert_scenario_narrative, get_latest_scenario_narrative,
    get_top_articles_across_scenarios, get_signals_for_window,
)

logger = logging.getLogger(__name__)

MODEL = "claude-haiku-4-5-20251001"

# ---------------------------------------------------------------------------
# Scenario definitions — 5 geopolitical scenarios for Middle East / Hormuz
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Scenario definitions — analyst consensus as of March 2026
# Sources: Goldman Sachs, JPMorgan, Morgan Stanley, Citi, Wood Mackenzie,
#          Rystad Energy, Deutsche Bank, ING, CSIS, Oxford Economics, EIA, IEA
#
# Brent prices: mid-point of bank/analyst consensus per scenario
# Product prices: based on observed crisis crack spread behavior (Mar 2026)
#   - Diesel crack surged +36% (Argus Media), ATF to 2yr high (Bloomberg)
#   - LPG +12% (Kpler), Asian naphtha premium highest since early 2023
# GRM: derived from product prices using Indian refinery product slate
# ---------------------------------------------------------------------------

SCENARIOS = {
    "managed_escalation": {
        "name": "Managed Escalation",
        "short": "Managed",
        "description": "Controlled tit-for-tat strikes calibrated to avoid full war. "
                       "Limited Hormuz disruption with periodic flare-ups, elevated insurance.",
        "preconditions": [
            "Both sides maintain back-channel communication or intermediary contact",
            "Strikes remain limited to military/infrastructure targets, no population centers",
            "No independent escalation by proxy forces (Hezbollah, Houthis) beyond current levels",
            "US maintains naval presence in Gulf without direct combat engagement",
        ],
        "invalidators": [
            "Direct strike on civilian population center causing mass casualties (-> Conflagration)",
            "Full Hormuz blockade announced by IRGC Navy (-> Conflagration)",
            "Formal ceasefire agreement signed by both parties (-> Ceasefire)",
        ],
        "horizons": {
            "1m":  {"oil": 100, "grm": 4.5, "stock": -22},
            "3m":  {"oil": 97, "grm": 5.5, "stock": -18},
            "6m":  {"oil": 95, "grm": 6.0, "stock": -14},
        },
        "products": {
            "1m":  {"diesel": 150, "petrol": 95, "atf": 145, "naphtha": 80, "fuel_oil": 70, "lpg": 31},
            "3m":  {"diesel": 145, "petrol": 92, "atf": 140, "naphtha": 78, "fuel_oil": 68, "lpg": 30},
            "6m":  {"diesel": 138, "petrol": 88, "atf": 135, "naphtha": 74, "fuel_oil": 65, "lpg": 28},
        },
    },
    "prolonged_standoff": {
        "name": "Prolonged Standoff",
        "short": "Prolong",
        "description": "Neither escalation nor resolution. Partial Hormuz disruption persists "
                       "with naval escort corridors. Elevated premiums, rerouted tankers.",
        "preconditions": [
            "No direct military strikes between principal parties for 30+ days",
            "Hormuz transit reduced 20-40% but not fully closed",
            "At least 2 OPEC members publicly oppose further escalation",
            "Insurance premiums for Gulf tankers exceed 3% of cargo value",
        ],
        "invalidators": [
            "Resumption of direct strikes between principals (-> Managed Escalation or Conflagration)",
            "Full Hormuz closure or mining of shipping lanes (-> Conflagration)",
            "Formal ceasefire signed (-> Ceasefire)",
        ],
        "horizons": {
            "1m":  {"oil": 92, "grm": 6.5, "stock": -14},
            "3m":  {"oil": 90, "grm": 7.0, "stock": -12},
            "6m":  {"oil": 85, "grm": 7.5, "stock": -8},
        },
        "products": {
            "1m":  {"diesel": 138, "petrol": 88, "atf": 133, "naphtha": 74, "fuel_oil": 65, "lpg": 29},
            "3m":  {"diesel": 135, "petrol": 86, "atf": 130, "naphtha": 72, "fuel_oil": 63, "lpg": 28},
            "6m":  {"diesel": 125, "petrol": 81, "atf": 122, "naphtha": 68, "fuel_oil": 59, "lpg": 26},
        },
    },
    "conflagration": {
        "name": "Conflagration",
        "short": "Conflag",
        "description": "Full regional war with complete Hormuz closure, refinery attacks, "
                       "and global supply chain disruption.",
        "preconditions": [
            "Direct military engagement between at least 2 state actors (not just proxies)",
            "Hormuz transit fully blocked or mined, tanker traffic halted",
            "Multiple Gulf state oil facilities targeted or damaged",
            "UN Security Council emergency session convened",
        ],
        "invalidators": [
            "UN-brokered ceasefire accepted by all parties (-> Ceasefire)",
            "Unilateral withdrawal by one principal (-> Prolonged Standoff)",
            "Internal regime collapse in aggressor state (-> Regime Change)",
        ],
        "horizons": {
            "1m":  {"oil": 140, "grm": -4.0, "stock": -40},
            "3m":  {"oil": 130, "grm": -2.0, "stock": -35},
            "6m":  {"oil": 120, "grm": 0.0,  "stock": -28},
        },
        "products": {
            "1m":  {"diesel": 210, "petrol": 125, "atf": 205, "naphtha": 88, "fuel_oil": 105, "lpg": 37},
            "3m":  {"diesel": 200, "petrol": 118, "atf": 195, "naphtha": 85, "fuel_oil": 98, "lpg": 35},
            "6m":  {"diesel": 182, "petrol": 110, "atf": 178, "naphtha": 80, "fuel_oil": 90, "lpg": 33},
        },
    },
    "ceasefire": {
        "name": "Ceasefire & Talks",
        "short": "Ceasefire",
        "description": "Formal ceasefire with ongoing negotiations. Markets normalize "
                       "rapidly, crude flows resume, risk premiums evaporate.",
        "preconditions": [
            "Both parties publicly agree to cessation of hostilities",
            "Third-party mediator actively engaged (US, China, UN, or Gulf state)",
            "No major military action by either side for 7+ consecutive days",
            "Tanker insurance premiums declining week-over-week",
        ],
        "invalidators": [
            "Major military strike by either party breaking ceasefire (-> Managed Escalation)",
            "Proxy forces launch significant attack undermining talks (-> Prolonged Standoff)",
            "Collapse of mediator credibility or withdrawal (-> Prolonged Standoff)",
        ],
        "horizons": {
            "1m":  {"oil": 72, "grm": 12.0, "stock": 12},
            "3m":  {"oil": 68, "grm": 13.0, "stock": 18},
            "6m":  {"oil": 63, "grm": 12.5, "stock": 15},
        },
        "products": {
            "1m":  {"diesel": 92, "petrol": 68, "atf": 90, "naphtha": 60, "fuel_oil": 47, "lpg": 25},
            "3m":  {"diesel": 88, "petrol": 65, "atf": 86, "naphtha": 58, "fuel_oil": 44, "lpg": 24},
            "6m":  {"diesel": 82, "petrol": 60, "atf": 80, "naphtha": 54, "fuel_oil": 41, "lpg": 22},
        },
    },
    "regime_change": {
        "name": "Regime Change",
        "short": "Regime",
        "description": "Internal power shift in key state creates unpredictable transition. "
                       "Temporary disruption followed by uncertain new equilibrium.",
        "preconditions": [
            "Credible reports of internal power struggle or military faction split",
            "Breakdown of command-and-control in at least one principal state",
            "International intelligence agencies signal imminent leadership change",
            "Capital flight or currency collapse in target state",
        ],
        "invalidators": [
            "Regime consolidates power and resumes normal operations (-> Prolonged Standoff)",
            "New regime immediately sues for peace (-> Ceasefire)",
            "Power vacuum triggers regional intervention (-> Conflagration)",
        ],
        "horizons": {
            "1m":  {"oil": 88, "grm": 8.0,  "stock": -8},
            "3m":  {"oil": 85, "grm": 9.0,  "stock": -5},
            "6m":  {"oil": 80, "grm": 9.5,  "stock": 0},
        },
        "products": {
            "1m":  {"diesel": 122, "petrol": 82, "atf": 118, "naphtha": 70, "fuel_oil": 59, "lpg": 28},
            "3m":  {"diesel": 118, "petrol": 80, "atf": 114, "naphtha": 68, "fuel_oil": 57, "lpg": 27},
            "6m":  {"diesel": 110, "petrol": 76, "atf": 106, "naphtha": 64, "fuel_oil": 54, "lpg": 25},
        },
    },
}

# ---------------------------------------------------------------------------
# Product price helpers
# ---------------------------------------------------------------------------

_PRODUCT_LIST = ["diesel", "petrol", "atf", "naphtha", "fuel_oil", "lpg"]

PRODUCT_NAMES = {
    "diesel": "Diesel", "petrol": "Petrol", "atf": "ATF",
    "lpg": "LPG", "naphtha": "Naphtha", "fuel_oil": "Fuel Oil",
}

GRM_WEIGHTS = {
    "diesel": 0.42, "petrol": 0.22, "naphtha": 0.12,
    "atf": 0.10, "fuel_oil": 0.08, "lpg": 0.06,
}


# Singapore vs US Gulf Coast product price differentials (USD/bbl)
_SINGAPORE_DIFF = {
    "diesel": -3.0, "petrol": -2.5, "atf": -2.0,
    "naphtha": 0.0, "fuel_oil": +10.0, "lpg": +5.0,
}


def get_indian_basket_price():
    """Compute Indian crude basket = 72% Dubai/Oman + 28% Brent."""
    with get_connection(readonly=True) as conn:
        dubai = conn.execute(
            "SELECT price FROM crude_prices WHERE benchmark='oman_dubai' ORDER BY date DESC LIMIT 1"
        ).fetchone()
        brent = conn.execute(
            "SELECT price FROM crude_prices WHERE benchmark='brent' ORDER BY date DESC LIMIT 1"
        ).fetchone()
    d = float(dubai["price"]) if dubai else None
    b = float(brent["price"]) if brent else 80.0
    if d:
        return round(0.72 * d + 0.28 * b, 2), d, b
    return round(b, 2), None, b


def get_current_product_prices():
    """Fetch latest product prices (Singapore-adjusted) for Indian refinery context."""
    basket, _, brent = get_indian_basket_price()
    with get_connection(readonly=True) as conn:
        rows = conn.execute(
            """SELECT product, price FROM product_prices
               WHERE unit='USD/bbl' AND product IN ('petrol','diesel','atf','lpg')
               GROUP BY product HAVING date = MAX(date)"""
        ).fetchall()
    prices = {}
    for r in rows:
        p = r["product"]
        prices[p] = round(r["price"] + _SINGAPORE_DIFF.get(p, 0), 2)
    if "petrol" in prices:
        prices["naphtha"] = round(prices["petrol"] * 0.90 + _SINGAPORE_DIFF["naphtha"], 2)
    else:
        prices["naphtha"] = round(basket * 0.81, 2)
    prices["fuel_oil"] = round(basket * 0.65 + _SINGAPORE_DIFF["fuel_oil"], 2)
    return prices


def compute_scenario_products(scenario_id, horizon):
    """Get hardcoded product prices for a scenario. Returns dict {product: price_usd_bbl}."""
    s = SCENARIOS[scenario_id]
    h = horizon if horizon in s.get("products", {}) else "3m"
    return dict(s.get("products", {}).get(h, {}))


def compute_ev_products(weights, horizon):
    """Compute probability-weighted expected product prices across scenarios."""
    ev = {p: 0.0 for p in _PRODUCT_LIST}
    for sid, w in weights.items():
        prods = compute_scenario_products(sid, horizon)
        for p in ev:
            ev[p] += w * prods.get(p, 0)
    return {p: round(v, 1) for p, v in ev.items()}


HORIZONS = ["1m", "3m", "6m"]
SCENARIO_IDS = list(SCENARIOS.keys())
DEFAULT_HORIZON = "1m"

# Prior probabilities (uniform)
PRIOR_WEIGHTS = {sid: 1.0 / len(SCENARIOS) for sid in SCENARIOS}


def _get_client():
    """Lazy-load Anthropic client."""
    import anthropic
    api_key = get_secret("ANTHROPIC_API_KEY")
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY not set")
    return anthropic.Anthropic(api_key=api_key)


# ---------------------------------------------------------------------------
# Article scoring — LLM rates each article against all 5 scenarios
# ---------------------------------------------------------------------------

_SIGNAL_SYSTEM = """You are a geopolitical intelligence analyst. You assess how news articles
affect the probability of 5 Middle East/Hormuz crisis scenarios for Indian oil markets.

Scenarios (with preconditions that must hold and invalidators that eliminate the scenario):
{scenarios}

SCORING RULES:
- If an article CONFIRMS a precondition, assign moderate positive signal (+0.3 to +0.6)
- If an article UNDERMINES a precondition, assign moderate negative signal (-0.3 to -0.6)
- If an article matches an INVALIDATOR, assign strong negative signal (-0.7 to -1.0)
- If an article is neutral to a scenario, assign near-zero signal (-0.1 to +0.1)

For each article, return a JSON object with scenario_id keys and objects containing:
- "signal": float from -1.0 (strongly reduces probability) to +1.0 (strongly increases)
- "reasoning": 1 sentence citing which precondition/invalidator is affected

Return ONLY valid JSON, no markdown fences."""

_SIGNAL_USER = """Rate these articles against all 5 scenarios.
Return a JSON array where each element has "article_id" and "signals" (object with scenario_id keys).

Articles:
{articles}"""


def analyze_articles(horizon=DEFAULT_HORIZON, batch_size=10):
    """Score unscored articles against all scenarios via LLM. Returns count scored."""
    articles = get_unscored_articles(horizon, limit=50)
    if not articles:
        logger.info("No unscored articles found")
        return 0

    scenario_desc = "\n".join(
        f"- {sid}: {s['name']} — {s['description']}\n"
        f"  Preconditions: {'; '.join(s.get('preconditions', []))}\n"
        f"  Invalidators: {'; '.join(s.get('invalidators', []))}"
        for sid, s in SCENARIOS.items()
    )

    client = _get_client()
    total_scored = 0

    for i in range(0, len(articles), batch_size):
        batch = articles[i:i + batch_size]
        articles_text = "\n".join(
            f"[ID:{a['id']}] {a['title']}"
            + (f" — {a['summary'][:200]}" if a.get('summary') else "")
            for a in batch
        )

        try:
            response = client.messages.create(
                model=MODEL,
                max_tokens=4096,
                system=_SIGNAL_SYSTEM.format(scenarios=scenario_desc),
                messages=[{"role": "user", "content": _SIGNAL_USER.format(articles=articles_text)}],
            )

            text = response.content[0].text.strip()
            if "```" in text:
                text = text.split("```")[1]
                if text.lower().startswith("json"):
                    text = text[4:]
                text = text.strip()

            results = json.loads(text)
            if isinstance(results, dict) and "article_id" in results:
                results = [results]

            rows = []
            for r in results:
                art_id = r.get("article_id")
                signals = r.get("signals", {})
                for sid, data in signals.items():
                    if sid in SCENARIOS:
                        if isinstance(data, (int, float)):
                            sig = max(-1.0, min(1.0, float(data)))
                            reasoning = ""
                        else:
                            sig = max(-1.0, min(1.0, float(data.get("signal", 0))))
                            reasoning = data.get("reasoning", "")
                        rows.append((art_id, sid, sig, reasoning, horizon))

            if rows:
                insert_article_signals_bulk(rows)
                total_scored += len(batch)
                logger.info(f"Scored {len(batch)} articles ({len(rows)} signals)")

        except Exception as e:
            logger.error(f"Batch scoring failed: {e}")

    return total_scored


# ---------------------------------------------------------------------------
# Weight computation — Bayesian update from article signals
# ---------------------------------------------------------------------------

def compute_weights(horizon=DEFAULT_HORIZON, generate_narratives=True):
    """Compute scenario probabilities from article signals.

    Returns dict {scenario_id: probability} summing to 1.0.
    If generate_narratives=True, also generates and stores LLM narratives.
    """
    with get_connection(readonly=True) as conn:
        rows = conn.execute(
            """SELECT s.scenario_id, AVG(s.signal) as avg_signal, COUNT(*) as cnt
               FROM article_signals s
               WHERE s.horizon = ?
               GROUP BY s.scenario_id""",
            (horizon,),
        ).fetchall()

    if not rows:
        logger.warning(f"No signals for horizon={horizon}, using prior weights")
        weights = dict(PRIOR_WEIGHTS)
    else:
        # Softmax-style: convert avg signals to probabilities
        import math
        signal_map = {r["scenario_id"]: r["avg_signal"] for r in rows}
        total_articles = sum(r["cnt"] for r in rows) // len(SCENARIOS) if rows else 0

        # Ensure all scenarios have a score (prior = 0 signal)
        scores = {}
        for sid in SCENARIOS:
            scores[sid] = signal_map.get(sid, 0.0)

        # Softmax with temperature
        temperature = 0.5
        exp_scores = {sid: math.exp(s / temperature) for sid, s in scores.items()}
        total_exp = sum(exp_scores.values())
        weights = {sid: exp_scores[sid] / total_exp for sid in SCENARIOS}

    # Compute expected values
    ev = _compute_ev(SCENARIOS, weights, horizon)

    # Record prediction for accuracy tracking
    try:
        from database.db_manager import insert_accuracy_snapshot
        insert_accuracy_snapshot(
            snapshot_date=datetime.utcnow().strftime("%Y-%m-%d"),
            horizon=horizon,
            weights=weights,
            ev_oil=ev["oil"],
            ev_grm=ev["grm"],
        )
    except Exception as e:
        logger.warning(f"Accuracy snapshot failed (non-fatal): {e}")

    ranges = _compute_ranges(SCENARIOS, horizon)

    if generate_narratives:
        try:
            narrative_data = generate_strategic_narrative(horizon, weights, ev, ranges, SCENARIOS)
            assessment_data = generate_scenario_assessments(weights, SCENARIOS, horizon)

            article_count = 0
            with get_connection(readonly=True) as conn:
                r = conn.execute(
                    "SELECT COUNT(DISTINCT article_id) as cnt FROM article_signals WHERE horizon=?",
                    (horizon,),
                ).fetchone()
                if r:
                    article_count = r["cnt"]

            insert_scenario_narrative(
                horizon=horizon,
                narrative=narrative_data.get("narrative", ""),
                oil_expl=narrative_data.get("oil_explanation", ""),
                grm_expl=narrative_data.get("grm_explanation", ""),
                stock_expl=narrative_data.get("stock_explanation", ""),
                assessments=assessment_data,
                weights=weights,
                count=article_count,
            )
            logger.info(f"Stored narrative + assessments for horizon={horizon}")
        except Exception as e:
            logger.error(f"Narrative generation failed: {e}")

    return weights


def _compute_ev(scenarios, weights, horizon):
    """Compute probability-weighted expected values for KPIs."""
    ev = {"oil": 0.0, "grm": 0.0, "stock": 0.0}
    for sid, w in weights.items():
        kpis = scenarios[sid]["horizons"].get(horizon, scenarios[sid]["horizons"]["3m"])
        for k in ev:
            ev[k] += w * kpis[k]
    return ev


def _compute_ranges(scenarios, horizon):
    """Compute min/max ranges across all scenarios."""
    oils = [s["horizons"].get(horizon, s["horizons"]["3m"])["oil"] for s in scenarios.values()]
    grms = [s["horizons"].get(horizon, s["horizons"]["3m"])["grm"] for s in scenarios.values()]
    stocks = [s["horizons"].get(horizon, s["horizons"]["3m"])["stock"] for s in scenarios.values()]
    return {
        "oil": (min(oils), max(oils)),
        "grm": (min(grms), max(grms)),
        "stock": (min(stocks), max(stocks)),
    }


# ---------------------------------------------------------------------------
# Strategic narrative generation — LLM synthesizes across articles
# ---------------------------------------------------------------------------

_NARRATIVE_SYSTEM = """You are a McKinsey senior partner. Ultra-concise. Every word earns its place.
Rules:
- HTML bullet list (<ul><li>). EXACTLY 3 bullets for narrative, max 15 words each.
- KPI explanations: 1 sentence, max 12 words. State the causal mechanism only.
- No filler. No hedging. No "it is worth noting" or "given that".
- Lead with the insight. Be specific: name countries, %, mechanisms.
- Bullet 1: What's happening now. Bullet 2: Key risk/driver. Bullet 3: So-what for Indian refiners."""

_NARRATIVE_USER = """Scenario probabilities: {weights}
Expected values: Brent ${oil_ev:.0f}/bbl (range {oil_range}), GRM ${grm_ev:.1f}/bbl (range {grm_range})

Top signals:
{articles}

Return ONLY valid JSON with exactly these keys:
- "narrative": HTML <ul><li> with EXACTLY 3 bullets. Max 15 words per bullet. No sub-bullets.
- "oil_explanation": 1 sentence, max 12 words. The causal mechanism only.
- "grm_explanation": 1 sentence, max 12 words. The causal mechanism only.

No markdown fences. No preamble."""


def generate_strategic_narrative(horizon, weights, ev, ranges, scenarios):
    """Generate strategic narrative + KPI explanations via LLM."""
    top_articles = get_top_articles_across_scenarios(limit=20)

    def _fmt_signals(sigs):
        parts = []
        for s in sigs[:3]:
            parts.append(f"{s['scenario_id']}={s['signal']:+.2f}")
        return ", ".join(parts)

    articles_text = "\n".join(
        f"- {a['title']} (signals: {_fmt_signals(a.get('signals', []))})"
        for a in top_articles
    )

    weights_text = ", ".join(
        f"{scenarios[sid]['name']}: {w:.0%}" for sid, w in weights.items()
    )

    client = _get_client()
    response = client.messages.create(
        model=MODEL,
        max_tokens=1024,
        system=_NARRATIVE_SYSTEM,
        messages=[{"role": "user", "content": _NARRATIVE_USER.format(
            weights=weights_text,
            oil_ev=ev["oil"], grm_ev=ev["grm"],
            oil_range=f"${ranges['oil'][0]}-{ranges['oil'][1]}",
            grm_range=f"${ranges['grm'][0]}-{ranges['grm'][1]}",
            articles=articles_text or "No articles scored yet.",
        )}],
    )

    text = response.content[0].text.strip()
    if "```" in text:
        text = text.split("```")[1]
        if text.lower().startswith("json"):
            text = text[4:]
        text = text.strip()

    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        data = {"narrative": text, "oil_explanation": "", "grm_explanation": "", "stock_explanation": ""}

    return data


# ---------------------------------------------------------------------------
# Per-scenario LLM assessments
# ---------------------------------------------------------------------------

_ASSESSMENT_SYSTEM = """You are a McKinsey senior partner. For each scenario, write exactly 1 sentence explaining
the probability — cite the specific signal or event driving it. No filler. Every word earns its place."""

_ASSESSMENT_USER = """Scenarios with probabilities and evidence:
{scenario_details}

Return ONLY valid JSON: {{"scenario_id": "one sharp sentence", ...}}
No markdown fences."""


def generate_scenario_assessments(weights, scenarios, horizon=DEFAULT_HORIZON):
    """Generate per-scenario LLM assessment explaining probability. Returns dict."""
    top_articles = get_top_articles_across_scenarios(limit=15)

    details = []
    for sid, s in scenarios.items():
        kpis = s["horizons"].get(horizon, s["horizons"]["3m"])
        art_list = [a for a in top_articles if any(
            sig["scenario_id"] == sid and abs(sig["signal"]) > 0.2
            for sig in a.get("signals", [])
        )]
        art_text = "; ".join(a["title"] for a in art_list[:3]) if art_list else "No strong signals"
        details.append(
            f"- {sid} ({s['name']}): {weights.get(sid, 0.2):.0%} probability. "
            f"Oil ${kpis['oil']}, GRM ${kpis['grm']}, Stock {kpis['stock']:+.0f}%. "
            f"Key articles: {art_text}"
        )

    client = _get_client()
    response = client.messages.create(
        model=MODEL,
        max_tokens=2048,
        system=_ASSESSMENT_SYSTEM,
        messages=[{"role": "user", "content": _ASSESSMENT_USER.format(
            scenario_details="\n".join(details),
        )}],
    )

    text = response.content[0].text.strip()
    if "```" in text:
        text = text.split("```")[1]
        if text.lower().startswith("json"):
            text = text[4:]
        text = text.strip()

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        logger.error(f"Could not parse assessments JSON: {text[:200]}")
        return {}


# ---------------------------------------------------------------------------
# Dynamic scenario price update — LLM extracts analyst consensus from recent news
# ---------------------------------------------------------------------------

_PRICE_SYSTEM = """You are a commodity pricing analyst. Given recent news articles about oil markets,
extract any mentioned analyst price targets, bank forecasts, or consensus estimates for Brent crude.

For each of the 5 scenarios below, estimate what the current analyst consensus Brent price would be,
based on the evidence in these articles. Use the BASE prices as defaults if articles provide no
relevant update for a scenario.

Scenarios and current base prices:
{scenario_prices}

Rules:
- Only adjust a scenario's price if articles contain SPECIFIC evidence (bank forecast, analyst quote, price target)
- Adjustments should be within +/-15% of the base price
- If no relevant evidence, keep the base price
- Return prices as integers (USD/bbl)"""

_PRICE_USER = """Recent articles (last 12-18 hours):
{articles}

Current Brent spot: ${current_brent}/bbl

Return ONLY valid JSON with this exact structure:
{{
    "scenario_id": {{"oil_1m": int, "oil_3m": int, "oil_6m": int, "reasoning": "1 sentence"}},
    ...for each scenario...
}}
No markdown fences."""


def update_scenario_prices_from_consensus():
    """Use LLM to extract analyst consensus and update scenario oil prices.

    Reads recent articles, asks LLM to extract any price targets/forecasts,
    and adjusts scenario oil prices accordingly. Returns dict of updated prices.
    """
    from database.db_manager import get_recent_articles_with_signals

    recent = get_recent_articles_with_signals(hours=18, limit=30)
    if not recent:
        logger.info("No recent articles for price consensus update")
        return None

    articles_text = "\n".join(
        f"- {a['title']}" + (f" — {a.get('summary', '')[:150]}" if a.get('summary') else "")
        for a in recent
    )

    basket, _, brent = get_indian_basket_price()

    scenario_prices_text = "\n".join(
        f"- {sid} ({s['name']}): " + ", ".join(f"{h}=${s['horizons'][h]['oil']}" for h in HORIZONS)
        for sid, s in SCENARIOS.items()
    )

    try:
        client = _get_client()
        response = client.messages.create(
            model=MODEL,
            max_tokens=2048,
            system=_PRICE_SYSTEM.format(scenario_prices=scenario_prices_text),
            messages=[{"role": "user", "content": _PRICE_USER.format(
                articles=articles_text,
                current_brent=brent or 80,
            )}],
        )

        text = response.content[0].text.strip()
        if "```" in text:
            text = text.split("```")[1]
            if text.lower().startswith("json"):
                text = text[4:]
            text = text.strip()

        updates = json.loads(text)
        applied = {}

        for sid, data in updates.items():
            if sid not in SCENARIOS:
                continue

            applied_data = {"reasoning": data.get("reasoning", "")}
            for h in HORIZONS:
                base = SCENARIOS[sid]["horizons"][h]["oil"]
                new_val = data.get(f"oil_{h}", base)
                # Clamp to +/-15% of base to prevent hallucinated extremes
                new_val = max(int(base * 0.85), min(int(base * 1.15), int(new_val)))
                SCENARIOS[sid]["horizons"][h]["oil"] = new_val
                applied_data[f"oil_{h}"] = new_val

            # Recalculate GRM based on updated oil price for each horizon
            for h in HORIZONS:
                crude = SCENARIOS[sid]["horizons"][h]["oil"]
                products = SCENARIOS[sid]["products"][h]
                grm = sum(
                    GRM_WEIGHTS.get(p, 0) * (products.get(p, 0) - crude)
                    for p in _PRODUCT_LIST
                )
                SCENARIOS[sid]["horizons"][h]["grm"] = round(grm, 1)
                applied_data[f"grm_{h}"] = SCENARIOS[sid]["horizons"][h]["grm"]

            applied[sid] = applied_data

        logger.info(f"Updated scenario prices from consensus: {len(applied)} scenarios adjusted")
        return applied

    except Exception as e:
        logger.error(f"Consensus price update failed: {e}")
        return None


# ---------------------------------------------------------------------------
# Momentum calculation — 12h signal avg vs 7-day baseline
# ---------------------------------------------------------------------------

def compute_momentum(horizon=DEFAULT_HORIZON):
    """Compare 12h average signal per scenario vs 7-day baseline.

    Returns dict {scenario_id: {"direction": "rising"|"falling"|"stable",
                                 "delta": float, "recent_avg": float, "baseline_avg": float}}
    """
    recent_signals = get_signals_for_window(hours=12)
    baseline_signals = get_signals_for_window(hours=168)  # 7 days

    # Group by scenario
    recent_by_scenario = defaultdict(list)
    baseline_by_scenario = defaultdict(list)

    cutoff_12h = (datetime.utcnow() - timedelta(hours=12)).isoformat()

    for s in baseline_signals:
        sid = s["scenario_id"]
        baseline_by_scenario[sid].append(s["signal"])
        created = s.get("created_at", "")
        if created >= cutoff_12h:
            recent_by_scenario[sid].append(s["signal"])

    # For recent, also include the directly fetched 12h signals
    for s in recent_signals:
        sid = s["scenario_id"]
        if sid not in recent_by_scenario or s["signal"] not in recent_by_scenario[sid]:
            recent_by_scenario[sid].append(s["signal"])

    result = {}
    for sid in SCENARIOS:
        recent_avg = sum(recent_by_scenario[sid]) / len(recent_by_scenario[sid]) if recent_by_scenario[sid] else 0.0
        baseline_avg = sum(baseline_by_scenario[sid]) / len(baseline_by_scenario[sid]) if baseline_by_scenario[sid] else 0.0
        delta = recent_avg - baseline_avg

        if abs(delta) < 0.05:
            direction = "stable"
        elif delta > 0:
            direction = "rising"
        else:
            direction = "falling"

        result[sid] = {
            "direction": direction,
            "delta": round(delta, 3),
            "recent_avg": round(recent_avg, 3),
            "baseline_avg": round(baseline_avg, 3),
        }

    return result


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    print("=== Scenario Engine ===")
    print(f"Scenarios: {', '.join(s['name'] for s in SCENARIOS.values())}")

    print("\n1. Scoring unscored articles...")
    scored = analyze_articles()
    print(f"   Scored {scored} articles")

    for h in HORIZONS:
        print(f"\n2. Computing weights for horizon={h}...")
        weights = compute_weights(h, generate_narratives=True)
        for sid, w in sorted(weights.items(), key=lambda x: -x[1]):
            print(f"   {SCENARIOS[sid]['name']:25s} {w:6.1%}")

        ev = _compute_ev(SCENARIOS, weights, h)
        print(f"   EV: Oil ${ev['oil']:.0f}/bbl | GRM ${ev['grm']:.1f}/bbl | Stock {ev['stock']:+.0f}%")

    print("\n3. Computing momentum...")
    momentum = compute_momentum()
    for sid, m in momentum.items():
        arrow = {"rising": "^", "falling": "v", "stable": "-"}[m["direction"]]
        print(f"   {SCENARIOS[sid]['short']:10s} [{arrow}] delta={m['delta']:+.3f}")

    print("\n4. Checking stored narrative...")
    n = get_latest_scenario_narrative("3m")
    if n:
        print(f"   Narrative ({len(n.get('narrative',''))} chars), weights: {n.get('weight_snapshot')}")
    else:
        print("   No narrative stored yet")

    print("\nDone.")
