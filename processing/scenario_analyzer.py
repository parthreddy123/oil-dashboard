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

SCENARIOS = {
    "quick_resolution": {
        "name": "Quick Resolution",
        "short": "Quick",
        "description": "Diplomatic breakthrough leads to rapid de-escalation within weeks. "
                       "Strait of Hormuz fully reopens, insurance premiums drop.",
        "horizons": {
            "3m":  {"oil": 68,  "grm": 11.5, "stock": 8},
            "6m":  {"oil": 65,  "grm": 12.0, "stock": 12},
            "12m": {"oil": 62,  "grm": 12.5, "stock": 15},
        },
    },
    "prolonged_standoff": {
        "name": "Prolonged Standoff",
        "short": "Prolong",
        "description": "Neither escalation nor resolution. Partial Hormuz disruption persists "
                       "with naval escort corridors. Elevated premiums, rerouted tankers.",
        "horizons": {
            "3m":  {"oil": 100, "grm": 7.0,  "stock": -12},
            "6m":  {"oil": 95,  "grm": 7.5,  "stock": -8},
            "12m": {"oil": 88,  "grm": 8.5,  "stock": -3},
        },
    },
    "conflagration": {
        "name": "Conflagration",
        "short": "Conflag",
        "description": "Full regional war with complete Hormuz closure, refinery attacks, "
                       "and global supply chain disruption.",
        "horizons": {
            "3m":  {"oil": 160, "grm": -2.0, "stock": -35},
            "6m":  {"oil": 140, "grm": 0.0,  "stock": -28},
            "12m": {"oil": 120, "grm": 3.0,  "stock": -18},
        },
    },
    "ceasefire": {
        "name": "Ceasefire & Talks",
        "short": "Ceasefire",
        "description": "Formal ceasefire with ongoing negotiations. Markets normalize "
                       "rapidly, crude flows resume, risk premiums evaporate.",
        "horizons": {
            "3m":  {"oil": 55,  "grm": 13.0, "stock": 18},
            "6m":  {"oil": 58,  "grm": 12.5, "stock": 15},
            "12m": {"oil": 60,  "grm": 12.0, "stock": 12},
        },
    },
    "regime_change": {
        "name": "Regime Change",
        "short": "Regime",
        "description": "Internal power shift in key state creates unpredictable transition. "
                       "Temporary disruption followed by uncertain new equilibrium.",
        "horizons": {
            "3m":  {"oil": 85,  "grm": 9.0,  "stock": -5},
            "6m":  {"oil": 80,  "grm": 9.5,  "stock": 0},
            "12m": {"oil": 75,  "grm": 10.0, "stock": 5},
        },
    },
}

HORIZONS = ["3m", "6m", "12m"]
SCENARIO_IDS = list(SCENARIOS.keys())
DEFAULT_HORIZON = "3m"

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

Scenarios:
{scenarios}

For each article, return a JSON object with scenario_id keys and objects containing:
- "signal": float from -1.0 (strongly reduces probability) to +1.0 (strongly increases)
- "reasoning": 1 sentence explaining why

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
        f"- {sid}: {s['name']} — {s['description']}"
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

_NARRATIVE_SYSTEM = """You are a senior geopolitical risk analyst advising Indian refinery executives.
Write concise, actionable strategic assessments about Middle East crisis impact on oil markets."""

_NARRATIVE_USER = """Given these scenario weights and top articles, produce a JSON response with:
1. "narrative": 3-5 sentence strategic assessment. What's happening? What does it mean for Reliance/Indian refiners? What to watch?
2. "oil_explanation": 1-2 sentences explaining the expected oil price (${oil_ev:.0f}/bbl)
3. "grm_explanation": 1-2 sentences explaining expected GRM (${grm_ev:.1f}/bbl)
4. "stock_explanation": 1-2 sentences explaining expected stock impact ({stock_ev:+.0f}%)

Scenario probabilities: {weights}

Expected values: Oil ${oil_ev:.0f}/bbl (range {oil_range}), GRM ${grm_ev:.1f}/bbl (range {grm_range}), Stock {stock_ev:+.0f}% (range {stock_range})

Top articles driving these assessments:
{articles}

Return ONLY valid JSON with keys: narrative, oil_explanation, grm_explanation, stock_explanation."""


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
            oil_ev=ev["oil"], grm_ev=ev["grm"], stock_ev=ev["stock"],
            oil_range=f"${ranges['oil'][0]}-{ranges['oil'][1]}",
            grm_range=f"${ranges['grm'][0]}-{ranges['grm'][1]}",
            stock_range=f"{ranges['stock'][0]:+.0f}% to {ranges['stock'][1]:+.0f}%",
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

_ASSESSMENT_SYSTEM = """You are a geopolitical risk analyst. For each scenario, explain in 2-3 sentences
why its probability is what it is, citing specific evidence from recent articles."""

_ASSESSMENT_USER = """Given these 5 scenarios and their computed probabilities, write a brief assessment
for each explaining the probability level.

{scenario_details}

Return ONLY valid JSON: {{"scenario_id": "assessment text", ...}}"""


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
