"""News impact tagger using keyword-based scoring with negation detection."""

import os
import re
import logging
import yaml

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from database.db_manager import get_connection

logger = logging.getLogger(__name__)

KEYWORDS_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "config", "keywords.yaml")

# Negation patterns that invert meaning
NEGATION_PATTERNS = [
    r"no\s+", r"not\s+", r"without\s+", r"lack\s+of\s+",
    r"unlikely\s+", r"rules?\s+out\s+", r"deny\s+", r"denies\s+",
]


def load_keywords():
    with open(KEYWORDS_PATH, "r") as f:
        config = yaml.safe_load(f)
    return config["impact_categories"], config.get("scoring", {})


def score_text(title, summary, categories, scoring_config):
    """Score a text against all impact categories.

    Returns (impact_tag, impact_score, impact_category).
    - Title matches count 2x (headlines are more indicative)
    - Negation detection inverts keyword matches
    """
    title_lower = (title or "").lower()
    summary_lower = (summary or "").lower()
    full_text = f"{title_lower} {summary_lower}"
    max_matches = scoring_config.get("max_keyword_matches", 3)

    best_category = None
    best_score = 0
    best_abs_score = 0

    for cat_name, cat_config in categories.items():
        base_score = cat_config["base_score"]
        matches = 0
        for keyword in cat_config["keywords"]:
            kw_lower = keyword.lower()
            pattern = r'\b' + re.escape(kw_lower) + r'\b'
            found_in_title = bool(re.search(pattern, title_lower))
            found_in_summary = bool(re.search(pattern, summary_lower))

            if found_in_title or found_in_summary:
                # Check negation in each text segment independently
                title_negated = False
                summary_negated = False
                for neg_pat in NEGATION_PATTERNS:
                    neg_check = neg_pat + r"(?:\w+\s+){0,2}" + re.escape(kw_lower)
                    if found_in_title and re.search(neg_check, title_lower):
                        title_negated = True
                    if found_in_summary and re.search(neg_check, summary_lower):
                        summary_negated = True

                # Score title and summary independently
                if found_in_title:
                    matches += -2 if title_negated else 2
                if found_in_summary and not found_in_title:
                    matches += -1 if summary_negated else 1

        if matches != 0:
            score = base_score * min(abs(matches), max_matches) / max_matches
            if matches < 0:
                score = -score

            if abs(score) > best_abs_score:
                best_abs_score = abs(score)
                best_score = score
                best_category = cat_name

    if best_category is None:
        return "neutral", 0.0, "none"  # "none" sentinel so IS NOT NULL, avoids re-processing

    if best_score > 0:
        tag = "bullish"
    elif best_score < 0:
        tag = "bearish"
    else:
        tag = "neutral"

    return tag, round(best_score, 3), best_category


def tag_all_untagged():
    """Tag all news articles that haven't been tagged yet (impact_category is NULL)."""
    categories, scoring_config = load_keywords()
    count = 0

    with get_connection() as conn:
        rows = conn.execute(
            "SELECT id, title, summary FROM news_articles WHERE impact_category IS NULL"
        ).fetchall()

        for row in rows:
            title = row["title"] or ""
            summary = row["summary"] or ""
            tag, score, category = score_text(title, summary, categories, scoring_config)

            conn.execute(
                "UPDATE news_articles SET impact_tag=?, impact_score=?, impact_category=? WHERE id=?",
                (tag, score, category, row["id"]),
            )
            count += 1

    logger.info(f"Tagged {count} articles")
    return count


def retag_all():
    """Re-tag ALL articles (not just untagged). Use after keyword list changes."""
    categories, scoring_config = load_keywords()
    count = 0

    with get_connection() as conn:
        rows = conn.execute(
            "SELECT id, title, summary FROM news_articles"
        ).fetchall()

        for row in rows:
            title = row["title"] or ""
            summary = row["summary"] or ""
            tag, score, category = score_text(title, summary, categories, scoring_config)

            conn.execute(
                "UPDATE news_articles SET impact_tag=?, impact_score=?, impact_category=? WHERE id=?",
                (tag, score, category, row["id"]),
            )
            count += 1

    logger.info(f"Re-tagged {count} articles")
    return count


def tag_article(title, summary=""):
    """Tag a single article. Returns (tag, score, category)."""
    categories, scoring_config = load_keywords()
    return score_text(title, summary, categories, scoring_config)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    count = tag_all_untagged()
    print(f"Tagged {count} articles")
