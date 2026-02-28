"""Claude API-powered PDF table extraction for PPAC reports."""

import os
import sys
import json
import base64
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from config.secrets_helper import get_secret

logger = logging.getLogger(__name__)


def _get_client():
    """Lazy-load Anthropic client."""
    import anthropic
    api_key = get_secret("ANTHROPIC_API_KEY")
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY not set")
    return anthropic.Anthropic(api_key=api_key)


def _read_pdf_pages(pdf_path, max_pages=10):
    """Read PDF pages as text using pdfplumber."""
    import pdfplumber
    pages_text = []
    with pdfplumber.open(pdf_path) as pdf:
        for i, page in enumerate(pdf.pages[:max_pages]):
            text = page.extract_text()
            if text:
                pages_text.append(f"--- Page {i+1} ---\n{text}")
    return "\n\n".join(pages_text)


def extract_refinery_data(pdf_path):
    """Extract Indian refinery throughput/capacity/utilization data from a PPAC PDF.

    Returns list of dicts: [{refinery, company, capacity_mmtpa, throughput_tmt, utilization_pct}, ...]
    """
    try:
        pdf_text = _read_pdf_pages(pdf_path)
        if not pdf_text or len(pdf_text) < 100:
            return []

        client = _get_client()
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=2000,
            messages=[{
                "role": "user",
                "content": f"""Extract refinery data from this PPAC report. Return ONLY valid JSON array.

Each entry should have: refinery (name), company (IOCL/BPCL/HPCL/RIL/MRPL/Nayara/CPCL/NRL/ONGC),
capacity_mmtpa (million metric tonnes per annum), throughput_tmt (thousand metric tonnes),
utilization_pct (percentage).

If a field is not available, use null. Skip total/subtotal rows.

PDF text:
{pdf_text[:8000]}

Return ONLY a JSON array like:
[{{"refinery": "Jamnagar DTA", "company": "RIL", "capacity_mmtpa": 33.0, "throughput_tmt": 2800, "utilization_pct": 101.8}}]"""
            }],
        )

        text = response.content[0].text.strip()
        # Extract JSON from response (handle markdown code blocks)
        if "```" in text:
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
            text = text.strip()

        data = json.loads(text)
        logger.info(f"Claude extracted {len(data)} refinery records from PDF")
        return data

    except Exception as e:
        logger.error(f"Claude refinery extraction failed: {e}")
        return []


def extract_trade_flows(pdf_path):
    """Extract crude oil import data by source country from PPAC PDF.

    Returns list of dicts: [{country, volume_tmt, value_musd}, ...]
    """
    try:
        pdf_text = _read_pdf_pages(pdf_path)
        if not pdf_text or len(pdf_text) < 100:
            return []

        client = _get_client()
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=2000,
            messages=[{
                "role": "user",
                "content": f"""Extract crude oil import data from this PPAC report. Return ONLY valid JSON array.

Each entry should have: country (source country name), volume_tmt (volume in thousand metric tonnes),
value_musd (value in million USD).

If a field is not available, use null. Skip total/subtotal rows.

PDF text:
{pdf_text[:8000]}

Return ONLY a JSON array like:
[{{"country": "Iraq", "volume_tmt": 1050, "value_musd": 6800}}]"""
            }],
        )

        text = response.content[0].text.strip()
        if "```" in text:
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
            text = text.strip()

        data = json.loads(text)
        logger.info(f"Claude extracted {len(data)} trade flow records from PDF")
        return data

    except Exception as e:
        logger.error(f"Claude trade flow extraction failed: {e}")
        return []


def extract_product_prices(pdf_path):
    """Extract petroleum product retail prices from PPAC PDF.

    Returns list of dicts: [{product, price, unit}, ...]
    """
    try:
        pdf_text = _read_pdf_pages(pdf_path)
        if not pdf_text or len(pdf_text) < 100:
            return []

        client = _get_client()
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=1000,
            messages=[{
                "role": "user",
                "content": f"""Extract petroleum product prices from this PPAC report. Return ONLY valid JSON array.

Products to look for: petrol (MS), diesel (HSD), ATF, LPG, kerosene (SKO), naphtha, fuel oil.
Each entry: product (lowercase key), price (numeric), unit (e.g. "INR/L" or "INR/cylinder").

PDF text:
{pdf_text[:8000]}

Return ONLY a JSON array like:
[{{"product": "petrol", "price": 94.72, "unit": "INR/L"}}]"""
            }],
        )

        text = response.content[0].text.strip()
        if "```" in text:
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
            text = text.strip()

        data = json.loads(text)
        logger.info(f"Claude extracted {len(data)} product prices from PDF")
        return data

    except Exception as e:
        logger.error(f"Claude product price extraction failed: {e}")
        return []
