"""PDF table extraction utility using pdfplumber."""

import os
import logging
import pdfplumber
import pandas as pd

logger = logging.getLogger(__name__)


def extract_tables_from_pdf(pdf_path, pages=None):
    """Extract all tables from a PDF file.

    Args:
        pdf_path: Path to PDF file
        pages: List of page numbers (0-indexed) or None for all pages

    Returns:
        List of pandas DataFrames
    """
    if not os.path.exists(pdf_path):
        logger.error(f"PDF not found: {pdf_path}")
        return []

    tables = []
    try:
        with pdfplumber.open(pdf_path) as pdf:
            page_range = pages if pages else range(len(pdf.pages))
            for page_num in page_range:
                if page_num >= len(pdf.pages):
                    continue
                page = pdf.pages[page_num]
                page_tables = page.extract_tables()
                for table in page_tables:
                    if table and len(table) > 1:
                        # Use first row as header
                        df = pd.DataFrame(table[1:], columns=table[0])
                        df = df.dropna(how="all")
                        if not df.empty:
                            tables.append(df)
                            logger.debug(f"Extracted table from page {page_num + 1}: {df.shape}")
    except Exception as e:
        logger.error(f"Error extracting tables from {pdf_path}: {e}")

    return tables


def extract_text_from_pdf(pdf_path, pages=None):
    """Extract text content from a PDF file."""
    if not os.path.exists(pdf_path):
        logger.error(f"PDF not found: {pdf_path}")
        return ""

    text_parts = []
    try:
        with pdfplumber.open(pdf_path) as pdf:
            page_range = pages if pages else range(len(pdf.pages))
            for page_num in page_range:
                if page_num >= len(pdf.pages):
                    continue
                page = pdf.pages[page_num]
                text = page.extract_text()
                if text:
                    text_parts.append(text)
    except Exception as e:
        logger.error(f"Error extracting text from {pdf_path}: {e}")

    return "\n".join(text_parts)


def find_table_with_keyword(tables, keyword):
    """Find the first table containing a specific keyword in any cell."""
    keyword_lower = keyword.lower()
    for df in tables:
        for col in df.columns:
            if keyword_lower in str(col).lower():
                return df
            for val in df[col].astype(str):
                if keyword_lower in val.lower():
                    return df
    return None


def clean_numeric(value):
    """Clean a string value to a float, handling common PDF artifacts."""
    if value is None:
        return None
    s = str(value).strip().replace(",", "").replace(" ", "")
    s = s.replace("–", "-").replace("−", "-")
    if not s or s == "-" or s.lower() in ("na", "n/a", "nil", ""):
        return None
    try:
        return float(s)
    except ValueError:
        return None
