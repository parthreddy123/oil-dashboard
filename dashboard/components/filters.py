"""Executive-grade filter components for dashboard sidebar."""

import streamlit as st
from datetime import datetime, timedelta


def date_range_filter(key_prefix="", default_days=90, default_preset_index=2):
    presets = {"1W": 7, "1M": 30, "3M": 90, "6M": 180, "YTD": None, "1Y": 365, "Custom": -1}
    preset = st.selectbox("Time Period", list(presets.keys()), index=default_preset_index, key=f"{key_prefix}_preset")

    today = datetime.now().date()
    if preset == "YTD":
        start_date = today.replace(month=1, day=1)
        end_date = today
    elif preset == "Custom":
        c1, c2 = st.columns(2)
        with c1:
            start_date = st.date_input("From", today - timedelta(days=default_days), key=f"{key_prefix}_start")
        with c2:
            end_date = st.date_input("To", today, key=f"{key_prefix}_end")
    else:
        start_date = today - timedelta(days=presets[preset])
        end_date = today

    st.caption(f"{start_date.strftime('%b %d, %Y')} - {end_date.strftime('%b %d, %Y')}")
    return start_date, end_date


def impact_filter(key_prefix=""):
    return st.multiselect(
        "Impact Filter", ["bullish", "bearish", "neutral"],
        default=["bullish", "bearish", "neutral"], key=f"{key_prefix}_impact",
        format_func=lambda x: {"bullish": "Bullish +", "bearish": "Bearish -", "neutral": "Neutral ~"}[x],
    )


def company_filter(key_prefix=""):
    companies = ["All", "IOCL", "BPCL", "HPCL", "RIL", "MRPL", "Nayara", "CPCL", "ONGC"]
    selected = st.selectbox("Company", companies, key=f"{key_prefix}_company")
    return None if selected == "All" else selected


def benchmark_filter(key_prefix=""):
    labels = {
        "brent": "Brent", "oman_dubai": "Dubai/Oman",
        "wti": "WTI", "murban": "Murban (est.)",
        "opec_basket": "OPEC Basket (est.)", "indian_basket": "Indian Basket",
    }
    return st.multiselect(
        "Benchmarks", list(labels.keys()), default=["brent", "oman_dubai", "murban"],
        key=f"{key_prefix}_benchmark",
        format_func=lambda x: labels[x],
    )
