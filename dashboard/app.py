"""Oil & Gas Intelligence Dashboard - Main Streamlit App."""

import os, sys
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import streamlit as st
from datetime import datetime, timezone
from database.db_manager import init_db
from dashboard.data_access import cached_latest_scrape
from dashboard.components.theme import GLOBAL_CSS, EMERALD, CORAL, AMBER, TEXT_MUTED, TEXT_DIM

init_db()

st.set_page_config(
    page_title="Oil & Gas Intelligence",
    page_icon="oil_drum",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(GLOBAL_CSS, unsafe_allow_html=True)

# --- Sidebar ---
with st.sidebar:
    st.markdown("""
    <div style="text-align:center;padding:1rem 0 1.5rem;border-bottom:1px solid rgba(255,255,255,0.06);margin-bottom:1rem;">
        <div style="font-size:2rem;margin-bottom:0.3rem;">&#9981;</div>
        <div style="font-size:1.1rem;font-weight:800;color:#F9FAFB;letter-spacing:-0.01em;">OIL & GAS</div>
        <div style="font-size:0.65rem;color:#00D4AA;text-transform:uppercase;letter-spacing:0.15em;font-weight:600;">
            Intelligence Platform</div>
    </div>""", unsafe_allow_html=True)

    NAV_ITEMS = {
        "Morning Brief": "&#9749;",
        "Overview": "&#128202;", "Crude Prices": "&#128738;", "Indian Refineries": "&#127981;",
        "Products & Cracks": "&#128200;", "Trade Flows": "&#128674;",
        "News & Impact": "&#128240;", "Geopolitical & Global": "&#127758;",
    }
    page = st.radio("NAVIGATION", list(NAV_ITEMS.keys()), index=0,
                     format_func=lambda x: f"{NAV_ITEMS[x]}  {x}")

    st.divider()

    # Data freshness indicator
    last_scrape = cached_latest_scrape()
    if last_scrape:
        scrape_time = last_scrape.get("started_at", "")
        try:
            scrape_dt = datetime.fromisoformat(str(scrape_time))
            age_hours = (datetime.now() - scrape_dt).total_seconds() / 3600
        except Exception:
            age_hours = 999

        if age_hours < 1:
            fc, fl = EMERALD, "LIVE"
        elif age_hours < 6:
            fc, fl = AMBER, "RECENT"
        elif age_hours < 24:
            fc, fl = CORAL, "STALE"
        else:
            fc, fl = CORAL, "OUTDATED"

        st.markdown(f"""
        <div style="background:rgba(255,255,255,0.03);border-radius:8px;padding:0.7rem;
            border:1px solid rgba(255,255,255,0.06);margin-bottom:0.5rem;">
            <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:4px;">
                <span style="font-size:0.68rem;color:{TEXT_MUTED};text-transform:uppercase;
                    letter-spacing:0.08em;font-weight:600;">Data Status</span>
                <span style="font-size:0.62rem;font-weight:700;color:{fc};
                    background:{fc}15;padding:2px 7px;border-radius:4px;border:1px solid {fc}40;">
                    &#9679; {fl}</span>
            </div>
            <div style="font-size:0.72rem;color:{TEXT_MUTED};">Last: {str(scrape_time)[:16]}</div>
        </div>""", unsafe_allow_html=True)
    else:
        st.warning("No data loaded. Click Refresh.")

    if st.button("Refresh Data", type="primary", use_container_width=True):
        with st.spinner("Refreshing all data sources..."):
            try:
                from processing.data_processor import full_refresh
                results = full_refresh()
                st.cache_data.clear()
                ok = sum(1 for r in results.values() if r.get("status") == "success")
                st.success(f"Done! {ok}/{len(results)} sources OK.")
                st.rerun()
            except Exception as e:
                st.error(f"Refresh failed: {e}")

    st.divider()
    st.caption("Sources: EIA, OilPrice, PPAC, OPEC, Google News")

# --- Page Router ---
if page == "Morning Brief":
    from dashboard.pages.p00_morning_brief import render
elif page == "Overview":
    from dashboard.pages.p01_overview import render
elif page == "Crude Prices":
    from dashboard.pages.p02_crude_prices import render
elif page == "Indian Refineries":
    from dashboard.pages.p03_indian_refineries import render
elif page == "Products & Cracks":
    from dashboard.pages.p04_products_cracks import render
elif page == "Trade Flows":
    from dashboard.pages.p05_trade_flows import render
elif page == "News & Impact":
    from dashboard.pages.p06_news_impact import render
elif page == "Geopolitical & Global":
    from dashboard.pages.p07_global_view import render

render()
