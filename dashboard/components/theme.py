"""Centralized dark executive theme for the Oil & Gas Dashboard."""

# -- Background & Surface --
BG_PRIMARY = "#0E1117"
BG_CARD = "#1A1F2E"
BG_ELEVATED = "#151A28"
BORDER_SUBTLE = "rgba(255,255,255,0.06)"

# -- Text --
TEXT_PRIMARY = "#F9FAFB"
TEXT_SECONDARY = "#9CA3AF"
TEXT_MUTED = "#6B7280"
TEXT_DIM = "#4B5563"

# -- Accent Palette --
TEAL = "#00D4AA"
CYAN = "#00D4FF"
BLUE = "#3B82F6"
EMERALD = "#10B981"
CORAL = "#EF4444"
AMBER = "#F59E0B"
GOLD = "#F0B429"
PURPLE = "#8B5CF6"

# -- Series Colors for Charts --
SERIES_COLORS = [CYAN, TEAL, AMBER, CORAL, PURPLE, "#EC4899", "#14B8A6", "#F97316"]

# -- Plotly base layout for dark theme --
PLOTLY_LAYOUT = dict(
    paper_bgcolor=BG_ELEVATED,
    plot_bgcolor=BG_ELEVATED,
    font=dict(family="Inter, system-ui, -apple-system, sans-serif", color=TEXT_SECONDARY, size=12),
    title=dict(font=dict(color=TEXT_PRIMARY, size=15), x=0, xanchor="left", pad=dict(l=10)),
    legend=dict(
        bgcolor="rgba(0,0,0,0)", bordercolor=BORDER_SUBTLE,
        font=dict(color=TEXT_SECONDARY, size=11),
        orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1,
    ),
    margin=dict(l=55, r=25, t=55, b=45),
    xaxis=dict(
        gridcolor="rgba(255,255,255,0.04)", linecolor="rgba(255,255,255,0.08)",
        zerolinecolor="rgba(255,255,255,0.08)",
        tickfont=dict(color=TEXT_MUTED, size=10),
        showspikes=True, spikecolor="#374151", spikethickness=1, spikedash="dot", spikemode="across",
    ),
    yaxis=dict(
        gridcolor="rgba(255,255,255,0.04)", linecolor="rgba(255,255,255,0.08)",
        zerolinecolor="rgba(255,255,255,0.08)",
        tickfont=dict(color=TEXT_MUTED, size=10),
        showspikes=True, spikecolor="#374151", spikethickness=1, spikedash="dot", spikemode="across",
    ),
    hoverlabel=dict(bgcolor="#1F2937", bordercolor="rgba(255,255,255,0.1)",
                    font=dict(color=TEXT_PRIMARY, size=12)),
    hovermode="x unified",
)


def apply_theme(fig, height=400, show_range_selector=False):
    """Apply executive dark theme to any Plotly figure."""
    fig.update_layout(**PLOTLY_LAYOUT, height=height)
    if show_range_selector:
        fig.update_layout(xaxis_rangeselector=dict(
            buttons=[
                dict(count=7, label="1W", step="day", stepmode="backward"),
                dict(count=1, label="1M", step="month", stepmode="backward"),
                dict(count=3, label="3M", step="month", stepmode="backward"),
                dict(count=6, label="6M", step="month", stepmode="backward"),
                dict(count=1, label="1Y", step="year", stepmode="backward"),
                dict(label="ALL", step="all"),
            ],
            bgcolor=BG_CARD, activecolor=CYAN,
            font=dict(color=TEXT_PRIMARY, size=10),
        ))
    return fig


PLOTLY_CONFIG = {
    "displayModeBar": True,
    "modeBarButtonsToRemove": ["lasso2d", "select2d"],
    "toImageButtonOptions": {"format": "png", "filename": "oil_dashboard_chart", "scale": 2},
    "displaylogo": False,
}


# -- Global CSS for Streamlit --
GLOBAL_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');

.block-container { padding-top: 1.5rem !important; padding-bottom: 1rem !important; max-width: 100% !important; }

[data-testid="stAppViewContainer"] { font-family: 'Inter', -apple-system, sans-serif; }
[data-testid="stHeader"] { background-color: rgba(0,0,0,0); }

[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #0A0E1A 0%, #111827 100%);
    border-right: 1px solid rgba(0, 212, 170, 0.15);
}

[data-testid="stMetric"] {
    background: linear-gradient(135deg, #1A1F2E 0%, #151A28 100%);
    border: 1px solid rgba(255, 255, 255, 0.06);
    border-radius: 12px; padding: 1rem 1.2rem;
    box-shadow: 0 4px 16px rgba(0, 0, 0, 0.3);
}
[data-testid="stMetric"]:hover {
    border-color: rgba(0, 212, 170, 0.2);
    box-shadow: 0 8px 24px rgba(0, 0, 0, 0.4);
}
[data-testid="stMetric"] label { color: #9CA3AF !important; font-size: 0.72rem !important;
    text-transform: uppercase; letter-spacing: 0.1em; font-weight: 600; }
[data-testid="stMetric"] [data-testid="stMetricValue"] {
    font-size: 1.5rem !important; font-weight: 700; color: #F9FAFB !important;
    font-variant-numeric: tabular-nums; }
[data-testid="stMetric"] [data-testid="stMetricDelta"] { font-size: 0.8rem !important; font-weight: 600; }

h1 { font-weight: 800 !important; letter-spacing: -0.02em; color: #F9FAFB !important; }
h2 { color: #E5E7EB !important; font-weight: 700 !important; font-size: 1.3rem !important;
     border-bottom: 2px solid rgba(0, 212, 170, 0.3); padding-bottom: 0.4rem; margin-top: 1.5rem !important; }
h3 { color: #D1D5DB !important; font-weight: 600 !important; font-size: 1.1rem !important; }
hr { border-color: rgba(255, 255, 255, 0.06) !important; }

[data-testid="stDataFrame"] { border: 1px solid rgba(255,255,255,0.06); border-radius: 8px; overflow: hidden; }

[data-testid="stPlotlyChart"] {
    border: 1px solid rgba(255,255,255,0.04); border-radius: 12px;
    padding: 0.5rem; background: #151A28;
}

.stButton > button[kind="primary"] {
    background: linear-gradient(135deg, #00D4AA 0%, #00B894 100%);
    border: none; font-weight: 600; border-radius: 8px;
}
.stButton > button[kind="primary"]:hover {
    box-shadow: 0 4px 16px rgba(0, 212, 170, 0.3);
}

::-webkit-scrollbar { width: 6px; height: 6px; }
::-webkit-scrollbar-track { background: #0E1117; }
::-webkit-scrollbar-thumb { background: #374151; border-radius: 3px; }
::-webkit-scrollbar-thumb:hover { background: #4B5563; }
</style>
"""
