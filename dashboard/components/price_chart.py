"""Professional dark-themed Plotly chart components."""

import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
from dashboard.components.theme import (
    SERIES_COLORS, BG_CARD, BG_ELEVATED, BORDER_SUBTLE,
    TEXT_PRIMARY, TEXT_SECONDARY, TEXT_MUTED,
    EMERALD, CORAL, GOLD, CYAN, TEAL, AMBER,
    apply_theme, PLOTLY_CONFIG,
)


def line_chart(df, x_col, y_cols, title="", y_title="Price (USD/bbl)",
               height=420, show_range_selector=False, annotate_last=True, fill_area=False):
    """Professional multi-line chart with annotations and crosshairs."""
    fig = go.Figure()
    if isinstance(y_cols, list):
        y_cols = {col: col for col in y_cols}

    for i, (col, name) in enumerate(y_cols.items()):
        if col not in df.columns:
            continue
        color = SERIES_COLORS[i % len(SERIES_COLORS)]
        fig.add_trace(go.Scatter(
            x=df[x_col], y=df[col], name=name,
            line=dict(color=color, width=2.2),
            fill="tozeroy" if (fill_area and i == 0) else None,
            fillcolor=f"rgba({int(color[1:3],16)},{int(color[3:5],16)},{int(color[5:7],16)},0.06)"
                if (fill_area and i == 0) else None,
            hovertemplate=f"<b>{name}</b>: %{{y:,.2f}}<extra></extra>",
        ))
        # Annotate latest value
        if annotate_last:
            series = df[col].dropna()
            if not series.empty:
                last_val = series.iloc[-1]
                last_x = df[x_col].iloc[series.index[-1]]
                fig.add_annotation(
                    x=last_x, y=last_val, text=f"  {last_val:,.2f}",
                    showarrow=False, xanchor="left",
                    font=dict(color=color, size=11), bgcolor="rgba(0,0,0,0.5)", borderpad=3,
                )

    fig.update_layout(title=title, yaxis_title=y_title)
    if "USD" in y_title or "Price" in y_title:
        fig.update_layout(yaxis_tickprefix="$")
    apply_theme(fig, height=height, show_range_selector=show_range_selector)
    return fig


def bar_chart(df, x_col, y_col, title="", color_col=None, height=400,
              horizontal=False, show_values=True, color_scale=None):
    """Dark-themed bar chart."""
    orientation = "h" if horizontal else "v"
    kw = dict(x=y_col if horizontal else x_col, y=x_col if horizontal else y_col,
              title=title, orientation=orientation, height=height)
    if color_col and color_scale:
        fig = px.bar(df, **kw, color=color_col, color_continuous_scale=color_scale)
    elif color_col:
        fig = px.bar(df, **kw, color=color_col, color_discrete_sequence=SERIES_COLORS)
    else:
        fig = px.bar(df, **kw)
        fig.update_traces(marker_color=CYAN)
    if show_values:
        fig.update_traces(texttemplate="%{value:,.1f}", textposition="outside",
                          textfont=dict(size=10, color=TEXT_SECONDARY))
    apply_theme(fig, height=height)
    return fig


def waterfall_chart(categories, values, title="", height=420):
    """Waterfall chart for GRM / balance breakdown."""
    measure = ["relative"] * (len(categories) - 1) + ["total"]
    fig = go.Figure(go.Waterfall(
        x=categories, y=values, measure=measure,
        connector=dict(line=dict(color="rgba(255,255,255,0.1)", width=1, dash="dot")),
        increasing=dict(marker=dict(color=EMERALD)),
        decreasing=dict(marker=dict(color=CORAL)),
        totals=dict(marker=dict(color=GOLD)),
        textposition="outside",
        text=[f"${v:+.2f}" if isinstance(v, (int, float)) else "" for v in values],
        textfont=dict(size=11, color=TEXT_PRIMARY),
    ))
    fig.update_layout(title=title, yaxis_title="USD/bbl", showlegend=False)
    apply_theme(fig, height=height)
    return fig


def treemap_chart(df, path_cols, values_col, title="", height=500):
    """Dark-themed treemap."""
    fig = px.treemap(df, path=path_cols, values=values_col, title=title,
                     color=values_col, color_continuous_scale=["#0E1117", "#00D4AA"], height=height)
    fig.update_traces(textfont=dict(color=TEXT_PRIMARY, size=12), marker=dict(cornerradius=4),
                      hovertemplate="<b>%{label}</b><br>Value: %{value:,.0f}<br>Share: %{percentParent:.1%}<extra></extra>")
    apply_theme(fig, height=height)
    fig.update_layout(margin=dict(l=8, r=8, t=50, b=8))
    return fig


def scatter_chart(df, x_col, y_col, color_col=None, size_col=None,
                  title="", hover_cols=None, height=420):
    """Themed scatter plot."""
    color_map = {"bullish": EMERALD, "bearish": CORAL, "neutral": TEXT_MUTED}
    fig = px.scatter(df, x=x_col, y=y_col, color=color_col, size=size_col,
                     title=title, hover_data=hover_cols, height=height,
                     color_discrete_map=color_map if color_col else None)
    fig.update_traces(marker=dict(line=dict(width=0.5, color="rgba(255,255,255,0.1)")))
    apply_theme(fig, height=height)
    return fig


def gauge_chart(value, title="", min_val=0, max_val=100, threshold=90, height=260):
    """Dark-themed gauge for utilization rates."""
    if value >= threshold:
        bar_color = CORAL
    elif value >= max_val * 0.7:
        bar_color = AMBER
    else:
        bar_color = EMERALD

    fig = go.Figure(go.Indicator(
        mode="gauge+number", value=value,
        number=dict(font=dict(color=TEXT_PRIMARY, size=32), suffix="%"),
        title=dict(text=title, font=dict(color=TEXT_SECONDARY, size=13)),
        gauge=dict(
            axis=dict(range=[min_val, max_val], tickfont=dict(color=TEXT_MUTED, size=10)),
            bar=dict(color=bar_color, thickness=0.75),
            bgcolor=BG_CARD, borderwidth=1, bordercolor="rgba(255,255,255,0.06)",
            steps=[
                dict(range=[min_val, max_val * 0.6], color="rgba(16,185,129,0.06)"),
                dict(range=[max_val * 0.6, max_val * 0.85], color="rgba(245,158,11,0.06)"),
                dict(range=[max_val * 0.85, max_val], color="rgba(239,68,68,0.06)"),
            ],
            threshold=dict(line=dict(color=CORAL, width=2), thickness=0.8, value=threshold),
        ),
    ))
    fig.update_layout(height=height, paper_bgcolor=BG_ELEVATED, plot_bgcolor=BG_ELEVATED,
                      font=dict(color=TEXT_SECONDARY), margin=dict(l=25, r=25, t=55, b=15))
    return fig


def donut_chart(labels, values, title="", center_text="", height=420, colors=None):
    """Donut chart (replaces pie charts)."""
    fig = go.Figure(go.Pie(
        labels=labels, values=values, hole=0.55,
        marker=dict(colors=colors or SERIES_COLORS[:len(labels)],
                    line=dict(color=BG_ELEVATED, width=2)),
        textinfo="percent+label", textfont=dict(size=11, color=TEXT_PRIMARY),
        hovertemplate="<b>%{label}</b><br>Value: %{value:,.0f}<br>Share: %{percent}<extra></extra>",
    ))
    annotations = []
    if center_text:
        annotations.append(dict(text=center_text, x=0.5, y=0.5, font_size=15,
                                font_color=TEXT_PRIMARY, showarrow=False))
    fig.update_layout(title=title, height=height, annotations=annotations)
    apply_theme(fig, height=height)
    return fig
