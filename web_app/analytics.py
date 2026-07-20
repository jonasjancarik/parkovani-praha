from typing import Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from constants import PARKER_DEFAULT, PARKER_DEFAULT_AGG, PARKER_MEASURES


def lod_include_avg(
    df: pd.DataFrame,
    group_dims: List[str],
    measure_cols: Iterable[str],
) -> pd.DataFrame:
    cols = list(measure_cols)
    grouped = df.groupby(group_dims + ["kod_useku"])[cols].mean().reset_index()
    return grouped.groupby(group_dims)[cols].sum().reset_index()


def add_annual_total_line(
    fig: go.Figure,
    series: pd.DataFrame,
    value_cols: str | Iterable[str],
    label: str = "Celkem",
) -> go.Figure:
    """Add a total line with one readable value label per year."""
    cols = [value_cols] if isinstance(value_cols, str) else list(value_cols)
    totals = (
        series.groupby("date", as_index=False)[cols]
        .sum()
        .sort_values("date")
        .reset_index(drop=True)
    )
    if totals.empty:
        return fig

    totals["total"] = totals[cols].sum(axis=1)
    totals["label"] = ""
    annual_first_rows = totals.groupby(totals["date"].dt.year).head(1).index
    latest_year = totals.iloc[-1]["date"].year
    annual_first_rows = annual_first_rows[
        totals.loc[annual_first_rows, "date"].dt.year != latest_year
    ]
    annual_first_rows = annual_first_rows[
        totals.loc[annual_first_rows, "total"] != 0
    ]
    totals.loc[annual_first_rows, "label"] = totals.loc[
        annual_first_rows, "total"
    ].map(lambda value: f"{value:,.0f}".replace(",", " "))
    totals.loc[totals.index[-1], "label"] = f"{totals.iloc[-1]['total']:,.0f}".replace(
        ",", " "
    )
    totals["hover_label"] = totals["total"].map(
        lambda value: f"{value:,.0f}".replace(",", " ")
    )
    totals["marker_size"] = totals["label"].map(lambda value: 6 if value else 0)

    fig.add_trace(
        go.Scatter(
            x=totals["date"],
            y=totals["total"],
            mode="lines+markers+text",
            name=label,
            line=dict(width=3, color="#5D3A9B"),
            marker=dict(size=totals["marker_size"], color="#5D3A9B"),
            text=totals["label"],
            textposition="top center",
            textfont=dict(size=12),
            cliponaxis=False,
            customdata=totals["hover_label"],
            hovertemplate="%{x|%d.%m.%Y}<br>Celkem: %{customdata}<extra></extra>",
        )
    )
    return fig


def zsj_annual_change(
    df: pd.DataFrame,
    date_range: Tuple[pd.Timestamp, pd.Timestamp],
    mestska_cast: Optional[str],
    naz_zsj: Optional[str],
    cutoff_pct: Optional[float] = None,
    stable_months: int = 3,
) -> pd.DataFrame:
    start_date, end_date = date_range
    mask = (df["date"] >= start_date) & (df["date"] <= end_date)
    if mestska_cast and mestska_cast != "All":
        mask &= df["mestska_cast"] == mestska_cast
    if naz_zsj and naz_zsj != "All":
        mask &= df["naz_zsj"] == naz_zsj
    scoped = df.loc[mask]
    if scoped.empty:
        return scoped

    results = []
    grouped = scoped.groupby(["kod_zsj", "naz_zsj", "mestska_cast"])
    for (kod_zsj, naz_zsj, mc), group in grouped:
        series = (
            group.groupby("date")[["POP_CELKEM", "parkovacich_mist_v_zps"]]
            .sum()
            .reset_index()
            .sort_values("date")
        )
        if len(series) < 2:
            continue
        start_method = "first_pop"
        start_idx = None
        if cutoff_pct is not None:
            spaces = series["parkovacich_mist_v_zps"].astype(float)
            prev = spaces.shift(1)
            pct_change = (spaces - prev).abs() / prev
            pct_change = pct_change.fillna(float("inf"))
            stable = pct_change <= cutoff_pct
            jump_idxs = pct_change[pct_change > cutoff_pct].index.tolist()
            candidate = jump_idxs[-1] + 1 if jump_idxs else 0
            if stable_months <= 1:
                if candidate < len(series) and stable.iloc[candidate]:
                    start_idx = candidate
                else:
                    stable_idxs = stable[stable].index.tolist()
                    stable_idxs = [i for i in stable_idxs if i >= candidate]
                    start_idx = stable_idxs[0] if stable_idxs else None
            else:
                window = stable.rolling(stable_months).apply(
                    lambda x: 1 if x.all() else 0, raw=True
                )
                stable_idxs = window[window == 1].index.tolist()
                stable_idxs = [
                    i for i in stable_idxs if i >= candidate + (stable_months - 1)
                ]
                if stable_idxs:
                    start_idx = stable_idxs[0] - (stable_months - 1)
            if start_idx is not None:
                positive_after = series.loc[start_idx:, "POP_CELKEM"] > 0
                if positive_after.any():
                    start_idx = positive_after[positive_after].index[0]
                    start_method = "stable_after_jump"
                else:
                    start_idx = None

        if start_idx is None:
            positive = series[series["POP_CELKEM"] > 0]
            if positive.empty:
                continue
            start_idx = positive.index[0]
            start_method = "first_pop"

        if start_idx >= len(series) - 1:
            continue

        start = series.loc[start_idx]
        end = series.iloc[-1]
        days = (end["date"] - start["date"]).days
        if days <= 0:
            continue
        annual_change = (end["POP_CELKEM"] - start["POP_CELKEM"]) / days * 365
        if start["POP_CELKEM"] == 0:
            percent_change = None
        else:
            percent_change = (end["POP_CELKEM"] - start["POP_CELKEM"]) / start[
                "POP_CELKEM"
            ]
        start_spaces = start["parkovacich_mist_v_zps"]
        end_spaces = end["parkovacich_mist_v_zps"]
        if start_spaces and start_spaces > 0:
            start_ratio = start["POP_CELKEM"] / start_spaces
        else:
            start_ratio = None
        if end_spaces and end_spaces > 0:
            end_ratio = end["POP_CELKEM"] / end_spaces
        else:
            end_ratio = None
        if start_ratio is None or end_ratio is None:
            ratio_delta = None
            ratio_pct = None
        else:
            ratio_delta = end_ratio - start_ratio
            ratio_pct = ratio_delta / start_ratio if start_ratio else None

        results.append(
            {
                "kod_zsj": kod_zsj,
                "naz_zsj": naz_zsj,
                "mestska_cast": mc,
                "start_method": start_method,
                "start_date": start["date"].date(),
                "end_date": end["date"].date(),
                "start_pop": start["POP_CELKEM"],
                "end_pop": end["POP_CELKEM"],
                "delta_pop": end["POP_CELKEM"] - start["POP_CELKEM"],
                "annual_change": annual_change,
                "percent_change": percent_change,
                "start_spaces": start_spaces,
                "end_spaces": end_spaces,
                "pop_per_space_start": start_ratio,
                "pop_per_space_end": end_ratio,
                "pop_per_space_delta": ratio_delta,
                "pop_per_space_pct": ratio_pct,
                "days": days,
            }
        )

    return pd.DataFrame(results)


def forecast_area_series(
    series_df: pd.DataFrame,
    horizon_months: int,
    window_months: int,
) -> Dict[str, Dict[str, pd.DataFrame]]:
    results: Dict[str, Dict[str, pd.DataFrame]] = {}
    for area, group in series_df.groupby("mestska_cast"):
        group = group.sort_values("date")
        if len(group) < 2:
            continue
        window = group.tail(window_months)
        y = window["pop_per_space"].to_numpy()
        x = np.arange(len(y))
        if len(x) < 2:
            continue
        slope, intercept = np.polyfit(x, y, 1)
        y_hat = slope * x + intercept
        resid = y - y_hat
        std = resid.std(ddof=1) if len(resid) > 1 else 0.0
        future_x = np.arange(len(y), len(y) + horizon_months)
        pred = slope * future_x + intercept
        last_date = group["date"].max()
        future_dates = pd.date_range(
            last_date + pd.offsets.MonthEnd(1),
            periods=horizon_months,
            freq="M",
        )
        forecast = pd.DataFrame(
            {
                "date": future_dates,
                "pop_per_space": pred,
                "lower": pred - std,
                "upper": pred + std,
            }
        )
        results[area] = {"actual": group, "forecast": forecast}
    return results


def build_area_forecast_figure(
    series_df: pd.DataFrame,
    horizon_months: int,
    window_months: int,
    max_cols: int = 5,
) -> go.Figure:
    forecasts = forecast_area_series(series_df, horizon_months, window_months)
    areas = sorted(forecasts.keys())
    if not areas:
        return go.Figure()
    cols = min(max_cols, len(areas))
    rows = int(np.ceil(len(areas) / cols))
    fig = make_subplots(
        rows=rows,
        cols=cols,
        subplot_titles=areas,
        shared_yaxes=True,
        horizontal_spacing=0.04,
        vertical_spacing=0.08,
    )
    for idx, area in enumerate(areas):
        row = idx // cols + 1
        col = idx % cols + 1
        actual = forecasts[area]["actual"]
        forecast = forecasts[area]["forecast"]
        showlegend = idx == 0
        fig.add_trace(
            go.Scatter(
                x=actual["date"],
                y=actual["pop_per_space"],
                mode="lines",
                name="Skutečnost",
                line=dict(color="#1f1c17"),
                showlegend=showlegend,
            ),
            row=row,
            col=col,
        )
        fig.add_trace(
            go.Scatter(
                x=forecast["date"],
                y=forecast["pop_per_space"],
                mode="lines",
                name="Predikce",
                line=dict(color="#1f1c17", dash="dash"),
                showlegend=showlegend,
            ),
            row=row,
            col=col,
        )
        fig.add_trace(
            go.Scatter(
                x=forecast["date"],
                y=forecast["upper"],
                mode="lines",
                line=dict(width=0),
                showlegend=False,
                hoverinfo="skip",
            ),
            row=row,
            col=col,
        )
        fig.add_trace(
            go.Scatter(
                x=forecast["date"],
                y=forecast["lower"],
                mode="lines",
                line=dict(width=0),
                fill="tonexty",
                fillcolor="rgba(31, 28, 23, 0.12)",
                showlegend=False,
                hoverinfo="skip",
            ),
            row=row,
            col=col,
        )
    fig.update_layout(height=240 * rows, margin=dict(l=20, r=20, t=40, b=20))
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(gridcolor="rgba(0,0,0,0.08)")
    return fig




def style_figure(fig: go.Figure) -> go.Figure:
    fig.update_layout(
        margin=dict(l=20, r=20, t=40, b=20),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="var(--plotly-text-color)"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
    )
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(gridcolor="var(--plotly-grid-color)")
    return fig


def parker_labels(detail_view: bool) -> List[str]:
    if detail_view:
        excluded = {"navstevnici"}
    else:
        excluded = {"navstevnici_platici", "navstevnici_neplatici", "prenosna"}
    return [label for label, col in PARKER_MEASURES.items() if col not in excluded]


def parker_default_labels(detail_view: bool) -> List[str]:
    default_cols = PARKER_DEFAULT if detail_view else PARKER_DEFAULT_AGG
    return [label for label, col in PARKER_MEASURES.items() if col in default_cols]
