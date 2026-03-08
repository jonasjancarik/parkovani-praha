import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from analytics import (
    build_area_forecast_figure,
    lod_include_avg,
    parker_default_labels,
    parker_labels,
    style_figure,
    zsj_annual_change,
)
from constants import PARKER_MEASURES, POP_MEASURES
from data import area_pop_per_space_series, zsj_pop_per_space_series


def render_tableau_view(
    data: pd.DataFrame,
    filtered: pd.DataFrame,
    permits_only: pd.DataFrame,
    date_range,
    typ_zony_values,
    mestska_filter,
    naz_zsj_filter,
):
    st.subheader("Podíl typu zón")
    if filtered.empty:
        st.info("Bez dat pro zvolené filtry.")
    else:
        zone_share = lod_include_avg(
            filtered, ["typ_zony"], ["parkovacich_mist_v_zps"]
        )
        fig_zone = px.pie(
            zone_share,
            values="parkovacich_mist_v_zps",
            names="typ_zony",
        )
        st.plotly_chart(style_figure(fig_zone), use_container_width=True)

    st.subheader("Podíl parkujících podle MČ")
    nav_detail = st.radio(
        "Návštěvníci",
        ["Detail", "Agregát"],
        horizontal=True,
        index=0,
        key="nav_mode_mc",
    )
    selected_parker = st.multiselect(
        "Typy parkujících (LOD)",
        parker_labels(nav_detail == "Detail"),
        default=parker_default_labels(nav_detail == "Detail"),
    )
    st.caption("Agregát nahrazuje platící/neplatící/přenosná, aby se nic nepočítalo 2×.")
    if filtered.empty or not selected_parker:
        st.info("Zvol filtry a typy parkujících.")
    else:
        selected_cols = [PARKER_MEASURES[label] for label in selected_parker]
        parkujici = lod_include_avg(filtered, ["typ_zony"], selected_cols)
        melted = parkujici.melt(
            id_vars=["typ_zony"],
            value_vars=selected_cols,
            var_name="measure",
            value_name="value",
        )
        label_map = {v: k for k, v in PARKER_MEASURES.items()}
        melted["label"] = melted["measure"].map(label_map)
        if melted["typ_zony"].nunique() > 1:
            fig_parkujici = px.pie(
                melted,
                values="value",
                names="label",
                facet_col="typ_zony",
            )
        else:
            fig_parkujici = px.pie(
                melted,
                values="value",
                names="label",
            )
        st.plotly_chart(style_figure(fig_parkujici), use_container_width=True)

    st.subheader("Vývoj počtu parkovacích míst")
    if filtered.empty:
        st.info("Bez dat pro zvolené filtry.")
    else:
        mist_series = lod_include_avg(
            filtered, ["date", "typ_zony"], ["parkovacich_mist_v_zps"]
        )
        fig_mist = px.area(
            mist_series,
            x="date",
            y="parkovacich_mist_v_zps",
            color="typ_zony",
        )
        st.plotly_chart(style_figure(fig_mist), use_container_width=True)

    st.subheader("Vývoj počtu parkovacích oprávnění")
    if filtered.empty:
        st.info("Bez dat pro zvolené filtry.")
    else:
        pop_selected = st.multiselect(
            "Typy POP (zobrazené)",
            list(POP_MEASURES.keys()),
            default=list(POP_MEASURES.keys()),
        )
        if not pop_selected:
            st.info("Vyber alespoň jeden typ POP.")
            st.stop()
        pop_cols = [POP_MEASURES[label] for label in pop_selected]
        pop_series = lod_include_avg(filtered, ["date"], pop_cols)
        pop_series["pop_main"] = pop_series[pop_cols].sum(axis=1)
        fig_pop = go.Figure()
        for col in pop_cols:
            label = [k for k, v in POP_MEASURES.items() if v == col][0]
            fig_pop.add_trace(
                go.Scatter(
                    x=pop_series["date"],
                    y=pop_series[col],
                    stackgroup="one",
                    mode="lines",
                    name=label,
                )
            )
        fig_pop.add_trace(
            go.Scatter(
                x=pop_series["date"],
                y=pop_series["pop_main"],
                mode="lines",
                name="POP Celkem (vybrané typy)",
                line=dict(width=3, color="#1f1c17"),
            )
        )
        st.plotly_chart(style_figure(fig_pop), use_container_width=True)

    st.subheader("Abs. počet parkujících")
    nav_detail_abs = st.radio(
        "Návštěvníci (vývoj)",
        ["Detail", "Agregát"],
        horizontal=True,
        index=0,
        key="nav_mode_abs",
    )
    selected_abs = st.multiselect(
        "Typy parkujících pro vývoj",
        parker_labels(nav_detail_abs == "Detail"),
        default=parker_default_labels(nav_detail_abs == "Detail"),
        key="abs_types",
    )
    st.caption("Agregát = návštěvníci dohromady.")
    if filtered.empty or not selected_abs:
        st.info("Zvol filtry a typy parkujících.")
    else:
        abs_cols = [PARKER_MEASURES[label] for label in selected_abs]
        abs_series = lod_include_avg(filtered, ["date"], abs_cols)
        fig_abs = px.area(
            abs_series,
            x="date",
            y=abs_cols,
        )
        st.plotly_chart(style_figure(fig_abs), use_container_width=True)

    st.subheader("ZSJ: průměrná roční změna počtu registrovaných vozidel")
    with st.expander("Stabilizace startu podle parkovacích míst", expanded=False):
        use_stable = st.checkbox("Použít stabilizaci", value=True)
        cutoff_pct = st.slider(
            "Max. meziměsíční změna parkovacích míst (%)",
            0,
            50,
            10,
        )
        stable_months = st.slider("Minimální počet stabilních měsíců", 1, 12, 3)
    if use_stable:
        start_note = (
            "Start = první měsíc s POP_CELKEM > 0 po posledním skoku v parkovacích místech."
        )
    else:
        start_note = "Start = první měsíc s POP_CELKEM > 0."
    st.caption(
        "Lineární přepočet z prvního a posledního data v rozsahu filtru. "
        f"{start_note} "
        "POP_CELKEM agregováno přes úseky, bez vlivu části dne."
    )
    cutoff_value = cutoff_pct / 100 if use_stable else None
    zsj_changes = zsj_annual_change(
        permits_only,
        date_range,
        mestska_filter,
        naz_zsj_filter,
        cutoff_pct=cutoff_value,
        stable_months=stable_months,
    )
    if zsj_changes.empty:
        st.info("Bez dat pro zvolené filtry.")
    else:
        fig_scatter = px.scatter(
            zsj_changes,
            x="start_pop",
            y="annual_change",
            color="mestska_cast",
            hover_name="naz_zsj",
            labels={"start_pop": "Start POP", "annual_change": "Roční změna"},
        )
        st.plotly_chart(style_figure(fig_scatter), use_container_width=True)

        st.subheader("Histogram změn")
        hist_metric = st.radio(
            "Metrika",
            ["POP na místo (%)", "POP celkem (%)"],
            horizontal=True,
            index=0,
        )
        if hist_metric == "POP na místo (%)":
            series = zsj_changes["pop_per_space_pct"].dropna() * 100
            title = "Procentuální změna POP na místo"
        else:
            series = zsj_changes["percent_change"].dropna() * 100
            title = "Procentuální změna POP celkem"
        if series.empty:
            st.info("Bez dat pro histogram.")
        else:
            with st.expander("Nastavení histogramu", expanded=False):
                min_val = float(series.min())
                max_val = float(series.max())
                hist_min = st.number_input(
                    "Min (%)",
                    value=min_val,
                    step=1.0,
                )
                hist_max = st.number_input(
                    "Max (%)",
                    value=max_val,
                    step=1.0,
                )
                bucket_size = st.number_input(
                    "Velikost bucketu (%)",
                    value=10.0,
                    min_value=1.0,
                    step=1.0,
                )
            if hist_min >= hist_max:
                st.warning("Min musí být menší než Max.")
            else:
                series = series[(series >= hist_min) & (series <= hist_max)]
                edges = []
                neg_start = -bucket_size
                while neg_start >= hist_min:
                    edges.append(neg_start)
                    neg_start -= bucket_size
                edges = sorted(edges)
                pos_start = 0.0
                while pos_start <= hist_max:
                    edges.append(pos_start)
                    pos_start += bucket_size
                if edges[0] > hist_min:
                    edges = [hist_min] + edges
                if edges[-1] < hist_max:
                    edges = edges + [hist_max]
                edges = sorted(set(edges))
                hist_df = pd.DataFrame({"Změna (%)": series})
                fig_hist = px.histogram(
                    hist_df,
                    x="Změna (%)",
                    nbins=len(edges) - 1,
                    title=title,
                )
                fig_hist.update_traces(
                    xbins=dict(start=edges[0], end=edges[-1], size=bucket_size)
                )
                st.plotly_chart(style_figure(fig_hist), use_container_width=True)

        display = zsj_changes.sort_values("annual_change", ascending=False)
        display["start_method"] = display["start_method"].replace(
            {"stable_after_jump": "stabilizace", "first_pop": "první POP"}
        )
        display = display[
            [
                "naz_zsj",
                "kod_zsj",
                "mestska_cast",
                "start_method",
                "annual_change",
                "percent_change",
                "delta_pop",
                "start_pop",
                "end_pop",
                "start_spaces",
                "end_spaces",
                "pop_per_space_start",
                "pop_per_space_end",
                "pop_per_space_delta",
                "pop_per_space_pct",
                "start_date",
                "end_date",
                "days",
            ]
        ]
        st.dataframe(
            display.style.format(
                {"percent_change": "{:.0%}", "pop_per_space_pct": "{:.0%}"},
                na_rep="",
            ),
            use_container_width=True,
        )

    st.subheader("ZSJ: POP na parkovací místo (vývoj)")
    st.caption("Agregace na úrovni ZSJ, část dne ignorována.")
    norm_mode = st.radio(
        "Normalizace",
        ["Surová hodnota", "Odchylka od startu (0)"],
        horizontal=True,
        index=0,
    )
    pop_series = zsj_pop_per_space_series(
        permits_only, date_range, mestska_filter, naz_zsj_filter
    )
    if pop_series.empty:
        st.info("Bez dat pro zvolené filtry.")
    else:
        if norm_mode == "Odchylka od startu (0)":
            pop_series = pop_series.sort_values("date")
            pop_series["baseline"] = pop_series.groupby("kod_zsj")[
                "pop_per_space"
            ].transform("first")
            pop_series["value"] = pop_series["pop_per_space"] - pop_series["baseline"]
            y_col = "value"
        else:
            y_col = "pop_per_space"
        fig_line = px.line(
            pop_series,
            x="date",
            y=y_col,
            color="naz_zsj",
            labels={"pop_per_space": "POP / místo", "value": "Změna od startu"},
        )
        st.plotly_chart(style_figure(fig_line), use_container_width=True)

    st.subheader("Oblast: oprávnění na počet stání (forecast)")
    with st.expander("Nastavení forecastu", expanded=False):
        default_types = [t for t in typ_zony_values if t in ("RES", "MIX")]
        area_types = st.multiselect(
            "Typy zón",
            typ_zony_values,
            default=default_types or typ_zony_values,
        )
        start_date = st.date_input(
            "Start (forecast)",
            value=date_range[0].date(),
            min_value=date_range[0].date(),
            max_value=date_range[1].date(),
        )
        origin_max = st.date_input(
            "Max datum vzniku úseku",
            value=date_range[1].date(),
            min_value=date_range[0].date(),
            max_value=date_range[1].date(),
        )
        window_months = st.slider("Okno trendu (měsíce)", 6, 60, 24)
        horizon_months = st.slider("Forecast (měsíce)", 3, 24, 12)
    area_series = area_pop_per_space_series(
        data,
        date_range,
        mestska_filter,
        area_types,
        origin_max=pd.Timestamp(origin_max),
    )
    if area_series.empty:
        st.info("Bez dat pro zvolené filtry.")
    else:
        area_series = area_series[area_series["date"] >= pd.Timestamp(start_date)]
        fig_area = build_area_forecast_figure(
            area_series, horizon_months, window_months
        )
        st.plotly_chart(style_figure(fig_area), use_container_width=True)
