import os

import pandas as pd
import plotly.express as px
import streamlit as st
from shapely.geometry import Point

from analytics import style_figure
from constants import POP_MEASURES
from geo import extract_lon_lat, geocode_with_mapy_cz


def render_address_view(
    data: pd.DataFrame,
    zone_index,
    zsj_mapping,
    cast_dne_values,
):
    st.subheader("Address insight")
    api_key = os.getenv("MAPY_CZ_API_KEY")
    if not api_key:
        st.warning("MAPY_CZ_API_KEY chybí v prostředí.")

    with st.form("address_form"):
        address = st.text_input("Adresa v Praze", value="")
        cast_dne_addr = st.selectbox(
            "Část dne",
            cast_dne_values,
            index=0,
        )
        submitted = st.form_submit_button("Najít adresu")

    if submitted:
        result = geocode_with_mapy_cz(address)
        if not result:
            st.error("Adresa nenalezena nebo chyba Mapy.cz.")
            st.stop()
        coords = extract_lon_lat(result)
        if not coords:
            st.error("Adresa bez souřadnic.")
            st.stop()
        lon, lat = coords
        zone, match_type = zone_index.find_zone(Point(lon, lat))
        if not zone:
            st.error("Nenalezen žádný úsek pro souřadnice.")
            st.stop()
        st.session_state["address_result"] = {
            "label": result.get("label") or address,
            "lon": lon,
            "lat": lat,
            "zone_code": zone.code,
            "match_type": match_type,
        }

    address_result = st.session_state.get("address_result")
    if not address_result:
        st.info("Zadej adresu a spusť vyhledání.")
        st.stop()

    zone_code = address_result["zone_code"]
    match_type = address_result["match_type"]
    zsj_meta = zsj_mapping.get(zone_code, {})
    st.markdown(
        f"**Adresa:** {address_result['label']}  \n"
        f"**Úsek:** {zone_code} ({match_type})  \n"
        f"**ZSJ:** {zsj_meta.get('kod_zsj', 'nezname')} - {zsj_meta.get('naz_zsj', 'nezname')}"
    )

    zone_data = data[(data["kod_useku"] == zone_code)]
    zone_data = zone_data[zone_data["cast_dne"] == cast_dne_addr]
    if zone_data.empty:
        zone_data = data[data["kod_useku"] == zone_code]

    oldest_date = zone_data["date"].min()
    newest_date = zone_data["date"].max()
    oldest_row = zone_data[zone_data["date"] == oldest_date]
    newest_row = zone_data[zone_data["date"] == newest_date]

    def safe_delta(series: pd.Series) -> float:
        return float(series.sum()) if not series.empty else 0.0

    permits_delta = safe_delta(newest_row["POP_CELKEM"]) - safe_delta(
        oldest_row["POP_CELKEM"]
    )
    spaces_delta = safe_delta(newest_row["parkovacich_mist_v_zps"]) - safe_delta(
        oldest_row["parkovacich_mist_v_zps"]
    )

    st.metric("Změna oprávnění", f"{permits_delta:,.0f}")
    st.metric("Změna míst v ZPS", f"{spaces_delta:,.0f}")

    series = (
        zone_data.groupby("date")[["parkovacich_mist_v_zps", "POP_CELKEM"]]
        .sum()
        .reset_index()
    )
    fig_spaces = px.line(
        series,
        x="date",
        y=["parkovacich_mist_v_zps", "POP_CELKEM"],
    )
    st.plotly_chart(style_figure(fig_spaces), use_container_width=True)

    occ_series = (
        zone_data.groupby("date")[["obsazenost", "respektovanost"]]
        .mean()
        .reset_index()
    )
    fig_occ = px.line(
        occ_series,
        x="date",
        y=["obsazenost", "respektovanost"],
    )
    st.plotly_chart(style_figure(fig_occ), use_container_width=True)

    zone_type_cols = [
        "rezidentska",
        "vlastnicka",
        "abonentska",
        "prenosna",
        "navstevnici_platici",
        "navstevnici_neplatici",
        "volna_mista",
    ]
    nav_detail_zone = st.radio(
        "Návštěvníci (adresa)",
        ["Detail", "Agregát"],
        horizontal=True,
        index=1,
        key="nav_mode_addr",
    )
    if nav_detail_zone == "Agregát":
        zone_type_cols = [
            "rezidentska",
            "vlastnicka",
            "abonentska",
            "navstevnici",
            "volna_mista",
        ]
    zone_types = zone_data.groupby("date")[zone_type_cols].sum().reset_index()
    fig_zone_types = px.area(
        zone_types,
        x="date",
        y=zone_type_cols,
    )
    st.plotly_chart(style_figure(fig_zone_types), use_container_width=True)

    pop_cols = list(POP_MEASURES.values())
    zone_pop = zone_data.groupby("date")[pop_cols].mean().reset_index()
    fig_zone_pop = px.area(
        zone_pop,
        x="date",
        y=pop_cols,
    )
    st.plotly_chart(style_figure(fig_zone_pop), use_container_width=True)
