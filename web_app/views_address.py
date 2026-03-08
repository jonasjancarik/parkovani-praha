import os

import pandas as pd
import plotly.express as px
import streamlit as st
from shapely.geometry import Point

from analytics import style_figure
from constants import POP_MEASURES
from data import radius_latest_snapshot, radius_spaces_series
from geo import extract_lon_lat, geocode_with_mapy_cz

MATCH_LABELS = {
    "inside": "uvnitř zóny",
    "nearest": "nejbližší úsek",
    "none": "bez shody",
}


def format_int(value: float) -> str:
    return f"{value:,.0f}".replace(",", " ")


def safe_total(series: pd.Series) -> float:
    if series.empty:
        return 0.0
    numeric = pd.to_numeric(series, errors="coerce")
    return float(numeric.fillna(0).sum())


def build_zone_hits_table(
    snapshot: pd.DataFrame,
    zone_hits,
) -> pd.DataFrame:
    distances = {zone.code: distance_m for zone, distance_m in zone_hits}
    table = snapshot.copy()
    table["vzdalenost_m"] = (
        table["kod_useku"].map(distances).fillna(0).round().astype(int)
    )
    table = table.rename(
        columns={
            "kod_useku": "Úsek",
            "naz_zsj": "ZSJ",
            "mestska_cast": "MČ",
            "typ_zony": "Typ zóny",
            "parkovacich_mist_v_zps": "Místa v ZPS",
            "vzdalenost_m": "Vzdálenost (m)",
        }
    )
    return table.sort_values(["Vzdálenost (m)", "Úsek"]).reset_index(drop=True)


def render_radius_insight(
    data: pd.DataFrame,
    address_result: dict,
    zone_index,
) -> None:
    point = Point(address_result["lon"], address_result["lat"])
    cast_dne_addr = address_result["cast_dne"]
    radius_m = int(address_result["radius_m"])
    zone_hits = zone_index.find_zones_within_radius(point, radius_m)

    st.markdown(f"### Okruh {radius_m} m")
    if not zone_hits:
        st.warning("V daném okruhu nejsou žádné zóny ZPS.")
        return

    zone_codes = [zone.code for zone, _ in zone_hits]
    series = radius_spaces_series(data, zone_codes, cast_dne_addr)
    latest_snapshot = radius_latest_snapshot(data, zone_codes, cast_dne_addr)

    if series.empty or latest_snapshot.empty:
        st.warning("Pro vybraný okruh nejsou dostupná data o parkovacích místech.")
        return

    latest_spaces = safe_total(series.iloc[[-1]]["parkovacich_mist_v_zps"])
    oldest_spaces = safe_total(series.iloc[[0]]["parkovacich_mist_v_zps"])
    spaces_delta = latest_spaces - oldest_spaces

    metric_cols = st.columns(3)
    metric_cols[0].metric("Úseků v okruhu", format_int(len(zone_hits)))
    metric_cols[1].metric("Místa v ZPS", format_int(latest_spaces))
    metric_cols[2].metric("Změna od začátku", format_int(spaces_delta))

    fig_spaces = px.line(
        series,
        x="date",
        y="parkovacich_mist_v_zps",
        markers=True,
        labels={
            "date": "Datum",
            "parkovacich_mist_v_zps": "Místa v ZPS",
        },
    )
    fig_spaces.update_traces(
        line=dict(color="#006d77", width=3),
        marker=dict(color="#d1495b", size=7),
    )
    fig_spaces.update_layout(showlegend=False, yaxis_title="Místa v ZPS")
    st.plotly_chart(style_figure(fig_spaces), use_container_width=True)

    st.dataframe(
        build_zone_hits_table(latest_snapshot, zone_hits),
        hide_index=True,
        use_container_width=True,
    )


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
        radius_m = st.slider(
            "Okruh (m)",
            min_value=100,
            max_value=1500,
            value=500,
            step=100,
        )
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
            "radius_m": radius_m,
            "cast_dne": cast_dne_addr,
        }

    address_result = st.session_state.get("address_result")
    if not address_result:
        st.info("Zadej adresu a spusť vyhledání.")
        st.stop()

    zone_code = address_result["zone_code"]
    cast_dne_addr = address_result["cast_dne"]
    match_type = address_result["match_type"]
    zsj_meta = zsj_mapping.get(zone_code, {})
    st.markdown(
        f"**Adresa:** {address_result['label']}  \n"
        f"**Referenční úsek:** {zone_code} ({MATCH_LABELS.get(match_type, match_type)})  \n"
        f"**ZSJ:** {zsj_meta.get('kod_zsj', 'nezname')} - {zsj_meta.get('naz_zsj', 'nezname')}  \n"
        f"**Okruh:** {address_result['radius_m']} m"
    )

    render_radius_insight(data, address_result, zone_index)

    st.markdown("### Detail referenčního úseku")
    zone_data = data[(data["kod_useku"] == zone_code)]
    zone_data = zone_data[zone_data["cast_dne"] == cast_dne_addr]
    if zone_data.empty:
        zone_data = data[data["kod_useku"] == zone_code]

    oldest_date = zone_data["date"].min()
    newest_date = zone_data["date"].max()
    oldest_row = zone_data[zone_data["date"] == oldest_date]
    newest_row = zone_data[zone_data["date"] == newest_date]

    permits_delta = safe_total(newest_row["POP_CELKEM"]) - safe_total(
        oldest_row["POP_CELKEM"]
    )
    spaces_delta = safe_total(newest_row["parkovacich_mist_v_zps"]) - safe_total(
        oldest_row["parkovacich_mist_v_zps"]
    )

    metric_cols = st.columns(2)
    metric_cols[0].metric("Změna oprávnění", format_int(permits_delta))
    metric_cols[1].metric("Změna míst v ZPS", format_int(spaces_delta))

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
