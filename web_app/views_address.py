import os
from typing import Optional

import folium
import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st
from shapely.geometry import Point, mapping
from streamlit_folium import st_folium

from analytics import style_figure
from constants import POP_MEASURES
from data import radius_latest_snapshot, radius_spaces_series, zone_capacity_history
from geo import (
    extract_lon_lat,
    geocode_with_mapy_cz,
    reverse_geocode_with_mapy_cz,
)

DEFAULT_CENTER = (50.0755, 14.4378)
DEFAULT_ZOOM = 12
ADDRESS_RESULT_KEY = "address_result"
LAST_MAP_CLICK_KEY = "address_last_map_click"
ADDRESS_QUERY_KEY = "address_query"
ADDRESS_QUERY_PENDING_KEY = "address_query_pending"
ADDRESS_ERROR_KEY = "address_error"
GENERIC_GEOCODE_LABELS = {"Adresa"}
OCCUPANCY_PRESSURE_THRESHOLD = 0.85
LOW_RESPECT_THRESHOLD = 0.80

MATCH_LABELS = {
    "inside": "uvnitř zóny",
    "nearest": "nejbližší úsek",
    "none": "bez shody",
}


def format_int(value: float) -> str:
    return f"{value:,.0f}".replace(",", " ")


def format_signed_int(value: float) -> str:
    if value > 0:
        return f"+{format_int(value)}"
    if value < 0:
        return f"-{format_int(abs(value))}"
    return "0"


def safe_total(series: pd.Series) -> float:
    if series.empty:
        return 0.0
    numeric = pd.to_numeric(series, errors="coerce")
    return float(numeric.fillna(0).sum())


def format_ratio(value: Optional[float]) -> str:
    if value is None or pd.isna(value):
        return "n/a"
    return f"{value:.2f}"


def format_pct(value: Optional[float]) -> str:
    if value is None or pd.isna(value):
        return "n/a"
    return f"{value:.0%}"


def format_point_label(lon: float, lat: float) -> str:
    return f"Bod z mapy ({lat:.5f}, {lon:.5f})"


def format_geocode_result_label(
    result: Optional[dict],
    fallback: str,
) -> str:
    if not result:
        return fallback

    label = result.get("label")
    if label and label not in GENERIC_GEOCODE_LABELS:
        return str(label)

    name = result.get("name")
    if name:
        location = result.get("location")
        if location:
            locality = str(location).split(",")[0].strip()
            if locality and locality not in str(name):
                return f"{name}, {locality}"
        return str(name)

    return fallback


def resolve_point_result(
    lon: float,
    lat: float,
    radius_m: int,
    cast_dne: str,
    zone_index,
    label: Optional[str] = None,
    source: str = "map",
) -> Optional[dict]:
    zone, match_type = zone_index.find_zone(Point(lon, lat))
    if not zone:
        return None

    result_label = label
    if not result_label:
        reverse = reverse_geocode_with_mapy_cz(lon, lat)
        result_label = format_geocode_result_label(
            reverse,
            format_point_label(lon, lat),
        )

    return {
        "label": result_label,
        "lon": lon,
        "lat": lat,
        "zone_code": zone.code,
        "match_type": match_type,
        "radius_m": radius_m,
        "cast_dne": cast_dne,
        "source": source,
    }


def build_zone_hits_table(
    snapshot: pd.DataFrame,
    zone_hits,
) -> pd.DataFrame:
    distances = {zone.code: distance_m for zone, distance_m in zone_hits}
    table = snapshot.copy()
    table["vzdalenost_m"] = (
        table["kod_useku"].map(distances).fillna(0).round().astype(int)
    )
    if {"POP_CELKEM", "parkovacich_mist_v_zps"}.issubset(table.columns):
        spaces = pd.to_numeric(table["parkovacich_mist_v_zps"], errors="coerce")
        permits = pd.to_numeric(table["POP_CELKEM"], errors="coerce")
        table["opravneni_na_misto"] = np.where(spaces > 0, permits / spaces, np.nan)
    table = table.rename(
        columns={
            "kod_useku": "Úsek",
            "naz_zsj": "ZSJ",
            "mestska_cast": "MČ",
            "typ_zony": "Typ zóny",
            "POP_CELKEM": "Oprávnění",
            "parkovacich_mist_v_zps": "Místa v ZPS",
            "opravneni_na_misto": "Oprávnění / místo",
            "obsazenost": "Obsazenost",
            "respektovanost": "Respektovanost",
            "vzdalenost_m": "Vzdálenost (m)",
        }
    )
    return table.sort_values(["Vzdálenost (m)", "Úsek"]).reset_index(drop=True)


def build_zone_area_lookup(
    data: pd.DataFrame,
    zsj_mapping,
    zone_codes,
) -> dict[str, str]:
    codes = list(dict.fromkeys(zone_codes))
    if not codes:
        return {}

    lookup = (
        data.loc[data["kod_useku"].isin(codes), ["kod_useku", "mestska_cast"]]
        .dropna(subset=["mestska_cast"])
        .drop_duplicates(subset=["kod_useku"])
        .set_index("kod_useku")["mestska_cast"]
        .to_dict()
    )
    for code in codes:
        mapped_area = zsj_mapping.get(code, {}).get("mestska_cast")
        if mapped_area:
            lookup[code] = mapped_area
    return lookup


def filter_zone_hits_to_same_area(
    data: pd.DataFrame,
    zsj_mapping,
    zone_hits,
    reference_zone_code: str,
) -> tuple[list, Optional[str], int]:
    zone_codes = [zone.code for zone, _ in zone_hits] + [reference_zone_code]
    area_lookup = build_zone_area_lookup(data, zsj_mapping, zone_codes)
    reference_area = area_lookup.get(reference_zone_code)
    if not reference_area:
        return zone_hits, None, 0

    filtered_hits = [
        (zone, distance_m)
        for zone, distance_m in zone_hits
        if area_lookup.get(zone.code) == reference_area
    ]
    excluded_count = len(zone_hits) - len(filtered_hits)
    return filtered_hits, reference_area, excluded_count


def filter_zone_hits_to_data_scope(
    data: pd.DataFrame,
    zone_hits,
    reference_zone_code: str,
) -> tuple[list, int, bool]:
    available_codes = set(data["kod_useku"].dropna().unique())
    filtered_hits = [
        (zone, distance_m)
        for zone, distance_m in zone_hits
        if zone.code in available_codes
    ]
    excluded_count = len(zone_hits) - len(filtered_hits)
    return filtered_hits, excluded_count, reference_zone_code in available_codes


def weighted_average(
    df: pd.DataFrame,
    value_col: str,
    weight_col: str,
) -> Optional[float]:
    if value_col not in df.columns or weight_col not in df.columns:
        return None

    values = pd.to_numeric(df[value_col], errors="coerce")
    weights = pd.to_numeric(df[weight_col], errors="coerce")
    mask = values.notna() & weights.notna() & (weights > 0)
    if not mask.any():
        return None

    return float((values[mask] * weights[mask]).sum() / weights[mask].sum())


def build_radius_policy_metrics(snapshot: pd.DataFrame) -> dict:
    spaces = safe_total(snapshot.get("parkovacich_mist_v_zps", pd.Series(dtype=float)))
    permits = safe_total(snapshot.get("POP_CELKEM", pd.Series(dtype=float)))
    permits_per_space = permits / spaces if spaces > 0 else None

    occupancy = weighted_average(
        snapshot,
        "obsazenost",
        "parkovacich_mist_v_zps",
    )
    respect = weighted_average(
        snapshot,
        "respektovanost",
        "parkovacich_mist_v_zps",
    )

    high_occupancy_zones = 0
    if "obsazenost" in snapshot.columns:
        occupancy_values = pd.to_numeric(snapshot["obsazenost"], errors="coerce")
        high_occupancy_zones = int(
            (occupancy_values >= OCCUPANCY_PRESSURE_THRESHOLD).sum()
        )

    low_respect_zones = 0
    if "respektovanost" in snapshot.columns:
        respect_values = pd.to_numeric(snapshot["respektovanost"], errors="coerce")
        low_respect_zones = int(
            (respect_values < LOW_RESPECT_THRESHOLD).sum()
        )

    return {
        "permits_per_space": permits_per_space,
        "occupancy": occupancy,
        "respect": respect,
        "high_occupancy_zones": high_occupancy_zones,
        "low_respect_zones": low_respect_zones,
    }


def build_address_map(
    address_result: Optional[dict],
    zone_hits,
    latest_snapshot: pd.DataFrame,
) -> folium.Map:
    if address_result:
        center = (address_result["lat"], address_result["lon"])
        zoom = 15
    else:
        center = DEFAULT_CENTER
        zoom = DEFAULT_ZOOM

    m = folium.Map(
        location=center,
        zoom_start=zoom,
        tiles="CartoDB positron",
        control_scale=True,
    )

    if not address_result:
        return m

    radius_m = int(address_result["radius_m"])
    distance_lookup = {zone.code: distance_m for zone, distance_m in zone_hits}
    spaces_lookup = {}
    if not latest_snapshot.empty:
        spaces_lookup = latest_snapshot.set_index("kod_useku")[
            "parkovacich_mist_v_zps"
        ].to_dict()

    folium.Circle(
        location=center,
        radius=radius_m,
        color="#d1495b",
        weight=2,
        fill=True,
        fill_opacity=0.08,
    ).add_to(m)

    folium.Marker(
        location=center,
        tooltip=address_result["label"],
        icon=folium.Icon(color="red", icon="map-marker", prefix="fa"),
    ).add_to(m)

    for zone, distance_m in zone_hits:
        is_reference = zone.code == address_result["zone_code"]
        color = "#d1495b" if is_reference else "#006d77"
        popup_html = (
            f"<strong>{zone.code}</strong><br>"
            f"Vzdálenost: {round(distance_m)} m<br>"
            f"Místa v ZPS: {format_int(spaces_lookup.get(zone.code, 0))}"
        )
        folium.GeoJson(
            data={
                "type": "Feature",
                "geometry": mapping(zone.geometry),
                "properties": {
                    "code": zone.code,
                    "distance_m": round(distance_lookup.get(zone.code, 0)),
                },
            },
            style_function=lambda _, color=color, is_reference=is_reference: {
                "fillColor": color,
                "color": color,
                "weight": 3 if is_reference else 1.5,
                "fillOpacity": 0.28 if is_reference else 0.14,
            },
            popup=folium.Popup(popup_html, max_width=260),
            tooltip=zone.code,
        ).add_to(m)

    return m


def build_map_component_key(address_result: Optional[dict]) -> str:
    if not address_result:
        return "address_selection_map_empty"
    return (
        "address_selection_map_"
        f"{round(address_result['lat'], 5)}_"
        f"{round(address_result['lon'], 5)}"
    )


def render_radius_insight(
    data: pd.DataFrame,
    address_result: dict,
    zone_hits,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    cast_dne_addr = address_result["cast_dne"]
    radius_m = int(address_result["radius_m"])

    st.markdown(f"### Okruh {radius_m} m")
    if not zone_hits:
        st.warning("V daném okruhu nejsou žádné zóny ZPS.")
        return pd.DataFrame(), pd.DataFrame()

    zone_codes = [zone.code for zone, _ in zone_hits]
    series = radius_spaces_series(data, zone_codes, cast_dne_addr)
    latest_snapshot = radius_latest_snapshot(data, zone_codes, cast_dne_addr)

    if series.empty or latest_snapshot.empty:
        st.warning("Pro vybraný okruh nejsou dostupná data o parkovacích místech.")
        return series, latest_snapshot

    latest_spaces = safe_total(series.iloc[[-1]]["parkovacich_mist_v_zps"])
    oldest_spaces = safe_total(series.iloc[[0]]["parkovacich_mist_v_zps"])
    spaces_delta = latest_spaces - oldest_spaces

    metric_cols = st.columns(3)
    metric_cols[0].metric("Úseků v okruhu", format_int(len(zone_hits)))
    metric_cols[1].metric("Místa v ZPS", format_int(latest_spaces))
    metric_cols[2].metric("Změna od začátku", format_int(spaces_delta))

    policy_metrics = build_radius_policy_metrics(latest_snapshot)
    pressure_cols = st.columns(5)
    pressure_cols[0].metric(
        "Oprávnění / místo",
        format_ratio(policy_metrics["permits_per_space"]),
    )
    pressure_cols[1].metric(
        "Obsazenost",
        format_pct(policy_metrics["occupancy"]),
    )
    pressure_cols[2].metric(
        "Respektovanost",
        format_pct(policy_metrics["respect"]),
    )
    pressure_cols[3].metric(
        "Úseky >85 % obs.",
        format_int(policy_metrics["high_occupancy_zones"]),
    )
    pressure_cols[4].metric(
        "Úseky <80 % resp.",
        format_int(policy_metrics["low_respect_zones"]),
    )

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
    render_zone_small_multiples(
        data,
        address_result,
        zone_hits,
        latest_snapshot,
    )
    return series, latest_snapshot


def build_zone_card_figure(zone_history: pd.DataFrame, is_reference: bool):
    line_color = "#d1495b" if is_reference else "#006d77"
    fill_color = "rgba(209, 73, 91, 0.16)" if is_reference else "rgba(0, 109, 119, 0.14)"
    fig = px.line(
        zone_history,
        x="date",
        y="parkovacich_mist_v_zps",
        markers=False,
    )
    fig.update_traces(
        line=dict(color=line_color, width=2),
        fill="tozeroy",
        fillcolor=fill_color,
        hovertemplate="%{x|%m/%Y}<br>%{y:.0f} míst<extra></extra>",
    )
    fig.update_layout(
        height=120,
        margin=dict(l=0, r=0, t=4, b=0),
        showlegend=False,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    fig.update_xaxes(visible=False, fixedrange=True)
    fig.update_yaxes(visible=False, fixedrange=True)
    return fig


def render_zone_small_multiples(
    data: pd.DataFrame,
    address_result: dict,
    zone_hits,
    latest_snapshot: pd.DataFrame,
) -> None:
    zone_codes = [zone.code for zone, _ in zone_hits]
    zone_history = zone_capacity_history(
        data,
        zone_codes,
        address_result["cast_dne"],
    )
    if zone_history.empty:
        return

    snapshot_lookup = latest_snapshot.set_index("kod_useku").to_dict("index")
    history_lookup = {
        code: group.sort_values("date")
        for code, group in zone_history.groupby("kod_useku")
    }

    st.markdown("### Zóny v okruhu")
    st.caption(
        "Mini grafy ukazují vývoj `parkovacich_mist_v_zps` po jednotlivých úsecích. "
        "Krátké vnitřní mezery ve zdroji se dopočítají jen když před i po mezeře zůstává kapacita stejná. "
        "Agregát i karty jsou navíc oříznuté na společné období, kdy mají všechny zahrnuté zóny data."
    )
    columns = st.columns(3)

    for idx, (zone, distance_m) in enumerate(zone_hits):
        history = history_lookup.get(zone.code)
        if history is None or history.empty:
            continue

        latest_spaces = safe_total(history.iloc[[-1]]["parkovacich_mist_v_zps"])
        oldest_spaces = safe_total(history.iloc[[0]]["parkovacich_mist_v_zps"])
        meta = snapshot_lookup.get(zone.code, {})
        is_reference = zone.code == address_result["zone_code"]

        with columns[idx % 3]:
            with st.container(border=True):
                title = f"**{zone.code}**"
                if is_reference:
                    title += " · referenční"
                st.markdown(title)
                st.caption(
                    f"{meta.get('naz_zsj', 'Neznámá ZSJ')} · "
                    f"{meta.get('typ_zony', 'n/a')} · "
                    f"{round(distance_m)} m"
                )
                st.markdown(
                    f"{format_int(latest_spaces)} míst · Δ {format_signed_int(latest_spaces - oldest_spaces)}"
                )
                st.plotly_chart(
                    build_zone_card_figure(history, is_reference),
                    key=(
                        f"zone_card_{zone.code}_"
                        f"{address_result['radius_m']}_"
                        f"{address_result['cast_dne']}"
                    ),
                    use_container_width=True,
                    config={"displayModeBar": False},
                )


def sync_selection_controls(address_result: Optional[dict], radius_m: int, cast_dne: str):
    if not address_result:
        return None

    updated = dict(address_result)
    updated["radius_m"] = radius_m
    updated["cast_dne"] = cast_dne
    st.session_state[ADDRESS_RESULT_KEY] = updated
    return updated


def apply_pending_address_query() -> None:
    pending_query = st.session_state.pop(ADDRESS_QUERY_PENDING_KEY, None)
    if pending_query is not None:
        st.session_state[ADDRESS_QUERY_KEY] = pending_query


def handle_map_click(
    map_state: dict,
    radius_m: int,
    cast_dne_addr: str,
    zone_index,
) -> None:
    clicked = map_state.get("last_clicked") if map_state else None
    if not clicked:
        return

    lon = float(clicked["lng"])
    lat = float(clicked["lat"])
    click_key = (round(lon, 6), round(lat, 6))
    if click_key == st.session_state.get(LAST_MAP_CLICK_KEY):
        return

    selection = resolve_point_result(
        lon,
        lat,
        radius_m,
        cast_dne_addr,
        zone_index,
        source="map",
    )
    st.session_state[LAST_MAP_CLICK_KEY] = click_key

    if not selection:
        st.session_state[ADDRESS_ERROR_KEY] = "Nenalezen žádný úsek pro vybraný bod."
        st.rerun()
        return

    st.session_state[ADDRESS_RESULT_KEY] = selection
    st.session_state[ADDRESS_QUERY_PENDING_KEY] = selection["label"]
    st.session_state[ADDRESS_ERROR_KEY] = None
    st.rerun()


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

    if data.empty or not cast_dne_values:
        st.warning("Boční filtry nevrací žádná data pro adresní report.")
        return

    apply_pending_address_query()

    controls_left, controls_mid, controls_right = st.columns([2.4, 1, 1])
    with controls_left:
        address = st.text_input("Adresa v Praze", key=ADDRESS_QUERY_KEY)
        search_clicked = st.button("Najít adresu", use_container_width=True)
    with controls_mid:
        radius_m = st.slider(
            "Okruh (m)",
            min_value=100,
            max_value=1500,
            value=500,
            step=100,
        )
    with controls_right:
        cast_dne_addr = st.selectbox(
            "Část dne",
            cast_dne_values,
            index=0,
        )

    if search_clicked:
        result = geocode_with_mapy_cz(address)
        if not result:
            st.session_state[ADDRESS_ERROR_KEY] = "Adresa nenalezena nebo chyba Mapy.cz."
        else:
            coords = extract_lon_lat(result)
            if not coords:
                st.session_state[ADDRESS_ERROR_KEY] = "Adresa bez souřadnic."
            else:
                selection = resolve_point_result(
                    coords[0],
                    coords[1],
                    radius_m,
                    cast_dne_addr,
                    zone_index,
                    label=format_geocode_result_label(result, address),
                    source="address",
                )
                if not selection:
                    st.session_state[ADDRESS_ERROR_KEY] = (
                        "Nenalezen žádný úsek pro zadanou adresu."
                    )
                else:
                    st.session_state[ADDRESS_RESULT_KEY] = selection
                    st.session_state[LAST_MAP_CLICK_KEY] = (
                        round(selection["lon"], 6),
                        round(selection["lat"], 6),
                    )
                    st.session_state[ADDRESS_ERROR_KEY] = None

    address_result = sync_selection_controls(
        st.session_state.get(ADDRESS_RESULT_KEY),
        radius_m,
        cast_dne_addr,
    )

    zone_hits = []
    latest_snapshot = pd.DataFrame()
    reference_area = None
    excluded_zone_count = 0
    data_scope_excluded_count = 0
    reference_in_scope = True
    if address_result:
        point = Point(address_result["lon"], address_result["lat"])
        raw_zone_hits = zone_index.find_zones_within_radius(point, radius_m)
        scoped_zone_hits, data_scope_excluded_count, reference_in_scope = (
            filter_zone_hits_to_data_scope(
                data,
                raw_zone_hits,
                address_result["zone_code"],
            )
        )
        zone_hits, reference_area, excluded_zone_count = filter_zone_hits_to_same_area(
            data,
            zsj_mapping,
            scoped_zone_hits,
            address_result["zone_code"],
        )
        zone_codes = [zone.code for zone, _ in zone_hits]
        latest_snapshot = radius_latest_snapshot(data, zone_codes, cast_dne_addr)

    st.caption(
        "Klikni do mapy pro výběr bodu. Adresní vyhledání i klik sdílí stejný okruh a část dne. "
        "Adresní report respektuje boční filtry."
    )
    map_state = st_folium(
        build_address_map(address_result, zone_hits, latest_snapshot),
        key=build_map_component_key(address_result),
        height=480,
        returned_objects=["last_clicked"],
        use_container_width=True,
    )
    handle_map_click(map_state, radius_m, cast_dne_addr, zone_index)

    error = st.session_state.get(ADDRESS_ERROR_KEY)
    if error:
        st.error(error)

    if not address_result:
        st.info("Zadej adresu nebo klikni do mapy.")
        st.stop()

    zone_code = address_result["zone_code"]
    match_type = address_result["match_type"]
    zsj_meta = zsj_mapping.get(zone_code, {})
    source_label = "mapa" if address_result.get("source") == "map" else "adresa"
    st.markdown(
        f"**Výběr:** {address_result['label']} ({source_label})  \n"
        f"**Referenční úsek:** {zone_code} ({MATCH_LABELS.get(match_type, match_type)})  \n"
        f"**Městská část pro okruh:** {reference_area or zsj_meta.get('mestska_cast', 'nezname')}  \n"
        f"**ZSJ:** {zsj_meta.get('kod_zsj', 'nezname')} - {zsj_meta.get('naz_zsj', 'nezname')}  \n"
        f"**Okruh:** {address_result['radius_m']} m"
    )
    if excluded_zone_count:
        st.caption(
            f"Z okruhu bylo vyřazeno {excluded_zone_count} úseků z jiné městské části."
        )
    if data_scope_excluded_count:
        st.caption(
            f"Počet úseků vyřazených bočními filtry: {data_scope_excluded_count}."
        )
    if not reference_in_scope:
        st.caption(
            "Referenční úsek neodpovídá bočním filtrům, proto není zahrnut do metrik okruhu."
        )

    render_radius_insight(data, address_result, zone_hits)

    st.markdown("### Detail referenčního úseku")
    zone_data = data[(data["kod_useku"] == zone_code)]
    zone_data = zone_data[zone_data["cast_dne"] == cast_dne_addr]
    if zone_data.empty:
        st.info("Referenční úsek není v aktuálních bočních filtrech.")
        return

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
