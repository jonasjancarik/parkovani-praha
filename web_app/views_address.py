import os
from typing import Optional

import folium
import pandas as pd
import plotly.express as px
import streamlit as st
from shapely.geometry import Point, mapping
from streamlit_folium import st_folium

from address_exports import (
    build_zone_hits_csv,
    build_zone_hits_geojson,
    export_file_stem,
)
from address_logic import (
    ADDRESS_QUERY_PARAM_KEYS,
    DEFAULT_RADIUS_M,
    MAX_RADIUS_M,
    MIN_RADIUS_M,
    address_query_params_to_selection_seed,
    address_result_to_query_params,
    address_selection_signature,
    build_policy_pressure_ranking,
    build_radius_policy_metrics,
    build_radius_scenario_row,
    build_radius_scope_for_point,
    build_zone_hits_table,
    clamp_radius,
    format_geocode_result_label,
    format_int,
    format_pct,
    format_point_label,
    format_ratio,
    format_signed_int,
    safe_total,
)
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
ADDRESS_RADIUS_KEY = "address_radius_m"
ADDRESS_CAST_DNE_KEY = "address_cast_dne"
ADDRESS_URL_SIGNATURE_KEY = "address_url_signature"
COMPARISON_RADII_KEY = "address_comparison_radii"

MATCH_LABELS = {
    "inside": "uvnitř zóny",
    "nearest": "nejbližší úsek",
    "none": "bez shody",
}


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
    legend_html = """
        <div style="
            position: fixed;
            bottom: 24px;
            left: 24px;
            z-index: 9999;
            background: rgba(255,255,255,0.92);
            border: 1px solid rgba(31,28,23,0.18);
            border-radius: 6px;
            padding: 8px 10px;
            font-size: 12px;
            color: #1f1c17;
            box-shadow: 0 2px 8px rgba(0,0,0,0.12);
        ">
            <div><span style="color:#d1495b;font-weight:700;">■</span> referenční úsek</div>
            <div><span style="color:#006d77;font-weight:700;">■</span> úseky v okruhu</div>
        </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))

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


def initialize_address_state_from_query_params(cast_dne_values, zone_index) -> None:
    seed = address_query_params_to_selection_seed(
        st.query_params.to_dict(),
        cast_dne_values,
    )
    if not seed:
        return

    seed_signature = address_selection_signature(seed)
    if seed_signature == st.session_state.get(ADDRESS_URL_SIGNATURE_KEY):
        return

    selection = resolve_point_result(
        seed["lon"],
        seed["lat"],
        seed["radius_m"],
        seed["cast_dne"],
        zone_index,
        label=seed["label"],
        source=seed["source"],
    )
    if not selection:
        return

    st.session_state[ADDRESS_RESULT_KEY] = selection
    st.session_state[ADDRESS_QUERY_KEY] = selection["label"]
    st.session_state[ADDRESS_RADIUS_KEY] = selection["radius_m"]
    st.session_state[ADDRESS_CAST_DNE_KEY] = selection["cast_dne"]
    st.session_state[ADDRESS_URL_SIGNATURE_KEY] = address_selection_signature(selection)


def sync_address_query_params(address_result: Optional[dict]) -> None:
    if not address_result:
        return

    signature = address_selection_signature(address_result)
    query_params = address_result_to_query_params(address_result)
    current_params = st.query_params.to_dict()
    if all(current_params.get(key) == value for key, value in query_params.items()):
        st.session_state[ADDRESS_URL_SIGNATURE_KEY] = signature
        return

    updated_params = dict(current_params)
    updated_params.update(query_params)
    st.query_params.from_dict(updated_params)
    st.session_state[ADDRESS_URL_SIGNATURE_KEY] = signature


def clear_address_selection() -> None:
    for key in [
        ADDRESS_RESULT_KEY,
        LAST_MAP_CLICK_KEY,
        ADDRESS_ERROR_KEY,
        ADDRESS_URL_SIGNATURE_KEY,
    ]:
        st.session_state.pop(key, None)
    st.session_state[ADDRESS_QUERY_PENDING_KEY] = ""

    updated_params = st.query_params.to_dict()
    for key in ADDRESS_QUERY_PARAM_KEYS:
        updated_params.pop(key, None)
    st.query_params.from_dict(updated_params)


def initialize_address_control_defaults(cast_dne_values) -> None:
    address_result = st.session_state.get(ADDRESS_RESULT_KEY)
    radius = clamp_radius((address_result or {}).get("radius_m", DEFAULT_RADIUS_M))
    if ADDRESS_RADIUS_KEY not in st.session_state:
        st.session_state[ADDRESS_RADIUS_KEY] = radius

    current_cast = st.session_state.get(ADDRESS_CAST_DNE_KEY) or (
        address_result or {}
    ).get("cast_dne")
    if current_cast not in cast_dne_values:
        current_cast = cast_dne_values[0]
    st.session_state[ADDRESS_CAST_DNE_KEY] = current_cast


def render_scope_status(scope) -> None:
    if scope.area_excluded_count:
        st.caption(
            f"Z okruhu bylo vyřazeno {scope.area_excluded_count} úseků z jiné městské části."
        )
    if scope.data_scope_excluded_count:
        st.caption(
            f"Počet úseků vyřazených bočními filtry: {scope.data_scope_excluded_count}."
        )
    if not scope.reference_in_scope:
        st.caption(
            "Referenční úsek neodpovídá bočním filtrům, proto není zahrnut do metrik okruhu."
        )


def render_radius_exports(
    address_result: dict,
    latest_snapshot: pd.DataFrame,
    zone_hits,
) -> None:
    if latest_snapshot.empty or not zone_hits:
        return

    file_stem = export_file_stem(address_result)
    export_cols = st.columns(2)
    export_cols[0].download_button(
        "Stáhnout CSV",
        data=build_zone_hits_csv(latest_snapshot, zone_hits),
        file_name=f"{file_stem}-useky.csv",
        mime="text/csv",
        use_container_width=True,
    )
    export_cols[1].download_button(
        "Stáhnout GeoJSON",
        data=build_zone_hits_geojson(latest_snapshot, zone_hits, address_result),
        file_name=f"{file_stem}-useky.geojson",
        mime="application/geo+json",
        use_container_width=True,
    )


def comparison_radius_options(current_radius: int) -> list[int]:
    radii = {MIN_RADIUS_M, 300, 500, 800, 1000, MAX_RADIUS_M, current_radius}
    return sorted(radius for radius in radii if MIN_RADIUS_M <= radius <= MAX_RADIUS_M)


def render_radius_comparison(
    data: pd.DataFrame,
    address_result: dict,
    zone_index,
    zsj_mapping,
) -> None:
    current_radius = clamp_radius(address_result["radius_m"])
    options = comparison_radius_options(current_radius)
    default = sorted({MIN_RADIUS_M, current_radius, MAX_RADIUS_M})

    with st.expander("Porovnání okruhů", expanded=True):
        selected_radii = st.multiselect(
            "Okruhy k porovnání",
            options,
            default=[radius for radius in default if radius in options],
            key=COMPARISON_RADII_KEY,
        )
        if not selected_radii:
            st.info("Vyber alespoň jeden okruh.")
            return

        rows = []
        for radius_m in sorted(selected_radii):
            scope = build_radius_scope_for_point(
                data,
                zsj_mapping,
                zone_index,
                address_result["lon"],
                address_result["lat"],
                radius_m,
                address_result["zone_code"],
            )
            row = build_radius_scenario_row(
                data,
                scope.zone_hits,
                address_result["cast_dne"],
                radius_m,
            )
            row["vyrazeno_mestska_cast"] = scope.area_excluded_count
            row["vyrazeno_filtry"] = scope.data_scope_excluded_count
            rows.append(row)

        comparison = pd.DataFrame(rows)
        if comparison.empty:
            st.info("Pro vybrané okruhy nejsou dostupná data.")
            return

        display = comparison.rename(
            columns={
                "radius_m": "Okruh (m)",
                "pocet_useku": "Úseků",
                "mista_v_zps": "Místa v ZPS",
                "zmena_mist": "Změna míst",
                "opravneni_na_misto": "Oprávnění / místo",
                "obsazenost": "Obsazenost",
                "respektovanost": "Respektovanost",
                "useky_vysoka_obsazenost": "Úseky >85 % obs.",
                "useky_nizka_respektovanost": "Úseky <80 % resp.",
                "vyrazeno_mestska_cast": "Vyřazeno MČ",
                "vyrazeno_filtry": "Vyřazeno filtry",
            }
        )
        st.dataframe(display, hide_index=True, use_container_width=True)

        metric_chart = comparison.melt(
            id_vars=["radius_m"],
            value_vars=["opravneni_na_misto", "obsazenost", "respektovanost"],
            var_name="metrika",
            value_name="hodnota",
        ).dropna(subset=["hodnota"])
        if not metric_chart.empty:
            fig = px.line(
                metric_chart,
                x="radius_m",
                y="hodnota",
                color="metrika",
                markers=True,
                labels={
                    "radius_m": "Okruh (m)",
                    "hodnota": "Hodnota",
                    "metrika": "Metrika",
                },
            )
            st.plotly_chart(style_figure(fig), use_container_width=True)


def render_policy_pressure_ranking(data: pd.DataFrame, cast_dne: str) -> None:
    ranking = build_policy_pressure_ranking(data, cast_dne, limit=15)
    if ranking.empty:
        return

    st.markdown("### Úseky s nejvyšším tlakem")
    chart = ranking.sort_values("tlakove_skore", ascending=True)
    fig = px.bar(
        chart,
        x="tlakove_skore",
        y="kod_useku",
        color="mestska_cast",
        orientation="h",
        labels={
            "tlakove_skore": "Tlakové skóre",
            "kod_useku": "Úsek",
            "mestska_cast": "MČ",
        },
    )
    st.plotly_chart(style_figure(fig), use_container_width=True)

    display = ranking[
        [
            "kod_useku",
            "naz_zsj",
            "mestska_cast",
            "typ_zony",
            "opravneni_na_misto",
            "obsazenost",
            "respektovanost",
            "tlakove_skore",
        ]
    ].rename(
        columns={
            "kod_useku": "Úsek",
            "naz_zsj": "ZSJ",
            "mestska_cast": "MČ",
            "typ_zony": "Typ zóny",
            "opravneni_na_misto": "Oprávnění / místo",
            "obsazenost": "Obsazenost",
            "respektovanost": "Respektovanost",
            "tlakove_skore": "Tlakové skóre",
        }
    )
    st.dataframe(display, hide_index=True, use_container_width=True)


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
    render_radius_exports(address_result, latest_snapshot, zone_hits)
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

    initialize_address_state_from_query_params(cast_dne_values, zone_index)
    apply_pending_address_query()
    initialize_address_control_defaults(cast_dne_values)

    controls_left, controls_mid, controls_right = st.columns([2.4, 1, 1])
    with controls_left:
        address = st.text_input("Adresa v Praze", key=ADDRESS_QUERY_KEY)
        search_clicked = st.button("Najít adresu", use_container_width=True)
    with controls_mid:
        radius_m = st.slider(
            "Okruh (m)",
            min_value=MIN_RADIUS_M,
            max_value=MAX_RADIUS_M,
            step=100,
            key=ADDRESS_RADIUS_KEY,
        )
    with controls_right:
        cast_dne_addr = st.selectbox(
            "Část dne",
            cast_dne_values,
            key=ADDRESS_CAST_DNE_KEY,
        )
        clear_clicked = st.button("Vymazat výběr", use_container_width=True)

    if clear_clicked:
        clear_address_selection()
        st.rerun()

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
    sync_address_query_params(address_result)

    zone_hits = []
    latest_snapshot = pd.DataFrame()
    scope = None
    if address_result:
        scope = build_radius_scope_for_point(
            data,
            zsj_mapping,
            zone_index,
            address_result["lon"],
            address_result["lat"],
            radius_m,
            address_result["zone_code"],
        )
        zone_hits = scope.zone_hits
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
    source_label = {
        "map": "mapa",
        "shared-link": "odkaz",
    }.get(address_result.get("source"), "adresa")
    st.markdown(
        f"**Výběr:** {address_result['label']} ({source_label})  \n"
        f"**Referenční úsek:** {zone_code} ({MATCH_LABELS.get(match_type, match_type)})  \n"
        f"**Městská část pro okruh:** {(scope.reference_area if scope else None) or zsj_meta.get('mestska_cast', 'nezname')}  \n"
        f"**ZSJ:** {zsj_meta.get('kod_zsj', 'nezname')} - {zsj_meta.get('naz_zsj', 'nezname')}  \n"
        f"**Okruh:** {address_result['radius_m']} m"
    )
    if scope:
        render_scope_status(scope)

    render_radius_insight(data, address_result, zone_hits)
    render_radius_comparison(data, address_result, zone_index, zsj_mapping)
    render_policy_pressure_ranking(data, cast_dne_addr)

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
