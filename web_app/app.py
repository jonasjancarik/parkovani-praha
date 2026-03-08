import streamlit as st

from data import apply_filters, build_date_range, load_parking_data, permits_base
from geo import load_zone_index, load_zsj_mapping
from views_address import render_address_view
from views_tableau import render_tableau_view


def inject_styles() -> None:
    st.markdown(
        """
        <style>
        @import url("https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;600;700&family=Newsreader:wght@400;600&display=swap");
        html, body, [class*="css"] {
            font-family: "Space Grotesk", "Newsreader", serif;
        }
        .stApp {
            background: radial-gradient(circle at 10% 10%, rgba(209, 73, 91, 0.18), transparent 40%),
                        radial-gradient(circle at 90% 0%, rgba(0, 121, 140, 0.12), transparent 45%),
                        linear-gradient(180deg, #f6f1e7, #efe3cf);
        }
        .block-container {
            padding-top: 2.5rem;
            padding-bottom: 4rem;
        }
        h1, h2, h3 {
            letter-spacing: -0.02em;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


@st.cache_data(show_spinner=False)
def get_data():
    return load_parking_data()


@st.cache_resource(show_spinner=False)
def get_zone_index():
    return load_zone_index()


@st.cache_data(show_spinner=False)
def get_zsj_mapping():
    return load_zsj_mapping()


@st.cache_data(show_spinner=False)
def get_permits_base(df):
    return permits_base(df)


st.set_page_config(page_title="Parkování Praha", layout="wide")
inject_styles()

st.title("Parkování Praha")
st.caption("Replika Tableau + adresa → report.")

data = get_data()
zone_index = get_zone_index()
zsj_mapping = get_zsj_mapping()
permits_only = get_permits_base(data)

min_date = data["date"].min().date()
max_date = data["date"].max().date()
cast_dne_values = sorted(data["cast_dne"].dropna().unique())
typ_zony_values = sorted(data["typ_zony"].dropna().unique())
mestska_values = sorted(data["mestska_cast"].dropna().unique())

with st.sidebar:
    st.header("Filtry")
    date_input = st.date_input(
        "Období",
        (min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )
    cast_dne_filter = st.multiselect(
        "Část dne",
        cast_dne_values,
        default=cast_dne_values,
    )
    mestska_filter = st.selectbox(
        "Městská část",
        ["All"] + mestska_values,
        index=0,
    )
    if mestska_filter == "All":
        naz_zsj_options = ["All"] + sorted(data["naz_zsj"].dropna().unique())
    else:
        naz_zsj_options = ["All"] + sorted(
            data.loc[data["mestska_cast"] == mestska_filter, "naz_zsj"]
            .dropna()
            .unique()
        )
    naz_zsj_filter = st.selectbox("ZSJ", naz_zsj_options, index=0)
    typ_zony_filter = st.multiselect(
        "Typ zóny",
        typ_zony_values,
        default=typ_zony_values,
    )

date_range = build_date_range(data, date_input)
filtered = apply_filters(
    data,
    date_range,
    cast_dne_filter,
    mestska_filter,
    naz_zsj_filter,
    typ_zony_filter,
)

tab_tableau, tab_address = st.tabs(["Tableau views", "Address insight"])

with tab_tableau:
    render_tableau_view(
        data,
        filtered,
        permits_only,
        date_range,
        typ_zony_values,
        mestska_filter,
        naz_zsj_filter,
    )

with tab_address:
    render_address_view(data, zone_index, zsj_mapping, cast_dne_values)
