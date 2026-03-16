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
        :root {
            --app-bg-layer-1: rgba(209, 73, 91, 0.18);
            --app-bg-layer-2: rgba(0, 121, 140, 0.12);
            --app-bg-base-1: #f6f1e7;
            --app-bg-base-2: #efe3cf;
            --plotly-text-color: #1f1c17;
            --plotly-grid-color: rgba(31, 28, 23, 0.10);
        }
        @media (prefers-color-scheme: dark) {
            :root {
                --app-bg-layer-1: rgba(209, 73, 91, 0.14);
                --app-bg-layer-2: rgba(0, 121, 140, 0.16);
                --app-bg-base-1: #11141b;
                --app-bg-base-2: #1a2029;
                --plotly-text-color: #f7f1e8;
                --plotly-grid-color: rgba(247, 241, 232, 0.14);
            }
        }
        :root[data-codex-theme="light"] {
            --app-bg-layer-1: rgba(209, 73, 91, 0.18);
            --app-bg-layer-2: rgba(0, 121, 140, 0.12);
            --app-bg-base-1: #f6f1e7;
            --app-bg-base-2: #efe3cf;
            --plotly-text-color: #1f1c17;
            --plotly-grid-color: rgba(31, 28, 23, 0.10);
        }
        :root[data-codex-theme="dark"] {
            --app-bg-layer-1: rgba(209, 73, 91, 0.14);
            --app-bg-layer-2: rgba(0, 121, 140, 0.16);
            --app-bg-base-1: #11141b;
            --app-bg-base-2: #1a2029;
            --plotly-text-color: #f7f1e8;
            --plotly-grid-color: rgba(247, 241, 232, 0.14);
        }
        html, body, [class*="css"] {
            font-family: "Space Grotesk", "Newsreader", serif;
        }
        .stApp {
            background: radial-gradient(circle at 10% 10%, var(--app-bg-layer-1), transparent 40%),
                        radial-gradient(circle at 90% 0%, var(--app-bg-layer-2), transparent 45%),
                        linear-gradient(180deg, var(--app-bg-base-1), var(--app-bg-base-2));
        }
        .block-container {
            padding-top: 2.5rem;
            padding-bottom: 4rem;
        }
        h1, h2, h3 {
            letter-spacing: -0.02em;
        }
        .js-plotly-plot .plotly svg text {
            fill: var(--plotly-text-color) !important;
        }
        .js-plotly-plot .gridlayer path,
        .js-plotly-plot .zerolinelayer path {
            stroke: var(--plotly-grid-color) !important;
        }
        .js-plotly-plot .legend rect {
            fill: transparent !important;
        }
        </style>
        <script>
        const applyCodexTheme = () => {
            const bodyColor = getComputedStyle(document.body).color || "";
            const values = bodyColor.match(/\\d+/g);
            if (!values || values.length < 3) return;
            const [r, g, b] = values.slice(0, 3).map(Number);
            const brightness = (r * 299 + g * 587 + b * 114) / 1000;
            document.documentElement.dataset.codexTheme = brightness > 160 ? "dark" : "light";
        };
        applyCodexTheme();
        new MutationObserver(applyCodexTheme).observe(document.body, {
            attributes: true,
            childList: true,
            subtree: true,
        });
        </script>
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
