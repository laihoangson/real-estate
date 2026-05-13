"""
Melbourne Property Price Dashboard - Streamlit app.

Run locally:
    streamlit run app.py

Deploy:
    Push to GitHub, then connect repo at https://share.streamlit.io
"""

import json
import sys
from datetime import datetime
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st
from streamlit_folium import st_folium
import folium

# Make production code importable.
ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "production"))

if "config" in sys.modules:
    del sys.modules["config"]
import config as prod_cfg
from train_pipeline import add_engineered_features, transform


# ============================================================
# PAGE CONFIG
# ============================================================

st.set_page_config(
    page_title="Melbourne Market Insight",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ============================================================
# THEME CSS (mirrors the old dashboard.html aesthetic)
# ============================================================

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600;700&family=DM+Serif+Display:ital@0;1&display=swap');

    /* Global background */
    .stApp { background-color: #f4f6fb; }

    /* Hide default Streamlit header decorations */
    header[data-testid="stHeader"] { background: transparent; }

    /* Font family across the app */
    html, body, [class*="css"] {
        font-family: 'DM Sans', sans-serif !important;
        color: #1a1d27;
    }

    /* Title / serif headings */
    h1, h2, h3 {
        font-family: 'DM Serif Display', serif !important;
        font-weight: 400 !important;
        color: #1a1d27 !important;
    }

    /* Eyebrow above title */
    .eyebrow {
        font-size: 10px; font-weight: 700; letter-spacing: 3px;
        text-transform: uppercase; color: #2563eb;
        margin-bottom: 6px; display: flex; align-items: center; gap: 8px;
    }
    .eyebrow::before {
        content: ''; display: block; width: 18px; height: 2px;
        background: #2563eb; border-radius: 2px;
    }

    /* Main title */
    .main-title {
        font-family: 'DM Serif Display', serif;
        font-size: 2.5rem; line-height: 1.1; font-weight: 400; color: #1a1d27;
        margin-bottom: 4px;
    }
    .main-title em { font-style: italic; color: #2563eb; }

    /* Live badge */
    .live-badge {
        display: inline-flex; align-items: center; gap: 7px;
        background: rgba(5,150,105,0.08);
        border: 1px solid rgba(5,150,105,0.18); color: #059669;
        padding: 6px 13px; border-radius: 100px;
        font-size: 11.5px; font-weight: 600;
    }
    .live-dot {
        width: 6px; height: 6px; background: #059669;
        border-radius: 50%; animation: livePulse 2s infinite;
    }
    @keyframes livePulse { 0%,100%{opacity:1;} 50%{opacity:0.35;} }

    /* KPI cards */
    .kpi-grid {
        display: grid; grid-template-columns: repeat(4, 1fr);
        gap: 14px; margin: 22px 0 22px 0;
    }
    .kpi-card {
        background: #ffffff; border: 1px solid rgba(0,0,0,0.07);
        border-radius: 14px; padding: 22px 22px 18px;
        position: relative; overflow: hidden;
        box-shadow: 0 1px 3px rgba(0,0,0,0.06), 0 1px 2px rgba(0,0,0,0.04);
        transition: border-color .2s, box-shadow .2s, transform .2s;
    }
    .kpi-card:hover {
        border-color: rgba(0,0,0,0.14);
        box-shadow: 0 4px 12px rgba(0,0,0,0.08);
        transform: translateY(-2px);
    }
    .kpi-card::after {
        content: ''; position: absolute; bottom: 0; left: 0; right: 0;
        height: 3px; opacity: 0.7;
    }
    .kpi-card.accent::after  { background: #2563eb; }
    .kpi-card.emerald::after { background: #059669; }
    .kpi-card.amber::after   { background: #d97706; }
    .kpi-card.violet::after  { background: #7c3aed; }

    .kpi-label {
        font-size: 10.5px; font-weight: 700; letter-spacing: 2px;
        text-transform: uppercase; color: #7c8499; margin-bottom: 10px;
    }
    .kpi-value {
        font-family: 'DM Serif Display', serif;
        font-size: 2.1rem; line-height: 1; font-weight: 400;
    }
    .kpi-value.accent  { color: #2563eb; }
    .kpi-value.emerald { color: #059669; }
    .kpi-value.amber   { color: #d97706; }
    .kpi-value.violet  { color: #7c3aed; }
    .kpi-sub {
        font-size: 11.5px; color: #7c8499; margin-top: 7px;
    }

    /* Tabs styling */
    button[data-baseweb="tab"] {
        font-family: 'DM Sans', sans-serif !important;
        font-weight: 600 !important; font-size: 13.5px !important;
    }

    /* Section subheaders inside tabs */
    .stApp .stMarkdown h3 {
        font-family: 'DM Sans', sans-serif !important;
        font-size: 10px !important; font-weight: 700 !important;
        letter-spacing: 2px !important; text-transform: uppercase !important;
        color: #7c8499 !important;
    }

    /* Sidebar background */
    section[data-testid="stSidebar"] { background-color: #ffffff; border-right: 1px solid rgba(0,0,0,0.06); }

    /* Caption styling */
    .stCaption, .caption-muted { color: #7c8499 !important; font-size: 11.5px !important; }

    @media (max-width: 768px) {
        .kpi-grid { grid-template-columns: 1fr 1fr; }
    }
</style>
""", unsafe_allow_html=True)


# ============================================================
# DATA LOADING (cached)
# ============================================================

@st.cache_data(ttl=3600)
def load_predictions():
    return pd.read_parquet(ROOT / "production" / "output" / "predictions_for_sale.parquet")


@st.cache_data(ttl=3600)
def load_decisions():
    with open(ROOT / "production" / "output" / "eda_decisions.json") as f:
        return json.load(f)


@st.cache_data(ttl=3600)
def load_metrics():
    with open(ROOT / "production" / "output" / "models" / "metrics.json") as f:
        return json.load(f)


@st.cache_data(ttl=3600)
def load_suburb_lookup():
    df = pd.read_parquet(ROOT / "production" / "output" / "cleaned_data.parquet")
    return (df[[
        "Suburb", "Postcode",
        "abs_median_income_weekly", "abs_median_age", "abs_population",
        "crime_rate_per_100k", "Propertycount", "dist_nearest_train_km",
    ]]
    .drop_duplicates(subset="Suburb")
    .set_index("Suburb"))


@st.cache_data(ttl=3600)
def load_geojson():
    path = ROOT / "data" / "melbourne_suburb_boundaries.geojson"
    if not path.exists():
        return None
    with open(path) as f:
        return json.load(f)


@st.cache_resource
def load_models():
    model_dir = ROOT / "production" / "output" / "models"
    return {
        "point":        joblib.load(model_dir / "model.pkl"),
        "q10":          joblib.load(model_dir / "model_q10.pkl"),
        "q90":          joblib.load(model_dir / "model_q90.pkl"),
        "preprocessor": joblib.load(model_dir / "preprocessor.pkl"),
    }


df          = load_predictions()
decisions   = load_decisions()
metrics     = load_metrics()
suburbs     = load_suburb_lookup()
models      = load_models()
geojson     = load_geojson()


# ============================================================
# RESOLVE LATEST UPDATE DATE
# ============================================================

# Use the most recent Last_Updated as the live update timestamp.
if "Last_Updated" in df.columns:
    last_updated_series = pd.to_datetime(df["Last_Updated"], errors="coerce")
    last_updated_dt = last_updated_series.max()
    if pd.isna(last_updated_dt):
        last_updated_str = "—"
    else:
        last_updated_str = last_updated_dt.strftime("%d %b %Y")
else:
    last_updated_str = "—"


# ============================================================
# SIDEBAR FILTERS
# ============================================================

st.sidebar.title("Filters")
st.sidebar.markdown(f"**Total listings**: {len(df):,}")
st.sidebar.markdown("---")

all_suburbs = sorted(df["Suburb"].unique().tolist())
sel_suburbs = st.sidebar.multiselect("Suburb", all_suburbs, default=[])

all_types = sorted(df["Property_Type"].unique().tolist())
sel_types = st.sidebar.multiselect("Property Type", all_types, default=[])

beds_min, beds_max = st.sidebar.slider("Beds", 0, 10, (0, 10))

price_min_k, price_max_k = st.sidebar.slider(
    "Predicted price range (AUD k)", 0, 5000, (0, 5000), step=50,
)

all_signals = ["Good Deal", "Fair", "Overpriced", "No Asking Price"]
sel_signals = st.sidebar.multiselect("Deal Signal", all_signals, default=all_signals)

st.sidebar.markdown("---")
st.sidebar.caption(f"Model: XGBoost | Test MAPE: {metrics['test_metrics']['mape']:.1f}%")


# ============================================================
# APPLY FILTERS
# ============================================================

def apply_filters(df):
    out = df.copy()
    if sel_suburbs:
        out = out[out["Suburb"].isin(sel_suburbs)]
    if sel_types:
        out = out[out["Property_Type"].isin(sel_types)]
    out = out[(out["Beds"].fillna(0) >= beds_min) &
              (out["Beds"].fillna(0) <= beds_max)]
    out = out[(out["Predicted_Price"] >= price_min_k * 1000) &
              (out["Predicted_Price"] <= price_max_k * 1000)]
    if sel_signals:
        out = out[out["Deal_Signal"].isin(sel_signals)]
    return out


df_f = apply_filters(df)

# ============================================================
# HEADER
# ============================================================

header_col1, header_col2 = st.columns([3, 1])

with header_col1:
    st.markdown('<div class="eyebrow">Real Estate Intelligence</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="main-title">Melbourne <em>Market</em> Insight</div>',
        unsafe_allow_html=True,
    )

with header_col2:
    st.markdown(
        f'<div style="text-align:right; padding-top: 18px;">'
        f'<span class="live-badge"><span class="live-dot"></span>'
        f'Updated on {last_updated_str}</span></div>',
        unsafe_allow_html=True,
    )


# ============================================================
# KPI CARDS (custom HTML, mirrors dashboard.html aesthetic)
# ============================================================

def fmt_money(v):
    if v is None or pd.isna(v):
        return "—"
    return f"${v:,.0f}"


total_listings   = len(df_f)
median_predicted = df_f["Predicted_Price"].median() if total_listings else None
n_good_deals     = (df_f["Deal_Signal"] == "Good Deal").sum() if total_listings else 0
n_overpriced     = (df_f["Deal_Signal"] == "Overpriced").sum() if total_listings else 0

pct_good = f"{n_good_deals / total_listings * 100:.1f}%" if total_listings else "—"
pct_over = f"{n_overpriced / total_listings * 100:.1f}%" if total_listings else "—"

kpi_html = f"""
<div class="kpi-grid">
    <div class="kpi-card accent">
        <div class="kpi-label">Filtered Listings</div>
        <div class="kpi-value accent">{total_listings:,}</div>
        <div class="kpi-sub">of {len(df):,} total</div>
    </div>
    <div class="kpi-card emerald">
        <div class="kpi-label">Median Predicted</div>
        <div class="kpi-value emerald">{fmt_money(median_predicted)}</div>
        <div class="kpi-sub">50th percentile</div>
    </div>
    <div class="kpi-card amber">
        <div class="kpi-label">Good Deals</div>
        <div class="kpi-value amber">{n_good_deals:,}</div>
        <div class="kpi-sub">{pct_good} of filtered</div>
    </div>
    <div class="kpi-card violet">
        <div class="kpi-label">Overpriced</div>
        <div class="kpi-value violet">{n_overpriced:,}</div>
        <div class="kpi-sub">{pct_over} of filtered</div>
    </div>
</div>
"""
st.markdown(kpi_html, unsafe_allow_html=True)


# ============================================================
# TABS (Map first)
# ============================================================

tab_map, tab_overview, tab_dealers, tab_predict = st.tabs(
    ["🗺️ Map", "📊 Overview", "🏆 Top Dealers", "🔮 Predict a property"]
)


# ============================================================
# TAB: MAP (Property points top, Suburb choropleth bottom)
# ============================================================

with tab_map:
    if len(df_f) == 0:
        st.warning("No listings to display on the map.")
    else:
        # ------------------------------------------------------
        # MAP 1: Property Points (individual markers, no cluster)
        # ------------------------------------------------------
        st.markdown("### Property Map")
        st.caption(
            f"Showing {min(len(df_f), 3000):,} individual listings "
            "(sampled if filter result exceeds 3,000 for browser performance). "
            "Each dot is a property, color-coded by deal signal."
        )

        map_df = df_f if len(df_f) <= 3000 else df_f.sample(3000, random_state=0)

        center_lat = float(map_df["Latitude"].median())
        center_lon = float(map_df["Longitude"].median())

        m1 = folium.Map(
            location=[center_lat, center_lon], zoom_start=10,
            tiles="CartoDB positron",
            prefer_canvas=True,                        # fast rendering for many dots
        )

        signal_colors = {
            "Good Deal":       "#059669",
            "Fair":            "#2563eb",
            "Overpriced":      "#e11d48",
            "No Asking Price": "#94a3b8",
        }

        for _, r in map_df.iterrows():
            color = signal_colors.get(r["Deal_Signal"], "#2563eb")
            asking = ("—" if pd.isna(r["Numeric_Price"])
                      else f"${r['Numeric_Price']:,.0f}")
            url_html = (
                f'<br><a href="{r["URL"]}" target="_blank" '
                f'style="color:#2563eb;font-weight:600;text-decoration:none;">View Listing →</a>'
                if "URL" in r and pd.notna(r["URL"]) else ""
            )
            popup = (
                f"<div style='font-family: DM Sans, sans-serif; min-width: 220px;'>"
                f"<b style='color:#1a1d27;'>{r['Suburb']}</b><br>"
                f"<span style='color:#7c8499;font-size:11px;'>{r['Property_Type']}</span><br><br>"
                f"<span style='color:#4a5168;'>"
                f"{int(r['Beds']) if not pd.isna(r['Beds']) else '—'}-bed, "
                f"{int(r['Baths']) if not pd.isna(r['Baths']) else '—'}-bath</span><br>"
                f"Asking: {asking}<br>"
                f"Predicted: <b style='color:{color};'>${r['Predicted_Price']:,.0f}</b><br>"
                f"<span style='color:#7c8499;font-size:11px;'>"
                f"Range: ${r['Predicted_Price_Lower']:,.0f} – "
                f"${r['Predicted_Price_Upper']:,.0f}</span><br>"
                f"<span style='display:inline-block; margin-top:6px; padding:2px 8px; "
                f"background:{color}22; color:{color}; border-radius:4px; "
                f"font-size:10px; font-weight:600; text-transform:uppercase;'>"
                f"{r['Deal_Signal']}</span>"
                f"{url_html}"
                f"</div>"
            )
            folium.CircleMarker(
                location=[r["Latitude"], r["Longitude"]],
                radius=5,
                color="rgba(255,255,255,0.6)",
                weight=1.2,
                fillColor=color,
                fillOpacity=0.85,
                popup=folium.Popup(popup, max_width=280),
            ).add_to(m1)

        # Legend overlay.
        legend_html = """
        <div style="position: absolute; bottom: 24px; left: 24px; z-index: 1000;
                    background: rgba(255,255,255,0.95); padding: 12px 16px;
                    border-radius: 11px; border: 1px solid rgba(0,0,0,0.08);
                    box-shadow: 0 4px 16px rgba(0,0,0,0.1);
                    font-family: 'DM Sans', sans-serif; font-size: 11.5px;
                    color: #4a5168;">
            <div style="font-weight: 700; letter-spacing: 2px; text-transform: uppercase;
                        font-size: 9.5px; color: #7c8499; margin-bottom: 9px;">
                Deal Signal
            </div>
            <div style="display:flex; align-items:center; gap:9px; margin-bottom:6px;">
                <span style="width:10px;height:10px;background:#059669;border-radius:3px;"></span>Good Deal
            </div>
            <div style="display:flex; align-items:center; gap:9px; margin-bottom:6px;">
                <span style="width:10px;height:10px;background:#2563eb;border-radius:3px;"></span>Fair
            </div>
            <div style="display:flex; align-items:center; gap:9px; margin-bottom:6px;">
                <span style="width:10px;height:10px;background:#e11d48;border-radius:3px;"></span>Overpriced
            </div>
            <div style="display:flex; align-items:center; gap:9px;">
                <span style="width:10px;height:10px;background:#94a3b8;border-radius:3px;"></span>No Asking Price
            </div>
        </div>
        """
        m1.get_root().html.add_child(folium.Element(legend_html))

        st_folium(m1, width=None, height=550, returned_objects=[],
                  key="property_map")

        st.markdown("---")

        # ------------------------------------------------------
        # MAP 2: Suburb Choropleth (median predicted price)
        # ------------------------------------------------------
        st.markdown("### Suburb Map")
        st.caption(
            "Suburb-level median predicted price. Hover a suburb for stats. "
            "Suburbs with fewer than 3 listings under current filters are shown in gray."
        )

        if geojson is None:
            st.warning(
                "Suburb boundaries GeoJSON not found at "
                "`data/melbourne_suburb_boundaries.geojson`. Skipping suburb map."
            )
        else:
            # Compute suburb stats from filtered data.
            sub_stats = (df_f.groupby("Suburb")
                         .agg(n              = ("Property_ID", "count"),
                              median_pred    = ("Predicted_Price", "median"),
                              mean_pred      = ("Predicted_Price", "mean"),
                              min_pred       = ("Predicted_Price", "min"),
                              max_pred       = ("Predicted_Price", "max"),
                              latitude       = ("Latitude", "mean"),
                              longitude      = ("Longitude", "mean"))
                         .reset_index())
            sub_stats["Suburb_upper"] = sub_stats["Suburb"].str.upper()

            # Detect GeoJSON suburb property key.
            sample_feat = geojson["features"][0]
            possible_keys = ["Suburb", "SUBURB", "suburb", "Name", "name",
                             "NAME", "vic_loca_2", "LOC_NAME", "loc_name"]
            geo_key = None
            for k in possible_keys:
                if k in sample_feat["properties"]:
                    geo_key = k
                    break
            if geo_key is None:
                geo_key = list(sample_feat["properties"].keys())[0]

            stats_lookup = sub_stats.set_index("Suburb_upper").to_dict("index")

            if len(sub_stats) > 0:
                center_lat2 = float(sub_stats["latitude"].median())
                center_lon2 = float(sub_stats["longitude"].median())
            else:
                center_lat2, center_lon2 = -37.8136, 144.9631

            m2 = folium.Map(
                location=[center_lat2, center_lon2], zoom_start=10,
                tiles="CartoDB positron",
            )

            def price_color(p):
                if p is None or pd.isna(p):
                    return "#cbd5e1"
                if p < 750_000:    return "#059669"
                if p < 1_500_000:  return "#2563eb"
                if p < 3_000_000:  return "#d97706"
                return "#e11d48"

            def style_function(feature):
                suburb_name = str(feature["properties"].get(geo_key, "")).upper()
                stats = stats_lookup.get(suburb_name)
                if stats and stats["n"] >= 3:
                    return {
                        "fillColor":   price_color(stats["median_pred"]),
                        "color":       "#ffffff",
                        "weight":      1,
                        "fillOpacity": 0.7,
                    }
                else:
                    return {
                        "fillColor":   "#e5e7eb",
                        "color":       "#cbd5e1",
                        "weight":      0.5,
                        "fillOpacity": 0.3,
                    }

            def highlight_function(feature):
                return {"weight": 3, "color": "#1f3a5f", "fillOpacity": 0.85}

            # Enrich GeoJSON properties so the tooltip can read them.
            for feat in geojson["features"]:
                suburb_name = str(feat["properties"].get(geo_key, "")).upper()
                stats = stats_lookup.get(suburb_name)
                if stats and stats["n"] >= 3:
                    feat["properties"]["_n"]      = int(stats["n"])
                    feat["properties"]["_median"] = f"${stats['median_pred']:,.0f}"
                    feat["properties"]["_mean"]   = f"${stats['mean_pred']:,.0f}"
                    feat["properties"]["_min"]    = f"${stats['min_pred']:,.0f}"
                    feat["properties"]["_max"]    = f"${stats['max_pred']:,.0f}"
                else:
                    feat["properties"]["_n"]      = 0
                    feat["properties"]["_median"] = "—"
                    feat["properties"]["_mean"]   = "—"
                    feat["properties"]["_min"]    = "—"
                    feat["properties"]["_max"]    = "—"

            folium.GeoJson(
                geojson,
                style_function=style_function,
                highlight_function=highlight_function,
                tooltip=folium.GeoJsonTooltip(
                    fields=[geo_key, "_n", "_median", "_mean", "_min", "_max"],
                    aliases=["Suburb:", "Listings:", "Median predicted:",
                             "Mean predicted:", "Min:", "Max:"],
                    sticky=True,
                    labels=True,
                    style="background-color: white; "
                          "border: 1px solid rgba(0,0,0,0.08); border-radius: 8px; "
                          "padding: 10px; font-family: 'DM Sans', sans-serif; "
                          "font-size: 12px; color: #1a1d27; "
                          "box-shadow: 0 4px 12px rgba(0,0,0,0.08);",
                ),
            ).add_to(m2)

            # Choropleth legend.
            legend_html2 = """
            <div style="position: absolute; bottom: 24px; left: 24px; z-index: 1000;
                        background: rgba(255,255,255,0.95); padding: 12px 16px;
                        border-radius: 11px; border: 1px solid rgba(0,0,0,0.08);
                        box-shadow: 0 4px 16px rgba(0,0,0,0.1);
                        font-family: 'DM Sans', sans-serif; font-size: 11.5px;
                        color: #4a5168;">
                <div style="font-weight: 700; letter-spacing: 2px; text-transform: uppercase;
                            font-size: 9.5px; color: #7c8499; margin-bottom: 9px;">
                    Median Predicted Price
                </div>
                <div style="display:flex;align-items:center;gap:9px;margin-bottom:6px;">
                    <span style="width:10px;height:10px;background:#059669;border-radius:3px;"></span>Under $750k
                </div>
                <div style="display:flex;align-items:center;gap:9px;margin-bottom:6px;">
                    <span style="width:10px;height:10px;background:#2563eb;border-radius:3px;"></span>$750k – $1.5M
                </div>
                <div style="display:flex;align-items:center;gap:9px;margin-bottom:6px;">
                    <span style="width:10px;height:10px;background:#d97706;border-radius:3px;"></span>$1.5M – $3M
                </div>
                <div style="display:flex;align-items:center;gap:9px;margin-bottom:6px;">
                    <span style="width:10px;height:10px;background:#e11d48;border-radius:3px;"></span>Over $3M
                </div>
                <div style="display:flex;align-items:center;gap:9px;">
                    <span style="width:10px;height:10px;background:#e5e7eb;border-radius:3px;"></span>&lt; 3 listings
                </div>
            </div>
            """
            m2.get_root().html.add_child(folium.Element(legend_html2))

            st_folium(m2, width=None, height=550, returned_objects=[],
                      key="suburb_map")
            
# ============================================================
# TAB: OVERVIEW (CHARTS)
# ============================================================

with tab_overview:
    if len(df_f) == 0:
        st.warning("No listings match the current filters.")
    else:
        col1, col2 = st.columns(2)

        # 1. Deal signal distribution (donut, color-coded by signal).
        with col1:
            st.markdown("### Deal Signal Distribution")
            sig_counts = df_f["Deal_Signal"].value_counts().reset_index()
            sig_counts.columns = ["Deal_Signal", "Count"]
            color_map = {
                "Good Deal":       "#059669",
                "Fair":            "#2563eb",
                "Overpriced":      "#e11d48",
                "No Asking Price": "#94a3b8",
            }
            fig = px.pie(sig_counts, values="Count", names="Deal_Signal",
                         color="Deal_Signal", color_discrete_map=color_map,
                         hole=0.55)
            fig.update_traces(textposition="inside", textinfo="percent",
                              textfont=dict(family="DM Sans", size=12, color="white"))
            fig.update_layout(
                height=360, margin=dict(t=10, b=10, l=10, r=10),
                font=dict(family="DM Sans", size=12, color="#4a5168"),
                paper_bgcolor="rgba(0,0,0,0)",
                legend=dict(orientation="h", yanchor="bottom", y=-0.15,
                            xanchor="center", x=0.5),
            )
            st.plotly_chart(fig, use_container_width=True)

        # 2. Property type bar (single color, no gradient).
        with col2:
            st.markdown("### Property Types")
            tcounts = (df_f["Property_Type"].value_counts()
                       .head(10).reset_index())
            tcounts.columns = ["Property_Type", "Count"]
            fig = px.bar(tcounts, x="Count", y="Property_Type",
                         orientation="h")
            fig.update_traces(marker_color="#2563eb", marker_line_width=0)
            fig.update_layout(
                height=360, margin=dict(t=10, b=10, l=10, r=10),
                yaxis={"categoryorder": "total ascending", "title": None},
                xaxis={"title": None, "gridcolor": "rgba(0,0,0,0.05)"},
                font=dict(family="DM Sans", size=12, color="#4a5168"),
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                showlegend=False,
            )
            st.plotly_chart(fig, use_container_width=True)

        st.markdown("---")

        # 3. Predicted price distribution (histogram).
        st.markdown("### Predicted Price Distribution")
        fig = px.histogram(df_f, x="Predicted_Price", nbins=60,
                           labels={"Predicted_Price": "Predicted price (AUD)"})
        fig.update_traces(marker_color="#2563eb", marker_line_width=0)
        fig.update_layout(
            height=340, margin=dict(t=10, b=10, l=10, r=10),
            yaxis={"title": None, "gridcolor": "rgba(0,0,0,0.05)"},
            xaxis={"title": None, "gridcolor": "rgba(0,0,0,0.05)"},
            font=dict(family="DM Sans", size=12, color="#4a5168"),
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            showlegend=False, bargap=0.02,
        )
        st.plotly_chart(fig, use_container_width=True)

        # 4. Top 15 suburbs by median predicted price (single color).
        st.markdown("### Top 15 Suburbs By Median Predicted Price")
        st.caption("Suburbs with at least 5 listings under the current filters.")
        sub_stats = (df_f.groupby("Suburb")
                     .agg(n=("Property_ID", "count"),
                          median_pred=("Predicted_Price", "median"))
                     .query("n >= 5")
                     .sort_values("median_pred", ascending=False)
                     .head(15)
                     .reset_index())
        if len(sub_stats) > 0:
            fig = px.bar(sub_stats, x="median_pred", y="Suburb",
                         orientation="h",
                         labels={"median_pred": "Median predicted (AUD)"})
            fig.update_traces(marker_color="#2563eb", marker_line_width=0)
            fig.update_layout(
                height=460, margin=dict(t=10, b=10, l=10, r=10),
                yaxis={"categoryorder": "total ascending", "title": None},
                xaxis={"title": None, "gridcolor": "rgba(0,0,0,0.05)"},
                font=dict(family="DM Sans", size=12, color="#4a5168"),
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                showlegend=False,
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Not enough listings per suburb under the current filters.")


# ============================================================
# TAB: TOP DEALERS
# ============================================================

with tab_dealers:
    st.markdown("### Top Deals: Predicted Price Vs Asking Price")
    st.caption(
        "Best opportunities (predicted significantly higher than asking) and "
        "most overpriced listings (predicted significantly lower than asking). "
        "Only listings with an asking price are included."
    )

    dealable = df_f[df_f["Numeric_Price"].notna()].copy()

    if len(dealable) == 0:
        st.warning("No listings with asking prices match the current filters.")
    else:
        dealable["Gap_AUD"] = dealable["Predicted_Price"] - dealable["Numeric_Price"]
        dealable["Gap_Pct"] = (dealable["Gap_AUD"] / dealable["Numeric_Price"]) * 100

        col_a, col_b = st.columns(2)
        col_a.metric("Eligible listings (with asking)", f"{len(dealable):,}")
        col_b.metric("Median gap %", f"{dealable['Gap_Pct'].median():.1f}%")

        st.markdown("---")

        # --------------------------------------------------
        # TOP 20 GOOD DEALS
        # --------------------------------------------------
        st.markdown("### 🟢 Top 20 Good Deals")
        st.caption("Predicted price exceeds asking price by the largest margin (% terms).")

        good = dealable.nlargest(20, "Gap_Pct").copy()
        good_display = good[[
            "Suburb", "Property_Type", "Beds", "Baths",
            "Numeric_Price", "Predicted_Price",
            "Predicted_Price_Lower", "Predicted_Price_Upper",
            "Gap_AUD", "Gap_Pct", "URL",
        ]].rename(columns={
            "Numeric_Price":         "Asking",
            "Predicted_Price":       "Predicted",
            "Predicted_Price_Lower": "Pred Low",
            "Predicted_Price_Upper": "Pred High",
            "Gap_AUD":               "Gap ($)",
            "Gap_Pct":               "Gap %",
            "Property_Type":         "Type",
            "URL":                   "Listing",
        })

        st.dataframe(
            good_display,
            use_container_width=True,
            height=520,
            column_config={
                "Asking":    st.column_config.NumberColumn(format="$%d"),
                "Predicted": st.column_config.NumberColumn(format="$%d"),
                "Pred Low":  st.column_config.NumberColumn(format="$%d"),
                "Pred High": st.column_config.NumberColumn(format="$%d"),
                "Gap ($)":   st.column_config.NumberColumn(format="$%d"),
                "Gap %":     st.column_config.NumberColumn(format="%.1f%%"),
                "Listing":   st.column_config.LinkColumn(display_text="View →"),
            },
            hide_index=True,
        )

        st.markdown("---")

        # --------------------------------------------------
        # TOP 20 OVERPRICED
        # --------------------------------------------------
        st.markdown("### 🔴 Top 20 Overpriced Listings")
        st.caption("Asking price exceeds predicted price by the largest margin (% terms).")

        bad = dealable.nsmallest(20, "Gap_Pct").copy()
        bad_display = bad[[
            "Suburb", "Property_Type", "Beds", "Baths",
            "Numeric_Price", "Predicted_Price",
            "Predicted_Price_Lower", "Predicted_Price_Upper",
            "Gap_AUD", "Gap_Pct", "URL",
        ]].rename(columns={
            "Numeric_Price":         "Asking",
            "Predicted_Price":       "Predicted",
            "Predicted_Price_Lower": "Pred Low",
            "Predicted_Price_Upper": "Pred High",
            "Gap_AUD":               "Gap ($)",
            "Gap_Pct":               "Gap %",
            "Property_Type":         "Type",
            "URL":                   "Listing",
        })

        st.dataframe(
            bad_display,
            use_container_width=True,
            height=520,
            column_config={
                "Asking":    st.column_config.NumberColumn(format="$%d"),
                "Predicted": st.column_config.NumberColumn(format="$%d"),
                "Pred Low":  st.column_config.NumberColumn(format="$%d"),
                "Pred High": st.column_config.NumberColumn(format="$%d"),
                "Gap ($)":   st.column_config.NumberColumn(format="$%d"),
                "Gap %":     st.column_config.NumberColumn(format="%.1f%%"),
                "Listing":   st.column_config.LinkColumn(display_text="View →"),
            },
            hide_index=True,
        )

        st.markdown("---")
        combined = pd.concat([
            good_display.assign(Category="Good Deal"),
            bad_display.assign(Category="Overpriced"),
        ])
        csv = combined.to_csv(index=False).encode("utf-8")
        st.download_button("📥 Download top deals as CSV", data=csv,
                           file_name="top_deals.csv",
                           mime="text/csv")
        
# ============================================================
# TAB: ON-DEMAND PREDICT
# ============================================================

with tab_predict:
    st.markdown("### Estimate A Property")
    st.caption(
        "Enter property details below to get an estimated price based on "
        "the current model. Predictions reflect the latest market level "
        "(current Year and Month are injected automatically)."
    )

    col_left, col_right = st.columns(2)

    with col_left:
        st.markdown("#### Location")
        suburb_input = st.selectbox(
            "Suburb",
            options=sorted(suburbs.index.tolist()),
            help="Pick from training-known suburbs for best accuracy.",
        )
        sub_match = df[df["Suburb"] == suburb_input].head(1)
        default_postcode = int(sub_match["Postcode"].iloc[0]) if len(sub_match) else 3000
        default_lat = float(sub_match["Latitude"].iloc[0]) if len(sub_match) else -37.81
        default_lon = float(sub_match["Longitude"].iloc[0]) if len(sub_match) else 144.96
        default_cbd = float(sub_match["Distance_to_CBD_km"].iloc[0]) if len(sub_match) else 10.0

        postcode_input = st.number_input("Postcode", value=default_postcode, step=1)
        lat_input      = st.number_input("Latitude",  value=default_lat,  format="%.5f", step=0.001)
        lon_input      = st.number_input("Longitude", value=default_lon,  format="%.5f", step=0.001)
        cbd_input      = st.number_input("Distance to CBD (km)", value=default_cbd,
                                          min_value=0.0, max_value=200.0, step=0.5)

    with col_right:
        st.markdown("#### Property Attributes")
        type_input = st.selectbox(
            "Property Type",
            options=sorted(df["Property_Type"].unique().tolist()),
        )
        beds_input  = st.number_input("Beds",       value=3, min_value=0, max_value=10, step=1)
        baths_input = st.number_input("Baths",      value=2, min_value=0, max_value=10, step=1)
        cars_input  = st.number_input("Car spaces", value=2, min_value=0, max_value=10, step=1)
        land_input  = st.number_input("Land size (sqm, 0 if apartment)",
                                      value=500.0, min_value=0.0, step=10.0)

    st.markdown("---")
    predict_btn = st.button("🔮 Estimate price", type="primary", use_container_width=True)

    if predict_btn:
        warnings_list = []

        sub_key = suburb_input.upper()
        if sub_key in suburbs.index:
            s = suburbs.loc[sub_key]
            income, age, pop, crime, pcount, dtrain = (
                float(s["abs_median_income_weekly"]),
                float(s["abs_median_age"]),
                float(s["abs_population"]),
                float(s["crime_rate_per_100k"]),
                float(s["Propertycount"]),
                float(s["dist_nearest_train_km"]),
            )
        else:
            warnings_list.append(
                f"Suburb '{suburb_input}' has no historical data - "
                "median fallbacks used for enrichment."
            )
            meds = models["preprocessor"]["numeric_medians"]
            income = meds.get("abs_median_income_weekly", 800)
            age    = meds.get("abs_median_age", 38)
            pop    = meds.get("abs_population", 20000)
            crime  = meds.get("crime_rate_per_100k", 6000)
            pcount = meds.get("Propertycount", 10000)
            dtrain = meds.get("dist_nearest_train_km", 1.0)

        land_types = set(decisions["land_property_types"])
        type_is_land = type_input in land_types
        no_rooms = (beds_input == 0) and (baths_input == 0)
        has_land = land_input > 0
        is_land = int(type_is_land or (no_rooms and has_land))

        lat_min, lat_max = decisions["metro_envelope"]["lat"]
        lon_min, lon_max = decisions["metro_envelope"]["lon"]
        out_of_metro = int(not (lat_min <= lat_input <= lat_max
                                and lon_min <= lon_input <= lon_max))

        now = datetime.now()
        row = {
            "Property_ID":              0,
            "Status":                   "For Sale",
            "Suburb":                   suburb_input.upper(),
            "Postcode":                 int(postcode_input),
            "Property_Type":            type_input,
            "Beds":                     float(beds_input) if not (no_rooms and is_land) else np.nan,
            "Baths":                    float(baths_input) if not (no_rooms and is_land) else np.nan,
            "Car_Spaces":               int(cars_input),
            "LandSize_sqm":             float(land_input) if land_input > 0 else np.nan,
            "Latitude":                 lat_input,
            "Longitude":                lon_input,
            "Distance_to_CBD_km":       cbd_input,
            "Raw_Price":                None,
            "Numeric_Price":            None,
            "abs_median_income_weekly": income,
            "abs_median_age":           age,
            "abs_population":           pop,
            "crime_rate_per_100k":      crime,
            "Propertycount":            pcount,
            "dist_nearest_train_km":    dtrain,
            "is_land":                  is_land,
            "out_of_metro":             out_of_metro,
            "Year":                     now.year,
            "Month":                    now.month,
        }
        user_df = pd.DataFrame([row])

        user_df = add_engineered_features(user_df, decisions)
        X = transform(user_df, models["preprocessor"])

        y_point = float(np.expm1(models["point"].predict(X))[0])
        y_q10   = float(np.expm1(models["q10"].predict(X))[0])
        y_q90   = float(np.expm1(models["q90"].predict(X))[0])
        lower = min(y_q10, y_point)
        upper = max(y_q90, y_point)
        width_pct = (upper - lower) / y_point * 100 if y_point > 0 else 0

        if y_point > 2_500_000:
            warnings_list.append("Prediction is in the luxury range (>$2.5M) - higher uncertainty (~23% MAPE).")
        elif y_point < 500_000:
            warnings_list.append("Prediction is in the budget range (<$500k) - higher uncertainty (~17% MAPE).")

        if type_input in decisions.get("new_build_types", []):
            warnings_list.append("New-build property type - training data is sparse for this category.")

        # Render the result as styled KPI cards mirroring the dashboard aesthetic.
        result_html = f"""
        <div class="kpi-grid" style="margin-top: 16px;">
            <div class="kpi-card emerald">
                <div class="kpi-label">Lower (10th pct)</div>
                <div class="kpi-value emerald">${lower:,.0f}</div>
                <div class="kpi-sub">Conservative estimate</div>
            </div>
            <div class="kpi-card accent">
                <div class="kpi-label">Point Estimate</div>
                <div class="kpi-value accent">${y_point:,.0f}</div>
                <div class="kpi-sub">Most likely value</div>
            </div>
            <div class="kpi-card amber">
                <div class="kpi-label">Upper (90th pct)</div>
                <div class="kpi-value amber">${upper:,.0f}</div>
                <div class="kpi-sub">Optimistic estimate</div>
            </div>
            <div class="kpi-card violet">
                <div class="kpi-label">Interval Width</div>
                <div class="kpi-value violet">{width_pct:.1f}%</div>
                <div class="kpi-sub">of point estimate</div>
            </div>
        </div>
        """
        st.markdown(result_html, unsafe_allow_html=True)

        st.caption(f"Inference date: {now.strftime('%d %b %Y')}")

        if warnings_list:
            st.warning("⚠️ " + " ".join(warnings_list))

        if sub_key in df["Suburb"].values:
            sub_median = df[df["Suburb"] == sub_key]["Predicted_Price"].median()
            diff = (y_point - sub_median) / sub_median * 100
            arrow = "↑" if diff > 0 else "↓"
            st.info(
                f"For comparison, the median predicted price across **{sub_key}** "
                f"For Sale listings is **${sub_median:,.0f}**. Your estimate is "
                f"**{arrow} {abs(diff):.1f}%** of the suburb median."
            )


# ============================================================
# FOOTER
# ============================================================

st.markdown("---")
st.caption(
    f"Model: XGBoost (n_estimators={metrics['hyperparameters']['n_estimators']}, "
    f"max_depth={metrics['hyperparameters']['max_depth']}) • "
    f"Validation RMSE ${metrics['validation_metrics']['rmse_aud']:,.0f} • "
    f"Test MAPE {metrics['test_metrics']['mape']:.2f}% • "
    f"Last training run on {metrics.get('trained_on', 'full Sold')}."
)
st.caption("Predictions update weekly. See the EDA and ML reports for methodology.")