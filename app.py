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
import plotly.graph_objects as go
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
    page_title="Melbourne Property Price Dashboard",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Global CSS matching dashboard.html theme ─────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:ital,opsz,wght@0,9..40,300;0,9..40,400;0,9..40,500;0,9..40,600;0,9..40,700;1,9..40,300&family=DM+Serif+Display:ital@0;1&display=swap');

:root {
    --bg:            #f4f6fb;
    --surface:       #ffffff;
    --surface2:      #f0f2f8;
    --border:        rgba(0,0,0,0.07);
    --border-hover:  rgba(0,0,0,0.14);
    --text:          #1a1d27;
    --text-muted:    #7c8499;
    --text-dim:      #4a5168;
    --accent:        #2563eb;
    --accent-light:  rgba(37,99,235,0.08);
    --emerald:       #059669;
    --emerald-light: rgba(5,150,105,0.08);
    --amber:         #d97706;
    --rose:          #e11d48;
    --violet:        #7c3aed;
    --shadow-sm:     0 1px 3px rgba(0,0,0,0.06), 0 1px 2px rgba(0,0,0,0.04);
    --shadow-md:     0 4px 12px rgba(0,0,0,0.08), 0 2px 4px rgba(0,0,0,0.04);
}

html, body, [class*="css"] {
    font-family: 'DM Sans', sans-serif !important;
    color: var(--text);
}

/* ── HEADER ── */
.dash-header {
    background: var(--surface);
    border-bottom: 1px solid var(--border);
    padding: 28px 0 24px;
    margin: -1rem -1rem 1.5rem -1rem;
    position: relative;
    overflow: hidden;
}
.dash-header::before {
    content: '';
    position: absolute;
    top: -60px; right: -60px;
    width: 320px; height: 320px;
    background: radial-gradient(circle, rgba(37,99,235,0.06) 0%, transparent 70%);
    pointer-events: none;
}
.dash-header-inner {
    padding: 0 2rem;
    display: flex;
    align-items: flex-end;
    justify-content: space-between;
    flex-wrap: wrap;
    gap: 16px;
}
.header-eyebrow {
    font-size: 10px;
    font-weight: 700;
    letter-spacing: 3px;
    text-transform: uppercase;
    color: var(--accent);
    margin-bottom: 8px;
    display: flex;
    align-items: center;
    gap: 8px;
}
.header-eyebrow::before {
    content: '';
    display: block;
    width: 18px; height: 2px;
    background: var(--accent);
    border-radius: 2px;
}
.header-title {
    font-family: 'DM Serif Display', serif;
    font-size: clamp(1.6rem, 3vw, 2.4rem);
    line-height: 1.1;
    color: var(--text);
    font-weight: 400;
    margin: 0;
}
.header-title em { font-style: italic; color: var(--accent); }
.header-right { display: flex; align-items: center; gap: 12px; flex-wrap: wrap; }
.live-badge {
    display: inline-flex;
    align-items: center;
    gap: 7px;
    background: var(--emerald-light);
    border: 1px solid rgba(5,150,105,0.18);
    color: var(--emerald);
    padding: 6px 14px;
    border-radius: 100px;
    font-size: 11.5px;
    font-weight: 600;
}
.live-dot {
    width: 6px; height: 6px;
    background: var(--emerald);
    border-radius: 50%;
    animation: livePulse 2s infinite;
    flex-shrink: 0;
}
@keyframes livePulse { 0%,100%{opacity:1;} 50%{opacity:0.35;} }
.model-chip {
    display: inline-flex;
    align-items: center;
    background: var(--accent-light);
    border: 1px solid rgba(37,99,235,0.2);
    color: var(--accent);
    padding: 6px 14px;
    border-radius: 100px;
    font-size: 11.5px;
    font-weight: 600;
}

/* ── STAT CARDS ── */
.stat-grid {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 14px;
    margin-bottom: 1.5rem;
}
.stat-card {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 14px;
    padding: 22px 22px 18px;
    position: relative;
    overflow: hidden;
    box-shadow: var(--shadow-sm);
    transition: border-color .2s, box-shadow .2s, transform .2s;
    animation: fadeUp .4s ease both;
}
.stat-card::after {
    content: '';
    position: absolute;
    bottom: 0; left: 0; right: 0;
    height: 3px;
    background: var(--bar-color, transparent);
    opacity: 0.7;
}
.stat-card:hover {
    border-color: var(--border-hover);
    box-shadow: var(--shadow-md);
    transform: translateY(-2px);
}
.stat-card-1 { --bar-color: var(--accent);  animation-delay: .05s; }
.stat-card-2 { --bar-color: var(--emerald); animation-delay: .10s; }
.stat-card-3 { --bar-color: var(--amber);   animation-delay: .15s; }
.stat-card-4 { --bar-color: var(--violet);  animation-delay: .20s; }

.stat-label {
    font-size: 10.5px;
    font-weight: 700;
    letter-spacing: 2px;
    text-transform: uppercase;
    color: var(--text-muted);
    margin-bottom: 10px;
}
.stat-value {
    font-family: 'DM Serif Display', serif;
    font-size: 2.1rem;
    line-height: 1;
    font-weight: 400;
}
.stat-value.c-accent  { color: var(--accent); }
.stat-value.c-emerald { color: var(--emerald); }
.stat-value.c-amber   { color: var(--amber); }
.stat-value.c-violet  { color: var(--violet); }
.stat-sub { font-size: 11.5px; color: var(--text-muted); margin-top: 7px; }

/* ── PANEL TITLE ── */
.panel-title {
    font-size: 10px;
    font-weight: 700;
    letter-spacing: 2px;
    text-transform: uppercase;
    color: var(--text-muted);
}

/* ── DIVIDER ── */
.dash-divider {
    border: none;
    border-top: 1px solid var(--border);
    margin: 1.5rem 0;
}

/* ── ANIMATIONS ── */
@keyframes fadeUp {
    from { opacity: 0; transform: translateY(14px); }
    to   { opacity: 1; transform: translateY(0); }
}

/* ── STREAMLIT OVERRIDES ── */
div[data-testid="stSidebar"] {
    background: var(--surface) !important;
    border-right: 1px solid var(--border) !important;
}
div[data-testid="stSidebar"] * { font-family: 'DM Sans', sans-serif !important; }

.stTabs [data-baseweb="tab-list"] { gap: 8px; }
.stTabs [data-baseweb="tab"] {
    font-family: 'DM Sans', sans-serif !important;
    font-weight: 600 !important;
    font-size: 13.5px !important;
    border-radius: 10px !important;
    padding: 10px 20px !important;
    border: 1.5px solid var(--border) !important;
    background: var(--surface) !important;
    color: var(--text-muted) !important;
}
.stTabs [aria-selected="true"] {
    background: var(--accent) !important;
    color: #fff !important;
    border-color: var(--accent) !important;
}

button[kind="primary"] {
    background: var(--accent) !important;
    border: none !important;
    border-radius: 10px !important;
    font-family: 'DM Sans', sans-serif !important;
    font-weight: 600 !important;
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


df        = load_predictions()
decisions = load_decisions()
metrics   = load_metrics()
suburbs   = load_suburb_lookup()
models    = load_models()
geojson   = load_geojson()


# ── Auto-detect last update date from data ────────────────────────────────
def _detect_last_updated(frame: pd.DataFrame) -> str:
    """Try common date-like columns and return the most recent date found."""
    for col in ["Last_Updated", "last_updated", "Date", "date", "SaleDate"]:
        if col in frame.columns:
            try:
                parsed = pd.to_datetime(frame[col], errors="coerce").dropna()
                if len(parsed):
                    return parsed.max().strftime("%Y-%m-%d")
            except Exception:
                pass
    return datetime.now().strftime("%Y-%m-%d")

last_updated = _detect_last_updated(df)


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

def apply_filters(frame):
    out = frame.copy()
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

st.markdown(f"""
<div class="dash-header">
  <div class="dash-header-inner">
    <div>
      <div class="header-eyebrow">Real Estate Intelligence</div>
      <h1 class="header-title">Melbourne <em>Property</em> Dashboard</h1>
    </div>
    <div class="header-right">
      <div class="live-badge">
        <span class="live-dot"></span>
        Updated {last_updated}
      </div>
      <div class="model-chip">XGBoost · MAPE {metrics['test_metrics']['mape']:.1f}%</div>
    </div>
  </div>
</div>
""", unsafe_allow_html=True)


# ============================================================
# KPI CARDS
# ============================================================

n_total   = len(df_f)
med_price = f"${df_f['Predicted_Price'].median():,.0f}" if n_total else "—"
n_good    = int((df_f["Deal_Signal"] == "Good Deal").sum()) if n_total else 0
pct_good  = f"{n_good / n_total * 100:.1f}%" if n_total else "—"
n_subs    = int(df_f["Suburb"].nunique()) if n_total else 0

st.markdown(f"""
<div class="stat-grid">
  <div class="stat-card stat-card-1">
    <div class="stat-label">Filtered Listings</div>
    <div class="stat-value c-accent">{n_total:,}</div>
    <div class="stat-sub">of {len(df):,} total</div>
  </div>
  <div class="stat-card stat-card-2">
    <div class="stat-label">Median Predicted</div>
    <div class="stat-value c-emerald">{med_price}</div>
    <div class="stat-sub">50th percentile</div>
  </div>
  <div class="stat-card stat-card-3">
    <div class="stat-label">Good Deals</div>
    <div class="stat-value c-amber">{n_good:,}</div>
    <div class="stat-sub">{pct_good} of filtered</div>
  </div>
  <div class="stat-card stat-card-4">
    <div class="stat-label">Suburbs</div>
    <div class="stat-value c-violet">{n_subs:,}</div>
    <div class="stat-sub">In current view</div>
  </div>
</div>
""", unsafe_allow_html=True)

st.markdown('<hr class="dash-divider">', unsafe_allow_html=True)


# ============================================================
# PLOTLY HELPER
# ============================================================

_PLOTLY_BASE = dict(
    font_family="DM Sans",
    font_color="#7c8499",
    plot_bgcolor="white",
    paper_bgcolor="white",
    margin=dict(t=10, b=10, l=10, r=10),
    showlegend=False,
)

COLOR_MAP_SIGNAL = {
    "Good Deal":       "#059669",
    "Fair":            "#2563eb",
    "Overpriced":      "#e11d48",
    "No Asking Price": "#94a3b8",
}

PRICE_RANGE_COLORS = {
    "0": "#059669",
    "1": "#2563eb",
    "2": "#d97706",
    "3": "#e11d48",
    "w": "#94a3b8",
}

def _price_key(p):
    if pd.isna(p):      return "w"
    if p < 750_000:     return "0"
    if p < 1_500_000:   return "1"
    if p < 3_000_000:   return "2"
    return "3"


# ============================================================
# TABS
# ============================================================

tab_map, tab_overview, tab_dealers, tab_predict = st.tabs(
    ["🗺️ Map", "📊 Overview", "🏆 Top Deals", "🔮 Predict"]
)


# ============================================================
# TAB: MAP
# ============================================================

with tab_map:
    if len(df_f) == 0:
        st.warning("No listings to display on the map.")
    else:
        # ── Property Points map (no cluster, individual markers) ─────────
        st.markdown('<div class="panel-title" style="margin-bottom:10px;">Property Map</div>',
                    unsafe_allow_html=True)
        map_df = df_f

        center_lat = float(map_df["Latitude"].median())
        center_lon = float(map_df["Longitude"].median())

        # prefer_canvas=True draws points directly on canvas — no MarkerCluster
        m1 = folium.Map(
            location=[center_lat, center_lon],
            zoom_start=10,
            tiles="CartoDB positron",
            prefer_canvas=True,
        )

        signal_colors = {
            "Good Deal":       "#059669",
            "Fair":            "#2563eb",
            "Overpriced":      "#e11d48",
            "No Asking Price": "#94a3b8",
        }

        for _, r in map_df.iterrows():
            color  = signal_colors.get(r["Deal_Signal"], "#2563eb")
            asking = ("—" if pd.isna(r.get("Numeric_Price"))
                      else f"${r['Numeric_Price']:,.0f}")
            has_url = "URL" in r.index and pd.notna(r.get("URL"))
            url_html = (
                f'<a href="{r["URL"]}" target="_blank" '
                f'style="display:block;text-align:center;margin-top:10px;'
                f'background:#f0f2f8;color:#2563eb;padding:8px;border-radius:8px;'
                f'font-weight:700;font-size:12px;text-decoration:none;'
                f'border:1px solid rgba(0,0,0,0.07);">View Listing →</a>'
                if has_url else ""
            )
            beds  = int(r["Beds"])  if not pd.isna(r.get("Beds"))  else "—"
            baths = int(r["Baths"]) if not pd.isna(r.get("Baths")) else "—"

            popup_html = f"""
            <div style="font-family:'DM Sans',sans-serif;min-width:250px;padding:16px;">
              <div style="display:flex;justify-content:space-between;
                          align-items:center;margin-bottom:8px;">
                <span style="font-size:10px;font-weight:700;padding:3px 9px;
                             border-radius:4px;text-transform:uppercase;
                             background:rgba(5,150,105,0.1);color:#059669;">
                  For Sale
                </span>
                <span style="font-size:11px;color:#7c8499;font-weight:500;">
                  {r["Suburb"]}
                </span>
              </div>
              <div style="font-size:13px;font-weight:600;color:#1a1d27;margin-bottom:8px;">
                {r["Property_Type"]} · {beds} bed · {baths} bath
              </div>
              <div style="font-family:'DM Serif Display',serif;font-size:1.4rem;
                          color:{color};margin-bottom:12px;">
                ${r["Predicted_Price"]:,.0f}
              </div>
              <div style="display:grid;grid-template-columns:1fr 1fr;gap:6px;">
                <div style="background:#f0f2f8;border-radius:7px;padding:7px 9px;">
                  <div style="font-size:9px;font-weight:700;letter-spacing:1.5px;
                              text-transform:uppercase;color:#7c8499;margin-bottom:2px;">
                    Asking
                  </div>
                  <div style="font-size:12px;font-weight:600;color:#4a5168;">{asking}</div>
                </div>
                <div style="background:#f0f2f8;border-radius:7px;padding:7px 9px;">
                  <div style="font-size:9px;font-weight:700;letter-spacing:1.5px;
                              text-transform:uppercase;color:#7c8499;margin-bottom:2px;">
                    Signal
                  </div>
                  <div style="font-size:12px;font-weight:600;color:{color};">
                    {r["Deal_Signal"]}
                  </div>
                </div>
                <div style="background:#f0f2f8;border-radius:7px;padding:7px 9px;
                            grid-column:span 2;">
                  <div style="font-size:9px;font-weight:700;letter-spacing:1.5px;
                              text-transform:uppercase;color:#7c8499;margin-bottom:2px;">
                    Range
                  </div>
                  <div style="font-size:12px;font-weight:600;color:#4a5168;">
                    ${r["Predicted_Price_Lower"]:,.0f} – ${r["Predicted_Price_Upper"]:,.0f}
                  </div>
                </div>
              </div>
              {url_html}
            </div>"""

            folium.CircleMarker(
                location=[r["Latitude"], r["Longitude"]],
                radius=5,
                color="rgba(255,255,255,0.7)",
                weight=1.2,
                fill=True,
                fill_color=color,
                fill_opacity=0.85,
                popup=folium.Popup(popup_html, max_width=300),
            ).add_to(m1)

        legend_html = """
        <div style="position:fixed;bottom:30px;left:30px;z-index:1000;
                    background:rgba(255,255,255,0.96);padding:13px 16px;
                    border-radius:11px;border:1px solid rgba(0,0,0,0.08);
                    box-shadow:0 4px 16px rgba(0,0,0,0.1);
                    font-family:'DM Sans',sans-serif;font-size:12px;">
          <div style="font-size:9.5px;font-weight:700;letter-spacing:2px;
                      text-transform:uppercase;color:#7c8499;margin-bottom:9px;">
            Deal Signal
          </div>
          <div style="margin-bottom:5px;color:#4a5168;">
            <span style="display:inline-block;width:10px;height:10px;
                         background:#059669;border-radius:3px;margin-right:8px;
                         vertical-align:middle;"></span>Good Deal
          </div>
          <div style="margin-bottom:5px;color:#4a5168;">
            <span style="display:inline-block;width:10px;height:10px;
                         background:#2563eb;border-radius:3px;margin-right:8px;
                         vertical-align:middle;"></span>Fair
          </div>
          <div style="margin-bottom:5px;color:#4a5168;">
            <span style="display:inline-block;width:10px;height:10px;
                         background:#e11d48;border-radius:3px;margin-right:8px;
                         vertical-align:middle;"></span>Overpriced
          </div>
          <div style="color:#4a5168;">
            <span style="display:inline-block;width:10px;height:10px;
                         background:#94a3b8;border-radius:3px;margin-right:8px;
                         vertical-align:middle;"></span>No Asking Price
          </div>
        </div>"""
        m1.get_root().html.add_child(folium.Element(legend_html))

        st_folium(m1, width=None, height=560, returned_objects=[], key="property_map")

        st.markdown('<hr class="dash-divider">', unsafe_allow_html=True)

        # ── Suburb Choropleth ─────────────────────────────────────────────
        st.markdown(
            '<div class="panel-title" style="margin-bottom:10px;">'
            'Suburb Map — Median Predicted Price</div>',
            unsafe_allow_html=True,
        )
        st.caption("Hover for suburb stats. Grey = fewer than 3 listings under current filters.")

        if geojson is None:
            st.warning("Suburb boundaries GeoJSON not found at `data/melbourne_suburb_boundaries.geojson`.")
        else:
            sub_stats = (df_f.groupby("Suburb")
                         .agg(n           = ("Property_ID", "count"),
                              median_pred  = ("Predicted_Price", "median"),
                              mean_pred    = ("Predicted_Price", "mean"),
                              min_pred     = ("Predicted_Price", "min"),
                              max_pred     = ("Predicted_Price", "max"),
                              latitude     = ("Latitude", "mean"),
                              longitude    = ("Longitude", "mean"))
                         .reset_index())
            sub_stats["Suburb_upper"] = sub_stats["Suburb"].str.upper()

            sample_feat  = geojson["features"][0]
            possible_keys = ["Suburb", "SUBURB", "suburb", "Name", "name",
                             "NAME", "vic_loca_2", "LOC_NAME", "loc_name"]
            geo_key = next(
                (k for k in possible_keys if k in sample_feat["properties"]),
                list(sample_feat["properties"].keys())[0],
            )

            stats_lookup = sub_stats.set_index("Suburb_upper").to_dict("index")

            c2_lat = float(sub_stats["latitude"].median()) if len(sub_stats) else -37.8136
            c2_lon = float(sub_stats["longitude"].median()) if len(sub_stats) else 144.9631

            m2 = folium.Map(location=[c2_lat, c2_lon], zoom_start=10,
                            tiles="CartoDB positron")

            def _price_color(p):
                if p is None or pd.isna(p): return "#cbd5e1"
                if p < 750_000:   return "#059669"
                if p < 1_500_000: return "#2563eb"
                if p < 3_000_000: return "#d97706"
                return "#e11d48"

            def _style_fn(feature):
                s = stats_lookup.get(
                    str(feature["properties"].get(geo_key, "")).upper()
                )
                if s and s["n"] >= 3:
                    return {"fillColor": _price_color(s["median_pred"]),
                            "color": "#ffffff", "weight": 1, "fillOpacity": 0.7}
                return {"fillColor": "#e5e7eb", "color": "#cbd5e1",
                        "weight": 0.5, "fillOpacity": 0.3}

            def _highlight_fn(feature):
                return {"weight": 3, "color": "#1f3a5f", "fillOpacity": 0.85}

            for feat in geojson["features"]:
                sn = str(feat["properties"].get(geo_key, "")).upper()
                s  = stats_lookup.get(sn)
                if s and s["n"] >= 3:
                    feat["properties"]["_n"]      = int(s["n"])
                    feat["properties"]["_median"] = f"${s['median_pred']:,.0f}"
                    feat["properties"]["_mean"]   = f"${s['mean_pred']:,.0f}"
                    feat["properties"]["_min"]    = f"${s['min_pred']:,.0f}"
                    feat["properties"]["_max"]    = f"${s['max_pred']:,.0f}"
                else:
                    feat["properties"]["_n"]      = 0
                    feat["properties"]["_median"] = "—"
                    feat["properties"]["_mean"]   = "—"
                    feat["properties"]["_min"]    = "—"
                    feat["properties"]["_max"]    = "—"

            folium.GeoJson(
                geojson,
                style_function=_style_fn,
                highlight_function=_highlight_fn,
                tooltip=folium.GeoJsonTooltip(
                    fields=[geo_key, "_n", "_median", "_mean", "_min", "_max"],
                    aliases=["Suburb:", "Listings:", "Median predicted:",
                             "Mean predicted:", "Min:", "Max:"],
                    sticky=True, labels=True,
                    style=("background-color:white;border:1px solid #ddd;"
                           "border-radius:8px;padding:10px;"
                           "font-family:'DM Sans',sans-serif;font-size:12px;"),
                ),
            ).add_to(m2)

            choropleth_legend = """
            <div style="position:fixed;bottom:30px;left:30px;z-index:1000;
                        background:rgba(255,255,255,0.96);padding:13px 16px;
                        border-radius:11px;border:1px solid rgba(0,0,0,0.08);
                        box-shadow:0 4px 16px rgba(0,0,0,0.1);
                        font-family:'DM Sans',sans-serif;font-size:12px;">
              <div style="font-size:9.5px;font-weight:700;letter-spacing:2px;
                          text-transform:uppercase;color:#7c8499;margin-bottom:9px;">
                Median Predicted Price
              </div>
              <div style="margin-bottom:5px;color:#4a5168;">
                <span style="display:inline-block;width:10px;height:10px;
                             background:#059669;border-radius:3px;margin-right:8px;
                             vertical-align:middle;"></span>Under $750k
              </div>
              <div style="margin-bottom:5px;color:#4a5168;">
                <span style="display:inline-block;width:10px;height:10px;
                             background:#2563eb;border-radius:3px;margin-right:8px;
                             vertical-align:middle;"></span>$750k – $1.5M
              </div>
              <div style="margin-bottom:5px;color:#4a5168;">
                <span style="display:inline-block;width:10px;height:10px;
                             background:#d97706;border-radius:3px;margin-right:8px;
                             vertical-align:middle;"></span>$1.5M – $3M
              </div>
              <div style="margin-bottom:5px;color:#4a5168;">
                <span style="display:inline-block;width:10px;height:10px;
                             background:#e11d48;border-radius:3px;margin-right:8px;
                             vertical-align:middle;"></span>Over $3M
              </div>
              <div style="color:#4a5168;">
                <span style="display:inline-block;width:10px;height:10px;
                             background:#e5e7eb;border-radius:3px;margin-right:8px;
                             vertical-align:middle;"></span>&lt; 3 listings
              </div>
            </div>"""
            m2.get_root().html.add_child(folium.Element(choropleth_legend))

            st_folium(m2, width=None, height=560, returned_objects=[], key="suburb_map")


# ============================================================
# TAB: OVERVIEW
# ============================================================

with tab_overview:
    if len(df_f) == 0:
        st.warning("No listings match the current filters.")
    else:
        col1, col2 = st.columns(2)

        with col1:
            st.markdown(
                '<div class="panel-title" style="margin-bottom:8px;">Deal Signal Distribution</div>',
                unsafe_allow_html=True,
            )
            sig_counts = df_f["Deal_Signal"].value_counts().reset_index()
            sig_counts.columns = ["Deal_Signal", "Count"]
            fig = px.pie(sig_counts, values="Count", names="Deal_Signal",
                         color="Deal_Signal", color_discrete_map=COLOR_MAP_SIGNAL, hole=0.45)
            fig.update_layout(height=320, **_PLOTLY_BASE, showlegend=True,
                              legend=dict(font=dict(family="DM Sans", size=12),
                                          orientation="h", y=-0.05))
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.markdown(
                '<div class="panel-title" style="margin-bottom:8px;">Property Types</div>',
                unsafe_allow_html=True,
            )
            tcounts = df_f["Property_Type"].value_counts().head(10).reset_index()
            tcounts.columns = ["Property_Type", "Count"]
            fig = px.bar(tcounts, x="Count", y="Property_Type", orientation="h")
            fig.update_traces(
                marker_color="rgba(37,99,235,0.12)",
                marker_line_color="rgba(37,99,235,0.5)",
                marker_line_width=1.5,
            )
            fig.update_layout(
                height=320, **_PLOTLY_BASE,
                yaxis={"categoryorder": "total ascending",
                       "tickfont": {"family": "DM Sans", "size": 12}},
                xaxis={"gridcolor": "rgba(0,0,0,0.05)",
                       "tickfont": {"family": "DM Sans", "size": 11}},
            )
            st.plotly_chart(fig, use_container_width=True)

        st.markdown('<hr class="dash-divider">', unsafe_allow_html=True)

        st.markdown(
            '<div class="panel-title" style="margin-bottom:8px;">Predicted Price Distribution</div>',
            unsafe_allow_html=True,
        )

        # Histogram colored by price bracket
        prices_arr = df_f["Predicted_Price"].dropna().values
        BIN = 250_000; MAX_BIN = 5_000_000
        bin_edges = list(range(0, MAX_BIN + 1, BIN)) + [max(prices_arr.max() + 1, MAX_BIN + 1)]
        bin_labels, bin_colors, bin_counts = [], [], []
        for i in range(len(bin_edges) - 1):
            lo, hi = bin_edges[i], bin_edges[i + 1]
            cnt = int(((prices_arr >= lo) & (prices_arr < hi)).sum())
            mid = (lo + hi) / 2
            label = (f"${lo/1e6:.1f}M" if lo >= 1e6 else f"${lo/1e3:.0f}k")
            bin_labels.append(label)
            bin_colors.append(PRICE_RANGE_COLORS[_price_key(mid)])
            bin_counts.append(cnt)

        fig = go.Figure(go.Bar(
            x=bin_labels, y=bin_counts,
            marker_color=[c + "20" for c in bin_colors],
            marker_line_color=bin_colors,
            marker_line_width=1.5,
        ))
        fig.update_layout(
            height=320, **_PLOTLY_BASE, bargap=0,
            xaxis={"tickangle": 45,
                   "tickfont": {"family": "DM Sans", "size": 9},
                   "gridcolor": "rgba(0,0,0,0)"},
            yaxis={"gridcolor": "rgba(0,0,0,0.05)",
                   "tickfont": {"family": "DM Sans", "size": 11}},
        )
        st.plotly_chart(fig, use_container_width=True)

        st.markdown('<hr class="dash-divider">', unsafe_allow_html=True)

        st.markdown(
            '<div class="panel-title" style="margin-bottom:8px;">'
            'Top 15 Suburbs by Median Predicted Price (min 5 listings)</div>',
            unsafe_allow_html=True,
        )
        sub_agg = (df_f.groupby("Suburb")
                   .agg(n=("Property_ID", "count"), median_pred=("Predicted_Price", "median"))
                   .query("n >= 5")
                   .sort_values("median_pred", ascending=False)
                   .head(15).reset_index())
        if len(sub_agg):
            fig = px.bar(sub_agg, x="median_pred", y="Suburb", orientation="h",
                         labels={"median_pred": "Median predicted (AUD)"})
            fig.update_traces(
                marker_color="rgba(225,29,72,0.1)",
                marker_line_color="rgba(225,29,72,0.5)",
                marker_line_width=1.5,
            )
            fig.update_layout(
                height=420, **_PLOTLY_BASE,
                yaxis={"categoryorder": "total ascending",
                       "tickfont": {"family": "DM Sans", "size": 12}},
                xaxis={"gridcolor": "rgba(0,0,0,0.05)",
                       "tickfont": {"family": "DM Sans", "size": 11},
                       "tickprefix": "$"},
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Not enough listings per suburb under current filters.")


# ============================================================
# TAB: TOP DEALS
# ============================================================

with tab_dealers:
    st.markdown(
        '<div class="panel-title" style="margin-bottom:4px;">'
        'Predicted Price vs Asking Price</div>',
        unsafe_allow_html=True,
    )
    st.caption(
        "Best opportunities (predicted > asking) and most overpriced listings. "
        "Only listings with an asking price are included."
    )

    dealable = df_f[df_f["Numeric_Price"].notna()].copy()

    if len(dealable) == 0:
        st.warning("No listings with asking prices match the current filters.")
    else:
        dealable["Gap_AUD"] = dealable["Predicted_Price"] - dealable["Numeric_Price"]
        dealable["Gap_Pct"] = (dealable["Gap_AUD"] / dealable["Numeric_Price"]) * 100

        col_a, col_b = st.columns(2)
        col_a.markdown(f"""
        <div class="stat-card stat-card-1" style="padding:16px 20px 14px;">
          <div class="stat-label">Eligible Listings</div>
          <div class="stat-value c-accent" style="font-size:1.7rem;">{len(dealable):,}</div>
          <div class="stat-sub">With asking price</div>
        </div>""", unsafe_allow_html=True)
        col_b.markdown(f"""
        <div class="stat-card stat-card-2" style="padding:16px 20px 14px;">
          <div class="stat-label">Median Gap</div>
          <div class="stat-value c-emerald" style="font-size:1.7rem;">{dealable['Gap_Pct'].median():.1f}%</div>
          <div class="stat-sub">Predicted vs asking</div>
        </div>""", unsafe_allow_html=True)

        st.markdown('<hr class="dash-divider">', unsafe_allow_html=True)

        DEAL_COLS = [
            "Suburb", "Property_Type", "Beds", "Baths",
            "Numeric_Price", "Predicted_Price",
            "Predicted_Price_Lower", "Predicted_Price_Upper",
            "Gap_AUD", "Gap_Pct", "URL",
        ]
        COL_RENAME = {
            "Numeric_Price":         "Asking",
            "Predicted_Price":       "Predicted",
            "Predicted_Price_Lower": "Pred Low",
            "Predicted_Price_Upper": "Pred High",
            "Gap_AUD":               "Gap ($)",
            "Gap_Pct":               "Gap %",
            "Property_Type":         "Type",
            "URL":                   "Listing",
        }
        COL_CFG = {
            "Asking":    st.column_config.NumberColumn(format="$%d"),
            "Predicted": st.column_config.NumberColumn(format="$%d"),
            "Pred Low":  st.column_config.NumberColumn(format="$%d"),
            "Pred High": st.column_config.NumberColumn(format="$%d"),
            "Gap ($)":   st.column_config.NumberColumn(format="$%d"),
            "Gap %":     st.column_config.NumberColumn(format="%.1f%%"),
            "Listing":   st.column_config.LinkColumn(display_text="View →"),
        }

        st.markdown("### 🟢 Top 20 Good Deals")
        st.caption("Predicted price exceeds asking price by the largest margin (% terms).")
        good_display = dealable.nlargest(20, "Gap_Pct")[DEAL_COLS].rename(columns=COL_RENAME)
        st.dataframe(good_display, use_container_width=True, height=520,
                     column_config=COL_CFG, hide_index=True)

        st.markdown('<hr class="dash-divider">', unsafe_allow_html=True)

        st.markdown("### 🔴 Top 20 Overpriced Listings")
        st.caption("Asking price exceeds predicted price by the largest margin (% terms).")
        bad_display = dealable.nsmallest(20, "Gap_Pct")[DEAL_COLS].rename(columns=COL_RENAME)
        st.dataframe(bad_display, use_container_width=True, height=520,
                     column_config=COL_CFG, hide_index=True)

        st.markdown('<hr class="dash-divider">', unsafe_allow_html=True)
        combined = pd.concat([
            good_display.assign(Category="Good Deal"),
            bad_display.assign(Category="Overpriced"),
        ])
        csv = combined.to_csv(index=False).encode("utf-8")
        st.download_button("📥 Download top deals as CSV", data=csv,
                           file_name=f"top_deals_{last_updated}.csv", mime="text/csv")


# ============================================================
# TAB: ON-DEMAND PREDICT
# ============================================================

with tab_predict:
    st.markdown(
        "Enter property details to get an estimated price from the current model. "
        "Year and Month are injected automatically."
    )

    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Location")
        suburb_input = st.selectbox(
            "Suburb",
            options=sorted(suburbs.index.tolist()),
            help="Pick from training-known suburbs for best accuracy.",
        )
        sub_match        = df[df["Suburb"] == suburb_input].head(1)
        default_postcode = int(sub_match["Postcode"].iloc[0])          if len(sub_match) else 3000
        default_lat      = float(sub_match["Latitude"].iloc[0])        if len(sub_match) else -37.81
        default_lon      = float(sub_match["Longitude"].iloc[0])       if len(sub_match) else 144.96
        default_cbd      = float(sub_match["Distance_to_CBD_km"].iloc[0]) if len(sub_match) else 10.0

        postcode_input = st.number_input("Postcode", value=default_postcode, step=1)
        lat_input      = st.number_input("Latitude",  value=default_lat, format="%.5f", step=0.001)
        lon_input      = st.number_input("Longitude", value=default_lon, format="%.5f", step=0.001)
        cbd_input      = st.number_input("Distance to CBD (km)", value=default_cbd,
                                          min_value=0.0, max_value=200.0, step=0.5)

    with col_right:
        st.subheader("Property Attributes")
        type_input  = st.selectbox("Property Type",
                                   options=sorted(df["Property_Type"].unique().tolist()))
        beds_input  = st.number_input("Beds",       value=3, min_value=0, max_value=10, step=1)
        baths_input = st.number_input("Baths",      value=2, min_value=0, max_value=10, step=1)
        cars_input  = st.number_input("Car spaces", value=2, min_value=0, max_value=10, step=1)
        land_input  = st.number_input("Land size (sqm, 0 if apartment)",
                                      value=500.0, min_value=0.0, step=10.0)

    st.markdown('<hr class="dash-divider">', unsafe_allow_html=True)
    predict_btn = st.button("🔮 Estimate Price", type="primary", use_container_width=True)

    if predict_btn:
        warnings_list = []
        sub_key = suburb_input.upper()

        if sub_key in suburbs.index:
            s = suburbs.loc[sub_key]
            income = float(s["abs_median_income_weekly"])
            age    = float(s["abs_median_age"])
            pop    = float(s["abs_population"])
            crime  = float(s["crime_rate_per_100k"])
            pcount = float(s["Propertycount"])
            dtrain = float(s["dist_nearest_train_km"])
        else:
            warnings_list.append(
                f"Suburb '{suburb_input}' has no historical data — median fallbacks used."
            )
            meds   = models["preprocessor"]["numeric_medians"]
            income = meds.get("abs_median_income_weekly", 800)
            age    = meds.get("abs_median_age", 38)
            pop    = meds.get("abs_population", 20000)
            crime  = meds.get("crime_rate_per_100k", 6000)
            pcount = meds.get("Propertycount", 10000)
            dtrain = meds.get("dist_nearest_train_km", 1.0)

        land_types   = set(decisions["land_property_types"])
        no_rooms     = (beds_input == 0) and (baths_input == 0)
        has_land     = land_input > 0
        is_land      = int((type_input in land_types) or (no_rooms and has_land))
        lat_min, lat_max = decisions["metro_envelope"]["lat"]
        lon_min, lon_max = decisions["metro_envelope"]["lon"]
        out_of_metro = int(
            not (lat_min <= lat_input <= lat_max and lon_min <= lon_input <= lon_max)
        )

        now = datetime.now()
        row = {
            "Property_ID":              0,
            "Status":                   "For Sale",
            "Suburb":                   sub_key,
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
        X       = transform(user_df, models["preprocessor"])

        y_point   = float(np.expm1(models["point"].predict(X))[0])
        y_q10     = float(np.expm1(models["q10"].predict(X))[0])
        y_q90     = float(np.expm1(models["q90"].predict(X))[0])
        lower     = min(y_q10, y_point)
        upper     = max(y_q90, y_point)
        width_pct = (upper - lower) / y_point * 100 if y_point > 0 else 0

        if y_point > 2_500_000:
            warnings_list.append("Luxury range (>$2.5M) — higher uncertainty (~23% MAPE).")
        elif y_point < 500_000:
            warnings_list.append("Budget range (<$500k) — higher uncertainty (~17% MAPE).")
        if type_input in decisions.get("new_build_types", []):
            warnings_list.append("New-build type — sparse training data for this category.")

        st.markdown("### Estimated Price")
        st.markdown(f"""
        <div class="stat-grid" style="grid-template-columns:repeat(3,1fr);margin-top:12px;">
          <div class="stat-card stat-card-1" style="padding:18px 20px 14px;">
            <div class="stat-label">Lower (10th pct)</div>
            <div class="stat-value c-accent" style="font-size:1.65rem;">${lower:,.0f}</div>
          </div>
          <div class="stat-card stat-card-2" style="padding:18px 20px 14px;border-width:2px;">
            <div class="stat-label">Point Estimate</div>
            <div class="stat-value c-emerald" style="font-size:1.65rem;">${y_point:,.0f}</div>
          </div>
          <div class="stat-card stat-card-3" style="padding:18px 20px 14px;">
            <div class="stat-label">Upper (90th pct)</div>
            <div class="stat-value c-amber" style="font-size:1.65rem;">${upper:,.0f}</div>
          </div>
        </div>
        """, unsafe_allow_html=True)

        st.markdown(
            f"**Interval width**: {width_pct:.1f}% &nbsp;·&nbsp; "
            f"**Inference date**: {now.strftime('%Y-%m-%d')}"
        )

        if warnings_list:
            st.warning("⚠️ " + "  ".join(warnings_list))

        if sub_key in df["Suburb"].values:
            sub_median = df[df["Suburb"] == sub_key]["Predicted_Price"].median()
            diff       = (y_point - sub_median) / sub_median * 100
            arrow      = "↑" if diff > 0 else "↓"
            st.info(
                f"Median predicted price across **{sub_key}** For Sale listings is "
                f"**${sub_median:,.0f}**. Your estimate is **{arrow} {abs(diff):.1f}%** of that."
            )


# ============================================================
# FOOTER
# ============================================================

st.markdown('<hr class="dash-divider">', unsafe_allow_html=True)
st.caption(
    f"Model: XGBoost "
    f"(n_estimators={metrics['hyperparameters']['n_estimators']}, "
    f"max_depth={metrics['hyperparameters']['max_depth']}) · "
    f"Validation RMSE ${metrics['validation_metrics']['rmse_aud']:,.0f} · "
    f"Test MAPE {metrics['test_metrics']['mape']:.2f}% · "
    f"Trained on {metrics.get('trained_on', 'full Sold dataset')} · "
    f"Data as of {last_updated}."
)