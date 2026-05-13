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
from branca.colormap import LinearColormap


# ============================================================
# IMPORT PRODUCTION CODE
# ============================================================

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
# THEME COLORS
# ============================================================

ACCENT       = "#2563eb"      # blue (primary)
EMERALD      = "#059669"      # green (Good Deal)
AMBER        = "#d97706"      # amber
ROSE         = "#e11d48"      # red (Overpriced)
VIOLET       = "#7c3aed"
SLATE        = "#94a3b8"
TEXT_MUTED   = "#7c8499"

DEAL_COLORS = {
    "Good Deal":       EMERALD,
    "Fair":            ACCENT,
    "Overpriced":      ROSE,
    "No Asking Price": SLATE,
}


# ============================================================
# DATA LOADING (cached)
# ============================================================

@st.cache_data(ttl=3600)
def load_predictions():
    path = ROOT / "production" / "output" / "predictions_for_sale.parquet"
    return pd.read_parquet(path)


@st.cache_data(ttl=3600)
def load_decisions():
    path = ROOT / "production" / "output" / "eda_decisions.json"
    with open(path) as f:
        return json.load(f)


@st.cache_data(ttl=3600)
def load_metrics():
    path = ROOT / "production" / "output" / "models" / "metrics.json"
    with open(path) as f:
        return json.load(f)


@st.cache_data(ttl=3600)
def load_suburb_lookup():
    path = ROOT / "production" / "output" / "cleaned_data.parquet"
    df = pd.read_parquet(path)
    return (df[[
        "Suburb", "Postcode",
        "abs_median_income_weekly", "abs_median_age", "abs_population",
        "crime_rate_per_100k", "Propertycount", "dist_nearest_train_km",
    ]]
    .drop_duplicates(subset="Suburb")
    .set_index("Suburb"))


@st.cache_data(ttl=3600)
def load_geojson():
    """Suburb boundaries GeoJSON."""
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


df         = load_predictions()
decisions  = load_decisions()
metrics    = load_metrics()
suburbs    = load_suburb_lookup()
geojson    = load_geojson()
models     = load_models()


# ============================================================
# SIDEBAR FILTERS
# ============================================================

st.sidebar.title("Filters")

snapshot = decisions.get("data_snapshot_date", "unknown")
st.sidebar.markdown(f"**Data snapshot**: {snapshot}")
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
st.sidebar.caption(
    f"Model: XGBoost  •  Test MAPE: {metrics['test_metrics']['mape']:.1f}%"
)


# ============================================================
# APPLY FILTERS
# ============================================================

def apply_filters(d):
    out = d.copy()
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

st.title("🏠 Melbourne Market Insight")
st.markdown(
    f"Predicted prices for **{len(df):,}** For Sale listings as of "
    f"**{snapshot}**. Filters in the sidebar update all views below."
)


# ============================================================
# TOP METRICS
# ============================================================

c1, c2, c3, c4 = st.columns(4)

c1.metric("Filtered listings", f"{len(df_f):,}", f"of {len(df):,}")

if len(df_f) > 0:
    c2.metric("Median predicted", f"${df_f['Predicted_Price'].median():,.0f}")
    gd = (df_f["Deal_Signal"] == "Good Deal").sum()
    c3.metric("Good Deals", f"{gd:,}", f"{gd/len(df_f)*100:.1f}%")
    op = (df_f["Deal_Signal"] == "Overpriced").sum()
    c4.metric("Overpriced", f"{op:,}", f"{op/len(df_f)*100:.1f}%")
else:
    c2.metric("Median predicted", "—")
    c3.metric("Good Deals", "—")
    c4.metric("Overpriced", "—")

st.markdown("---")


# ============================================================
# TABS
# ============================================================

tab_map, tab_overview, tab_predict = st.tabs(
    ["🗺️ Map", "📊 Overview", "🔮 Predict a property"]
)


# ============================================================
# TAB 1: MAP
# ============================================================

with tab_map:
    if len(df_f) == 0:
        st.warning("No listings to display on the map.")
    else:
        # ─── PROPERTY POINTS MAP ───
        st.subheader("Property locations")
        st.caption(
            f"Showing all **{len(df_f):,}** filtered listings as individual points "
            "(rendered via Canvas for performance). Click a marker for details."
        )

        # Center.
        c_lat = float(df_f["Latitude"].median())
        c_lon = float(df_f["Longitude"].median())

        # prefer_canvas=True renders all markers in a single canvas element,
        # which scales to tens of thousands of points without DOM overhead.
        m1 = folium.Map(
            location=[c_lat, c_lon],
            zoom_start=10,
            tiles="CartoDB positron",
            prefer_canvas=True,
        )

        # Pre-format strings to avoid f-string overhead inside the loop.
        for r in df_f.itertuples(index=False):
            color   = DEAL_COLORS.get(r.Deal_Signal, ACCENT)
            beds    = "—" if pd.isna(r.Beds)  else int(r.Beds)
            baths   = "—" if pd.isna(r.Baths) else int(r.Baths)
            asking  = "—" if pd.isna(r.Numeric_Price) else f"${r.Numeric_Price:,.0f}"
            url_btn = (
                f'<br><a href="{r.URL}" target="_blank" '
                f'style="display:inline-block;margin-top:6px;color:{ACCENT};'
                f'font-weight:600;text-decoration:none;">View on Domain →</a>'
                if hasattr(r, "URL") and isinstance(r.URL, str) and r.URL.startswith("http")
                else ""
            )

            popup_html = (
                f'<div style="font-family:sans-serif;min-width:220px">'
                f'<b style="font-size:13px">{r.Suburb}</b> · '
                f'<span style="font-size:11px;color:#777">{r.Property_Type}</span><br>'
                f'<span style="font-size:11px">{beds} bed · {baths} bath</span><br>'
                f'<span style="font-size:11px">Asking: {asking}</span><br>'
                f'<span style="font-size:13px;color:{color};font-weight:700">'
                f'Predicted ${r.Predicted_Price:,.0f}</span><br>'
                f'<span style="font-size:10.5px;color:#777">'
                f'Range ${r.Predicted_Price_Lower:,.0f} – ${r.Predicted_Price_Upper:,.0f}</span><br>'
                f'<span style="font-size:11px;color:{color};font-weight:600">{r.Deal_Signal}</span>'
                f'{url_btn}'
                f'</div>'
            )

            folium.CircleMarker(
                location=[r.Latitude, r.Longitude],
                radius=4,
                color=color,
                weight=0.5,
                fill=True,
                fill_opacity=0.75,
                popup=folium.Popup(popup_html, max_width=260),
            ).add_to(m1)

        # Legend.
        legend_html = (
            '<div style="position:fixed;bottom:30px;right:30px;z-index:9999;'
            'background:white;padding:10px 14px;border-radius:8px;'
            'box-shadow:0 2px 8px rgba(0,0,0,0.15);font-family:sans-serif;font-size:12px">'
            '<b style="font-size:10px;letter-spacing:1.5px;color:#888">DEAL SIGNAL</b><br>'
        )
        for label, col in DEAL_COLORS.items():
            legend_html += (
                f'<div style="display:flex;align-items:center;gap:6px;margin-top:4px">'
                f'<span style="display:inline-block;width:10px;height:10px;'
                f'border-radius:50%;background:{col}"></span>{label}</div>'
            )
        legend_html += "</div>"
        m1.get_root().html.add_child(folium.Element(legend_html))

        st_folium(
            m1,
            width=None,
            height=550,
            returned_objects=[],
            key="property_map",
        )

        st.markdown("---")

        # ─── SUBURB CHOROPLETH MAP ───
        st.subheader("Suburb price overview")
        st.caption(
            "Each suburb polygon is colored by its **median predicted price**. "
            "Hover for stats. Light gray suburbs have no listings under the current filters."
        )

        if geojson is None:
            st.warning(
                "Suburb boundaries GeoJSON not found at "
                "`data/melbourne_suburb_boundaries.geojson`. "
                "Run `etl/get_geojson.py` to generate it."
            )
        else:
            # Compute suburb-level stats on filtered data.
            sub_stats = (df_f.groupby("Suburb")
                         .agg(n=("Property_ID", "count"),
                              median=("Predicted_Price", "median"),
                              mean  =("Predicted_Price", "mean"),
                              p_min =("Predicted_Price", "min"),
                              p_max =("Predicted_Price", "max"))
                         .reset_index())
            sub_dict = sub_stats.set_index("Suburb").to_dict(orient="index")

            # Color scale based on the global median distribution
            # (use clipping at the 5th–95th percentile to avoid outlier washout).
            p5  = float(df["Predicted_Price"].quantile(0.05))
            p95 = float(df["Predicted_Price"].quantile(0.95))

            colormap = LinearColormap(
                colors=["#059669", "#2563eb", "#d97706", "#e11d48"],
                vmin=p5, vmax=p95,
                caption="Median predicted price (AUD)",
            )

            m2 = folium.Map(
                location=[c_lat, c_lon],
                zoom_start=10,
                tiles="CartoDB positron",
            )

            def style_fn(feature):
                sub_name = str(feature["properties"].get("Suburb", "")).upper()
                stats = sub_dict.get(sub_name)
                if stats and stats["median"] > 0:
                    fill = colormap(stats["median"])
                    return {
                        "fillColor":   fill,
                        "color":       "#ffffff",
                        "weight":      0.7,
                        "fillOpacity": 0.7,
                    }
                return {
                    "fillColor":   "#e2e8f0",
                    "color":       "#ffffff",
                    "weight":      0.5,
                    "fillOpacity": 0.3,
                }

            def highlight_fn(feature):
                return {"weight": 2.5, "color": "#1a1d27", "fillOpacity": 0.85}

            def tooltip_html(feature):
                sub_name = str(feature["properties"].get("Suburb", "")).upper()
                stats = sub_dict.get(sub_name)
                if stats:
                    return (
                        f"<b>{sub_name}</b><br>"
                        f"Listings: {stats['n']:,}<br>"
                        f"Median: ${stats['median']:,.0f}<br>"
                        f"Mean:   ${stats['mean']:,.0f}<br>"
                        f"Min:    ${stats['p_min']:,.0f}<br>"
                        f"Max:    ${stats['p_max']:,.0f}"
                    )
                return f"<b>{sub_name}</b><br><i>No listings in filter</i>"

            # Inject custom tooltip via GeoJsonTooltip with formatted fields.
            # The simpler path: use GeoJson with `tooltip=folium.GeoJsonTooltip`
            # but folium can't compute derived strings - so we mutate properties.
            for feat in geojson["features"]:
                sub_name = str(feat["properties"].get("Suburb", "")).upper()
                stats = sub_dict.get(sub_name)
                if stats:
                    feat["properties"]["_n"]      = f"{stats['n']:,}"
                    feat["properties"]["_median"] = f"${stats['median']:,.0f}"
                    feat["properties"]["_mean"]   = f"${stats['mean']:,.0f}"
                    feat["properties"]["_min"]    = f"${stats['p_min']:,.0f}"
                    feat["properties"]["_max"]    = f"${stats['p_max']:,.0f}"
                else:
                    feat["properties"]["_n"]      = "0"
                    feat["properties"]["_median"] = "—"
                    feat["properties"]["_mean"]   = "—"
                    feat["properties"]["_min"]    = "—"
                    feat["properties"]["_max"]    = "—"

            folium.GeoJson(
                geojson,
                style_function=style_fn,
                highlight_function=highlight_fn,
                tooltip=folium.GeoJsonTooltip(
                    fields=["Suburb", "_n", "_median", "_mean", "_min", "_max"],
                    aliases=["Suburb", "Listings", "Median", "Mean", "Min", "Max"],
                    sticky=True,
                    labels=True,
                    style=(
                        "background-color: white; "
                        "border: 1px solid #e2e8f0; "
                        "border-radius: 6px; "
                        "padding: 8px 10px; "
                        "font-family: sans-serif; "
                        "font-size: 12px;"
                    ),
                ),
            ).add_to(m2)

            colormap.add_to(m2)

            st_folium(
                m2,
                width=None,
                height=550,
                returned_objects=[],
                key="suburb_map",
            )


# ============================================================
# TAB 2: OVERVIEW
# ============================================================

with tab_overview:
    if len(df_f) == 0:
        st.warning("No listings match the current filters.")
    else:
        # ─── ROW 1: Deal Signal donut + Property Types bar ───
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Deal signal distribution")
            sig_counts = df_f["Deal_Signal"].value_counts().reset_index()
            sig_counts.columns = ["Deal_Signal", "Count"]
            fig = px.pie(
                sig_counts, values="Count", names="Deal_Signal",
                color="Deal_Signal", color_discrete_map=DEAL_COLORS,
                hole=0.55,
            )
            fig.update_traces(textposition="inside", textinfo="percent+label")
            fig.update_layout(
                height=350, margin=dict(t=10, b=10, l=10, r=10),
                showlegend=True,
                legend=dict(orientation="h", y=-0.05),
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.subheader("Property types")
            tcounts = (df_f["Property_Type"].value_counts()
                       .head(10).reset_index())
            tcounts.columns = ["Property_Type", "Count"]
            fig = px.bar(
                tcounts, x="Count", y="Property_Type",
                orientation="h",
            )
            fig.update_traces(marker_color=ACCENT, marker_line_width=0)
            fig.update_layout(
                height=350, margin=dict(t=10, b=10, l=10, r=10),
                yaxis={"categoryorder": "total ascending"},
                showlegend=False,
            )
            st.plotly_chart(fig, use_container_width=True)

        st.markdown("---")

        # ─── ROW 2: Histogram ───
        st.subheader("Predicted price distribution")
        fig = px.histogram(
            df_f, x="Predicted_Price", nbins=60,
            labels={"Predicted_Price": "Predicted price (AUD)"},
        )
        fig.update_traces(marker_color=ACCENT, marker_line_width=0)
        fig.update_layout(
            height=320, margin=dict(t=10, b=10, l=10, r=10),
            showlegend=False, bargap=0.02,
        )
        st.plotly_chart(fig, use_container_width=True)

        st.markdown("---")

        # ─── ROW 3: Asking vs Predicted scatter ───
        st.subheader("Asking vs Predicted price")
        st.caption(
            "Each point is a listing. The dashed line marks "
            "asking = predicted. Listings below the line have asking prices "
            "above the model's estimate (overpriced); above the line are bargains."
        )

        scatter_df = df_f.dropna(subset=["Numeric_Price"]).copy()
        if len(scatter_df) > 0:
            # Sample to keep plot responsive.
            n_show = min(len(scatter_df), 8000)
            sample = scatter_df.sample(n_show, random_state=0) if len(scatter_df) > n_show else scatter_df

            fig = px.scatter(
                sample,
                x="Numeric_Price", y="Predicted_Price",
                color="Deal_Signal",
                color_discrete_map=DEAL_COLORS,
                opacity=0.55,
                hover_data={
                    "Suburb": True,
                    "Property_Type": True,
                    "Beds": True,
                    "Numeric_Price": ":$,",
                    "Predicted_Price": ":$,",
                },
                labels={
                    "Numeric_Price":   "Asking price (AUD)",
                    "Predicted_Price": "Predicted price (AUD)",
                },
            )
            fig.update_traces(marker=dict(size=4))

            # Add y=x reference line.
            lo = min(sample["Numeric_Price"].min(), sample["Predicted_Price"].min())
            hi = max(sample["Numeric_Price"].max(), sample["Predicted_Price"].max())
            fig.add_trace(go.Scatter(
                x=[lo, hi], y=[lo, hi],
                mode="lines",
                line=dict(color="#1a1d27", width=1.5, dash="dash"),
                name="Asking = Predicted",
                hoverinfo="skip",
            ))

            fig.update_layout(
                height=480, margin=dict(t=10, b=10, l=10, r=10),
                legend=dict(orientation="h", y=-0.12),
            )
            if n_show < len(scatter_df):
                st.caption(f"Sampled {n_show:,} of {len(scatter_df):,} priced listings for performance.")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No priced listings under the current filters.")

        st.markdown("---")

        # ─── ROW 4: Box plot - Predicted price by Property Type ───
        st.subheader("Predicted price by property type")
        st.caption(
            "Box plot shows the spread of predicted prices per property type. "
            "Wider boxes mean higher within-type variation."
        )

        # Use only top types by listing count to avoid clutter.
        top_types = df_f["Property_Type"].value_counts().head(8).index.tolist()
        box_df = df_f[df_f["Property_Type"].isin(top_types)]

        fig = px.box(
            box_df, x="Property_Type", y="Predicted_Price",
            labels={
                "Property_Type":   "Property type",
                "Predicted_Price": "Predicted price (AUD)",
            },
        )
        fig.update_traces(marker_color=ACCENT, line_color=ACCENT)
        fig.update_layout(
            height=380, margin=dict(t=10, b=10, l=10, r=10),
            xaxis={"categoryorder": "median descending"},
            showlegend=False,
        )
        st.plotly_chart(fig, use_container_width=True)

        st.markdown("---")

        # ─── ROW 5: Distance to CBD vs Predicted ───
        st.subheader("Distance to CBD vs Predicted price")
        st.caption(
            "Density scatter showing how predicted price decays with distance from the CBD."
        )

        cbd_df = df_f.copy()
        n_show = min(len(cbd_df), 8000)
        cbd_sample = cbd_df.sample(n_show, random_state=0) if len(cbd_df) > n_show else cbd_df

        fig = px.scatter(
            cbd_sample,
            x="Distance_to_CBD_km", y="Predicted_Price",
            color="Deal_Signal",
            color_discrete_map=DEAL_COLORS,
            opacity=0.45,
            labels={
                "Distance_to_CBD_km": "Distance to CBD (km)",
                "Predicted_Price":    "Predicted price (AUD)",
            },
            hover_data={
                "Suburb": True,
                "Property_Type": True,
                "Beds": True,
                "Predicted_Price": ":$,",
            },
        )
        fig.update_traces(marker=dict(size=4))
        fig.update_layout(
            height=420, margin=dict(t=10, b=10, l=10, r=10),
            legend=dict(orientation="h", y=-0.12),
        )
        st.plotly_chart(fig, use_container_width=True)

        st.markdown("---")

        # ─── ROW 6: Top suburbs (no viridis, single color) ───
        col_h, col_l = st.columns(2)

        with col_h:
            st.subheader("Top 15 suburbs by median predicted (min 5 listings)")
            high = (df_f.groupby("Suburb")
                    .agg(n=("Property_ID", "count"),
                         median_pred=("Predicted_Price", "median"))
                    .query("n >= 5")
                    .sort_values("median_pred", ascending=False)
                    .head(15)
                    .reset_index())
            if len(high) > 0:
                fig = px.bar(
                    high, x="median_pred", y="Suburb",
                    orientation="h",
                    labels={"median_pred": "Median predicted (AUD)"},
                )
                fig.update_traces(marker_color=ROSE, marker_line_width=0)
                fig.update_layout(
                    height=480, margin=dict(t=10, b=10, l=10, r=10),
                    yaxis={"categoryorder": "total ascending"},
                    showlegend=False,
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Not enough listings per suburb under current filters.")

        with col_l:
            st.subheader("Bottom 15 suburbs by median predicted (min 5 listings)")
            low = (df_f.groupby("Suburb")
                   .agg(n=("Property_ID", "count"),
                        median_pred=("Predicted_Price", "median"))
                   .query("n >= 5")
                   .sort_values("median_pred", ascending=True)
                   .head(15)
                   .reset_index())
            if len(low) > 0:
                fig = px.bar(
                    low, x="median_pred", y="Suburb",
                    orientation="h",
                    labels={"median_pred": "Median predicted (AUD)"},
                )
                fig.update_traces(marker_color=EMERALD, marker_line_width=0)
                fig.update_layout(
                    height=480, margin=dict(t=10, b=10, l=10, r=10),
                    yaxis={"categoryorder": "total descending"},
                    showlegend=False,
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Not enough listings per suburb under current filters.")


# ============================================================
# TAB 3: ON-DEMAND PREDICT
# ============================================================

with tab_predict:
    st.markdown(
        "Enter property details below to get an estimated price based on "
        "the current model. Predictions reflect the latest market level "
        "(current Year and Month are injected automatically)."
    )

    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Location")
        suburb_input = st.selectbox(
            "Suburb",
            options=sorted(suburbs.index.tolist()),
            help="Pick from training-known suburbs for best accuracy.",
        )

        sub_match = df[df["Suburb"] == suburb_input].head(1)
        default_postcode = int(sub_match["Postcode"].iloc[0])  if len(sub_match) else 3000
        default_lat      = float(sub_match["Latitude"].iloc[0])  if len(sub_match) else -37.81
        default_lon      = float(sub_match["Longitude"].iloc[0]) if len(sub_match) else 144.96
        default_cbd      = float(sub_match["Distance_to_CBD_km"].iloc[0]) if len(sub_match) else 10.0

        postcode_input = st.number_input("Postcode",  value=default_postcode, step=1)
        lat_input      = st.number_input("Latitude",  value=default_lat,  format="%.5f", step=0.001)
        lon_input      = st.number_input("Longitude", value=default_lon,  format="%.5f", step=0.001)
        cbd_input      = st.number_input("Distance to CBD (km)", value=default_cbd,
                                          min_value=0.0, max_value=200.0, step=0.5)

    with col_right:
        st.subheader("Property attributes")
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
            "Beds":                     float(beds_input)  if not (no_rooms and is_land) else np.nan,
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

        st.markdown("### Estimated price")
        c1, c2, c3 = st.columns(3)
        c1.metric("Lower (10th pct)", f"${lower:,.0f}")
        c2.metric("Point estimate",   f"${y_point:,.0f}")
        c3.metric("Upper (90th pct)", f"${upper:,.0f}")

        st.markdown(
            f"**Interval width**: {width_pct:.1f}% of point estimate  •  "
            f"**Inference date**: {now.strftime('%Y-%m-%d')}"
        )

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