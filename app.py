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
from folium.plugins import MarkerCluster

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

st.title("🏠 Melbourne Property Price Dashboard")
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
# TABS (Map first)
# ============================================================

tab_map, tab_overview, tab_dealers, tab_predict = st.tabs(
    ["🗺️ Map", "📊 Overview", "🏆 Top Dealers", "🔮 Predict a property"]
)

# ============================================================
# TAB: MAP (Property Points trên, Suburb Choropleth dưới)
# ============================================================

with tab_map:
    if len(df_f) == 0:
        st.warning("No listings to display on the map.")
    else:
        # ----------------------------------------------------
        # MAP 1: Property Points
        # ----------------------------------------------------
        st.subheader("Property Map")
        st.caption(
            f"Showing {min(len(df_f), 2000):,} individual listings "
            "(sampled if filter result exceeds 2,000 for performance). "
            "Color = deal signal. Click a marker for details."
        )

        map_df = df_f if len(df_f) <= 2000 else df_f.sample(2000, random_state=0)

        center_lat = float(map_df["Latitude"].median())
        center_lon = float(map_df["Longitude"].median())

        m1 = folium.Map(location=[center_lat, center_lon], zoom_start=10,
                        tiles="CartoDB positron")

        cluster = MarkerCluster().add_to(m1)
        signal_colors = {
            "Good Deal":       "green",
            "Fair":            "blue",
            "Overpriced":      "red",
            "No Asking Price": "gray",
        }

        for _, r in map_df.iterrows():
            color = signal_colors.get(r["Deal_Signal"], "blue")
            asking = ("—" if pd.isna(r["Numeric_Price"])
                      else f"${r['Numeric_Price']:,.0f}")
            url_html = (
                f'<br><a href="{r["URL"]}" target="_blank" '
                f'style="color:#2563eb;font-weight:600;">View Listing →</a>'
                if "URL" in r and pd.notna(r["URL"]) else ""
            )
            popup = (
                f"<b>{r['Suburb']}</b><br>"
                f"{r['Property_Type']}<br>"
                f"{int(r['Beds']) if not pd.isna(r['Beds']) else '—'}-bed, "
                f"{int(r['Baths']) if not pd.isna(r['Baths']) else '—'}-bath<br>"
                f"Asking: {asking}<br>"
                f"Predicted: <b>${r['Predicted_Price']:,.0f}</b><br>"
                f"Range: ${r['Predicted_Price_Lower']:,.0f} – "
                f"${r['Predicted_Price_Upper']:,.0f}<br>"
                f"<i>{r['Deal_Signal']}</i>"
                f"{url_html}"
            )
            folium.CircleMarker(
                location=[r["Latitude"], r["Longitude"]],
                radius=5, color=color, fill=True, fill_opacity=0.7,
                popup=folium.Popup(popup, max_width=280),
            ).add_to(cluster)

        # Legend.
        legend_html = """
        <div style="position: fixed; bottom: 30px; left: 30px; z-index: 1000;
                    background: rgba(255,255,255,0.95); padding: 12px 16px;
                    border-radius: 10px; border: 1px solid rgba(0,0,0,0.08);
                    box-shadow: 0 4px 16px rgba(0,0,0,0.1); font-family: sans-serif;
                    font-size: 12px;">
            <div style="font-weight: 700; letter-spacing: 1.5px; text-transform: uppercase;
                        font-size: 10px; color: #7c8499; margin-bottom: 8px;">
                Deal Signal
            </div>
            <div style="margin-bottom: 4px;"><span style="display:inline-block;width:10px;height:10px;background:green;border-radius:3px;margin-right:8px;"></span>Good Deal</div>
            <div style="margin-bottom: 4px;"><span style="display:inline-block;width:10px;height:10px;background:blue;border-radius:3px;margin-right:8px;"></span>Fair</div>
            <div style="margin-bottom: 4px;"><span style="display:inline-block;width:10px;height:10px;background:red;border-radius:3px;margin-right:8px;"></span>Overpriced</div>
            <div><span style="display:inline-block;width:10px;height:10px;background:gray;border-radius:3px;margin-right:8px;"></span>No Asking Price</div>
        </div>
        """
        m1.get_root().html.add_child(folium.Element(legend_html))

        st_folium(m1, width=None, height=550, returned_objects=[],
                  key="property_map")

        st.markdown("---")

        # ----------------------------------------------------
        # MAP 2: Suburb Choropleth (median predicted price)
        # ----------------------------------------------------
        st.subheader("Suburb Map")
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
                              median_asking  = ("Numeric_Price", "median"),
                              latitude       = ("Latitude", "mean"),
                              longitude      = ("Longitude", "mean"))
                         .reset_index())
            sub_stats["Suburb_upper"] = sub_stats["Suburb"].str.upper()

            # Get GeoJSON suburb key (different files use different property names).
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

            # Build stats lookup keyed by uppercase suburb name.
            stats_lookup = sub_stats.set_index("Suburb_upper").to_dict("index")

            # Center map.
            if len(sub_stats) > 0:
                center_lat2 = float(sub_stats["latitude"].median())
                center_lon2 = float(sub_stats["longitude"].median())
            else:
                center_lat2, center_lon2 = -37.8136, 144.9631

            m2 = folium.Map(location=[center_lat2, center_lon2], zoom_start=10,
                            tiles="CartoDB positron")

            # Color buckets for median predicted price.
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

            # Enrich GeoJSON with stats fields so tooltip can read them.
            for feat in geojson["features"]:
                suburb_name = str(feat["properties"].get(geo_key, "")).upper()
                stats = stats_lookup.get(suburb_name)
                if stats and stats["n"] >= 3:
                    feat["properties"]["_n"]       = int(stats["n"])
                    feat["properties"]["_median"]  = f"${stats['median_pred']:,.0f}"
                    feat["properties"]["_mean"]    = f"${stats['mean_pred']:,.0f}"
                    feat["properties"]["_min"]     = f"${stats['min_pred']:,.0f}"
                    feat["properties"]["_max"]     = f"${stats['max_pred']:,.0f}"
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
                    style="background-color: white; border: 1px solid #ddd; "
                          "border-radius: 6px; padding: 8px; font-size: 12px;",
                ),
            ).add_to(m2)

            # Choropleth legend.
            legend_html2 = """
            <div style="position: fixed; bottom: 30px; left: 30px; z-index: 1000;
                        background: rgba(255,255,255,0.95); padding: 12px 16px;
                        border-radius: 10px; border: 1px solid rgba(0,0,0,0.08);
                        box-shadow: 0 4px 16px rgba(0,0,0,0.1); font-family: sans-serif;
                        font-size: 12px;">
                <div style="font-weight: 700; letter-spacing: 1.5px; text-transform: uppercase;
                            font-size: 10px; color: #7c8499; margin-bottom: 8px;">
                    Median Predicted Price
                </div>
                <div style="margin-bottom: 4px;"><span style="display:inline-block;width:10px;height:10px;background:#059669;border-radius:3px;margin-right:8px;"></span>Under $750k</div>
                <div style="margin-bottom: 4px;"><span style="display:inline-block;width:10px;height:10px;background:#2563eb;border-radius:3px;margin-right:8px;"></span>$750k – $1.5M</div>
                <div style="margin-bottom: 4px;"><span style="display:inline-block;width:10px;height:10px;background:#d97706;border-radius:3px;margin-right:8px;"></span>$1.5M – $3M</div>
                <div style="margin-bottom: 4px;"><span style="display:inline-block;width:10px;height:10px;background:#e11d48;border-radius:3px;margin-right:8px;"></span>Over $3M</div>
                <div><span style="display:inline-block;width:10px;height:10px;background:#e5e7eb;border-radius:3px;margin-right:8px;"></span>< 3 listings</div>
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

        # 1. Deal signal distribution (pie chart, kept color-coded by signal).
        with col1:
            st.subheader("Deal signal distribution")
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
                         hole=0.4)
            fig.update_layout(height=350, margin=dict(t=10, b=10, l=10, r=10))
            st.plotly_chart(fig, use_container_width=True)

        # 2. Property_Type distribution (bar, no color gradient).
        with col2:
            st.subheader("Property types")
            tcounts = (df_f["Property_Type"].value_counts()
                       .head(10).reset_index())
            tcounts.columns = ["Property_Type", "Count"]
            fig = px.bar(tcounts, x="Count", y="Property_Type",
                         orientation="h")
            fig.update_traces(marker_color="#2563eb")
            fig.update_layout(height=350, margin=dict(t=10, b=10, l=10, r=10),
                              yaxis={"categoryorder": "total ascending"},
                              showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

        st.markdown("---")

        # 3. Predicted price distribution (histogram).
        st.subheader("Predicted price distribution")
        fig = px.histogram(df_f, x="Predicted_Price", nbins=60,
                           labels={"Predicted_Price": "Predicted price (AUD)"})
        fig.update_traces(marker_color="#2563eb")
        fig.update_layout(height=350, margin=dict(t=10, b=10, l=10, r=10),
                          showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

        # 4. Top 15 suburbs by median predicted price (bar, no color gradient).
        st.subheader("Top 15 suburbs by median predicted price (min 5 listings)")
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
            fig.update_traces(marker_color="#2563eb")
            fig.update_layout(height=450, margin=dict(t=10, b=10, l=10, r=10),
                              yaxis={"categoryorder": "total ascending"},
                              showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Not enough listings per suburb under the current filters.")


# ============================================================
# TAB: TOP DEALERS
# ============================================================

with tab_dealers:
    st.subheader("Top deals: predicted price vs asking price")
    st.caption(
        "Best opportunities (predicted significantly higher than asking) and "
        "most overpriced listings (predicted significantly lower than asking). "
        "Only listings with an asking price are included."
    )

    # Subset with asking price.
    dealable = df_f[df_f["Numeric_Price"].notna()].copy()

    if len(dealable) == 0:
        st.warning("No listings with asking prices match the current filters.")
    else:
        # Calculate gap metrics.
        dealable["Gap_AUD"] = dealable["Predicted_Price"] - dealable["Numeric_Price"]
        dealable["Gap_Pct"] = (dealable["Gap_AUD"] / dealable["Numeric_Price"]) * 100

        col_a, col_b = st.columns(2)
        col_a.metric("Eligible listings (with asking)", f"{len(dealable):,}")
        col_b.metric("Median gap %", f"{dealable['Gap_Pct'].median():.1f}%")

        st.markdown("---")

        # ----------------------------------------------------
        # TOP 20 GOOD DEALS (predicted > asking)
        # ----------------------------------------------------
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

        # ----------------------------------------------------
        # TOP 20 OVERPRICED (predicted < asking)
        # ----------------------------------------------------
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

        # Download both tables combined.
        st.markdown("---")
        combined = pd.concat([
            good_display.assign(Category="Good Deal"),
            bad_display.assign(Category="Overpriced"),
        ])
        csv = combined.to_csv(index=False).encode("utf-8")
        st.download_button("📥 Download top deals as CSV", data=csv,
                           file_name=f"top_deals_{snapshot}.csv",
                           mime="text/csv")


# ============================================================
# TAB: ON-DEMAND PREDICT
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

        st.markdown("### Estimated price")
        cc1, cc2, cc3 = st.columns(3)
        cc1.metric("Lower (10th pct)", f"${lower:,.0f}")
        cc2.metric("Point estimate",  f"${y_point:,.0f}")
        cc3.metric("Upper (90th pct)", f"${upper:,.0f}")

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