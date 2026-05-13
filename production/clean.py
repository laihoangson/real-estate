"""
Clean stage: reproduces EDA Sections 2-3 (data quality + outlier handling).

Reads:  data/melbourne_price_data_enriched.csv
Writes: production/output/cleaned_data.parquet
        production/output/eda_decisions.json

Usage:
    python production/clean.py
"""

import json
import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# Make config importable when running this file directly.
sys.path.append(str(Path(__file__).resolve().parent))
import config as cfg


# ============================================================
# LOGGING
# ============================================================

def setup_logger(name="clean"):
    cfg.ensure_dirs()
    log_path = cfg.LOG_DIR / f"{name}.log"
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s",
                            datefmt="%Y-%m-%d %H:%M:%S")
    fh = logging.FileHandler(log_path, mode="w", encoding="utf-8")
    fh.setFormatter(fmt)
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.addHandler(fh); logger.addHandler(sh)
    return logger


log = setup_logger("clean")


# ============================================================
# LOAD
# ============================================================

def load_raw():
    log.info(f"Loading raw CSV: {cfg.INPUT_CSV}")
    if not cfg.INPUT_CSV.exists():
        raise FileNotFoundError(f"Input file not found: {cfg.INPUT_CSV}")

    df = pd.read_csv(cfg.INPUT_CSV)
    df.columns = [c.lstrip("\ufeff") for c in df.columns]
    log.info(f"Loaded {len(df):,} rows, {len(df.columns)} columns")
    return df


# ============================================================
# PARSE DATES, SPLIT GROUPS
# ============================================================

def parse_and_split(df):
    df["Date_parsed"] = pd.to_datetime(df["Date"], format="%d %b %Y", errors="coerce")
    df["Year"]  = df["Date_parsed"].dt.year
    df["Month"] = df["Date_parsed"].dt.month

    df_sold    = df[df["Status"] == "Sold"].copy()
    df_forsale = df[df["Status"] == "For Sale"].copy()
    log.info(f"Sold: {len(df_sold):,} | For Sale: {len(df_forsale):,}")
    return df_sold, df_forsale


# ============================================================
# REMOVALS IDENTIFIED IN EDA SECTION 2
# ============================================================

def apply_section2_removals(df_sold, df_forsale):
    # 1. Drop Sold rows with missing Date.
    n0 = len(df_sold)
    df_sold = df_sold.dropna(subset=["Date_parsed"]).copy()
    log.info(f"Sold: dropped {n0 - len(df_sold):,} rows with missing Date "
             f"({n0:,} -> {len(df_sold):,})")

    # 2. Drop For Sale broken-scrape rows (NaN in Property_Type AND LandSize AND Raw_Price).
    broken = (df_forsale["Property_Type"].isna() &
              df_forsale["LandSize_sqm"].isna() &
              df_forsale["Raw_Price"].isna())
    n0 = len(df_forsale)
    df_forsale = df_forsale[~broken].copy()
    log.info(f"For Sale: dropped {n0 - len(df_forsale):,} broken-scrape rows "
             f"({n0:,} -> {len(df_forsale):,})")

    # 3. For Sale duplicates: keep most recent Last_Updated.
    df_forsale["Last_Updated_dt"] = pd.to_datetime(df_forsale["Last_Updated"], errors="coerce")
    n0 = len(df_forsale)
    df_forsale = (df_forsale
                  .sort_values("Last_Updated_dt", ascending=False)
                  .drop_duplicates(subset="Property_ID", keep="first")
                  .drop(columns=["Last_Updated_dt"])
                  .reset_index(drop=True))
    log.info(f"For Sale: dropped {n0 - len(df_forsale):,} older duplicates "
             f"({n0:,} -> {len(df_forsale):,})")

    return df_sold, df_forsale


# ============================================================
# OUTLIER HANDLING (EDA SECTION 3)
# ============================================================

def flag_land(d):
    d = d.copy()
    type_is_land = d["Property_Type"].isin(cfg.LAND_TYPES)
    no_rooms     = (d["Beds"] == 0) & (d["Baths"] == 0)
    has_land     = d["LandSize_sqm"].fillna(0) > 0
    d["is_land"] = (type_is_land | (no_rooms & has_land)).astype(int)
    return d


def compute_landsize_caps(df_sold):
    """Compute per-Property_Type LandSize cap from Sold positive-LandSize rows."""
    caps = {}
    for ptype in df_sold["Property_Type"].dropna().unique():
        sub = df_sold[(df_sold["Property_Type"] == ptype) &
                      (df_sold["LandSize_sqm"] > 0)]["LandSize_sqm"]
        if len(sub) < 10:
            continue
        q = (cfg.LANDSIZE_CAP_QUANTILE_RURAL if ptype in cfg.RURAL_LARGE
             else cfg.LANDSIZE_CAP_QUANTILE_RESIDENTIAL)
        caps[ptype] = float(sub.quantile(q))
    log.info(f"Computed LandSize caps for {len(caps)} Property_Types")
    return caps


def clean_frame(d, land_caps, is_sold):
    d = d.copy()

    # Property_Type: NaN -> "Unknown".
    d["Property_Type"] = d["Property_Type"].fillna("Unknown")

    # Cap Beds and Car_Spaces at 10.
    d["Beds"]       = d["Beds"].clip(upper=10)
    d["Car_Spaces"] = d["Car_Spaces"].clip(upper=10)

    # Baths: cap at Beds*2 when Beds > 0, then absolute cap at 10.
    mask = (d["Beds"] > 0) & (d["Baths"] > d["Beds"] * 2)
    d.loc[mask, "Baths"] = d.loc[mask, "Beds"] * 2
    d["Baths"] = d["Baths"].clip(upper=10)

    # Beds=0 on non-land -> NaN for ML imputation.
    mask = (d["Beds"] == 0) & (d["is_land"] == 0)
    d.loc[mask, ["Beds", "Baths"]] = np.nan

    # LandSize=0 on non-apartment, non-land -> NaN for ML imputation.
    bad_zero = ((d["LandSize_sqm"] == 0) &
                (~d["Property_Type"].isin(cfg.APT_LIKE)) &
                (d["is_land"] == 0))
    d.loc[bad_zero, "LandSize_sqm"] = np.nan

    # LandSize upper cap per Property_Type.
    for ptype, cap in land_caps.items():
        mask = (d["Property_Type"] == ptype) & (d["LandSize_sqm"] > cap)
        d.loc[mask, "LandSize_sqm"] = cap

    # Out-of-metro flag.
    d["out_of_metro"] = ((~d["Latitude"].between(cfg.LAT_MIN, cfg.LAT_MAX)) |
                        (~d["Longitude"].between(cfg.LON_MIN, cfg.LON_MAX))).astype(int)

    if is_sold:
        priced = d["Numeric_Price"].notna()
        keep = (~priced) | (d["Numeric_Price"].between(cfg.PRICE_FLOOR, cfg.PRICE_CEILING))
        n_drop = (~keep).sum()
        d = d[keep].copy()
        log.info(f"Sold: dropped {n_drop:,} rows with price < {cfg.PRICE_FLOOR:,} or > {cfg.PRICE_CEILING:,}")
    else:
        # For Sale: don't drop rows (we still want them on the map and in volume
        # stats), but NaN-ify obviously broken prices so they're treated as
        # "no asking price" downstream. This catches parse errors like
        # "Contact agent for price" → $1, "POA" → $0, etc., which would
        # otherwise create false "deals" in the Top Deals tab.
        priced = d["Numeric_Price"].notna()
        bad_price = priced & (~d["Numeric_Price"].between(cfg.PRICE_FLOOR, cfg.PRICE_CEILING))
        n_bad = bad_price.sum()
        if n_bad > 0:
            d.loc[bad_price, "Numeric_Price"] = np.nan
            d.loc[bad_price, "Raw_Price"]     = "Contact Agent"
            log.info(f"For Sale: NaN-ified {n_bad:,} rows with price < {cfg.PRICE_FLOOR:,} or > {cfg.PRICE_CEILING:,}")
        else:
            log.info("For Sale: no rows with out-of-range prices")

    return d


def identify_rare_types(df_combined):
    """Identify Property_Type categories with fewer than threshold occurrences."""
    counts = df_combined["Property_Type"].value_counts(dropna=False)
    rare = counts[counts < cfg.RARE_TYPE_THRESHOLD].index.tolist()
    log.info(f"Identified {len(rare)} rare Property_Type categories")
    return rare


# ============================================================
# SAVE
# ============================================================

def save_outputs(df_sold, df_forsale, land_caps, rare_types):
    cfg.ensure_dirs()

    out = pd.concat([df_sold, df_forsale], ignore_index=True)
    out.to_parquet(cfg.CLEANED_PARQUET, index=False)
    log.info(f"Saved cleaned data -> {cfg.CLEANED_PARQUET} "
             f"({len(out):,} rows, {len(out.columns)} cols)")

    decisions = {
        "data_snapshot_date":       str(df_sold["Date_parsed"].max().date()),
        "land_property_types":      sorted(cfg.LAND_TYPES),
        "apt_like_types":           sorted(cfg.APT_LIKE),
        "rural_large_types":        sorted(cfg.RURAL_LARGE),
        "residential_dense_types":  sorted(cfg.RESIDENTIAL_DENSE),
        "new_build_types":          sorted(cfg.NEW_BUILD_TYPES),
        "rare_property_types":      rare_types,
        "price_floor":              cfg.PRICE_FLOOR,
        "price_ceiling":            cfg.PRICE_CEILING,
        "landsize_caps_by_type":    {k: round(v, 0) for k, v in land_caps.items()},
        "metro_envelope":           {"lat": [cfg.LAT_MIN, cfg.LAT_MAX],
                                     "lon": [cfg.LON_MIN, cfg.LON_MAX]},
        "target_transform":         "log1p",
        "split_strategy":           "time_based",
        "split_ratios":             cfg.SPLIT_RATIOS,
    }
    with open(cfg.EDA_DECISIONS_JSON, "w") as f:
        json.dump(decisions, f, indent=2, default=str)
    log.info(f"Saved decisions -> {cfg.EDA_DECISIONS_JSON}")


# ============================================================
# MAIN
# ============================================================

def main():
    log.info("=" * 60)
    log.info("CLEAN STAGE START")
    log.info("=" * 60)

    df = load_raw()
    df_sold, df_forsale = parse_and_split(df)
    df_sold, df_forsale = apply_section2_removals(df_sold, df_forsale)

    df_sold    = flag_land(df_sold)
    df_forsale = flag_land(df_forsale)

    # Compute caps once on Sold (training-side parameter).
    land_caps = compute_landsize_caps(df_sold)

    df_sold    = clean_frame(df_sold,    land_caps, is_sold=True)
    df_forsale = clean_frame(df_forsale, land_caps, is_sold=False)

    # Identify rare types after cleaning, on combined frame.
    rare_types = identify_rare_types(pd.concat([df_sold, df_forsale], ignore_index=True))

    save_outputs(df_sold, df_forsale, land_caps, rare_types)

    log.info("=" * 60)
    log.info(f"CLEAN STAGE DONE | Sold: {len(df_sold):,} | For Sale: {len(df_forsale):,}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()