"""
Predict stage: load saved models, predict prices for For Sale listings,
inject current Year/Month, compute deal signals, save predictions parquet.

Reads:  production/output/cleaned_data.parquet
        production/output/eda_decisions.json
        production/output/models/model.pkl
        production/output/models/model_q10.pkl
        production/output/models/model_q90.pkl
        production/output/models/preprocessor.pkl
Writes: production/output/predictions_for_sale.parquet

Usage:
    python production/predict.py
"""

import json
import logging
import sys
from datetime import datetime
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parent))
import config as cfg
from train import add_engineered_features, transform   # reuse


# ============================================================
# LOGGING
# ============================================================

def setup_logger(name="predict"):
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


log = setup_logger("predict")


# ============================================================
# LOAD
# ============================================================

def load_inputs():
    if not cfg.CLEANED_PARQUET.exists():
        raise FileNotFoundError(f"Run clean.py first. Missing: {cfg.CLEANED_PARQUET}")
    for p in [cfg.MODEL_POINT_PKL, cfg.MODEL_Q10_PKL,
              cfg.MODEL_Q90_PKL, cfg.PREPROCESSOR_PKL]:
        if not p.exists():
            raise FileNotFoundError(f"Run train.py first. Missing: {p}")

    df = pd.read_parquet(cfg.CLEANED_PARQUET)
    with open(cfg.EDA_DECISIONS_JSON, "r") as f:
        decisions = json.load(f)

    model_point = joblib.load(cfg.MODEL_POINT_PKL)
    model_q10   = joblib.load(cfg.MODEL_Q10_PKL)
    model_q90   = joblib.load(cfg.MODEL_Q90_PKL)
    preproc     = joblib.load(cfg.PREPROCESSOR_PKL)

    log.info(f"Loaded data: {len(df):,} rows")
    log.info(f"Loaded models and preprocessor")
    return df, decisions, model_point, model_q10, model_q90, preproc


# ============================================================
# INJECT CURRENT YEAR/MONTH
# ============================================================

def inject_inference_time(df_forsale):
    """Use current calendar date so predictions reflect today's market level."""
    now = datetime.now()
    df_forsale["Year"]  = now.year
    df_forsale["Month"] = now.month
    log.info(f"Injected Year={now.year}, Month={now.month} into For Sale")
    return df_forsale


# ============================================================
# DEAL SIGNAL
# ============================================================

def classify_deal(row):
    asking = row["Numeric_Price"]
    if pd.isna(asking):
        return "No Asking Price"
    pred = row["Predicted_Price"]
    if pred < cfg.OVERPRICED_THRESHOLD * asking:
        return "Overpriced"
    if pred > cfg.GOOD_DEAL_THRESHOLD * asking:
        return "Good Deal"
    return "Fair"


# ============================================================
# SAVE
# ============================================================

def save_predictions(df_forsale):
    out_cols = [
        "Property_ID", "Status", "Suburb", "Postcode", "Property_Type",
        "Beds", "Baths", "Car_Spaces", "LandSize_sqm",
        "Latitude", "Longitude", "Distance_to_CBD_km",
        "Raw_Price", "Numeric_Price",
        "Predicted_Price", "Predicted_Price_Lower", "Predicted_Price_Upper",
        "Interval_Width_Pct", "Deal_Signal",
        "is_land", "out_of_metro", "is_new_build",
        "Last_Updated",
    ]
    df_forsale[out_cols].to_parquet(cfg.PREDICTIONS_PARQUET, index=False)
    log.info(f"Saved predictions -> {cfg.PREDICTIONS_PARQUET}")
    log.info(f"  Rows: {len(df_forsale):,}")


# ============================================================
# MAIN
# ============================================================

def main():
    log.info("=" * 60)
    log.info("PREDICT STAGE START")
    log.info("=" * 60)

    df, decisions, m_point, m_q10, m_q90, preproc = load_inputs()

    # Engineer features and isolate For Sale.
    df = add_engineered_features(df, decisions)
    df_forsale = df[df["Status"] == "For Sale"].copy()
    log.info(f"For Sale inference set: {len(df_forsale):,} rows")

    # Inject current date.
    df_forsale = inject_inference_time(df_forsale)

    # Build feature matrix.
    X_fs = transform(df_forsale, preproc)
    log.info(f"Feature matrix: {X_fs.shape}")

    # Predict point + lower + upper.
    y_point = m_point.predict(X_fs)
    y_q10   = m_q10.predict(X_fs)
    y_q90   = m_q90.predict(X_fs)

    df_forsale["Predicted_Price"]       = np.expm1(y_point).round(-3)
    df_forsale["Predicted_Price_Lower"] = np.expm1(y_q10).round(-3)
    df_forsale["Predicted_Price_Upper"] = np.expm1(y_q90).round(-3)

    # Enforce monotonic ordering (rare floating-point inversions).
    df_forsale["Predicted_Price_Lower"] = df_forsale[
        ["Predicted_Price_Lower", "Predicted_Price"]].min(axis=1)
    df_forsale["Predicted_Price_Upper"] = df_forsale[
        ["Predicted_Price_Upper", "Predicted_Price"]].max(axis=1)

    df_forsale["Interval_Width_Pct"] = (
        (df_forsale["Predicted_Price_Upper"] - df_forsale["Predicted_Price_Lower"])
        / df_forsale["Predicted_Price"] * 100
    ).round(1)

    df_forsale["Deal_Signal"] = df_forsale.apply(classify_deal, axis=1)

    # Summary log.
    log.info(f"Predicted price: median ${df_forsale['Predicted_Price'].median():,.0f}, "
             f"mean ${df_forsale['Predicted_Price'].mean():,.0f}")
    log.info(f"Median interval width: {df_forsale['Interval_Width_Pct'].median():.1f}%")
    log.info(f"Deal signal distribution:")
    for sig, n in df_forsale["Deal_Signal"].value_counts().items():
        log.info(f"  {sig:20s} {n:6,}  ({n/len(df_forsale)*100:.1f}%)")

    save_predictions(df_forsale)

    log.info("=" * 60)
    log.info("PREDICT STAGE DONE")
    log.info("=" * 60)


if __name__ == "__main__":
    main()