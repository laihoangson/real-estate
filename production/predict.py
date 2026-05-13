"""
Stage 3: Predict prices for For Sale listings using the trained XGBoost models.

Loads cleaned data + 3 models (point, q10, q90), applies feature engineering and
preprocessing, predicts on For Sale rows, classifies deal signals against asking
prices, and writes both Parquet (for analytics) and CSV (for the static dashboard).

Usage:
    python production/predict.py
"""

import json
import sys
from datetime import datetime
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

# Make sibling modules importable.
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

if "config" in sys.modules:
    del sys.modules["config"]
import config as cfg
from train_pipeline import add_engineered_features, transform


# ============================================================
# LOGGER
# ============================================================

def setup_logger(name):
    import logging
    cfg.ensure_dirs()
    log = logging.getLogger(name)
    log.setLevel(logging.INFO)
    log.handlers.clear()

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s",
                            datefmt="%Y-%m-%d %H:%M:%S")

    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    log.addHandler(sh)

    fh = logging.FileHandler(cfg.LOG_DIR / "predict.log", mode="w", encoding="utf-8")
    fh.setFormatter(fmt)
    log.addHandler(fh)

    return log


log = setup_logger("predict")


# ============================================================
# DEAL SIGNAL CLASSIFIER
# ============================================================

def classify_deal(predicted, asking, threshold=0.10):
    """
    - 'No Asking Price' if asking is missing.
    - 'Good Deal'       if asking < predicted * (1 - threshold).
    - 'Overpriced'      if asking > predicted * (1 + threshold).
    - 'Fair'            otherwise.
    """
    if pd.isna(asking):
        return "No Asking Price"
    lo = predicted * (1 - threshold)
    hi = predicted * (1 + threshold)
    if asking < lo:
        return "Good Deal"
    if asking > hi:
        return "Overpriced"
    return "Fair"


# ============================================================
# MAIN
# ============================================================

def main():
    log.info("=" * 60)
    log.info("STAGE 3: PREDICT FOR SALE")
    log.info("=" * 60)

    # ─── 1. Load cleaned data + decisions ───
    cleaned = pd.read_parquet(cfg.OUTPUT_DIR / "cleaned_data.parquet")
    log.info(f"Loaded cleaned data: {len(cleaned):,} rows")

    with open(cfg.OUTPUT_DIR / "eda_decisions.json") as f:
        decisions = json.load(f)

    # ─── 2. Load models + preprocessor ───
    model_dir   = cfg.MODEL_DIR
    model_point = joblib.load(model_dir / "model.pkl")
    model_q10   = joblib.load(model_dir / "model_q10.pkl")
    model_q90   = joblib.load(model_dir / "model_q90.pkl")
    preprocessor = joblib.load(model_dir / "preprocessor.pkl")
    log.info("Loaded 3 models + preprocessor")

    # ─── 3. Filter For Sale rows ───
    fs = cleaned[cleaned["Status"] == "For Sale"].copy()
    log.info(f"For Sale subset: {len(fs):,} rows")

    if len(fs) == 0:
        log.warning("No For Sale rows to predict on. Exiting.")
        return

    # ─── 4. Inject current Year / Month (predict at today's market level) ───
    now = datetime.now()
    fs["Year"]  = now.year
    fs["Month"] = now.month
    log.info(f"Injected Year={now.year}, Month={now.month} for inference")

    # ─── 5. Feature engineering + transform ───
    fs = add_engineered_features(fs, decisions)
    X = transform(fs, preprocessor)
    log.info(f"Feature matrix shape: {X.shape}")

    # ─── 6. Predict ───
    log.info("Running predictions...")
    y_point = np.expm1(model_point.predict(X))
    y_q10   = np.expm1(model_q10.predict(X))
    y_q90   = np.expm1(model_q90.predict(X))

    # Enforce ordering against floating-point inversions.
    lower = np.minimum(y_q10, y_point)
    upper = np.maximum(y_q90, y_point)

    interval_width_pct = np.where(y_point > 0, (upper - lower) / y_point * 100, 0)

    fs["Predicted_Price"]       = np.round(y_point, -3)
    fs["Predicted_Price_Lower"] = np.round(lower,   -3)
    fs["Predicted_Price_Upper"] = np.round(upper,   -3)
    fs["Interval_Width_Pct"]    = np.round(interval_width_pct, 1)

    # ─── 7. Deal signal ───
    fs["Deal_Signal"] = [
        classify_deal(p, a)
        for p, a in zip(fs["Predicted_Price"], fs["Numeric_Price"])
    ]
    signal_counts = fs["Deal_Signal"].value_counts()
    log.info("Deal signal distribution:")
    for sig, n in signal_counts.items():
        log.info(f"  {sig:18s} {n:>6,}  ({n/len(fs)*100:.1f}%)")

    log.info(f"Median interval width: {fs['Interval_Width_Pct'].median():.1f}%")

    # ─── 8. Save predictions (Parquet + CSV) ───
    out_cols = [
        "Property_ID", "Status", "Suburb", "Postcode", "Property_Type",
        "Beds", "Baths", "Car_Spaces", "LandSize_sqm",
        "Latitude", "Longitude", "Distance_to_CBD_km",
        "Raw_Price", "Numeric_Price",
        "Predicted_Price", "Predicted_Price_Lower", "Predicted_Price_Upper",
        "Interval_Width_Pct", "Deal_Signal",
        "is_land", "out_of_metro", "is_new_build",
        "Last_Updated", "URL",
    ]
    # Defensive: drop cols that might be missing.
    out_cols = [c for c in out_cols if c in fs.columns]
    out_df = fs[out_cols]

    out_path = cfg.OUTPUT_DIR / "predictions_for_sale.parquet"
    out_df.to_parquet(out_path, index=False)
    log.info(f"Wrote {len(out_df):,} predictions to {out_path}")

    csv_path = cfg.OUTPUT_DIR / "predictions_for_sale.csv"
    out_df.to_csv(csv_path, index=False)
    log.info(f"Wrote CSV mirror to {csv_path}")

    log.info("STAGE 3 COMPLETE")


if __name__ == "__main__":
    main()