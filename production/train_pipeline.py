"""
Stage 2: Train XGBoost models on Sold data.

- Builds engineered features and preprocessor from training data.
- Trains point estimator + q10 + q90 quantile models using locked hyperparameters.
- Reports validation and test metrics, then retrains on full Sold for production.
- Exports: model.pkl, model_q10.pkl, model_q90.pkl, preprocessor.pkl,
           metrics.json, model.onnx, model_q10.onnx, model_q90.onnx,
           preprocessor_meta.json (for browser-side feature transform).

Usage:
    python production/train_pipeline.py
"""

import json
import sys
from datetime import datetime
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from xgboost import XGBRegressor

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

if "config" in sys.modules:
    del sys.modules["config"]
import config as cfg


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

    fh = logging.FileHandler(cfg.LOG_DIR / "train.log", mode="w", encoding="utf-8")
    fh.setFormatter(fmt)
    log.addHandler(fh)

    return log


log = setup_logger("train")


# ============================================================
# FEATURE ENGINEERING
# ============================================================

def add_engineered_features(df, decisions):
    """
    Adds:
      - is_new_build flag based on Property_Type vs new-build category list.
      - Maps rare Property_Type values to 'Other'.
    """
    df = df.copy()

    new_build = set(decisions.get("new_build_types", []))
    df["is_new_build"] = df["Property_Type"].isin(new_build).astype(int)

    rare = set(decisions.get("rare_property_types", []))
    if rare:
        df["Property_Type"] = df["Property_Type"].where(
            ~df["Property_Type"].isin(rare), "Other"
        )

    return df


# ============================================================
# PREPROCESSOR
# ============================================================

NUMERIC_COLS = [
    "Beds", "Baths", "Car_Spaces", "LandSize_sqm", "Distance_to_CBD_km",
    "abs_median_income_weekly", "abs_median_age", "abs_population",
    "crime_rate_per_100k", "Propertycount", "dist_nearest_train_km",
    "Latitude", "Longitude",
]
TIME_COLS = ["Year", "Month"]
FLAG_COLS = ["is_land", "out_of_metro", "is_new_build"]


def build_preprocessor(train_df, decisions):
    """Fit imputation medians, suburb frequencies, and property type list from training data."""
    numeric_medians = {c: float(train_df[c].median()) for c in NUMERIC_COLS}

    suburb_freq = train_df["Suburb"].value_counts().to_dict()
    suburb_freq = {k: int(v) for k, v in suburb_freq.items()}

    property_types = sorted(train_df["Property_Type"].unique().tolist())

    feature_order = (
        NUMERIC_COLS
        + TIME_COLS
        + FLAG_COLS
        + ["Suburb_freq"]
        + [f"PT_{t}" for t in property_types]
    )

    preprocessor = {
        "numeric_cols":     NUMERIC_COLS,
        "time_cols":        TIME_COLS,
        "flag_cols":        FLAG_COLS,
        "numeric_medians":  numeric_medians,
        "suburb_freq":      suburb_freq,
        "property_types":   property_types,
        "feature_order":    feature_order,
    }
    return preprocessor


def transform(df, preprocessor):
    """Apply preprocessor to a dataframe and return the feature matrix as numpy."""
    out = pd.DataFrame(index=df.index)

    for c in preprocessor["numeric_cols"]:
        med = preprocessor["numeric_medians"][c]
        out[c] = df[c].fillna(med) if c in df.columns else med

    for c in preprocessor["time_cols"]:
        out[c] = df[c] if c in df.columns else 0

    for c in preprocessor["flag_cols"]:
        out[c] = df[c].fillna(0).astype(int) if c in df.columns else 0

    out["Suburb_freq"] = df["Suburb"].map(preprocessor["suburb_freq"]).fillna(0).astype(int)

    for t in preprocessor["property_types"]:
        out[f"PT_{t}"] = (df["Property_Type"] == t).astype(int)

    out = out[preprocessor["feature_order"]]
    return out.values.astype(np.float32)


# ============================================================
# METRICS
# ============================================================

def compute_metrics(y_true, y_pred, label):
    """Returns dict with RMSE (AUD), MAPE, R²."""
    from sklearn.metrics import mean_squared_error, r2_score

    rmse = float(np.sqrt(mean_squared_error(y_true, y_pred)))
    mape = float(np.mean(np.abs((y_true - y_pred) / y_true)) * 100)
    r2   = float(r2_score(y_true, y_pred))

    log.info(f"[{label}] RMSE ${rmse:,.0f}  MAPE {mape:.2f}%  R² {r2:.3f}")
    return {"rmse_aud": rmse, "mape": mape, "r2": r2}


# ============================================================
# ONNX EXPORT
# ============================================================

def export_onnx_models(model_point, model_q10, model_q90, n_features, out_dir):
    """Convert 3 XGBoost regressors to ONNX for browser inference."""
    from onnxmltools import convert_xgboost
    from onnxmltools.convert.common.data_types import FloatTensorType

    initial_types = [("input", FloatTensorType([None, n_features]))]

    for name, mdl in [("model", model_point),
                      ("model_q10", model_q10),
                      ("model_q90", model_q90)]:
        onnx = convert_xgboost(mdl, initial_types=initial_types)
        path = out_dir / f"{name}.onnx"
        with open(path, "wb") as f:
            f.write(onnx.SerializeToString())
        size_mb = path.stat().st_size / (1024 * 1024)
        log.info(f"Exported ONNX: {path.name}  ({size_mb:.2f} MB)")


def export_preprocessor_meta(preprocessor, decisions, out_path):
    """Save preprocessor + decisions metadata for JS-side feature transform."""
    meta = {
        "feature_order":       preprocessor["feature_order"],
        "numeric_cols":        preprocessor["numeric_cols"],
        "time_cols":           preprocessor["time_cols"],
        "flag_cols":           preprocessor["flag_cols"],
        "numeric_medians":     preprocessor["numeric_medians"],
        "suburb_freq":         preprocessor["suburb_freq"],
        "property_types":      preprocessor["property_types"],
        "new_build_types":     decisions.get("new_build_types", []),
        "rare_property_types": decisions.get("rare_property_types", []),
        "land_property_types": decisions.get("land_property_types", []),
        "metro_envelope":      decisions.get("metro_envelope", {}),
    }
    with open(out_path, "w") as f:
        json.dump(meta, f, indent=2)
    log.info(f"Wrote preprocessor metadata to {out_path}")


# ============================================================
# MAIN
# ============================================================

def main():
    log.info("=" * 60)
    log.info("STAGE 2: TRAIN XGBOOST")
    log.info("=" * 60)

    # ─── 1. Load cleaned data + decisions ───
    cleaned = pd.read_parquet(cfg.CLEANED_PARQUET)
    log.info(f"Loaded cleaned data: {len(cleaned):,} rows")

    with open(cfg.EDA_DECISIONS_JSON) as f:
        decisions = json.load(f)

    # ─── 2. Filter Sold + drop rows without price/date ───
    # NOTE: clean.py saves rows with Numeric_Price NaN (Price Withheld) - we need
    # to filter those out here because we can't train without a target.
    # The parsed datetime column was saved by clean.py as 'Date_parsed'.
    sold = cleaned[cleaned["Status"] == "Sold"].copy()
    log.info(f"Sold rows from cleaned: {len(sold):,}")

    n0 = len(sold)
    sold = sold.dropna(subset=["Numeric_Price"])
    log.info(f"After dropping NaN price: {len(sold):,} ({n0 - len(sold):,} dropped)")

    n0 = len(sold)
    sold = sold.dropna(subset=["Date_parsed"])
    log.info(f"After dropping NaN Date_parsed: {len(sold):,} ({n0 - len(sold):,} dropped)")

    sold = sold.sort_values("Date_parsed").reset_index(drop=True)
    sold["Year"]  = sold["Date_parsed"].dt.year.astype(int)
    sold["Month"] = sold["Date_parsed"].dt.month.astype(int)

    log.info(f"Sold training set: {len(sold):,} rows "
             f"from {sold['Date_parsed'].min().date()} "
             f"to {sold['Date_parsed'].max().date()}")

    # ─── 3. Time-based split 70/15/15 ───
    n = len(sold)
    n_train = int(n * cfg.SPLIT_RATIOS["train"])
    n_val   = int(n * cfg.SPLIT_RATIOS["val"])

    train = sold.iloc[:n_train].copy()
    val   = sold.iloc[n_train:n_train + n_val].copy()
    test  = sold.iloc[n_train + n_val:].copy()
    log.info(f"Train: {len(train):,} (to {train['Date_parsed'].max().date()})")
    log.info(f"Val:   {len(val):,}   (to {val['Date_parsed'].max().date()})")
    log.info(f"Test:  {len(test):,}  (to {test['Date_parsed'].max().date()})")

    # ─── 4. Engineered features ───
    train = add_engineered_features(train, decisions)
    val   = add_engineered_features(val,   decisions)
    test  = add_engineered_features(test,  decisions)

    # ─── 5. Build preprocessor on training fold ───
    preprocessor = build_preprocessor(train, decisions)
    log.info(f"Preprocessor: {len(preprocessor['feature_order'])} features")

    X_train = transform(train, preprocessor)
    X_val   = transform(val,   preprocessor)
    X_test  = transform(test,  preprocessor)

    y_train = np.log1p(train["Numeric_Price"].values)
    y_val   = np.log1p(val["Numeric_Price"].values)
    y_test  = np.log1p(test["Numeric_Price"].values)

    # ─── 6. Train point estimator on train, evaluate on val ───
    hp = cfg.LOCKED_HYPERPARAMETERS
    log.info(f"Hyperparameters: {hp}")

    model_eval = XGBRegressor(**hp)
    model_eval.fit(X_train, y_train)

    y_val_pred  = np.expm1(model_eval.predict(X_val))
    val_metrics = compute_metrics(val["Numeric_Price"].values, y_val_pred, "VAL")

    # ─── 7. Train on train+val, evaluate on test ───
    X_trv = np.concatenate([X_train, X_val], axis=0)
    y_trv = np.concatenate([y_train, y_val], axis=0)

    model_test = XGBRegressor(**hp)
    model_test.fit(X_trv, y_trv)

    y_test_pred  = np.expm1(model_test.predict(X_test))
    test_metrics = compute_metrics(test["Numeric_Price"].values, y_test_pred, "TEST")

    # ─── 8. Retrain on full Sold for production ───
    log.info("Retraining on full Sold (train+val+test) for production")
    full_sold = pd.concat([train, val, test], axis=0)
    preprocessor_full = build_preprocessor(full_sold, decisions)

    X_full = transform(full_sold, preprocessor_full)
    y_full = np.log1p(full_sold["Numeric_Price"].values)

    model_point = XGBRegressor(**hp)
    model_point.fit(X_full, y_full)

    # Quantile models (override objective + add quantile_alpha).
    hp_q = {k: v for k, v in hp.items() if k != "objective"}
    model_q10 = XGBRegressor(objective="reg:quantileerror", quantile_alpha=0.1, **hp_q)
    model_q10.fit(X_full, y_full)

    model_q90 = XGBRegressor(objective="reg:quantileerror", quantile_alpha=0.9, **hp_q)
    model_q90.fit(X_full, y_full)
    log.info("All 3 production models trained on full Sold")

    # ─── 9. Save artifacts ───
    cfg.MODEL_DIR.mkdir(parents=True, exist_ok=True)

    joblib.dump(model_point,        cfg.MODEL_POINT_PKL)
    joblib.dump(model_q10,          cfg.MODEL_Q10_PKL)
    joblib.dump(model_q90,          cfg.MODEL_Q90_PKL)
    joblib.dump(preprocessor_full,  cfg.PREPROCESSOR_PKL)
    log.info("Saved 3 pkl models + preprocessor")

    # Metrics JSON
    metrics = {
        "trained_on":         "full Sold (train+val+test)",
        "training_run_date":  datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "n_train":            len(train),
        "n_val":               len(val),
        "n_test":              len(test),
        "n_full":              len(full_sold),
        "hyperparameters":    hp,
        "validation_metrics": val_metrics,
        "test_metrics":       test_metrics,
    }
    with open(cfg.METRICS_JSON, "w") as f:
        json.dump(metrics, f, indent=2)
    log.info("Wrote metrics.json")

    # ONNX exports
    n_features = X_full.shape[1]
    export_onnx_models(model_point, model_q10, model_q90, n_features, cfg.MODEL_DIR)

    # Preprocessor metadata for browser-side transform
    export_preprocessor_meta(preprocessor_full, decisions,
                             cfg.MODEL_DIR / "preprocessor_meta.json")

    log.info("STAGE 2 COMPLETE")


if __name__ == "__main__":
    main()