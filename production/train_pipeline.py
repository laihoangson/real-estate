"""
Train stage: feature engineering + fit XGBoost models with locked hyperparameters.

Reads:  production/output/cleaned_data.parquet
        production/output/eda_decisions.json
Writes: production/output/models/model.pkl
        production/output/models/model_q10.pkl
        production/output/models/model_q90.pkl
        production/output/models/preprocessor.pkl
        production/output/models/metrics.json

Usage:
    python production/train.py
"""

import json
import logging
import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

sys.path.append(str(Path(__file__).resolve().parent))
import config as cfg


# ============================================================
# LOGGING
# ============================================================

def setup_logger(name="train"):
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


log = setup_logger("train")


# ============================================================
# LOAD
# ============================================================

def load_data():
    if not cfg.CLEANED_PARQUET.exists():
        raise FileNotFoundError(f"Run clean.py first. Missing: {cfg.CLEANED_PARQUET}")
    df = pd.read_parquet(cfg.CLEANED_PARQUET)

    with open(cfg.EDA_DECISIONS_JSON, "r") as f:
        decisions = json.load(f)

    log.info(f"Loaded {len(df):,} rows from {cfg.CLEANED_PARQUET}")
    return df, decisions


# ============================================================
# FEATURE ENGINEERING (ML SECTION 3)
# ============================================================

def add_engineered_features(d, decisions):
    """Add is_new_build flag and group rare Property_Types into 'Other'."""
    d = d.copy()
    rare = set(decisions["rare_property_types"])
    d["Property_Type"] = d["Property_Type"].where(~d["Property_Type"].isin(rare), "Other")
    new_build = set(decisions["new_build_types"])
    d["is_new_build"] = d["Property_Type"].isin(new_build).astype(int)
    return d


# ============================================================
# PREPROCESSING (FREQUENCY ENCODING + ONE-HOT + IMPUTATION)
# ============================================================

class FrequencyEncoder:
    def fit(self, series):
        self.freq_ = series.value_counts().to_dict()
        return self

    def transform(self, series):
        return series.map(self.freq_).fillna(0).astype(int)

    def fit_transform(self, series):
        return self.fit(series).transform(series)


def fit_preprocessor(train_df):
    """Fit frequency encoder, capture Property_Type column list and numeric medians."""
    suburb_enc = FrequencyEncoder().fit(train_df["Suburb"])
    ptype_cols = sorted(train_df["Property_Type"].unique().tolist())
    medians = train_df[cfg.NUMERIC_FEATURES + cfg.TIME_FEATURES].median()

    log.info(f"Fitted preprocessor: "
             f"{len(suburb_enc.freq_)} suburbs, "
             f"{len(ptype_cols)} property types")

    return {
        "suburb_freq_map":       suburb_enc.freq_,
        "property_type_columns": ptype_cols,
        "numeric_medians":       medians.to_dict(),
    }


def transform(d, preproc):
    """Apply preprocessor to a dataframe, return feature matrix."""
    d = d.copy()

    # Frequency-encode Suburb (unseen -> 0).
    d["Suburb_freq"] = d["Suburb"].map(preproc["suburb_freq_map"]).fillna(0).astype(int)

    # One-hot Property_Type using train-time column list.
    ohe = pd.get_dummies(d["Property_Type"], prefix="ptype")
    for col in [f"ptype_{c}" for c in preproc["property_type_columns"]]:
        if col not in ohe.columns:
            ohe[col] = 0
    ohe = ohe[[f"ptype_{c}" for c in preproc["property_type_columns"]]]

    # Numeric + time + flag block.
    num_block = d[cfg.NUMERIC_FEATURES + cfg.TIME_FEATURES + cfg.FLAG_FEATURES + ["Suburb_freq"]].copy()

    # Impute numeric/time medians.
    medians = pd.Series(preproc["numeric_medians"])
    num_block[cfg.NUMERIC_FEATURES + cfg.TIME_FEATURES] = (
        num_block[cfg.NUMERIC_FEATURES + cfg.TIME_FEATURES].fillna(medians)
    )

    X = pd.concat([num_block.reset_index(drop=True),
                   ohe.reset_index(drop=True)], axis=1).astype(float)
    return X


# ============================================================
# TIME-BASED 70/15/15 SPLIT
# ============================================================

def time_split(df_train_pool):
    df_sorted = df_train_pool.sort_values("Date_parsed").reset_index(drop=True)
    n = len(df_sorted)
    train_end = int(n * cfg.SPLIT_RATIOS["train"])
    val_end   = int(n * (cfg.SPLIT_RATIOS["train"] + cfg.SPLIT_RATIOS["val"]))

    train = df_sorted.iloc[:train_end].copy()
    val   = df_sorted.iloc[train_end:val_end].copy()
    test  = df_sorted.iloc[val_end:].copy()

    log.info(f"Split: train={len(train):,} | val={len(val):,} | test={len(test):,}")
    log.info(f"  Train end: {train['Date_parsed'].max().date()}")
    log.info(f"  Val end:   {val['Date_parsed'].max().date()}")
    log.info(f"  Test end:  {test['Date_parsed'].max().date()}")
    return train, val, test


# ============================================================
# METRICS
# ============================================================

def metrics(y_true_log, y_pred_log):
    y_true = np.expm1(y_true_log)
    y_pred = np.expm1(y_pred_log)
    return {
        "rmse_log": float(np.sqrt(mean_squared_error(y_true_log, y_pred_log))),
        "mae_log":  float(mean_absolute_error(y_true_log, y_pred_log)),
        "r2":       float(r2_score(y_true_log, y_pred_log)),
        "rmse_aud": float(np.sqrt(mean_squared_error(y_true, y_pred))),
        "mae_aud":  float(mean_absolute_error(y_true, y_pred)),
        "mape":     float(np.mean(np.abs((y_true - y_pred) / y_true)) * 100),
    }


# ============================================================
# TRAIN
# ============================================================

def train_point_model(X, y):
    log.info(f"Training point model: {cfg.LOCKED_HYPERPARAMETERS}")
    model = xgb.XGBRegressor(**cfg.LOCKED_HYPERPARAMETERS).fit(X, y)
    return model


def train_quantile_model(X, y, alpha):
    params = {**cfg.LOCKED_HYPERPARAMETERS,
              "objective":      "reg:quantileerror",
              "quantile_alpha": alpha}
    log.info(f"Training quantile model alpha={alpha}")
    return xgb.XGBRegressor(**params).fit(X, y)


# ============================================================
# SAVE
# ============================================================

def save_artifacts(model_point, model_q10, model_q90, preproc, val_metrics, test_metrics, n_train, n_val, n_test):
    cfg.ensure_dirs()

    joblib.dump(model_point, cfg.MODEL_POINT_PKL)
    joblib.dump(model_q10,   cfg.MODEL_Q10_PKL)
    joblib.dump(model_q90,   cfg.MODEL_Q90_PKL)
    log.info(f"Saved 3 models -> {cfg.MODEL_DIR}")

    joblib.dump(preproc, cfg.PREPROCESSOR_PKL)
    log.info(f"Saved preprocessor -> {cfg.PREPROCESSOR_PKL}")

    summary = {
        "hyperparameters":       cfg.LOCKED_HYPERPARAMETERS,
        "n_train":               n_train,
        "n_val":                 n_val,
        "n_test":                n_test,
        "validation_metrics":    val_metrics,
        "test_metrics":          test_metrics,
        "trained_on":            "full Sold (train+val+test)",
    }
    with open(cfg.METRICS_JSON, "w") as f:
        json.dump(summary, f, indent=2, default=str)
    log.info(f"Saved metrics -> {cfg.METRICS_JSON}")


# ============================================================
# MAIN
# ============================================================

def main():
    log.info("=" * 60)
    log.info("TRAIN STAGE START")
    log.info("=" * 60)

    df, decisions = load_data()

    # Engineer features on the whole frame (Sold + For Sale).
    df = add_engineered_features(df, decisions)

    # Training pool: Sold with valid Numeric_Price.
    df_sold = df[df["Status"] == "Sold"].copy()
    train_pool = df_sold.dropna(subset=["Numeric_Price"]).copy()
    log.info(f"Training pool: {len(train_pool):,} Sold rows with valid price")

    # Time-based split for evaluation.
    train_df, val_df, test_df = time_split(train_pool)

    # Fit preprocessor on train fold only.
    preproc = fit_preprocessor(train_df)

    # Transform all folds.
    X_train = transform(train_df, preproc)
    X_val   = transform(val_df,   preproc)
    X_test  = transform(test_df,  preproc)
    y_train = np.log1p(train_df["Numeric_Price"].values)
    y_val   = np.log1p(val_df["Numeric_Price"].values)
    y_test  = np.log1p(test_df["Numeric_Price"].values)

    # First training pass on train only - measure val and test metrics for reporting.
    log.info("Pass 1: train on train fold only to measure val/test performance")
    intermediate = xgb.XGBRegressor(**cfg.LOCKED_HYPERPARAMETERS).fit(X_train, y_train)
    val_m  = metrics(y_val,  intermediate.predict(X_val))
    test_m = metrics(y_test, intermediate.predict(X_test))
    log.info(f"Val:  RMSE={val_m['rmse_aud']:,.0f} | MAPE={val_m['mape']:.2f}% | R2={val_m['r2']:.4f}")
    log.info(f"Test: RMSE={test_m['rmse_aud']:,.0f} | MAPE={test_m['mape']:.2f}% | R2={test_m['r2']:.4f}")

    # Second pass: retrain point + quantile models on FULL Sold for production inference.
    log.info("Pass 2: retrain on full Sold (train+val+test) for production inference")
    train_pool_pp = add_engineered_features(train_pool, decisions)   # already done, idempotent
    preproc_full = fit_preprocessor(train_pool_pp)
    X_full = transform(train_pool_pp, preproc_full)
    y_full = np.log1p(train_pool_pp["Numeric_Price"].values)

    model_point = train_point_model(X_full, y_full)
    model_q10   = train_quantile_model(X_full, y_full, alpha=0.1)
    model_q90   = train_quantile_model(X_full, y_full, alpha=0.9)

    save_artifacts(
        model_point, model_q10, model_q90, preproc_full,
        val_m, test_m,
        n_train=len(train_df), n_val=len(val_df), n_test=len(test_df),
    )

    log.info("=" * 60)
    log.info("TRAIN STAGE DONE")
    log.info("=" * 60)


if __name__ == "__main__":
    main()