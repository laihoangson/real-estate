"""
Central configuration for the Melbourne property price pipeline.

All paths, constants, and hyperparameters live here so the rest of
the scripts stay focused on logic.
"""

from pathlib import Path

# ============================================================
# PATHS
# ============================================================

# Project root: assumes config.py lives in production/.
ROOT       = Path(__file__).resolve().parents[1]
DATA_DIR   = ROOT / "data"
OUTPUT_DIR = ROOT / "production" / "output"
MODEL_DIR  = OUTPUT_DIR / "models"
LOG_DIR    = OUTPUT_DIR / "logs"

# Input.
INPUT_CSV = DATA_DIR / "melbourne_price_data_enriched.csv"

# Intermediate and final outputs.
CLEANED_PARQUET     = OUTPUT_DIR / "cleaned_data.parquet"
EDA_DECISIONS_JSON  = OUTPUT_DIR / "eda_decisions.json"
PREDICTIONS_PARQUET = OUTPUT_DIR / "predictions_for_sale.parquet"

# Models.
MODEL_POINT_PKL    = MODEL_DIR / "model.pkl"
MODEL_Q10_PKL      = MODEL_DIR / "model_q10.pkl"
MODEL_Q90_PKL      = MODEL_DIR / "model_q90.pkl"
PREPROCESSOR_PKL   = MODEL_DIR / "preprocessor.pkl"
METRICS_JSON       = MODEL_DIR / "metrics.json"

# Hyperparameters were tuned in the notebook; production reads them from here.
# These are copied from report/models/metrics.json (the notebook winner).
LOCKED_HYPERPARAMETERS = {
    "n_estimators":     1532,
    "max_depth":        6,
    "learning_rate":    0.05,
    "subsample":        0.8,
    "colsample_bytree": 1.0,
    "tree_method":      "hist",
    "random_state":     0,
    "n_jobs":           -1,
    "verbosity":        0,
}

# ============================================================
# CLEANING CONSTANTS (mirror EDA Sections 2-3)
# ============================================================

LAND_TYPES = {
    "Vacant land", "New land", "Development Site",
    "Farm", "Farmlet", "Grazing", "Livestock",
    "Mixed Farming", "Specialist Farm",
}

APT_LIKE = {
    "Apartment / Unit / Flat", "Studio", "Penthouse",
    "Block of Units", "Car Space",
}

RESIDENTIAL_DENSE = {
    "House", "Townhouse", "Villa", "Semi-Detached",
    "Terrace", "Duplex", "New House & Land",
    "New Apartments / Off the Plan", "New Home Designs",
    "Block of Units",
}

RURAL_LARGE = {"Acreage / Semi-Rural", "Rural", "Rural Lifestyle"}

NEW_BUILD_TYPES = {
    "New House & Land", "New Apartments / Off the Plan",
    "New Home Designs", "New land",
}

# Outlier thresholds.
PRICE_FLOOR   = 50_000
PRICE_CEILING = 20_000_000

# Melbourne metro envelope.
LAT_MIN, LAT_MAX = -38.6, -37.4
LON_MIN, LON_MAX = 144.4, 145.7

# Rare-type threshold for the "Other" grouping.
RARE_TYPE_THRESHOLD = 50

# LandSize cap quantile per type group.
LANDSIZE_CAP_QUANTILE_RURAL       = 0.999
LANDSIZE_CAP_QUANTILE_RESIDENTIAL = 0.99

# ============================================================
# FEATURE LISTS (mirror ML Section 3)
# ============================================================

NUMERIC_FEATURES = [
    "Beds", "Baths", "Car_Spaces", "LandSize_sqm",
    "Distance_to_CBD_km", "dist_nearest_train_km",
    "Latitude", "Longitude",
    "Propertycount",
    "abs_median_income_weekly", "abs_median_age",
    "abs_population", "crime_rate_per_100k",
]

TIME_FEATURES        = ["Year", "Month"]
FLAG_FEATURES        = ["is_land", "out_of_metro", "is_new_build"]
CATEGORICAL_FEATURES = ["Property_Type"]
FREQ_ENCODED         = ["Suburb"]

# ============================================================
# SPLIT
# ============================================================

SPLIT_RATIOS = {"train": 0.70, "val": 0.15, "test": 0.15}

# ============================================================
# INFERENCE TIME INJECTION
# ============================================================

# Updated by weekly_update.py at runtime to the current date.
# Default fallback values - used if explicit override is not provided.
INFERENCE_YEAR  = 2026
INFERENCE_MONTH = 5

# ============================================================
# DEAL SIGNAL
# ============================================================

GOOD_DEAL_THRESHOLD = 1.10   # predicted > 1.10 * asking -> Good Deal
OVERPRICED_THRESHOLD = 0.90  # predicted < 0.90 * asking -> Overpriced


def ensure_dirs():
    """Create all output directories if they don't exist."""
    for d in [OUTPUT_DIR, MODEL_DIR, LOG_DIR]:
        d.mkdir(parents=True, exist_ok=True)