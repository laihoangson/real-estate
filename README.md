# Melbourne Property Price Project

End-to-end pipeline for scraping, enriching, analyzing, and predicting Melbourne property prices. The project scrapes Domain.com.au, enriches data with ABS demographics and crime statistics, trains an XGBoost model, and serves predictions through a static dashboard that runs inference directly in the browser via ONNX Runtime Web.

---

## Live links

**Landing page**: [https://laihoangson.github.io/real-estate/](https://laihoangson.github.io/real-estate/)

**Live dashboard**: [https://laihoangson.github.io/real-estate/dashboard.html](https://laihoangson.github.io/real-estate/dashboard.html)

**Reports**:
- [EDA Report](https://laihoangson.github.io/real-estate/report/eda_report.html)
- [ML Report](https://laihoangson.github.io/real-estate/report/ml_report.html)

---

## Project structure

```
real-estate/
├── data/                              Raw and enriched data
│   ├── melbourne_price_data.csv               (scraped from Domain)
│   ├── melbourne_price_data_enriched.csv      (joined with ABS, crime, train distance)
│   └── melbourne_suburb_boundaries.geojson    (suburb polygons)
│
├── etl/                               ETL scripts (run weekly by GitHub Actions)
│   ├── extract_house_price.py                 Scrape Domain listings
│   ├── enrich_property_data.py                Join demographic and crime data
│   └── get_geojson.py                         Fetch suburb boundaries
│
├── report/                            Notebooks and rendered reports
│   ├── eda_report.ipynb                       EDA development notebook
│   ├── eda_report.html                        Rendered EDA report
│   ├── ml_report.ipynb                        ML development notebook
│   ├── ml_report.html                         Rendered ML report
│   ├── models/                                Models saved from the notebook
│   └── report_data/                           Datasets from the notebook
│
├── production/                        Production pipeline (runs weekly)
│   ├── config.py                              Constants, paths, hyperparameters
│   ├── clean.py                               Data cleaning and outlier handling
│   ├── train_pipeline.py                      Feature engineering, model training, ONNX export
│   ├── predict.py                             For Sale inference + deal signals
│   ├── weekly_update.py                       Orchestrator: clean → train → predict
│   └── output/                                Pipeline outputs
│       ├── cleaned_data.parquet
│       ├── eda_decisions.json
│       ├── predictions_for_sale.parquet
│       ├── predictions_for_sale.csv           (mirror, consumed by the dashboard)
│       └── models/
│           ├── model.pkl                      XGBoost point estimator
│           ├── model_q10.pkl                  Lower bound (10th percentile)
│           ├── model_q90.pkl                  Upper bound (90th percentile)
│           ├── model.onnx                     ONNX exports for browser inference
│           ├── model_q10.onnx
│           ├── model_q90.onnx
│           ├── preprocessor.pkl
│           ├── preprocessor_meta.json         (feature schema for JS-side transform)
│           └── metrics.json
│
├── .github/workflows/
│   ├── scraper.yml                            Domain scraping
│   ├── geo.yml                                Fetch suburb boundaries
│   └── production_update.yml                  clean + train + predict
│
├── dashboard.html                     Static dashboard (Leaflet + Chart.js + ONNX Runtime)
├── index.html                         Landing page (GitHub Pages root)
├── requirements.txt                   Python dependencies
└── README.md
```

---

## Pipeline architecture

```
┌─────────────────────┐
│ Domain.com.au       │
└──────────┬──────────┘
           │ scraper.yml (weekly)
           ▼
┌─────────────────────┐     ┌──────────────────┐
│ melbourne_price_    │ ◄── │ ABS demographics │
│ data.csv            │     │ Crime data       │
└──────────┬──────────┘     │ Train stations   │
           │                └──────────────────┘
           │ enrich_property_data.py
           ▼
┌─────────────────────┐
│ melbourne_price_    │
│ data_enriched.csv   │
└──────────┬──────────┘
           │ production_update.yml (Sundays 9 AM Melbourne)
           ▼
┌─────────────────────────────────────────────┐
│ clean.py → train_pipeline.py → predict.py   │
└──────────┬──────────────────────────────────┘
           │
           ▼
┌────────────────────────────────────┐
│ predictions CSV + 3 ONNX models    │ ◄── Static dashboard (browser)
│ + preprocessor_meta.json           │ ◄── GitHub Pages reports
└────────────────────────────────────┘
```

---

## Modeling approach

Three model families were benchmarked on a time-based 70/15/15 split:

| Model | Validation RMSE | MAPE | R² |
|---|---|---|---|
| Linear (Ridge, α=1000) | $447,645 | 23.1% | 0.635 |
| Random Forest | $324,712 | 13.1% | 0.849 |
| **XGBoost** (winner) | **$295,156** | **12.8%** | **0.864** |

Final XGBoost retrained on full Sold data is used for production inference. Test fold (Feb-May 2026) achieves RMSE $203,832, MAPE 11.83%, R² 0.869.

### Key design choices

- **Target**: `log1p(Numeric_Price)` to handle right-skew.

- **Time features**: `Year` and `Month` as integers. At For Sale inference, current calendar values are injected so predictions reflect today's market level.

- **Categorical encoding**: Property_Type one-hot, Suburb frequency-encoded (~540 cardinality).

- **Quantile models**: two additional XGBoost models with q=0.1 and q=0.9 produce 80% prediction intervals.

- **Deal signal**: comparing predicted price to asking price flags "Good Deal" (>10% below ask), "Fair", or "Overpriced" (>10% above ask).

- **ONNX export**: all three models are converted to ONNX format so the dashboard can run inference client-side via ONNX Runtime Web (no backend server, no cold starts).

---

## Dashboard

The dashboard is a single static HTML file hosted on GitHub Pages. It has three tabs:

- **Map**: Two modes - Points (every For Sale listing colored by Deal Signal) and Suburb Choropleth (polygons colored by median predicted price). Both modes respect global filters.

- **Overview**: Six charts updating live with filters - price distribution, deal signal donut, property types, top 10 suburbs by volume, top 15 suburbs by median price (toggle highest/lowest), and a configurable scatter plot of predicted price vs any of 7 numeric features.

- **Predict**: On-demand price estimation. Fill in property details, click Estimate, and the XGBoost model runs in your browser using ONNX Runtime Web. Returns a point estimate plus an 80% prediction interval (10th-90th percentile), comparison to suburb median, and warnings for known weak segments (luxury, budget, new-build, cold-start suburbs).

All data and models are loaded from the same repo as the dashboard, so weekly pipeline updates flow through automatically with no separate deployment step.

---

## Automation

GitHub Actions workflows handle scheduled runs:

- **scraper.yml**: scrapes Domain.com.au into `data/melbourne_price_data.csv`.

- **geo.yml**: refreshes suburb boundary GeoJSON.

- **production_update.yml**: runs `weekly_update.py`, then commits updated `production/output/` back to the repo.

GitHub Pages re-publishes automatically on every commit, so the dashboard reflects the latest predictions within minutes of the workflow finishing.

---

## Known limitations

- **Luxury and budget segments are weaker**: prices >$2.5M show ~23% MAPE; prices <$500k show ~17% MAPE. The dashboard surfaces wider prediction intervals for these ranges.

- **New-build property types** (New House & Land, New Apartments / Off the Plan, etc.) make up 30% of For Sale supply but only 0.3% of historical Sold data. The `is_new_build` flag partially compensates, but predictions on these listings carry more uncertainty.

- **Cold-start suburbs**: 8 For Sale suburbs (12 listings, 0.03%) have no historical Sold data. Frequency-encoded as zero; Postcode and Latitude/Longitude provide fallback signal.

---

## Tech stack

- **Data**: pandas, numpy, pyarrow

- **ML**: scikit-learn, XGBoost, SHAP, onnxmltools

- **Dashboard**: Leaflet, Chart.js, ONNX Runtime Web, PapaParse

- **Reports**: Jupyter, matplotlib, seaborn

- **Automation**: GitHub Actions

- **Hosting**: GitHub Pages (everything - landing, reports, dashboard, data, models)