# ShelfSense — Grocery Price Intelligence Platform
# CLAUDE.md — Project context for Claude Code

## What This Project Does

ShelfSense is an end-to-end data pipeline that:
1. Ingests real-time grocery prices from the Kroger API + external signals (USDA, BLS, NOAA)
2. Stores and transforms data through Snowflake (raw → staging → marts)
3. Applies causal inference (DiD, RDD) to explain *why* prices change
4. Forecasts next 14-day prices using Prophet + XGBoost (Snowpark ML)
5. Classifies each item as "buy now" or "wait" with a probability score
6. Surfaces everything in a Streamlit dashboard

The core insight: most price trackers tell you *what* the price is.
ShelfSense tells you *when* it will drop and *why* it changed.

---

## Tech Stack

| Layer | Tool |
|---|---|
| Orchestration | Apache Airflow (local via Docker) |
| Data Warehouse | Snowflake (free trial or existing account) |
| Transformations | SQL + dbt Core (free) |
| ML / Forecasting | Python: Prophet, XGBoost, scikit-learn, Snowpark ML |
| Causal Inference | Python: linearmodels, statsmodels |
| Dashboard | Streamlit (deployed to Streamlit Community Cloud) |
| Language | Python 3.11+ |
| Infra | Docker Compose (local), .env for secrets |

---

## Project Directory Structure

```
shelfsense/
├── CLAUDE.md                  # This file
├── README.md
├── .env.example               # Template — never commit .env
├── .gitignore
├── docker-compose.yml         # Airflow local setup
├── requirements.txt
│
├── ingestion/                 # API connectors
│   ├── kroger_client.py       # Kroger OAuth + Products API
│   ├── usda_client.py         # USDA NASS commodity prices
│   ├── bls_client.py          # BLS PPI / CPI
│   ├── noaa_client.py         # NOAA weather events
│   └── open_prices_client.py  # Open Food Facts Open Prices
│
├── dags/                      # Airflow DAGs
│   ├── daily_prices_dag.py    # Runs every day at 7am
│   ├── weekly_signals_dag.py  # USDA releases every Tuesday
│   └── ml_retrain_dag.py      # Retrains models weekly
│
├── snowflake/                 # Snowflake setup scripts
│   ├── setup.sql              # Creates DB, schemas, warehouses, roles
│   ├── raw/
│   │   ├── kroger_prices_raw.sql
│   │   └── external_signals_raw.sql
│   └── marts/
│       ├── price_history.sql
│       ├── promo_calendar.sql
│       └── external_signals.sql
│
├── dbt/                       # dbt transformation models
│   ├── dbt_project.yml
│   ├── profiles.yml.example
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_kroger_prices.sql
│   │   │   ├── stg_usda_commodities.sql
│   │   │   └── stg_noaa_weather.sql
│   │   └── marts/
│   │       ├── mart_price_history.sql
│   │       ├── mart_promo_calendar.sql
│   │       └── mart_buy_signals.sql
│   └── tests/
│       └── price_not_negative.sql
│
├── ml/
│   ├── forecaster.py          # Prophet + XGBoost price forecasting
│   ├── classifier.py          # "Buy now or wait" binary classifier
│   ├── causal/
│   │   ├── diff_in_diff.py    # DiD: effect of supply shocks
│   │   ├── rdd.py             # RDD: detect promo cycle breakpoints
│   │   └── iv_analysis.py     # IV: upstream commodity → retail price
│   └── evaluate.py            # Model evaluation + metrics logging
│
├── dashboard/
│   └── app.py                 # Streamlit app
│
└── tests/
    ├── test_kroger_client.py
    ├── test_transformations.py
    └── test_ml_models.py
```

---

## Environment Variables

Copy `.env.example` to `.env` and fill in values. Never commit `.env`.

```bash
# Kroger API (get from developer.kroger.com)
KROGER_CLIENT_ID=your_client_id
KROGER_CLIENT_SECRET=your_client_secret
KROGER_BASE_URL=https://api.kroger.com/v1

# Snowflake
SNOWFLAKE_ACCOUNT=your_account_identifier
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_DATABASE=SHELFSENSE
SNOWFLAKE_WAREHOUSE=SHELFSENSE_WH
SNOWFLAKE_ROLE=SHELFSENSE_ROLE

# USDA NASS (get from quickstats.nass.usda.gov/api)
USDA_API_KEY=your_usda_key

# BLS (get from www.bls.gov/developers)
BLS_API_KEY=your_bls_key

# NOAA (get from www.ncdc.noaa.gov/cdo-web/token)
NOAA_TOKEN=your_noaa_token

# Airflow
AIRFLOW_UID=50000
```

---

## Data Sources

### 1. Kroger API (Primary — Free)
- Register: https://developer.kroger.com
- Auth: OAuth 2.0 client credentials flow
- Key endpoints:
  - `GET /v1/products?filter.term={query}&filter.locationId={store_id}` — prices per store
  - `GET /v1/locations?filter.zipCode={zip}` — find store IDs near a zip code
  - `GET /v1/products/{productId}` — single product details
- Rate limit: 10,000 calls/day on free tier
- Returns: price, promo price, sale start/end dates, product category, UPC

### 2. USDA NASS API (Free)
- Register: https://quickstats.nass.usda.gov/api
- Use for: weekly wholesale commodity prices (eggs, beef, chicken, produce)
- Key params: `commodity_desc`, `statisticcat_desc=PRICE RECEIVED`, `freq_desc=WEEKLY`
- This is the upstream signal for causal models — retail prices lag USDA prices by 1-3 weeks

### 3. BLS Public API (Free)
- Register: https://www.bls.gov/developers/api_signature_v2.htm
- Series IDs to track:
  - `APU0000708111` — eggs, grade A large, per dozen
  - `APU0000703112` — ground beef per pound
  - `CUUR0000SAF11` — CPI food at home
- Use for: macro cost pressure signal in XGBoost features

### 4. NOAA Climate Data API (Free)
- Token: https://www.ncdc.noaa.gov/cdo-web/token
- Use for: frost events, heat waves, drought — supply shock signals
- Dataset: `GHCND` (daily summaries), filter by state + date range

### 5. Open Food Facts Open Prices (Free, No Key)
- Docs: https://prices.openfoodfacts.org/api/docs
- Use for: historical price baseline across stores (crowdsourced)
- Good for bootstrapping history before your pipeline starts collecting

---

## Snowflake Schema Design

```
Database: SHELFSENSE
├── Schema: RAW          -- Landing zone, VARIANT columns, no transforms
├── Schema: STAGING      -- Typed, cleaned, no business logic
└── Schema: MARTS        -- Business-ready, analytics-facing tables
```

### Key Mart Tables

**mart_price_history**
```
item_id, item_name, upc, category,
store_id, store_name, store_zip, store_state,
price_date, regular_price, promo_price, is_on_sale,
price_delta_7d, price_delta_30d, price_vs_90d_avg
```

**mart_promo_calendar**
```
item_id, store_id,
sale_start_date, sale_end_date, sale_depth_pct,
days_since_last_sale, avg_days_between_sales,
predicted_next_sale_date  -- populated by ML model
```

**mart_external_signals**
```
signal_date, signal_type (USDA/BLS/NOAA),
commodity, value, yoy_change_pct,
weeks_to_retail_impact  -- lag estimated from IV analysis
```

**mart_buy_signals** (ML output)
```
item_id, store_id, signal_date,
predicted_price_7d, predicted_price_14d,
buy_now_probability, recommendation (BUY_NOW / WAIT / NEUTRAL),
model_version, confidence_interval_lower, confidence_interval_upper
```

---

## Pipeline Logic

### Daily DAG (`daily_prices_dag.py`)
```
1. Refresh Kroger OAuth token
2. For each tracked item (500 items × 3 store locations):
   a. Call Kroger Products API
   b. Write raw JSON to Snowflake RAW.KROGER_PRICES_RAW (VARIANT)
3. Run dbt staging models (stg_kroger_prices)
4. Run dbt mart models (mart_price_history, mart_promo_calendar)
5. Run ML classifier → write to mart_buy_signals
6. Log run metadata
```

### Weekly Signals DAG (`weekly_signals_dag.py`)
```
Runs Tuesday 9am (after USDA weekly release)
1. Pull USDA commodity prices
2. Pull BLS series updates
3. Pull NOAA weather anomalies (past 7 days)
4. Write to RAW.EXTERNAL_SIGNALS_RAW
5. Run dbt staging + mart_external_signals
6. Retrigger causal inference scripts (DiD, IV)
```

---

## ML Model Specs

### Model 1: Price Forecaster
- **Algorithm**: Prophet (seasonality) + XGBoost (residuals + exogenous features)
- **Target**: `regular_price` at item × store level, next 14 days
- **Features**:
  - Rolling 7/14/30/90-day price averages
  - Days since last promotion
  - USDA commodity price (lagged 1-3 weeks)
  - BLS PPI change (lagged 2 weeks)
  - NOAA anomaly flag (frost/heat in production region)
  - Day of week, week of month, month
- **Train/test split**: time-based (train on t-90 to t-14, test on t-14 to t)
- **Evaluation**: MAE, MAPE per category

### Model 2: Buy-Now Classifier
- **Algorithm**: XGBoost classifier
- **Target**: binary — did price drop ≥10% within next 7 days? (1/0)
- **Features**: all of the above + current price vs. 90d avg percentile
- **Threshold**: default 0.6 probability → "BUY NOW" recommendation
- **Evaluation**: precision, recall, F1, AUC-ROC

### Model 3: Anomaly Detector
- **Algorithm**: Isolation Forest
- **Target**: flag abnormal price spikes (z-score > 2.5 vs. rolling 30d)
- **Use**: trigger causal investigation, filter ML training data

---

## Causal Inference Specs

### Difference-in-Differences (DiD) — `causal/diff_in_diff.py`
**Question**: What is the causal effect of a commodity supply shock on retail prices?
**Setup**:
- Treatment group: stores in states affected by supply shock (e.g., egg shortage in Iowa)
- Control group: stores in unaffected states
- Pre/post: 4 weeks before and after shock event
- Model: `price ~ treatment + post + treatment*post + store_FE + time_FE`
- Output: estimated ATT (average treatment effect on the treated) per shock event

### Regression Discontinuity Design (RDD) — `causal/rdd.py`
**Question**: Do prices have sharp discontinuities at end-of-month / pre-holiday thresholds?
**Setup**:
- Running variable: day-of-year or days-to-month-end
- Outcome: probability of item being on sale
- Bandwidth: ±7 days around threshold
- Use `rdrobust` or manual local linear regression
- Output: estimated jump size at each threshold → informs promo cycle predictions

### Instrumental Variables (IV) — `causal/iv_analysis.py`
**Question**: How much of a retail price change is supply-driven vs. margin-driven?
**Setup**:
- Instrument: USDA wholesale commodity price (affects retail but isn't caused by consumer demand)
- Endogenous variable: retail price
- First stage: `retail_price ~ usda_price + controls`
- Second stage: use fitted values to estimate causal elasticity
- Output: commodity pass-through rate per category (e.g., "a 10% egg wholesale spike → 6.2% retail spike within 2 weeks")

---

## Coding Conventions

- All Snowflake SQL: uppercase keywords, snake_case table/column names
- Python: black formatter, type hints on all functions, docstrings on all classes
- Secrets: always from `os.environ`, never hardcoded
- Logging: use Python `logging` module, not `print()`
- Error handling: all API calls wrapped in try/except with retry logic (max 3 attempts, exponential backoff)
- Tests: pytest, aim for >80% coverage on ingestion + ML modules
- dbt: all models have a description in `schema.yml`, all mart models have at least one test

---

## Key Commands

```bash
# Setup
pip install -r requirements.txt
cp .env.example .env  # fill in your credentials

# Start Airflow locally
docker-compose up airflow-init
docker-compose up

# Run dbt
cd dbt
dbt deps
dbt run --select staging
dbt run --select marts
dbt test

# Run ML models manually
python ml/forecaster.py --item "eggs" --store_id 01400943
python ml/classifier.py --backtest --days 90

# Run causal analysis
python ml/causal/diff_in_diff.py --event "bird_flu_2024"
python ml/causal/iv_analysis.py --category "dairy"

# Run dashboard locally
streamlit run dashboard/app.py
```

---

## Build Order (Follow This Sequence)

1. **Snowflake setup** — run `snowflake/setup.sql`, create DB/schemas/warehouse/role
2. **Kroger API client** — `ingestion/kroger_client.py`, OAuth flow + products endpoint
3. **Raw ingestion to Snowflake** — write JSON VARIANT to RAW schema
4. **dbt staging models** — clean and type the raw data
5. **dbt mart: price_history** — build the core analytical table
6. **USDA + BLS clients** — external signals ingestion
7. **dbt mart: external_signals** — join signals by date
8. **Airflow DAGs** — wire up the daily + weekly pipelines
9. **Prophet forecaster** — baseline price forecasting per item
10. **XGBoost layer** — add exogenous features, improve forecaster
11. **Buy-now classifier** — binary ML model
12. **Causal: DiD** — supply shock analysis
13. **Causal: RDD** — promo cycle detection
14. **Causal: IV** — commodity pass-through estimation
15. **Streamlit dashboard** — wire up everything to a UI
16. **Deploy** — Streamlit Community Cloud (free)

---

## Portfolio Framing

When describing this project in interviews or on your resume:

> "Built an end-to-end grocery price intelligence platform using Snowflake, Airflow, and Python. Applied causal inference (Difference-in-Differences, Regression Discontinuity) to isolate the effect of commodity supply shocks on retail prices, and built an XGBoost + Prophet forecaster to predict 14-day price movements with a 'buy now or wait' recommendation engine."

Relevant for: Hims & Hers (consumer pricing), Visa (transaction intelligence), SeatGeek (demand modeling), any e-commerce or data engineering role.

---

## Useful Links

| Resource | URL |
|---|---|
| Kroger Developer Portal | https://developer.kroger.com |
| Kroger API Reference | https://developer.kroger.com/reference |
| USDA NASS API | https://quickstats.nass.usda.gov/api |
| BLS Developer Guide | https://www.bls.gov/developers |
| NOAA CDO Token Request | https://www.ncdc.noaa.gov/cdo-web/token |
| Open Prices API Docs | https://prices.openfoodfacts.org/api/docs |
| Snowflake Free Trial | https://signup.snowflake.com |
| dbt Core Docs | https://docs.getdbt.com |
| Prophet Docs | https://facebook.github.io/prophet |
| Streamlit Community Cloud | https://streamlit.io/cloud |
| linearmodels (IV/DiD) | https://bashtage.github.io/linearmodels |
| rdrobust (RDD) | https://pypi.org/project/rdrobust |