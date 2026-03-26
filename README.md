# Auto Insurance Risk Analytics Pipeline

## Overview
An end-to-end data engineering pipeline that processes ~58 000 auto insurance policy records through a **medallion architecture** (bronze → silver → gold) on Databricks with Delta Lake. The pipeline produces analytics-ready claim-rate summary tables segmented by customer demographics, vehicle attributes, and safety features.

## Business Problem
Insurance organisations need to understand how customer demographics, vehicle characteristics, safety features, and regional density influence claim occurrence. Raw policy-level data is not directly suitable for analytics — it contains duplicates, inconsistent text, missing values, and unvalidated ranges that require systematic cleansing before any reliable analysis.

## Solution Architecture

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  Kaggle CSV  │────►│ Bronze Delta │────►│ Silver Delta │────►│  Gold Delta  │
│  (raw data)  │     │  (as-is)     │     │  (cleansed)  │     │ (aggregated) │
└──────────────┘     └──────────────┘     └──────────────┘     └──────────────┘
                           │                     │                     │
                     explicit schema       dedup, clean,        claim-rate by
                     validation            standardise,         region, segment,
                                           outlier filter       fuel, age, NCAP
                                                │
                                    ┌───────────┴───────────┐
                                    │                       │
                              ┌──────────┐          ┌──────────────┐
                              │ DQ Checks│          │ MERGE INTO   │
                              │ nulls,   │          │ (incremental │
                              │ dups,    │          │  upsert)     │
                              │ ranges   │          └──────────────┘
                              └──────────┘
                                    │
                              ┌──────────┐
                              │  Audit   │
                              │  Log     │
                              └──────────┘
```

## Tech Stack
- **Processing**: PySpark, Delta Lake
- **Platform**: Databricks (Community Edition compatible — uses Volumes)
- **Language**: Python, SQL
- **Testing**: pytest (pure-Python unit tests for transformation logic)
- **CI/CD**: GitHub Actions (flake8 + sqlfluff + pytest)
- **Code Quality**: pre-commit hooks (trailing whitespace, YAML validation, large file guard)
- **Data Source**: Kaggle auto insurance dataset (~58 000 records, 41 columns)

## Repository Structure

```
├── notebooks/
│   ├── config.py                    # Centralised paths, thresholds, env toggle
│   ├── 01_bronze_ingestion.py       # Raw CSV → Bronze Delta (explicit schema)
│   ├── 02_silver_transformation.py  # Dedup, clean, standardise, filter outliers
│   ├── 03_gold_aggregation.py       # Business aggregations by 6 dimensions
│   ├── 04_data_quality_checks.py    # Nulls, dups, ranges, domains, reconciliation
│   ├── 05_master_pipeline.py        # End-to-end orchestration with retry + audit
│   ├── 06_export_to_csv.py          # Gold/Silver → CSV for Power BI
│   ├── 07_incremental_merge.py      # Delta MERGE INTO upsert pattern
│   └── 08_pipeline_audit.py         # Delta audit log for pipeline runs
├── tests/
│   ├── test_transformations.py      # Unit tests for silver/gold logic
│   └── test_config.py               # Unit tests for config paths & thresholds
├── sql/
│   └── gold_queries.sql             # 10 analytical queries on gold/silver layers
├── docs/
│   └── project_overview.md          # Detailed project documentation
├── data/
│   └── raw/                         # (CSV excluded from git — see Data Setup)
├── .github/
│   └── workflows/
│       └── ci.yml                   # Lint + test + validation on push/PR
├── .pre-commit-config.yaml          # Pre-commit hooks config
├── requirements-dev.txt             # Dev dependencies (pytest, flake8, etc.)
└── README.md
```

## Data Setup
The source dataset is not included in the repository to keep it lightweight. To set up:

1. Download the dataset from Kaggle: [Auto Insurance Claims Dataset](https://www.kaggle.com/)
2. In Databricks, navigate to **Catalog → workspace → default → raw** volume
3. Upload the CSV — it will be available at `/Volumes/workspace/default/raw/Insurance claims data.csv`

## Centralised Configuration
All paths and thresholds are managed in `notebooks/config.py`:
- **Environment toggle**: switch between `dev`, `staging`, and `prod` via Databricks widget or `PIPELINE_ENV` env var
- **Layer paths**: `RAW_PATH`, `BRONZE_PATH`, `SILVER_PATH`, `GOLD_BASE`, `EXPORT_PATH`, `AUDIT_LOG_PATH`
- **DQ thresholds**: customer age range, vehicle age range, NCAP rating range, max row drop %
- **Pipeline settings**: retry attempts, backoff delay

## Processing Layers

### Bronze Layer
Reads the raw CSV with an **explicit `StructType` schema** (no `inferSchema`) and persists as Delta. Includes row-count assertion to verify write integrity.

### Silver Layer
- **Deduplication** on `policy_id`
- **Text standardisation**: trim + uppercase on all categorical columns
- **Boolean encoding**: `Yes/No` → `1/0` for 17 safety-feature columns
- **Null handling**: `claim_status` nulls default to `0`
- **Domain enforcement**: `claim_status` restricted to `{0, 1}`
- **Outlier filtering**: configurable thresholds from `config.py`
- **Row-count reconciliation** between bronze input and silver output

### Gold Layer
Pre-aggregated summary tables with `total_policies`, `total_claims`, `claim_rate`, `avg_subscription_length`, and `avg_customer_age`:

| Gold Table                      | Dimension          |
|---------------------------------|--------------------|
| `claim_rate_by_region`          | region_code        |
| `claim_rate_by_segment`         | segment            |
| `claim_rate_by_fuel_type`       | fuel_type          |
| `claim_rate_by_vehicle_age_band`| vehicle_age_band   |
| `claim_rate_by_customer_age_band`| customer_age_band |
| `claim_rate_by_ncap_rating`     | ncap_rating        |
| `portfolio_summary`             | overall portfolio  |

### Incremental Load (MERGE INTO)
`07_incremental_merge.py` demonstrates Delta Lake upsert using `MERGE INTO`:
- Reads new/changed rows from a staging directory
- Upserts into the bronze table keyed on `policy_id`
- Logs merge metrics (inserts vs updates)
- Shows `DESCRIBE HISTORY` for audit trail

## Data Quality Checks
Six categories of validation run against the silver layer:
1. **Null audit** — column-level null counts
2. **Duplicate detection** — `policy_id` uniqueness (assertion)
3. **Domain enforcement** — `claim_status` must be `{0, 1}` (assertion)
4. **Range validation** — configurable thresholds from `config.py`
5. **Categorical domain** — fuel_type, transmission_type, rear_brakes_type against known values
6. **Cross-layer reconciliation** — bronze → silver row-drop percentage flagged if > threshold

## Pipeline Audit Log
`08_pipeline_audit.py` provides:
- `log_audit_entry()` — appends run metadata (run_id, step, status, rows, duration) to a Delta table
- `get_audit_log()` — retrieves recent audit entries
- `get_last_success()` — finds last successful run for a given step
- Called automatically by `05_master_pipeline.py` after each step

## SQL Analytics
The `sql/gold_queries.sql` file includes 10 queries covering:
- Overall portfolio claim rate
- Top regions by claim rate (with minimum credibility threshold)
- Fuel type risk ranking with window functions
- Customer age band analysis with portfolio share
- Cross-dimension fuel x segment analysis
- Subscription length bucket analysis
- Safety feature impact comparison (ESC, brake assist, parking sensors)

## Pipeline Orchestration
`05_master_pipeline.py` runs all steps via Databricks `%run` with:
- **Per-step timing** and summary table
- **Unique run_id** for traceability
- **Audit logging** to Delta after each step
- **Retry helper** with exponential backoff for programmatic steps

```
Step 1/4: Bronze ingestion          ──►  12.3s
Step 2/4: Silver transformation     ──►  18.7s
Step 3/4: Gold aggregation          ──►   8.1s
Step 4/4: Data quality checks       ──►   5.4s
────────────────────────────────────────────────
TOTAL                               ──►  44.5s
```

## Testing
Pure-Python unit tests validate transformation logic without a Spark cluster:

```bash
pip install -r requirements-dev.txt
pytest tests/ -v
```

Tests cover:
- Boolean encoding (Yes/No → 1/0)
- Text standardisation (trim + upper)
- Claim status domain validation and null fill
- Range filter logic (customer age, vehicle age, NCAP)
- Age band assignment (vehicle and customer)
- Config path construction and threshold sanity
- Environment variable override

## CI/CD
GitHub Actions runs on every push to `main` and on pull requests:
- **flake8** — Python linting for notebooks
- **sqlfluff** — SQL linting (Databricks dialect)
- **pytest** — unit tests for transformation and config logic
- **File validation** — checks all required files exist and are non-empty
- **Large file guard** — warns if files > 500 KB are committed

## Pre-commit Hooks
```bash
pip install pre-commit
pre-commit install
```
Hooks run automatically before each commit:
- Trailing whitespace removal
- End-of-file fixer
- YAML validation
- Large file check (500 KB limit)
- flake8 linting

## How to Run
1. Upload the Kaggle CSV to the `raw` volume at **Catalog → workspace → default → raw**
2. Import the `notebooks/` folder into your Databricks workspace
3. Run `05_master_pipeline.py` for end-to-end execution, or run each notebook individually:
   - `01_bronze_ingestion.py`
   - `02_silver_transformation.py`
   - `03_gold_aggregation.py`
   - `04_data_quality_checks.py`
4. For incremental loads, drop new CSV files into the `raw/incremental/` directory and run `07_incremental_merge.py`
5. Run queries from `sql/gold_queries.sql` in a Databricks SQL editor
6. View pipeline audit history by running `08_pipeline_audit.py` standalone

## Key Engineering Practices
- **Centralised configuration** — single source of truth for all paths and thresholds
- **Environment support** — dev/staging/prod via widget or env var
- **Explicit schemas** — no `inferSchema`, full `StructType` definition
- **Idempotent writes** — `overwrite` mode with `overwriteSchema` for reproducibility
- **Incremental loading** — Delta `MERGE INTO` for upsert pattern
- **Audit trail** — Delta-backed execution log with run_id traceability
- **Automated testing** — pytest unit tests runnable in CI without Spark
- **Pre-commit hooks** — code quality enforced before commits
- **CI/CD pipeline** — lint + test + validate on every push

## Status
Production-ready pipeline with medallion architecture, incremental loading, audit logging, comprehensive data quality checks, SQL analytics, unit tests, orchestration, and CI/CD.
