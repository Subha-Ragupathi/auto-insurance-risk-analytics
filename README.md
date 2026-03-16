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
                                         ┌──────────────┐
                                         │   DQ Checks  │
                                         │  nulls, dups │
                                         │  ranges, cats│
                                         └──────────────┘
```

## Tech Stack
- **Processing**: PySpark, Delta Lake
- **Platform**: Databricks (Unity Catalog Volumes)
- **Language**: Python, SQL
- **CI/CD**: GitHub Actions (flake8 + sqlfluff linting)
- **Data Source**: Kaggle auto insurance dataset (~58 000 records, 41 columns)

## Repository Structure

```
├── notebooks/
│   ├── 01_bronze_ingestion.py        # Raw CSV → Bronze Delta (explicit schema)
│   ├── 02_silver_transformation.py   # Dedup, clean, standardise, filter outliers
│   ├── 03_gold_aggregation.py        # Business aggregations by 6 dimensions
│   ├── 04_data_quality_checks.py     # Nulls, dups, ranges, domains, reconciliation
│   └── 05_master_pipeline.py         # End-to-end orchestration with timing
├── sql/
│   └── gold_queries.sql              # 10 analytical queries on gold/silver layers
├── docs/
│   └── project_overview.md           # Detailed project documentation
├── data/
│   └── raw/                          # (CSV excluded from git — see Data Setup)
├── .github/
│   └── workflows/
│       └── ci.yml                    # Lint + validation on push/PR
└── README.md
```

## Data Setup
The source dataset is not included in the repository to keep it lightweight. To set up:

1. Download the dataset from Kaggle: [Auto Insurance Claims Dataset](https://www.kaggle.com/)
2. Place the CSV as `data/raw/Insurance claims data.csv`
3. Upload to your Databricks Volume: `/Volumes/workspace/default/raw/`

## Processing Layers

### Bronze Layer
Reads the raw CSV with an **explicit `StructType` schema** (no `inferSchema`) and persists as Delta. Includes row-count assertion to verify write integrity.

### Silver Layer
- **Deduplication** on `policy_id`
- **Text standardisation**: trim + uppercase on all categorical columns
- **Boolean encoding**: `Yes/No` → `1/0` for 17 safety-feature columns
- **Null handling**: `claim_status` nulls default to `0`
- **Domain enforcement**: `claim_status` restricted to `{0, 1}`
- **Outlier filtering**: `customer_age` [18–100], `vehicle_age` [0–25], `ncap_rating` [0–5]
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

## Data Quality Checks
Six categories of validation run against the silver layer:
1. **Null audit** — column-level null counts
2. **Duplicate detection** — `policy_id` uniqueness (assertion)
3. **Domain enforcement** — `claim_status` must be `{0, 1}` (assertion)
4. **Range validation** — age, vehicle age, NCAP rating boundaries
5. **Categorical domain** — fuel_type, transmission_type, rear_brakes_type against known values
6. **Cross-layer reconciliation** — bronze → silver row-drop percentage flagged if > 20%

## SQL Analytics
The `sql/gold_queries.sql` file includes 10 queries covering:
- Overall portfolio claim rate
- Top regions by claim rate (with minimum credibility threshold)
- Fuel type risk ranking with window functions
- Customer age band analysis with portfolio share
- Cross-dimension fuel × segment analysis
- Subscription length bucket analysis
- Safety feature impact comparison (ESC, brake assist, parking sensors)

## Pipeline Orchestration
`05_master_pipeline.py` runs all four steps via Databricks `%run` with per-step timing:

```
Step 1/4: Bronze ingestion          ──►  12.3s
Step 2/4: Silver transformation     ──►  18.7s
Step 3/4: Gold aggregation          ──►   8.1s
Step 4/4: Data quality checks       ──►   5.4s
────────────────────────────────────────────────
TOTAL                               ──►  44.5s
```

## CI/CD
GitHub Actions runs on every push to `main` and on pull requests:
- **flake8** — Python linting for notebooks
- **sqlfluff** — SQL linting (Databricks dialect)
- **File validation** — checks all required files exist and are non-empty
- **Large file guard** — warns if files > 500 KB are committed

## How to Run
1. Upload the Kaggle CSV to `/Volumes/workspace/default/raw/`
2. Import the `notebooks/` folder into your Databricks workspace
3. Run `05_master_pipeline.py` for end-to-end execution, or run each notebook individually:
   - `01_bronze_ingestion.py`
   - `02_silver_transformation.py`
   - `03_gold_aggregation.py`
   - `04_data_quality_checks.py`
4. Run queries from `sql/gold_queries.sql` in a Databricks SQL editor

## Future Enhancements
- Parameterise notebook paths using Databricks widgets for environment flexibility
- Add Delta Lake table versioning with `DESCRIBE HISTORY` for audit trail
- Integrate with Databricks Jobs for scheduled execution with retry and alerting
- Build a Power BI / Databricks SQL dashboard on top of gold tables
- Add schema evolution handling for upstream data changes
- Implement SCD Type 2 for tracking policy attribute changes over time

## Status
Core pipeline implementation complete with medallion architecture, comprehensive data quality checks, SQL analytics, orchestration, and CI/CD.
