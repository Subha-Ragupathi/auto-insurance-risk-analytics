# Project Overview — Auto Insurance Risk Analytics

## Objective
Build an end-to-end data engineering pipeline that transforms raw auto
insurance policy data into analytics-ready claim-rate summaries, using a
medallion architecture on Databricks with Delta Lake.

## Business Context
Insurance providers need to segment their policy portfolio by risk factors
such as customer demographics, vehicle attributes, safety features, and
regional density.  This pipeline automates the cleansing, standardisation,
and aggregation of ~58 000 policy records so that analysts can query
pre-built gold tables without touching raw data.

## Architecture
```
Kaggle CSV  ──►  Bronze (Delta)  ──►  Silver (Delta)  ──►  Gold (Delta)
   │                │                      │                    │
   │  raw ingest    │  dedup, clean,       │  aggregate by      │
   │  explicit      │  standardise,        │  region, segment,  │
   │  schema        │  outlier filter,     │  fuel, age, NCAP   │
   │                │  bool encoding       │                    │
   └────────────────┴──────────────────────┴────────────────────┘
                          │
                    DQ checks run
                    after silver layer
```

## Data Quality Strategy
1. **Null audit** across all columns after silver cleansing.
2. **Duplicate detection** on `policy_id` (primary key).
3. **Domain enforcement**: `claim_status` restricted to {0, 1}.
4. **Range validation**: `customer_age` 18–100, `vehicle_age` 0–25,
   `ncap_rating` 0–5.
5. **Categorical domain checks**: fuel_type, transmission_type,
   rear_brakes_type validated against known values.
6. **Cross-layer reconciliation**: bronze-to-silver row-drop percentage
   flagged if > 20 %.

## Gold Layer Outputs
| Table                          | Group-by dimension  |
|--------------------------------|---------------------|
| claim_rate_by_region           | region_code         |
| claim_rate_by_segment          | segment             |
| claim_rate_by_fuel_type        | fuel_type           |
| claim_rate_by_vehicle_age_band | vehicle_age_band    |
| claim_rate_by_customer_age_band| customer_age_band   |
| claim_rate_by_ncap_rating      | ncap_rating         |
| portfolio_summary              | (overall)           |

Each table includes: `total_policies`, `total_claims`, `claim_rate`,
`avg_subscription_length`, and `avg_customer_age`.

## SQL Analytics
The `sql/gold_queries.sql` file contains 10 production-ready queries
including cross-dimension analysis, portfolio share calculations,
safety-feature impact comparison, and subscription-bucket breakdowns.

## Orchestration
The master pipeline notebook (`05_master_pipeline.py`) runs all four
steps sequentially via Databricks `%run`, with per-step timing and a
final execution summary.

## CI / CD
A GitHub Actions workflow (`.github/workflows/ci.yml`) runs on every
push to `main`:
- **flake8** lint on Python notebooks
- **sqlfluff** lint on SQL queries (Databricks dialect)
- Large-file check to prevent bloating the repo
- Required-file existence and non-empty validation
