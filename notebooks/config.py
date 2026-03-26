# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline Configuration
# MAGIC Centralised paths and settings for all pipeline notebooks.
# MAGIC Switch `ENV` between `dev`, `staging`, and `prod` to change targets.

# COMMAND ----------

import os

# COMMAND ----------

# --- environment toggle ------------------------------------------------------
# Override via Databricks widget or environment variable; defaults to "prod".
try:
    ENV = dbutils.widgets.get("env")  # noqa: F821
except Exception:
    ENV = os.getenv("PIPELINE_ENV", "prod")

# COMMAND ----------

# --- base paths per environment ----------------------------------------------
_VOLUME_ROOT = {
    "dev":     "/Volumes/workspace/dev",
    "staging": "/Volumes/workspace/staging",
    "prod":    "/Volumes/workspace/default",
}

VOLUME_ROOT = _VOLUME_ROOT.get(ENV, _VOLUME_ROOT["prod"])

# COMMAND ----------

# --- layer paths -------------------------------------------------------------
RAW_PATH = f"{VOLUME_ROOT}/raw/Insurance claims data.csv"
BRONZE_PATH = f"{VOLUME_ROOT}/bronze/insurance_policy_data"
SILVER_PATH = f"{VOLUME_ROOT}/silver/insurance_policy_data_clean"
GOLD_BASE = f"{VOLUME_ROOT}/gold/"
EXPORT_PATH = f"{VOLUME_ROOT}/raw/exports/"
AUDIT_LOG_PATH = f"{VOLUME_ROOT}/audit/pipeline_audit_log"

# COMMAND ----------

# --- gold table registry -----------------------------------------------------
GOLD_TABLES = {
    "claim_rate_by_region":            "region_code",
    "claim_rate_by_segment":           "segment",
    "claim_rate_by_fuel_type":         "fuel_type",
    "claim_rate_by_vehicle_age_band":  "vehicle_age_band",
    "claim_rate_by_customer_age_band": "customer_age_band",
    "claim_rate_by_ncap_rating":       "ncap_rating",
}

GOLD_TABLE_NAMES = list(GOLD_TABLES.keys()) + ["portfolio_summary"]

# COMMAND ----------

# --- pipeline settings -------------------------------------------------------
RETRY_MAX_ATTEMPTS = 3
RETRY_BASE_DELAY_SECONDS = 5

# --- data quality thresholds -------------------------------------------------
MAX_ROW_DROP_PCT = 20
CUSTOMER_AGE_MIN = 18
CUSTOMER_AGE_MAX = 100
VEHICLE_AGE_MIN = 0
VEHICLE_AGE_MAX = 25
NCAP_RATING_MIN = 0
NCAP_RATING_MAX = 5
