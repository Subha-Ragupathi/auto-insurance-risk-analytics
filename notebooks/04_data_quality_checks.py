# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality Checks
# MAGIC Validates the silver layer for nulls, duplicates, domain violations,
# MAGIC range outliers, and cross-layer row-count reconciliation.  Raises an
# MAGIC assertion error if any critical check fails.

# COMMAND ----------

from pyspark.sql.functions import col, count, when, lit, sum as _sum
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("data_quality")

# COMMAND ----------

# --- configuration -----------------------------------------------------------
BRONZE_PATH = "/Volumes/workspace/default/bronze/insurance_policy_data"
SILVER_PATH = "/Volumes/workspace/default/silver/insurance_policy_data_clean"

dq_issues = []  # collect non-critical warnings

# COMMAND ----------

# --- load layers --------------------------------------------------------------
try:
    bronze_df = spark.read.format("delta").load(BRONZE_PATH)
    silver_df = spark.read.format("delta").load(SILVER_PATH)
    bronze_count = bronze_df.count()
    silver_count = silver_df.count()
    logger.info(f"Bronze rows: {bronze_count} | Silver rows: {silver_count}")
except Exception as e:
    logger.error(f"Failed to load layers for DQ: {e}")
    raise

# COMMAND ----------

# --- 1. null check (all columns) ---------------------------------------------
logger.info("Running null check on silver layer...")
null_counts = silver_df.select([
    count(when(col(c).isNull(), c)).alias(c) for c in silver_df.columns
])
null_results = null_counts.collect()[0].asDict()
cols_with_nulls = {k: v for k, v in null_results.items() if v > 0}

if cols_with_nulls:
    logger.warning(f"Columns with null values: {cols_with_nulls}")
    dq_issues.append(f"Nulls found: {cols_with_nulls}")
else:
    logger.info("No null values detected in silver layer.")

# COMMAND ----------

# --- 2. duplicate check on policy_id -----------------------------------------
logger.info("Running duplicate check on policy_id...")
dup_df = silver_df.groupBy("policy_id").count().filter(col("count") > 1)
dup_count = dup_df.count()

assert dup_count == 0, f"CRITICAL: {dup_count} duplicate policy_ids found in silver layer."
logger.info(f"Duplicate policy_ids: {dup_count} (passed)")

# COMMAND ----------

# --- 3. domain validation — claim_status must be 0 or 1 ----------------------
logger.info("Validating claim_status domain...")
invalid_claim = silver_df.filter(~col("claim_status").isin(0, 1)).count()

assert invalid_claim == 0, f"CRITICAL: {invalid_claim} rows with invalid claim_status."
logger.info(f"Invalid claim_status rows: {invalid_claim} (passed)")

# COMMAND ----------

# --- 4. range checks ----------------------------------------------------------
logger.info("Running range validations...")

range_checks = {
    "customer_age_below_18":  silver_df.filter(col("customer_age") < 18).count(),
    "customer_age_above_100": silver_df.filter(col("customer_age") > 100).count(),
    "vehicle_age_negative":   silver_df.filter(col("vehicle_age") < 0).count(),
    "vehicle_age_above_25":   silver_df.filter(col("vehicle_age") > 25).count(),
    "ncap_below_0":           silver_df.filter(col("ncap_rating") < 0).count(),
    "ncap_above_5":           silver_df.filter(col("ncap_rating") > 5).count(),
    "subscription_negative":  silver_df.filter(col("subscription_length") < 0).count(),
    "airbags_negative":       silver_df.filter(col("airbags") < 0).count(),
}

for check_name, violation_count in range_checks.items():
    if violation_count > 0:
        dq_issues.append(f"Range violation — {check_name}: {violation_count} rows")
        logger.warning(f"Range violation — {check_name}: {violation_count}")
    else:
        logger.info(f"Range check passed: {check_name}")

# COMMAND ----------

# --- 5. categorical domain checks --------------------------------------------
logger.info("Running categorical domain checks...")

VALID_FUEL_TYPES = {"PETROL", "DIESEL", "CNG"}
VALID_TRANSMISSION = {"MANUAL", "AUTOMATIC"}
VALID_BRAKES = {"DRUM", "DISC"}

fuel_violations = silver_df.filter(~col("fuel_type").isin(VALID_FUEL_TYPES)).count()
trans_violations = silver_df.filter(~col("transmission_type").isin(VALID_TRANSMISSION)).count()
brake_violations = silver_df.filter(~col("rear_brakes_type").isin(VALID_BRAKES)).count()

for name, cnt in [("fuel_type", fuel_violations), ("transmission_type", trans_violations), ("rear_brakes_type", brake_violations)]:
    if cnt > 0:
        dq_issues.append(f"Unexpected values in {name}: {cnt} rows")
        logger.warning(f"Unexpected values in {name}: {cnt} rows")
    else:
        logger.info(f"Categorical check passed: {name}")

# COMMAND ----------

# --- 6. cross-layer row-count reconciliation ----------------------------------
logger.info("Running cross-layer reconciliation...")
drop_pct = round((1 - silver_count / bronze_count) * 100, 2) if bronze_count > 0 else 0

logger.info(
    f"Bronze → Silver row drop: {bronze_count - silver_count} rows ({drop_pct}%)"
)

if drop_pct > 20:
    dq_issues.append(
        f"High row drop from bronze to silver: {drop_pct}% — review cleansing rules."
    )
    logger.warning(f"Row drop exceeds 20% threshold: {drop_pct}%")

# COMMAND ----------

# --- summary ------------------------------------------------------------------
if dq_issues:
    logger.warning("=== DQ WARNINGS ===")
    for issue in dq_issues:
        logger.warning(f"  - {issue}")
else:
    logger.info("All data quality checks passed with no warnings.")

logger.info("Data quality validation complete.")
