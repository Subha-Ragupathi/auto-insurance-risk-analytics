# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Cleansing & Standardisation
# MAGIC Applies deduplication, type normalisation, null handling, outlier
# MAGIC filtering, and business-rule enforcement on top of the bronze Delta table.

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

from pyspark.sql.functions import col, trim, upper, when, lit, lower
from pyspark.sql.types import IntegerType
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("silver_transformation")

# COMMAND ----------

# --- load bronze --------------------------------------------------------------
try:
    df = spark.read.format("delta").load(BRONZE_PATH)
    bronze_count = df.count()
    logger.info(f"Bronze input row count: {bronze_count}")
except Exception as e:
    logger.error(f"Failed to read bronze layer: {e}")
    raise

# COMMAND ----------

# --- deduplication ------------------------------------------------------------
deduped_df = df.dropDuplicates(["policy_id"])
dup_count = bronze_count - deduped_df.count()
logger.info(f"Duplicates removed: {dup_count}")

# COMMAND ----------

# --- standardise text fields --------------------------------------------------
TEXT_COLS = [
    "fuel_type", "engine_type", "segment", "rear_brakes_type",
    "transmission_type", "steering_type", "region_code", "model",
]

clean_df = deduped_df
for c in TEXT_COLS:
    clean_df = clean_df.withColumn(c, upper(trim(col(c))))

# COMMAND ----------

# --- standardise boolean-style columns (Yes/No → 1/0) ------------------------
BOOL_COLS = [
    "is_esc", "is_adjustable_steering", "is_tpms", "is_parking_sensors",
    "is_parking_camera", "is_front_fog_lights", "is_rear_window_wiper",
    "is_rear_window_washer", "is_rear_window_defogger", "is_brake_assist",
    "is_power_door_locks", "is_central_locking", "is_power_steering",
    "is_driver_seat_height_adjustable", "is_day_night_rear_view_mirror",
    "is_ecw", "is_speed_alert",
]

for c in BOOL_COLS:
    clean_df = clean_df.withColumn(
        c,
        when(lower(trim(col(c))) == "yes", lit(1))
        .when(lower(trim(col(c))) == "no", lit(0))
        .otherwise(lit(None))
        .cast(IntegerType())
    )

# COMMAND ----------

# --- null handling for target column ------------------------------------------
clean_df = clean_df.withColumn(
    "claim_status",
    when(col("claim_status").isNull(), lit(0)).otherwise(col("claim_status"))
)

# COMMAND ----------

# --- enforce claim_status domain (must be 0 or 1) ----------------------------
invalid_claim = clean_df.filter(~col("claim_status").isin(0, 1)).count()
if invalid_claim > 0:
    logger.warning(f"Rows with invalid claim_status (not 0/1): {invalid_claim}. Filtering out.")
    clean_df = clean_df.filter(col("claim_status").isin(0, 1))

# COMMAND ----------

# --- outlier filtering --------------------------------------------------------
clean_df = (
    clean_df
    .filter(
        (col("customer_age") >= CUSTOMER_AGE_MIN)
        & (col("customer_age") <= CUSTOMER_AGE_MAX)
    )
    .filter(
        (col("vehicle_age") >= VEHICLE_AGE_MIN)
        & (col("vehicle_age") <= VEHICLE_AGE_MAX)
    )
    .filter(
        (col("ncap_rating") >= NCAP_RATING_MIN)
        & (col("ncap_rating") <= NCAP_RATING_MAX)
    )
)

silver_count = clean_df.count()
logger.info(f"Silver output row count: {silver_count}")
logger.info(f"Rows removed during cleansing: {bronze_count - silver_count}")

# COMMAND ----------

# --- persist ------------------------------------------------------------------
try:
    clean_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(SILVER_PATH)
    persisted_count = spark.read.format("delta").load(SILVER_PATH).count()
    assert persisted_count == silver_count, (
        f"Row count mismatch: expected={silver_count}, persisted={persisted_count}"
    )
    logger.info("Silver layer created successfully.")
except Exception as e:
    logger.error(f"Silver write failed: {e}")
    raise
