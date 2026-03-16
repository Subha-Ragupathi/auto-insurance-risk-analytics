# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Raw Ingestion
# MAGIC Reads the Kaggle auto insurance CSV and persists it as a Delta table
# MAGIC with an explicit schema. No transformations are applied at this stage.

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bronze_ingestion")

# COMMAND ----------

# --- configuration -----------------------------------------------------------
RAW_PATH = "/Volumes/workspace/default/raw/Insurance claims data.csv"
BRONZE_PATH = "/Volumes/workspace/default/bronze/insurance_policy_data"

# COMMAND ----------

# --- explicit schema ----------------------------------------------------------
INSURANCE_SCHEMA = StructType([
    StructField("policy_id",              StringType(),  False),
    StructField("subscription_length",    DoubleType(),  True),
    StructField("vehicle_age",            DoubleType(),  True),
    StructField("customer_age",           IntegerType(), True),
    StructField("region_code",            StringType(),  True),
    StructField("region_density",         IntegerType(), True),
    StructField("segment",               StringType(),  True),
    StructField("model",                  StringType(),  True),
    StructField("fuel_type",              StringType(),  True),
    StructField("max_torque",             StringType(),  True),
    StructField("max_power",              StringType(),  True),
    StructField("engine_type",            StringType(),  True),
    StructField("airbags",               IntegerType(), True),
    StructField("is_esc",                 StringType(),  True),
    StructField("is_adjustable_steering", StringType(),  True),
    StructField("is_tpms",               StringType(),  True),
    StructField("is_parking_sensors",     StringType(),  True),
    StructField("is_parking_camera",      StringType(),  True),
    StructField("rear_brakes_type",       StringType(),  True),
    StructField("displacement",           IntegerType(), True),
    StructField("cylinder",              IntegerType(), True),
    StructField("transmission_type",      StringType(),  True),
    StructField("steering_type",          StringType(),  True),
    StructField("turning_radius",         DoubleType(),  True),
    StructField("length",                IntegerType(), True),
    StructField("width",                 IntegerType(), True),
    StructField("gross_weight",          IntegerType(), True),
    StructField("is_front_fog_lights",    StringType(),  True),
    StructField("is_rear_window_wiper",   StringType(),  True),
    StructField("is_rear_window_washer",  StringType(),  True),
    StructField("is_rear_window_defogger", StringType(), True),
    StructField("is_brake_assist",        StringType(),  True),
    StructField("is_power_door_locks",    StringType(),  True),
    StructField("is_central_locking",     StringType(),  True),
    StructField("is_power_steering",      StringType(),  True),
    StructField("is_driver_seat_height_adjustable", StringType(), True),
    StructField("is_day_night_rear_view_mirror",    StringType(), True),
    StructField("is_ecw",                StringType(),  True),
    StructField("is_speed_alert",         StringType(),  True),
    StructField("ncap_rating",           IntegerType(), True),
    StructField("claim_status",          IntegerType(), True),
])

# COMMAND ----------

# --- ingestion ----------------------------------------------------------------
try:
    df = (
        spark.read
        .option("header", True)
        .schema(INSURANCE_SCHEMA)
        .csv(RAW_PATH)
    )

    raw_count = df.count()
    logger.info(f"Raw CSV row count: {raw_count}")
    assert raw_count > 0, "Source CSV is empty — aborting bronze write."

    df.write.format("delta").mode("overwrite").save(BRONZE_PATH)

    bronze_count = spark.read.format("delta").load(BRONZE_PATH).count()
    logger.info(f"Bronze Delta row count: {bronze_count}")
    assert raw_count == bronze_count, (
        f"Row count mismatch: raw={raw_count}, bronze={bronze_count}"
    )

    logger.info("Bronze layer created successfully.")

except Exception as e:
    logger.error(f"Bronze ingestion failed: {e}")
    raise
