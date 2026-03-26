# Databricks notebook source
# MAGIC %md
# MAGIC # Incremental Load — MERGE INTO (Upsert)
# MAGIC Demonstrates an incremental ingestion pattern using Delta Lake
# MAGIC `MERGE INTO`.  New or changed rows from a staging area are upserted
# MAGIC into the bronze table keyed on `policy_id`.
# MAGIC
# MAGIC This notebook is designed to run **after** the initial full load
# MAGIC (`01_bronze_ingestion.py`) and handles subsequent partial file drops.

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

current_timestamp, lit
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("incremental_merge")

# COMMAND ----------

# --- configuration -----------------------------------------------------------
INCREMENTAL_RAW_PATH = f"{VOLUME_ROOT}/raw/incremental/"

# COMMAND ----------

# --- read incoming incremental file ------------------------------------------
try:
    incoming_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .csv(INCREMENTAL_RAW_PATH)
    )
    incoming_count = incoming_df.count()
    logger.info(f"Incremental file row count: {incoming_count}")
    assert incoming_count > 0, "Incremental file is empty — nothing to merge."
except Exception as e:
    logger.error(f"Failed to read incremental file: {e}")
    raise

# COMMAND ----------

# --- add audit columns -------------------------------------------------------
incoming_df = (
    incoming_df
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source", lit("incremental"))
)

# COMMAND ----------

# --- register temp views for SQL MERGE --------------------------------------
incoming_df.createOrReplaceTempView("incoming_policies")

# COMMAND ----------

# --- MERGE INTO bronze table -------------------------------------------------
# Upsert logic:
#   MATCHED     → update all columns (policy changed)
#   NOT MATCHED → insert new row
logger.info("Running MERGE INTO bronze table...")

merge_sql = f"""
MERGE INTO delta.`{BRONZE_PATH}` AS target
USING incoming_policies AS source
ON target.policy_id = source.policy_id
WHEN MATCHED THEN
    UPDATE SET
        target.subscription_length = source.subscription_length,
        target.vehicle_age         = source.vehicle_age,
        target.customer_age        = source.customer_age,
        target.region_code         = source.region_code,
        target.region_density      = source.region_density,
        target.segment             = source.segment,
        target.model               = source.model,
        target.fuel_type           = source.fuel_type,
        target.max_torque          = source.max_torque,
        target.max_power           = source.max_power,
        target.engine_type         = source.engine_type,
        target.airbags             = source.airbags,
        target.is_esc              = source.is_esc,
        target.is_adjustable_steering = source.is_adjustable_steering,
        target.is_tpms             = source.is_tpms,
        target.is_parking_sensors  = source.is_parking_sensors,
        target.is_parking_camera   = source.is_parking_camera,
        target.rear_brakes_type    = source.rear_brakes_type,
        target.displacement        = source.displacement,
        target.cylinder            = source.cylinder,
        target.transmission_type   = source.transmission_type,
        target.steering_type       = source.steering_type,
        target.turning_radius      = source.turning_radius,
        target.length              = source.length,
        target.width               = source.width,
        target.gross_weight        = source.gross_weight,
        target.is_front_fog_lights = source.is_front_fog_lights,
        target.is_rear_window_wiper   = source.is_rear_window_wiper,
        target.is_rear_window_washer  = source.is_rear_window_washer,
        target.is_rear_window_defogger = source.is_rear_window_defogger,
        target.is_brake_assist     = source.is_brake_assist,
        target.is_power_door_locks = source.is_power_door_locks,
        target.is_central_locking  = source.is_central_locking,
        target.is_power_steering   = source.is_power_steering,
        target.is_driver_seat_height_adjustable = source.is_driver_seat_height_adjustable,
        target.is_day_night_rear_view_mirror    = source.is_day_night_rear_view_mirror,
        target.is_ecw              = source.is_ecw,
        target.is_speed_alert      = source.is_speed_alert,
        target.ncap_rating         = source.ncap_rating,
        target.claim_status        = source.claim_status
WHEN NOT MATCHED THEN
    INSERT *
"""

merge_result = spark.sql(merge_sql)

# COMMAND ----------

# --- log merge metrics -------------------------------------------------------
merge_metrics = merge_result.collect()[0]
logger.info(
    f"MERGE complete — "
    f"inserted: {merge_metrics['num_inserted_rows']}, "
    f"updated: {merge_metrics['num_updated_rows']}"
)

# COMMAND ----------

# --- verify final bronze count -----------------------------------------------
final_count = spark.read.format("delta").load(BRONZE_PATH).count()
logger.info(f"Bronze table final row count: {final_count}")

# COMMAND ----------

# --- Delta history for audit trail -------------------------------------------
history_df = spark.sql(f"DESCRIBE HISTORY delta.`{BRONZE_PATH}` LIMIT 5")
history_df.show(truncate=False)

logger.info("Incremental merge completed successfully.")
