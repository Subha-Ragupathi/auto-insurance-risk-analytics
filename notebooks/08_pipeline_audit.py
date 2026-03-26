# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline Audit Log
# MAGIC Records execution metadata for every pipeline run into a Delta audit
# MAGIC table.  Each row captures the step name, status, row counts, duration,
# MAGIC and timestamp.  Designed to be called from the master pipeline after
# MAGIC each step completes.

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, TimestampType
)
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("pipeline_audit")

# COMMAND ----------

# --- audit schema ------------------------------------------------------------
AUDIT_SCHEMA = StructType([
    StructField("run_id",        StringType(),    False),
    StructField("step_name",     StringType(),    False),
    StructField("status",        StringType(),    False),
    StructField("row_count",     LongType(),      True),
    StructField("duration_sec",  DoubleType(),    True),
    StructField("error_message", StringType(),    True),
    StructField("recorded_at",   TimestampType(), True),
])

# COMMAND ----------

# --- helper: write a single audit entry -------------------------------------
def log_audit_entry(
    spark_session, run_id, step_name, status,
    row_count=None, duration_sec=None, error_message=None
):
    """Append one audit row to the Delta audit log table."""
    row = spark_session.createDataFrame(
        [(run_id, step_name, status, row_count, duration_sec, error_message, None)],
        schema=AUDIT_SCHEMA
    ).withColumn("recorded_at", current_timestamp())

    row.write.format("delta").mode("append").save(AUDIT_LOG_PATH)
    logger.info(
        f"Audit logged: run={run_id} step={step_name} "
        f"status={status} rows={row_count} duration={duration_sec}s"
    )

# COMMAND ----------

# --- helper: read audit log --------------------------------------------------
def get_audit_log(spark_session, limit=50):
    """Return the most recent audit entries."""
    return (
        spark_session.read.format("delta").load(AUDIT_LOG_PATH)
        .orderBy("recorded_at", ascending=False)
        .limit(limit)
    )

# COMMAND ----------

# --- helper: get last successful run for a step -----------------------------
def get_last_success(spark_session, step_name):
    """Return the timestamp of the last successful run for a given step."""
    audit_df = spark_session.read.format("delta").load(AUDIT_LOG_PATH)
    return (
        audit_df
        .filter(
            (audit_df.step_name == step_name)
            & (audit_df.status == "SUCCESS")
        )
        .orderBy("recorded_at", ascending=False)
        .limit(1)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick Preview (standalone run)
# MAGIC When this notebook is run directly, display the most recent audit entries.

# COMMAND ----------

try:
    recent = get_audit_log(spark, limit=20)  # noqa: F821
    recent.show(truncate=False)
except Exception:
    logger.info("No audit log found yet — run the master pipeline first.")
