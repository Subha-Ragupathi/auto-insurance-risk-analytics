# Databricks notebook source
# MAGIC %md
# MAGIC # Master Pipeline — End-to-End Orchestration
# MAGIC Executes the full medallion pipeline in sequence:
# MAGIC bronze → silver → gold → data quality.
# MAGIC
# MAGIC Each step is timed, and execution metadata is written to a Delta
# MAGIC audit log via `08_pipeline_audit`.

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %run ./08_pipeline_audit

# COMMAND ----------

import time
import uuid
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("master_pipeline")

pipeline_start = time.time()
step_timings = {}
RUN_ID = str(uuid.uuid4())[:8]
logger.info(f"Pipeline run_id: {RUN_ID}")

# COMMAND ----------

# --- retry helper (for programmatic steps) -----------------------------------
def run_with_retry(func, step_name, max_attempts=RETRY_MAX_ATTEMPTS,
                   base_delay=RETRY_BASE_DELAY_SECONDS):
    """Execute *func* with exponential-backoff retry.

    Used for programmatic steps.  Databricks `%run` cells cannot be
    wrapped in Python retry logic, so those steps rely on the built-in
    Databricks Jobs retry policy in production.
    """
    for attempt in range(1, max_attempts + 1):
        try:
            return func()
        except Exception as exc:
            if attempt == max_attempts:
                logger.error(
                    f"Step '{step_name}' failed after {max_attempts} attempts: {exc}"
                )
                raise
            delay = base_delay * (2 ** (attempt - 1))
            logger.warning(
                f"Step '{step_name}' attempt {attempt} failed: {exc}. "
                f"Retrying in {delay}s..."
            )
            time.sleep(delay)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Bronze Ingestion

# COMMAND ----------

logger.info("Step 1/4: Starting bronze ingestion...")
step_start = time.time()

# COMMAND ----------

# MAGIC %run ./01_bronze_ingestion

# COMMAND ----------

step_timings["bronze_ingestion"] = round(time.time() - step_start, 2)
bronze_rows = spark.read.format("delta").load(BRONZE_PATH).count()  # noqa: F821
log_audit_entry(
    spark, RUN_ID, "bronze_ingestion", "SUCCESS",  # noqa: F821
    row_count=bronze_rows,
    duration_sec=step_timings["bronze_ingestion"]
)
logger.info(f"Step 1/4: Bronze ingestion complete ({step_timings['bronze_ingestion']}s)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Silver Transformation

# COMMAND ----------

logger.info("Step 2/4: Starting silver transformation...")
step_start = time.time()

# COMMAND ----------

# MAGIC %run ./02_silver_transformation

# COMMAND ----------

step_timings["silver_transformation"] = round(time.time() - step_start, 2)
silver_rows = spark.read.format("delta").load(SILVER_PATH).count()  # noqa: F821
log_audit_entry(
    spark, RUN_ID, "silver_transformation", "SUCCESS",  # noqa: F821
    row_count=silver_rows,
    duration_sec=step_timings["silver_transformation"]
)
logger.info(f"Step 2/4: Silver transformation complete ({step_timings['silver_transformation']}s)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Gold Aggregation

# COMMAND ----------

logger.info("Step 3/4: Starting gold aggregation...")
step_start = time.time()

# COMMAND ----------

# MAGIC %run ./03_gold_aggregation

# COMMAND ----------

step_timings["gold_aggregation"] = round(time.time() - step_start, 2)
log_audit_entry(
    spark, RUN_ID, "gold_aggregation", "SUCCESS",  # noqa: F821
    duration_sec=step_timings["gold_aggregation"]
)
logger.info(f"Step 3/4: Gold aggregation complete ({step_timings['gold_aggregation']}s)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Data Quality Checks

# COMMAND ----------

logger.info("Step 4/4: Starting data quality checks...")
step_start = time.time()

# COMMAND ----------

# MAGIC %run ./04_data_quality_checks

# COMMAND ----------

step_timings["data_quality"] = round(time.time() - step_start, 2)
log_audit_entry(
    spark, RUN_ID, "data_quality", "SUCCESS",  # noqa: F821
    duration_sec=step_timings["data_quality"]
)
logger.info(f"Step 4/4: Data quality checks complete ({step_timings['data_quality']}s)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Summary

# COMMAND ----------

total_time = round(time.time() - pipeline_start, 2)
logger.info("=" * 60)
logger.info("PIPELINE EXECUTION SUMMARY")
logger.info(f"  Run ID: {RUN_ID}")
logger.info("=" * 60)
for step_name, duration in step_timings.items():
    logger.info(f"  {step_name:30s} {duration:>8.2f}s")
logger.info("-" * 60)
logger.info(f"  {'TOTAL':30s} {total_time:>8.2f}s")
logger.info("=" * 60)

log_audit_entry(
    spark, RUN_ID, "pipeline_complete", "SUCCESS",  # noqa: F821
    duration_sec=total_time
)
logger.info("End-to-end pipeline completed successfully.")
