# Databricks notebook source
# MAGIC %md
# MAGIC # Master Pipeline — End-to-End Orchestration
# MAGIC Executes the full medallion pipeline in sequence:
# MAGIC bronze → silver → gold → data quality.

# COMMAND ----------

import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("master_pipeline")

pipeline_start = time.time()
step_timings = {}

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
logger.info(f"Step 4/4: Data quality checks complete ({step_timings['data_quality']}s)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Summary

# COMMAND ----------

total_time = round(time.time() - pipeline_start, 2)
logger.info("=" * 60)
logger.info("PIPELINE EXECUTION SUMMARY")
logger.info("=" * 60)
for step_name, duration in step_timings.items():
    logger.info(f"  {step_name:30s} {duration:>8.2f}s")
logger.info("-" * 60)
logger.info(f"  {'TOTAL':30s} {total_time:>8.2f}s")
logger.info("=" * 60)
logger.info("End-to-end pipeline completed successfully.")
