# Databricks notebook source
# MAGIC %md
# MAGIC # Export Gold Tables to CSV
# MAGIC Exports all gold-layer Delta tables to CSV for Power BI import.
# MAGIC Files are saved to the `raw` volume for easy download.

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("export_to_csv")

# COMMAND ----------

for table_name in GOLD_TABLE_NAMES:
    try:
        df = spark.read.format("delta").load(f"{GOLD_BASE}{table_name}")
        output_file = f"{EXPORT_PATH}{table_name}"
        df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_file)
        logger.info(f"Exported {table_name} — {df.count()} rows")
    except Exception as e:
        logger.error(f"Failed to export {table_name}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Also export clean silver data for Power BI detailed analysis

# COMMAND ----------

try:
    silver_df = spark.read.format("delta").load(SILVER_PATH)
    silver_df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{EXPORT_PATH}silver_clean")
    logger.info(f"Exported silver_clean — {silver_df.count()} rows")
except Exception as e:
    logger.error(f"Failed to export silver data: {e}")

# COMMAND ----------

logger.info("All exports complete. Download CSVs from Catalog → workspace → default → raw → exports")
