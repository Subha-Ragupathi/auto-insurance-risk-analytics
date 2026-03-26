# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer — Business Aggregations
# MAGIC Derives analytical summary tables from the silver layer.  Each gold
# MAGIC table is a pre-aggregated view designed for downstream BI dashboards
# MAGIC and ad-hoc SQL analytics.

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

from pyspark.sql.functions import (
    col, count, sum as _sum, avg, min as _min, max as _max, round as _round, when
)
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("gold_aggregation")

# COMMAND ----------

# --- load silver --------------------------------------------------------------
try:
    df = spark.read.format("delta").load(SILVER_PATH)
    silver_count = df.count()
    logger.info(f"Silver input row count: {silver_count}")
except Exception as e:
    logger.error(f"Failed to read silver layer: {e}")
    raise

# COMMAND ----------

# --- derive age bands ---------------------------------------------------------
df = df.withColumn(
    "vehicle_age_band",
    when(col("vehicle_age") <= 1, "0-1")
    .when(col("vehicle_age") <= 3, "2-3")
    .when(col("vehicle_age") <= 5, "4-5")
    .otherwise("5+")
).withColumn(
    "customer_age_band",
    when(col("customer_age") < 25, "Under 25")
    .when(col("customer_age") < 35, "25-34")
    .when(col("customer_age") < 45, "35-44")
    .when(col("customer_age") < 55, "45-54")
    .otherwise("55+")
)

# COMMAND ----------

# --- helper: build standard claim-rate summary --------------------------------
def build_claim_summary(source_df, group_col, table_name):
    """Aggregate claim metrics grouped by a single dimension."""
    summary = (
        source_df.groupBy(group_col)
        .agg(
            count("policy_id").alias("total_policies"),
            _sum("claim_status").alias("total_claims"),
            _round(avg("claim_status"), 4).alias("claim_rate"),
            _round(avg("subscription_length"), 2).alias("avg_subscription_length"),
            _round(avg("customer_age"), 1).alias("avg_customer_age"),
        )
        .orderBy(col("claim_rate").desc())
    )
    return summary

# COMMAND ----------

# --- build and persist each gold table ----------------------------------------
for table_name, group_col in GOLD_TABLES.items():
    try:
        summary = build_claim_summary(df, group_col, table_name)
        output_path = f"{GOLD_BASE}{table_name}"
        summary.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(output_path)

        row_count = spark.read.format("delta").load(output_path).count()
        logger.info(f"Gold table '{table_name}' written — {row_count} rows.")
    except Exception as e:
        logger.error(f"Failed to write gold table '{table_name}': {e}")
        raise

# COMMAND ----------

# --- cross-dimension summary (overall portfolio) -----------------------------
portfolio_summary = df.agg(
    count("policy_id").alias("total_policies"),
    _sum("claim_status").alias("total_claims"),
    _round(avg("claim_status"), 4).alias("overall_claim_rate"),
    _round(avg("subscription_length"), 2).alias("avg_subscription_length"),
    _min("customer_age").alias("min_customer_age"),
    _max("customer_age").alias("max_customer_age"),
)

portfolio_summary.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{GOLD_BASE}portfolio_summary")
logger.info("Gold layer created successfully — all tables written.")
