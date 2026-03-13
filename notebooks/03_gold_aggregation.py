from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum, avg, when, col

spark = SparkSession.builder \
    .appName("auto_insurance_gold") \
    .getOrCreate()

silver_path = "/Volumes/workspace/default/silver/insurance_policy_data_clean"
gold_base_path = "/Volumes/workspace/default/gold/"

df = spark.read.format("delta").load(silver_path)

df = df.withColumn(
    "vehicle_age_band",
    when(col("vehicle_age") <= 1, "0-1")
    .when(col("vehicle_age") <= 3, "2-3")
    .when(col("vehicle_age") <= 5, "4-5")
    .otherwise("5+")
).withColumn(
    "customer_age_band",
    when(col("customer_age") < 25, "<25")
    .when(col("customer_age") < 35, "25-34")
    .when(col("customer_age") < 45, "35-44")
    .when(col("customer_age") < 55, "45-54")
    .otherwise("55+")
)

claim_rate_by_region = df.groupBy("region_code").agg(
    count("policy_id").alias("total_policies"),
    sum("claim_status").alias("claim_count"),
    avg("claim_status").alias("claim_rate")
)

claim_rate_by_segment = df.groupBy("segment").agg(
    count("policy_id").alias("total_policies"),
    sum("claim_status").alias("claim_count"),
    avg("claim_status").alias("claim_rate")
)

claim_rate_by_fuel_type = df.groupBy("fuel_type").agg(
    count("policy_id").alias("total_policies"),
    sum("claim_status").alias("claim_count"),
    avg("claim_status").alias("claim_rate")
)

claim_rate_by_vehicle_age_band = df.groupBy("vehicle_age_band").agg(
    count("policy_id").alias("total_policies"),
    sum("claim_status").alias("claim_count"),
    avg("claim_status").alias("claim_rate")
)

claim_rate_by_customer_age_band = df.groupBy("customer_age_band").agg(
    count("policy_id").alias("total_policies"),
    sum("claim_status").alias("claim_count"),
    avg("claim_status").alias("claim_rate")
)

claim_rate_by_ncap_rating = df.groupBy("ncap_rating").agg(
    count("policy_id").alias("total_policies"),
    sum("claim_status").alias("claim_count"),
    avg("claim_status").alias("claim_rate")
)

display(claim_rate_by_region)

claim_rate_by_region.write.format("delta").mode("overwrite").save(gold_base_path + "claim_rate_by_region")
claim_rate_by_segment.write.format("delta").mode("overwrite").save(gold_base_path + "claim_rate_by_segment")
claim_rate_by_fuel_type.write.format("delta").mode("overwrite").save(gold_base_path + "claim_rate_by_fuel_type")
claim_rate_by_vehicle_age_band.write.format("delta").mode("overwrite").save(gold_base_path + "claim_rate_by_vehicle_age_band")
claim_rate_by_customer_age_band.write.format("delta").mode("overwrite").save(gold_base_path + "claim_rate_by_customer_age_band")
claim_rate_by_ncap_rating.write.format("delta").mode("overwrite").save(gold_base_path + "claim_rate_by_ncap_rating")

print("Gold insurance risk summary tables created successfully")
