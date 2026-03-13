from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, when

spark = SparkSession.builder \
    .appName("auto_insurance_silver") \
    .getOrCreate()

bronze_path = "/Volumes/workspace/default/bronze/insurance_policy_data"
silver_path = "/Volumes/workspace/default/silver/insurance_policy_data_clean"

df = spark.read.format("delta").load(bronze_path)

clean_df = df.dropDuplicates(["policy_id"]) \
    .withColumn("model", trim(col("model"))) \
    .withColumn("fuel_type", upper(trim(col("fuel_type")))) \
    .withColumn("engine_type", upper(trim(col("engine_type")))) \
    .withColumn("segment", upper(trim(col("segment")))) \
    .withColumn("rear_brakes_type", upper(trim(col("rear_brakes_type")))) \
    .withColumn("transmission_type", upper(trim(col("transmission_type")))) \
    .withColumn("steering_type", upper(trim(col("steering_type")))) \
    .withColumn("claim_status", when(col("claim_status").isNull(), 0).otherwise(col("claim_status")))

display(clean_df)

clean_df.write.format("delta").mode("overwrite").save(silver_path)
print("Silver insurance policy data created successfully")
