from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when

spark = SparkSession.builder \
    .appName("auto_insurance_dq") \
    .getOrCreate()

silver_path = "/Volumes/workspace/default/silver/insurance_policy_data_clean"

df = spark.read.format("delta").load(silver_path)

null_check = df.select([
    count(when(col(c).isNull(), c)).alias(c) for c in df.columns
])

duplicate_check = df.groupBy("policy_id").count().filter(col("count") > 1)

print("Null counts:")
display(null_check)

print("Duplicate policy IDs:")
display(duplicate_check)
