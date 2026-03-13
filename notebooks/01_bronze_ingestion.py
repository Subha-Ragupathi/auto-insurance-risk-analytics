from pyspark.sql import SparkSession

spark=SparkSession.builder\
    .appName("auto_insurance_claims")\
        .getOrCreate()
    
base_input_path="/Volumes/workspace/default/raw/Insurance claims data.csv"
bronze_path="/Volumes/workspace/default/bronze/insurance_policy_data"

df=spark.read.option('header',True).option('inferschema',True).csv(base_input_path)
df.write.format("delta").mode("overwrite").save(bronze_path)
df.printSchema()
print("Bronze insurance policy data created successfully")
display(df)