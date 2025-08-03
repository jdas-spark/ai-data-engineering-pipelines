from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

# Start Spark Session
spark = SparkSession.builder.appName("ETL Pipeline").getOrCreate()

# Read input CSV from S3
df = spark.read.csv("s3a://my-bucket/input/sales.csv", header=True, inferSchema=True)

# Clean and transform data
cleaned_df = df.withColumn("sales_date", to_date(col("timestamp"))) \
               .filter(col("amount") > 0)

# Aggregate data by region
agg_df = cleaned_df.groupBy("region").sum("amount")

# Write output to Delta format in S3
agg_df.write.format("delta").mode("overwrite").save("s3a://my-bucket/output/agg_sales")
