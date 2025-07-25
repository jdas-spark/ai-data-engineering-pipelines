from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

spark = SparkSession.builder.appName("ETL Pipeline").getOrCreate()
df = spark.read.csv("s3a://my-bucket/input/sales.csv", header=True, inferSchema=True)
cleaned_df = df.withColumn("sales_date", to_date(col("timestamp"))).filter(col("amount") > 0)
agg_df = cleaned_df.groupBy("region").sum("amount")
agg_df.write.format("delta").mode("overwrite").save("s3a://my-bucket/output/agg_sales")
