from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

# Start Spark Session
spark = SparkSession.builder.appName("ETL Pipeline").getOrCreate()

# Read input CSV from Azure ADLS Gen2
df = spark.read.csv("abfss://my-container@my-storage-account.dfs.core.windows.net/input/sales.csv", header=True, inferSchema=True)

# Clean and transform data
cleaned_df = df.withColumn("sales_date", to_date(col("timestamp"))) \
               .filter(col("amount") > 0)

# Aggregate data by region
agg_df = cleaned_df.groupBy("region").sum("amount")

# Write output to Delta format in Azure ADLS Gen2
agg_df.write.format("delta").mode("overwrite").save("abfss://my-container@my-storage-account.dfs.core.windows.net/output/agg_sales")
