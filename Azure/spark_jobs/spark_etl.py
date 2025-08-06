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

# Select and reorder columns as needed for Snowflake table
final_df = cleaned_df.select("region", "amount", "sales_date")  # Change columns as needed

# Write the result as CSV to /tmp, to be loaded by the snowflake_load.py script
final_df.toPandas().to_csv('/tmp/transformed_sales.csv', index=False)

# Optional: Confirm the output file exists
import os
print("CSV file created:", os.path.exists('/tmp/transformed_sales.csv'))