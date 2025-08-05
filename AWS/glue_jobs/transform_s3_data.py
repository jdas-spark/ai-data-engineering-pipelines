from pyspark.sql.functions import col, when
from awsglue.context import GlueContext
from pyspark.context import SparkContext

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

df = spark.read.csv("s3://my-bucket/raw/transactions", header=True, inferSchema=True)

df_scored = df.withColumn(
    "fraud_score",
    when(col("amount") > 1000, 0.9).when(col("merchant") == "suspicious", 0.8).otherwise(0.1)
)

df_scored.write.format("parquet").mode("overwrite").save("s3://my-bucket/processed/transactions_scored")
