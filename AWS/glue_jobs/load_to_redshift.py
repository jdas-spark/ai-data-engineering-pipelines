from awsglue.context import GlueContext
from pyspark.context import SparkContext

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": ["s3://my-bucket/processed/transactions_scored"]},
    format="parquet"
)

glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=dynamic_frame,
    catalog_connection="redshift-conn",
    connection_options={
        "dbtable": "public.transaction_scores",
        "database": "analytics"
    },
    redshift_tmp_dir="s3://my-bucket/temp/"
)
