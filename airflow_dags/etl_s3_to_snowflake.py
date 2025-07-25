
from airflow import DAG
from airflow.providers.amazon.aws.transfers.s3_to_local import S3ToLocalOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1
}

with DAG('etl_s3_to_snowflake',
         schedule_interval='@daily',
         default_args=default_args,
         catchup=False) as dag:

    download_file = S3ToLocalOperator(
        task_id='download_s3_file',
        aws_conn_id='aws_default',
        bucket='my-bucket',
        key='data/myfile.csv',
        filename='/tmp/myfile.csv'
    )

    transform_data = DatabricksRunNowOperator(
        task_id='run_databricks_job',
        databricks_conn_id='databricks_default',
        job_id=123
    )

    load_to_snowflake = SnowflakeOperator(
        task_id='load_to_snowflake',
        sql="""
            COPY INTO my_table
            FROM @my_stage/my_transformed_data.csv
            FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1);
        """,
        snowflake_conn_id='snowflake_default'
    )

    download_file >> transform_data >> load_to_snowflake
