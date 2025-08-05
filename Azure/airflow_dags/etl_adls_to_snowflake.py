from airflow import DAG
from airflow.providers.microsoft.azure.transfers.wasb_to_local import WasbToLocalOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime
import os

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

# Get the absolute paths to scripts for clarity and portability
AIRFLOW_HOME = os.getenv('AIRFLOW_HOME', '/opt/airflow')
SPARK_JOB_PATH = os.path.join(AIRFLOW_HOME, 'spark_jobs', 'transform_data.py')

with DAG(
    'etl_adls_to_snowflake',
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False
) as dag:

    download_file = WasbToLocalOperator(
        task_id='download_adls_file',
        wasb_conn_id='azure_data_lake_default',
        container_name='my-container',
        blob_name='data/myfile.csv',
        file_path='/tmp/myfile.csv'
    )

    transform_data = DatabricksRunNowOperator(
        task_id='run_databricks_job',
        databricks_conn_id='databricks_default',
        job_id=123,
        notebook_params={"python_script_path": SPARK_JOB_PATH}
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
