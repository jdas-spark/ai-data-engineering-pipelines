from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id="transaction_data_pipeline",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["transactions", "glue", "aws"],
) as dag:

    extract = AwsGlueJobOperator(
        task_id="extract_rds_to_s3",
        job_name="extract_rds_to_s3",
        aws_conn_id="aws_default",
        script_location="s3://scripts-bucket/glue_jobs/extract_rds_to_s3.py",
        region_name="us-east-1"
    )

    transform = AwsGlueJobOperator(
        task_id="transform_s3_data",
        job_name="transform_s3_data",
        aws_conn_id="aws_default",
        script_location="s3://scripts-bucket/glue_jobs/transform_s3_data.py",
        region_name="us-east-1"
    )

    load = AwsGlueJobOperator(
        task_id="load_to_redshift",
        job_name="load_to_redshift",
        aws_conn_id="aws_default",
        script_location="s3://scripts-bucket/glue_jobs/load_to_redshift.py",
        region_name="us-east-1"
    )

    extract >> transform >> load
