## Architecture Flow: Data Pipeline

[CSV in S3]
↓
[Airflow DAG]
↓
[Trigger Databricks Job → PySpark]
↓
[Writes Delta Lake Output to S3]
↓
[Airflow: COPY INTO Snowflake]
↓
[Snowflake Table for BI/Analytics]
