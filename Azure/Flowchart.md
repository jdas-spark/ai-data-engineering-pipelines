## Architecture Flow: Data Pipeline

[CSV in ADLS]
↓
[Airflow DAG]
↓
[Trigger Databricks Job → PySpark]
↓
[Writes Delta Lake Output to ADLS]
↓
[Airflow: COPY INTO Snowflake]
↓
[Snowflake Table for BI/Analytics]
