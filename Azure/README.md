# AI-Driven Data Engineering Pipelines

This repository showcases end-to-end batch data pipelines built with Python, Airflow, Spark, Databricks, ADLS, and Snowflake.

## Technologies Used
- Apache Airflow
- PySpark on Databricks / AWS Glue
- Azure ADLS Gen2
- Snowflake
- Delta Lake
- Python

## Pipeline Example
**Pipeline**: ADLS ➜ Transform with Databricks ➜ Load to Snowflake  
**Scheduler**: Apache Airflow  
**Monitoring**: Retry logic, Airflow UI, logging

## Concepts Covered
- Data partitioning, caching, broadcast joins in Spark
- Airflow retries, sensors, DAG idempotency
- Secure access to cloud storage (IAM, secrets)
