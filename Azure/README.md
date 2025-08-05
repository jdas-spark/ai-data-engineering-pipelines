# AI-Driven Data Engineering Pipelines

This repository showcases end-to-end batch data pipelines built with Python, Airflow, Spark, Databricks, AWS, and Snowflake.

## Technologies Used
- Apache Airflow
- PySpark on Databricks / AWS Glue
- AWS S3, EMR
- Snowflake
- Delta Lake
- Python

## Pipeline Example
**Pipeline**: S3 ➜ Transform with Databricks ➜ Load to Snowflake  
**Scheduler**: Apache Airflow  
**Monitoring**: Retry logic, Airflow UI, logging

## Concepts Covered
- Data partitioning, caching, broadcast joins in Spark
- Airflow retries, sensors, DAG idempotency
- Secure access to cloud storage (IAM, secrets)
