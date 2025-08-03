## Purpose:
This repository demonstrates an end-to-end AI-driven batch data pipeline architecture using modern cloud and big data tools.

## High-Level Architecture:
Source: AWS S3 (Data storage/ingestion layer)

Transformation: Databricks/Spark (Data cleaning, transformation, AI/ML prep)

Load: Snowflake (Modern data warehousing/analytics platform)

Orchestration: Apache Airflow (Schedules & automates pipeline tasks)

Monitoring:Built-in retry logic, observable Airflow UI, logging.

## Design flow
Pipeline: S3 ➜ Transform with Databricks ➜ Load to Snowflake

Scheduler: Apache Airflow

Monitoring: Retry logic, Airflow UI, logging
