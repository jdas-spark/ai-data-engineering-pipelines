# AI-Driven Transaction Scoring Pipeline (Airflow + AWS)

This project extracts transaction data from Amazon RDS, scores it using AWS Glue (PySpark), and loads it to Amazon Redshift. Airflow DAG orchestrates the pipeline.

### Stack
- Apache Airflow
- AWS Glue (Spark)
- Amazon RDS (MySQL)
- Amazon S3
- Amazon Redshift

### Glue Jobs
1. **Extract**: Pull from RDS → S3
2. **Transform**: Score fraud risk → S3
3. **Load**: Push scored data to Redshift

### DAG File
See `airflow/dag_transaction_pipeline.py` for orchestration.

### To Run:
- Create Glue jobs and upload scripts to S3
- Update DAG and deploy to Airflow
- Trigger DAG to run ETL pipeline
