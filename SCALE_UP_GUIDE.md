# Scale-Up Guide: Transitioning to Production

This architecture simulates an event-driven data pipeline using local files and SQLite. Moving to a cloud-native, petabyte-scale production environment on AWS requires swapping these local primitives for managed cloud services.

## Current vs. Production Architecture

| Component | MVP (Current) | Production (AWS) |
| :--- | :--- | :--- |
| **Data Ingestion** | Local File System (JSON) | AWS API Gateway + AWS Kinesis Data Streams / Apache Kafka (MSK) |
| **Storage (Raw)** | Local `landing_zone/` folder | Amazon S3 (Raw Data Lake Bucket) |
| **Compute / ETL** | Cron / local Python Script | AWS Lambda (for streaming) or AWS Glue / EMR Spark (for batch) |
| **Database** | SQLite | Amazon RDS for PostgreSQL, Amazon Redshift, or Snowflake |

## Production Migration Steps

### 1. Ingestion via Apache Kafka (Amazon MSK)
Instead of the simulator writing JSON files to a local disk, production sensors will establish a lightweight connection (MQTT or HTTP) and push events to a **Kafka Topic**. Kafka will act as a buffer, ensuring no data is dropped even during massive traffic spikes.

### 2. S3 as the Data Lake
AWS S3 is infinitely scalable. We configure an S3 bucket to be our new `landing_zone`.
Instead of globbing local directories, we use **Amazon Kinesis Firehose** or Kafka Connect to automatically flush our Kafka streams in compressed micro-batches (e.g., Parquet format) directly into our Raw S3 Bucket.

### 3. Serverless ETL with AWS Lambda
The `etl_pipeline.py` script will be modified to run inside an **AWS Lambda function**. We configure an S3 Event Notification so that every time a new Parquet file drops into the raw bucket, the Lambda immediately wakes up, parses the file, applies Pandas/Polars logic to validate the schema, and writes the cleaned data to an S3 "Processed" bucket or directly to the Data Warehouse.

### 4. Robust Database
SQLite is file-based and cannot handle concurrent connections gracefully. We swap the connection string from SQLite to an **Amazon RDS for PostgreSQL** cluster. For true analytical scaling, the data could be shipped to a columnar warehouse like **Amazon Redshift**.

## Expected Pipeline Flow
`Sensors -> API Gateway -> Apache Kafka -> S3 (Raw) -> Lambda (ETL + Validation) -> Amazon PostgreSQL RDS`
