# Telemetry Data Pipeline MVP

This project is a functional, modular Minimum Viable Product (MVP) of a cloud-native data pipeline. It is designed to illustrate the core concepts of data ingestion, schema validation, data cleaning, and structured storage. 

It simulates ingesting petabyte-scale telemetry data from mechatronics sensors, staging it, executing an ETL (Extract, Transform, Load) job using Pandas, and storing it in a relational database for analytics.

## Project Structure

- `simulate_telemetry.py`: Acts as the **Data Source**. Simulates real-time telemetry from IoT sensors (temperature, vibration, status) and streams JSON files into a local landing zone.
- `etl_pipeline.py`: The **Processor**. Periodically batch-processes the JSON files, validates the schema structure, cleans anomalies using Pandas, and loads the output into a database.
- `schema.sql`: The **Data Warehouse Structure**. Defines the Destination SQL Tables and Indexes for downstream analytics.
- `telemetry.db`: The **Target Database** (SQLite). 
- `SCALE_UP_GUIDE.md`: An architectural roadmap explaining how these local file and SQLite components map directly to AWS managed services (S3, Lambda, Postgres) for production.

## Component Breakdown

### 1. Ingestion (`simulate_telemetry.py`)
In a real-world scenario, devices emit data streams. This script acts as an edge device or sensor array, continuously generating JSON payloads.
* **Why JSON?** It is the standard format for unstructured edge/telemetry data.
* **Landing Zone:** The script dumps data into a `landing_zone/` folder, simulating an Amazon S3 "Raw" Bucket or a Kafka Topic.

### 2. ETL Framework (`etl_pipeline.py`)
This script handles the core data engineering tasks:
* **Extraction:** Reads the raw JSON files from the `landing_zone/`.
* **Validation:** Checks if critical required fields (`event_id`, `timestamp`, `device_id`) are present. Corrupted data is segregated.
* **Transformation (Clean):** Uses **Pandas** to process the data logically. It fills in missing values (e.g., `UNKNOWN` status) and filters out completely impossible values (like negative Kelvin temperatures), ensuring data quality.
* **Load:** Uses Pandas to write the cleaned rows directly to the target SQLite tables.
* **Archival:** Moves the processed JSON files to an `archive/` folder to prevent reprocessing and ensure idempotency.

### 3. Database Schema (`schema.sql`)
A robust architecture requires structured destination data. The schema defines strict types (e.g., `NUMERIC` for temperatures) and incorporates database **indexes** on the timestamp and device ID to ensure downstream queries remain extremely fast even as the table grows.

## How to Run locally

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```
2. **Start the Simulator:**
   Open a terminal and run the simulator to start generating data into the `landing_zone`:
   ```bash
   python simulate_telemetry.py
   ```
3. **Run the ETL Pipeline:**
   Leave the simulator running, and open a second terminal. Run the ETL script to pick up those files, clean them, and load them into the `telemetry.db`:
   ```bash
   python etl_pipeline.py
   ```

## Production Scalability
See `SCALE_UP_GUIDE.md` for details on how this pipeline seamlessly transitions from local files and SQLite to an enterprise-grade AWS architecture using Kinesis, S3, AWS Lambda, and Amazon RDS.
