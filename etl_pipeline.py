import os
import glob
import json
import pandas as pd
import sqlite3
from datetime import datetime

LANDING_ZONE = "landing_zone"
ARCHIVE_ZONE = "archive"
DB_NAME = "telemetry.db"

SCHEMA_REQUIRED_FIELDS = ["event_id", "timestamp", "device_id"]

def ensure_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)

def setup_database():
    """Initialize SQLite database with schema."""
    conn = sqlite3.connect(DB_NAME)
    with open("schema.sql", "r") as f:
        conn.executescript(f.read())
    return conn

def extract_data(landing_zone):
    """Extract JSON files from landing zone."""
    files = glob.glob(os.path.join(landing_zone, "*.json"))
    raw_data = []
    processed_files = []
    
    for file in files:
        with open(file, "r") as f:
            try:
                data = json.load(f)
                raw_data.append(data)
                processed_files.append(file)
            except json.JSONDecodeError:
                print(f"Error reading {file}, skipping.")
                
    return raw_data, processed_files

def validate_schema(data):
    """Validate that required fields are present."""
    valid_data = []
    invalid_data = []
    for record in data:
        if all(field in record for field in SCHEMA_REQUIRED_FIELDS):
            valid_data.append(record)
        else:
            invalid_data.append(record)
    return valid_data, invalid_data

def transform_data(data):
    """Clean and transform data using Pandas."""
    if not data:
        return pd.DataFrame()
        
    df = pd.DataFrame(data)
    
    # Fill missing status with UNKNOWN
    if 'status' in df.columns:
        df['status'] = df['status'].fillna("UNKNOWN")
        
    # Drop rows with impossible temperature values (e.g., negative Kelvin equivalent)
    if 'temperature' in df.columns:
        df = df[df['temperature'] > -273.15]
        
    df['processed_at'] = datetime.utcnow().isoformat()
    return df

def load_data(df, conn):
    """Load cleaned data into SQLite database."""
    if df.empty:
        return
    
    df.to_sql("telemetry_data", conn, if_exists="append", index=False)
    print(f"Loaded {len(df)} records into the database.")

def archive_files(files):
    """Move processed files to archive zone."""
    ensure_dir(ARCHIVE_ZONE)
    for file in files:
        basename = os.path.basename(file)
        os.rename(file, os.path.join(ARCHIVE_ZONE, basename))

def main():
    ensure_dir(LANDING_ZONE)
    conn = setup_database()
    
    print("Starting ETL Process...")
    raw_data, files = extract_data(LANDING_ZONE)
    print(f"Extracted {len(raw_data)} records.")
    
    valid_data, invalid_data = validate_schema(raw_data)
    if invalid_data:
        print(f"Found {len(invalid_data)} records failing schema validation.")
        
    df_clean = transform_data(valid_data)
    
    load_data(df_clean, conn)
    
    archive_files(files)
    print("ETL Process completed.")
    
    conn.close()

if __name__ == "__main__":
    main()
