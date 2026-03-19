-- SQLite / PostgreSQL Schema for Telemetry Data

CREATE TABLE IF NOT EXISTS telemetry_data (
    event_id VARCHAR(36) PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    device_id VARCHAR(50) NOT NULL,
    temperature NUMERIC,
    vibration_freq NUMERIC,
    status VARCHAR(20) DEFAULT 'UNKNOWN',
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for querying by time and device
CREATE INDEX idx_telemetry_timestamp ON telemetry_data(timestamp);
CREATE INDEX idx_telemetry_device ON telemetry_data(device_id);
