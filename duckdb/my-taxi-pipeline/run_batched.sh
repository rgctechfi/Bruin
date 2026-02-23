#!/usr/bin/env bash

set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <start-date YYYY-MM-DD> <end-date YYYY-MM-DD> [batch-days]"
  echo "Example: $0 2025-10-01 2025-11-01 1"
  exit 1
fi

START_DATE="$1"
END_DATE="$2"
BATCH_DAYS="${3:-1}"

if ! [[ "$BATCH_DAYS" =~ ^[0-9]+$ ]] || [[ "$BATCH_DAYS" -lt 1 ]]; then
  echo "batch-days must be a positive integer (got: $BATCH_DAYS)"
  exit 1
fi

if [[ "$START_DATE" > "$END_DATE" ]]; then
  echo "start-date must be <= end-date"
  exit 1
fi

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PIPELINE_PATH="$PROJECT_ROOT/pipeline/pipeline.yml"
CONFIG_PATH="$PROJECT_ROOT/.bruin.yml"

echo "Bootstrapping empty incremental tables if needed..."
bruin query \
  --config-file "$CONFIG_PATH" \
  --environment default \
  --connection duckdb-default \
  --query "
    CREATE SCHEMA IF NOT EXISTS ingestion;
    CREATE TABLE IF NOT EXISTS ingestion.trips (
      pickup_datetime TIMESTAMP,
      dropoff_datetime TIMESTAMP,
      pickup_location_id INTEGER,
      dropoff_location_id INTEGER,
      fare_amount DOUBLE,
      payment_type INTEGER,
      taxi_type VARCHAR,
      extracted_at TIMESTAMP
    );

    CREATE SCHEMA IF NOT EXISTS staging;
    CREATE TABLE IF NOT EXISTS staging.trips (
      pickup_datetime TIMESTAMP,
      dropoff_datetime TIMESTAMP,
      pickup_location_id INTEGER,
      dropoff_location_id INTEGER,
      fare_amount DOUBLE,
      taxi_type VARCHAR,
      payment_type_name VARCHAR
    );
    CREATE SCHEMA IF NOT EXISTS reports;
    CREATE TABLE IF NOT EXISTS reports.trips_report (
      trip_date DATE,
      taxi_type VARCHAR,
      payment_type VARCHAR,
      trip_count BIGINT,
      total_fare DOUBLE,
      avg_fare DOUBLE
    );
  " >/dev/null

current_start="$START_DATE"

while [[ "$current_start" < "$END_DATE" ]]; do
  next_end="$(date -I -d "$current_start + $BATCH_DAYS day")"
  if [[ "$next_end" > "$END_DATE" ]]; then
    next_end="$END_DATE"
  fi

  echo "Running incremental batch: $current_start -> $next_end"
  bruin run "$PIPELINE_PATH" \
    --config-file "$CONFIG_PATH" \
    --environment default \
    --workers 1 \
    --start-date "$current_start" \
    --end-date "$next_end"

  current_start="$next_end"
done

echo "All batches completed successfully."

#using : 
#cd duckdb/my-taxi-pipeline
#./run_batched.sh 2025-10-01 2025-11-01 1
