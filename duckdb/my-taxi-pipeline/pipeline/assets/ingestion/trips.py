"""@bruin

name: ingestion.trips

type: python

image: python:3.11

connection: duckdb-default

materialization:
  type: table
  strategy: append

columns:
  - name: pickup_datetime
    type: timestamp
    checks:
      - name: not_null
  - name: dropoff_datetime
    type: timestamp
    checks:
      - name: not_null
  - name: pickup_location_id
    type: integer
  - name: dropoff_location_id
    type: integer
  - name: fare_amount
    type: double
  - name: payment_type
    type: integer
  - name: taxi_type
    type: string
  - name: extracted_at
    type: timestamp

@bruin"""

import io
import os
import json
import pandas as pd
import requests
from dateutil.relativedelta import relativedelta

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
DATA_AVAILABLE_UNTIL_EXCLUSIVE = pd.Timestamp("2025-12-01")
OUTPUT_COLUMNS = [
    "pickup_datetime",
    "dropoff_datetime",
    "pickup_location_id",
    "dropoff_location_id",
    "fare_amount",
    "payment_type",
    "taxi_type",
    "extracted_at",
]
SOURCE_TO_OUTPUT_COLUMNS = {
    "yellow": {
        "tpep_pickup_datetime": "pickup_datetime",
        "tpep_dropoff_datetime": "dropoff_datetime",
        "PULocationID": "pickup_location_id",
        "DOLocationID": "dropoff_location_id",
        "fare_amount": "fare_amount",
        "payment_type": "payment_type",
    },
    "green": {
        "lpep_pickup_datetime": "pickup_datetime",
        "lpep_dropoff_datetime": "dropoff_datetime",
        "PULocationID": "pickup_location_id",
        "DOLocationID": "dropoff_location_id",
        "fare_amount": "fare_amount",
        "payment_type": "payment_type",
    },
}


def iterate_months(start_date: pd.Timestamp, end_date: pd.Timestamp):
    month_start = start_date.replace(day=1)
    while month_start < end_date:
        yield month_start
        month_start += relativedelta(months=1)


def build_source_url(taxi_type: str, month_start: pd.Timestamp) -> str:
    return f"{BASE_URL}/{taxi_type}_tripdata_{month_start:%Y-%m}.parquet"


def read_parquet_file(file_bytes: bytes, taxi_type: str) -> pd.DataFrame:
    source_columns = list(SOURCE_TO_OUTPUT_COLUMNS[taxi_type].keys())
    buffer = io.BytesIO(file_bytes)
    try:
        return pd.read_parquet(buffer, columns=source_columns)
    except Exception:
        buffer.seek(0)
        return pd.read_parquet(buffer)


def normalize_columns(raw_frame: pd.DataFrame, taxi_type: str, extracted_at: pd.Timestamp) -> pd.DataFrame:
    source_map = SOURCE_TO_OUTPUT_COLUMNS[taxi_type]
    available_columns = {column.lower(): column for column in raw_frame.columns}
    rename_map = {}
    missing_columns = []

    for source_column, output_column in source_map.items():
        actual_column = available_columns.get(source_column.lower())
        if actual_column is None:
            missing_columns.append(source_column)
            continue
        rename_map[actual_column] = output_column

    if missing_columns:
        raise ValueError(
            f"Missing expected columns for taxi type '{taxi_type}': {', '.join(missing_columns)}"
        )

    frame = raw_frame.rename(columns=rename_map).copy()
    frame["pickup_datetime"] = pd.to_datetime(frame["pickup_datetime"], errors="coerce")
    frame["dropoff_datetime"] = pd.to_datetime(frame["dropoff_datetime"], errors="coerce")
    frame["pickup_location_id"] = pd.to_numeric(frame["pickup_location_id"], errors="coerce").astype("Int64")
    frame["dropoff_location_id"] = pd.to_numeric(frame["dropoff_location_id"], errors="coerce").astype("Int64")
    frame["fare_amount"] = pd.to_numeric(frame["fare_amount"], errors="coerce")
    frame["payment_type"] = pd.to_numeric(frame["payment_type"], errors="coerce").astype("Int64")
    frame["taxi_type"] = taxi_type
    frame["extracted_at"] = extracted_at

    return frame[OUTPUT_COLUMNS]


def materialize():
    start_date = pd.Timestamp(os.environ["BRUIN_START_DATE"])
    end_date = pd.Timestamp(os.environ["BRUIN_END_DATE"])
    taxi_types = json.loads(os.environ.get("BRUIN_VARS", "{}")).get("taxi_types", ["yellow"])

    if not taxi_types:
        taxi_types = ["yellow"]

    if start_date >= DATA_AVAILABLE_UNTIL_EXCLUSIVE:
        print(
            f"No NYC TLC trip data available for this window. "
            f"Requested start date: {start_date.date()}, latest supported month end: 2025-11-30."
        )
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    effective_end_date = min(end_date, DATA_AVAILABLE_UNTIL_EXCLUSIVE)
    extracted_at = pd.Timestamp.now(tz="UTC").tz_localize(None)
    result_frames = []

    for taxi_type in taxi_types:
        if taxi_type not in SOURCE_TO_OUTPUT_COLUMNS:
            raise ValueError(
                f"Unsupported taxi_type '{taxi_type}'. Supported values: {', '.join(SOURCE_TO_OUTPUT_COLUMNS)}"
            )

        for month_start in iterate_months(start_date, effective_end_date):
            source_url = build_source_url(taxi_type, month_start)
            response = requests.get(source_url, timeout=180)
            if response.status_code in (403, 404):
                print(f"Skipping unavailable monthly file: {source_url}")
                continue
            response.raise_for_status()

            raw_frame = read_parquet_file(response.content, taxi_type)
            normalized_frame = normalize_columns(raw_frame, taxi_type, extracted_at)

            windowed_frame = normalized_frame[
                (normalized_frame["pickup_datetime"] >= start_date)
                & (normalized_frame["pickup_datetime"] < effective_end_date)
            ]
            if not windowed_frame.empty:
                result_frames.append(windowed_frame.reset_index(drop=True))

    if not result_frames:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    return pd.concat(result_frames, ignore_index=True)
