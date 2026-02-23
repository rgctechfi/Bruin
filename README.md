<h1 align="center">
  <img src="./ressources/bruin_logo.png" alt="Bruin logo" width="42" />
  <span>𝘽𝙧𝙪𝙞𝙣</span>
</h1>

<p align="center">
  <img src="./ressources/bruin_banner.png" alt="Bruin banner" width="720" />
</p>

<p align="center">
  <em>"What if Airbyte, Airflow, dbt, and Great Expectations had a lovechild?" Zoomcamp</em>
</p>

## Bruin Pipelines

Bruin is an end-to-end data platform that combines:

- ingestion
- transformation, cleaning, modeling, and aggregation with SQL, Python, and R
- orchestration, scheduling, dependency management, and pipeline run management
- governance through built-in quality checks and validation for accuracy and consistency
- metadata management, including lineage, ownership, and documentation

Bruin lets you keep code logic, configurations, dependencies, and quality checks in one place.

**Core Concepts**

- **Asset**: Any data artifact that carries value (table, view, file, ML model, etc.)
- **Pipeline**: A group of assets executed together in dependency order
- **Environment**: A named set of connection configs (e.g., `default`, `production`) so the same pipeline can run locally and in production
- **Connection**: Credentials to authenticate with external data sources & destinations
- **Pipeline run**: A single execution instance with specific dates and configuration

**Workflow**

<p align="center">
  <img src="./ressources/workflow.png" alt="Bruin banner" width="1440" />
</p>

## Quickstart: `my-taxi-pipeline` (DuckDB)

This section explains the SQL/Python flow for `duckdb/my-taxi-pipeline` and the essential commands to run ingestion, transformations, and reporting.

### 1) Prerequisites

- Bruin CLI installed (`bruin version`)
- DuckDB driver installed for Bruin (`dbc install duckdb`)
- Run commands from `duckdb/my-taxi-pipeline` for simpler paths

```bash
cd duckdb/my-taxi-pipeline
export BRUIN_CONFIG_FILE=.bruin.yml
```

### 2) File Structure (What Each File Does)

- `.bruin.yml`: environments and connections (`duckdb-default`)
- `pipeline/pipeline.yml`: pipeline definition (name, schedule, variables, default connections)
- `pipeline/assets/ingestion/payment_lookup.asset.yml`: CSV seed lookup table -> `ingestion.payment_lookup`
- `pipeline/assets/ingestion/trips.py`: Python ingestion of TLC parquet files -> `ingestion.trips`
- `pipeline/assets/staging/trips.sql`: cleaning, deduplication, enrichment -> `staging.trips` (`time_interval` strategy)
- `pipeline/assets/reports/trips_report.sql`: final aggregation -> `reports.trips_report`
- `run_batched.sh`: batch execution (date-window slicing) to reduce memory pressure

### 3) Essential Commands

Validate:

```bash
bruin validate ./pipeline/pipeline.yml --environment default --config-file .bruin.yml
```

Run only the ingestion asset:

```bash
bruin run ./pipeline/assets/ingestion/trips.py --environment default --config-file .bruin.yml --start-date 2025-10-01 --end-date 2025-10-02
```

Run the full pipeline on a small window (recommended):

```bash
bruin run ./pipeline/pipeline.yml --environment default --config-file .bruin.yml --workers 1 --start-date 2025-10-01 --end-date 2025-10-02
```

Quick sanity checks on row counts/results:

```bash
bruin query --connection duckdb-default --environment default --config-file .bruin.yml --query "SELECT COUNT(*) AS c FROM ingestion.trips;"
bruin query --connection duckdb-default --environment default --config-file .bruin.yml --query "SELECT COUNT(*) AS c FROM staging.trips;"
bruin query --connection duckdb-default --environment default --config-file .bruin.yml --query "SELECT * FROM reports.trips_report ORDER BY trip_date, taxi_type, payment_type LIMIT 20;"
```

### 4) Batch Execution (Anti-Out Of Memory)

When `staging.trips` becomes too heavy for memory, run the batch script:

```bash
./run_batched.sh 2025-10-01 2025-11-01 1
```

- argument 1: start date
- argument 2: end date (exclusive)
- argument 3: batch size in days (`1` = one day per run)

This script:

- bootstraps incremental tables if missing (`ingestion.trips`, `staging.trips`, `reports.trips_report`)
- executes the pipeline window by window
- forces `--workers 1` to avoid DuckDB write lock conflicts

### 5) Important Notes

- TLC data is only available through November 2025. Use windows <= `2025-11-30`.
- To avoid DuckDB lock issues, avoid concurrent runs against the same `duckdb.db`.
- To reset cleanly if needed:

```bash
bruin query --connection duckdb-default --environment default --config-file .bruin.yml --query "DROP TABLE IF EXISTS reports.trips_report; DROP TABLE IF EXISTS staging.trips; DROP TABLE IF EXISTS ingestion.trips;"
```
