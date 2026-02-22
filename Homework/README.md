# Module 5 Homework: Data Platforms with Bruin

In this homework, we'll use Bruin to build a complete data pipeline, from ingestion to reporting.

## Setup

- Install Bruin CLI:

```bash
curl -LsSf https://getbruin.com/install/cli | sh
```

- Initialize the zoomcamp template:

```bash
bruin init zoomcamp my-pipeline
```

- Configure your `.bruin.yml` with a DuckDB connection.
- Follow the tutorial in the main module README.

After completing the setup, you should have a working NYC taxi data pipeline.

---

## Questions & Choices

### Question 1. Bruin Pipeline Structure

In a Bruin project, what are the required files/directories?

- `bruin.yml` and `assets/`
- `.bruin.yml` and `pipeline.yml` (assets can be anywhere)
- `.bruin.yml` and `pipeline/` with `pipeline.yml` and `assets/`
- `pipeline.yml` and `assets/` only

Selected answer:

```text
Justification: Bruin aims to be flexible. The .bruin.yml file at the root defines the connections (DuckDB, BigQuery, etc.), and each asset folder contains a pipeline.yml (or metadata in comments in SQL/Python files) to define the logic. Unlike other tools, it does not impose a rigid folder structure for assets.
```

<p align="center">
  <img src="https://img.shields.io/badge/Answer-.bruin.yml and pipeline.yml (assets can be anywhere)-darkgreen" alt="Answer Q1">
</p>

---

### Question 2. Materialization Strategies

You're building a pipeline that processes NYC taxi data organized by month based on `pickup_datetime`. Which materialization strategy should you use for the staging layer that deduplicates and cleans the data?

- `append` - always add new rows
- `replace` - truncate and rebuild entirely
- `time_interval` - incremental based on a time column
- `view` - create a virtual table only

Selected answer:

```text
Justification: For NYC taxi data (often voluminous and partitioned by month), doing a replace (rebuild everything) is too costly. The time_interval strategy allows precisely deleting the time window that we are about to reload, thus avoiding duplicates while being resource-efficient.
```

<p align="center">
  <img src="https://img.shields.io/badge/Answer-time_interval incremental based on a time column-darkgreen" alt="Answer Q2">
</p>

---

### Question 3. Pipeline Variables

You have the following variable defined in `pipeline.yml`:

```yaml
variables:
  taxi_types:
    type: array
    items:
      type: string
    default: ["yellow", "green"]
```

How do you override this when running the pipeline to only process yellow taxis?

- `bruin run --taxi-types yellow`
- `bruin run --var taxi_types=yellow`
- `bruin run --var 'taxi_types=["yellow"]'`
- `bruin run --set taxi_types=["yellow"]`

Selected answer:

```text
When manipulating arrays in the command line, the syntax must respect JSON or YAML format so that the CLI correctly interprets the list, hence the importance of quotes and brackets.
```


<p align="center">
  <img src="https://img.shields.io/badge/Answer-bruin%20run%20--var%20%27taxi_types%3D%5B%22yellow%22%5D%27-darkgreen" alt="Answer Q3">
</p>

---

### Question 4. Running with Dependencies

You've modified the `ingestion/trips.py` asset and want to run it plus all downstream assets. Which command should you use?

- `bruin run ingestion.trips --all`
- `bruin run ingestion/trips.py --downstream`
- `bruin run pipeline/trips.py --recursive`
- `bruin run --select ingestion.trips+`

Selected answer:

```text
Justification: The + symbol at the end of an asset name is a common convention in the data world (popularized by dbt). It means: "Execute this asset AND everything that follows from it (its children)". This is crucial when you modify the source and want the subsequent transformations to be updated.
```

<p align="center">
  <img src="https://img.shields.io/badge/Answer-bruin run --select ingestion.trips+-darkgreen" alt="Answer Q4">
</p>

---

### Question 5. Quality Checks

You want to ensure the `pickup_datetime` column in your `trips` table never has `NULL` values. Which quality check should you add to your asset definition?

- `unique: true`
- `not_null: true`
- `positive: true`
- `accepted_values: [not_null]`

Selected answer:

```text
In Data Engineering, not_null on a timestamp column is the basis of robustness. If your pickup_datetime is null, your partitioning and time-based analyses collapse. It's a unit test for your data.
```

<p align="center">
  <img src="https://img.shields.io/badge/Answer-not_null: true-darkgreen" alt="Answer Q5">
</p>

---

### Question 6. Lineage and Dependencies

After building your pipeline, you want to visualize the dependency graph between assets. Which Bruin command should you use?

- `bruin graph`
- `bruin dependencies`
- `bruin lineage`
- `bruin show`

Selected answer:

```text
Justification: The lineage command generates the visual or textual representation of parent-child relationships between your tables and scripts. It's the debugging tool par excellence for understanding why a piece of data is erroneous by tracing back to the source.
```

<p align="center">
  <img src="https://img.shields.io/badge/Answer-bruin lineage-darkgreen" alt="Answer Q6">
</p>

---

### Question 7. First-Time Run

You're running a Bruin pipeline for the first time on a new DuckDB database. What flag should you use to ensure tables are created from scratch?

- `--create`
- `--init`
- `--full-refresh`
- `--truncate`

Selected answer:

```text
Justification: Cost/complexity analysis: The --full-refresh forces Bruin to ignore incremental states and recreate the tables. It's essential during the first run or if you modify the schema of an existing table (Data Definition Language - DDL).
```

<p align="center">
  <img src="https://img.shields.io/badge/Answer-fullrefresh-darkgreen" alt="Answer Q7">
</p>