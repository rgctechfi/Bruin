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
TBD
```

<p align="center">
  <img src="https://img.shields.io/badge/Answer-TBD-lightgrey" alt="Answer Q1">
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
TBD
```

<p align="center">
  <img src="https://img.shields.io/badge/Answer-TBD-lightgrey" alt="Answer Q2">
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
TBD
```

<p align="center">
  <img src="https://img.shields.io/badge/Answer-TBD-lightgrey" alt="Answer Q3">
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
TBD
```

<p align="center">
  <img src="https://img.shields.io/badge/Answer-TBD-lightgrey" alt="Answer Q4">
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
TBD
```

<p align="center">
  <img src="https://img.shields.io/badge/Answer-TBD-lightgrey" alt="Answer Q5">
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
TBD
```

<p align="center">
  <img src="https://img.shields.io/badge/Answer-TBD-lightgrey" alt="Answer Q6">
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
TBD
```

<p align="center">
  <img src="https://img.shields.io/badge/Answer-TBD-lightgrey" alt="Answer Q7">
</p>

---

## Submitting the solutions

- Form for submitting: <https://courses.datatalks.club/de-zoomcamp-2026/homework/hw5>
