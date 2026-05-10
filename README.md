# Medallion NYC Taxi Orchestrated

## Project Summary

This project extends the original **NYC Yellow Taxi medallion architecture** solution into an **orchestrated data pipeline**.

The implementation is based on the **NYC Yellow Taxi dataset**, stored as monthly Parquet files and processed with **DuckDB + SQL**, controlled by **Python**, and orchestrated with **Dagster**.

The goal of this version was to move from a medallion-only data solution to a more complete pipeline approach with:

- a **single entry point**
- **full** and **incremental** execution modes
- **pipeline orchestration**
- **basic idempotent behavior**
- a clearer and more maintainable project structure

---

## Technology Stack

The project uses the following tools:

- **Python** – pipeline control and script execution
- **DuckDB** – analytical processing engine
- **SQL** – transformations between medallion layers
- **Dagster** – pipeline orchestration
- **Parquet** – raw source data format
- **Git / GitHub** – version control and project visibility

---

## Why DuckDB Was Chosen

The assignment allowed the use of **Spark or another Big Data processing engine**.

For this project, **DuckDB** was selected because:

- it works very well with large Parquet datasets
- it allows fast analytical SQL processing
- it is easy to run locally
- it integrates cleanly with a medallion-style architecture
- it fits the existing NYC Taxi project without rewriting the whole solution from scratch

This choice made it possible to focus on **pipeline design, orchestration, execution modes, and idempotency**, which were the main goals of this extension.

---

## Project Architecture

The project follows a **medallion architecture** with three layers.

### Bronze Layer
The Bronze layer stores raw ingested records loaded from NYC Yellow Taxi Parquet files.

Purpose:
- keep original source data
- preserve raw structure
- separate ingestion from cleaning logic

### Silver Layer
The Silver layer contains cleaned and standardized records.

Purpose:
- correct types and formats
- filter invalid or suspicious values
- prepare the dataset for analysis

### Gold Layer
The Gold layer contains aggregated, business-ready analytical outputs.

Purpose:
- prepare reporting-ready tables
- perform aggregations
- expose curated results for analysis

---

## Logical Pipeline Flow

The pipeline works as follows:

```text
NYC Taxi source files (Parquet + lookup CSV)
        |
        v
Download / Ingestion
        |
        v
Bronze Layer (raw data)
        |
        v
Silver Layer (cleaned data)
        |
        v
Gold Layer (aggregated data)
        |
        v
Data quality checks / analytics
```

In addition to that, orchestration is handled by Dagster:

```text
Dagster
   |
   +--> full_pipeline_job
   |
   +--> incremental_pipeline_job
```

Both jobs use the same central execution point:

```text
scripts/main.py
```

---

## How the Pipeline Was Implemented

### Step 1 – Source Data Download
The project uses:

- `scripts/download_data.py`

This script downloads monthly NYC Yellow Taxi Parquet files and lookup data.  
If a file already exists locally, it is skipped instead of being downloaded again.

### Step 2 – Full Pipeline Execution
The full pipeline mode is triggered through:

```bash
python scripts/main.py --mode full
```

Internally, this starts:

- `scripts/run_full.py`

Which then executes:
- data download
- Bronze SQL script
- Silver SQL script
- Gold SQL script

### Step 3 – Incremental Pipeline Execution
The incremental pipeline mode is triggered through:

```bash
python scripts/main.py --mode incremental
```

Internally, this starts:

- `scripts/run_incremental.py`

Current behavior:
- checks current state
- skips already downloaded source files
- runs through a dedicated incremental execution path
- stores execution metadata in `pipeline_state.json`

This means incremental execution is already separated at the control level and prepared for further extension into stricter partition-based processing.

### Step 4 – SQL-Based Medallion Transformations
The transformations are executed using SQL files:

- `sql/01_bronze.sql`
- `sql/02_silver.sql`
- `sql/03_gold.sql`
- `sql/04_data_quality_checks.sql`

This keeps the pipeline modular and easy to understand.

### Step 5 – Orchestration with Dagster
Dagster was added as the orchestration layer.

The orchestration file:

- `orchestration/dagster_pipeline.py`

contains two jobs:

- `full_pipeline_job`
- `incremental_pipeline_job`

Dagster provides:
- a visual UI
- repeatable job execution
- clearer pipeline structure
- separation between orchestration and transformation logic

Dagster can be started with:

```bash
python -m dagster dev -f orchestration/dagster_pipeline.py
```

---

## Idempotency in Practice

Idempotency means the pipeline should behave safely when run more than once.

In this project, this was addressed by:

- not downloading files again if they already exist
- using a dedicated pipeline state file
- separating full and incremental runs
- keeping execution repeatable through a central entry point
- using orchestration jobs that can be rerun in a controlled way

---

## Data Quality Considerations

The project also includes data quality handling.

Typical data risks in NYC Taxi data include:

- null or missing values
- invalid timestamps
- negative or unrealistic distances
- incorrect fare values
- inconsistent passenger counts

These issues are handled through the Silver layer and additional data quality checks.

---

## How to Run the Project

### 1. Create and activate a virtual environment

```bash
python -m venv .venv
.venv\Scripts\activate
```

### 2. Install dependencies

```bash
python -m pip install -r requirements.txt
python -m pip install dagster dagster-webserver
```

### 3. Run the full pipeline

```bash
python scripts/main.py --mode full
```

### 4. Run the incremental pipeline

```bash
python scripts/main.py --mode incremental
```

### 5. Run Dagster UI

```bash
python -m dagster dev -f orchestration/dagster_pipeline.py
```

---

# Data Product Consumption Package

This repository version also contains a ready data product package for the class assignment **Przygotowanie Produktu Danych do konsumpcji**.

## Data Product

**Name:** NYC Taxi Monthly Borough Statistics  
**Gold table:** `gold_monthly_borough_stats`  
**Contract:** `data_product_contract.yaml`  
**Product Card:** `docs/NYC_Taxi_Data_Product_Card.docx` and `docs/NYC_Taxi_Data_Product_Card.pdf`

## Run everything

```bash
python scripts/main.py --mode full
python scripts/generate_data_product_metrics.py
python scripts/export_gold_dataset.py
```

## Quality metrics

The package defines and calculates 5 Data Quality metrics:

| Metric | Definition | Threshold | Cadence |
|---|---|---:|---|
| gold_row_count | Number of Gold table rows | > 0 | Every pipeline run |
| gold_key_completeness | Non-null `trip_month`, `pickup_borough`, `dropoff_borough` | >= 95% | Every pipeline run |
| positive_trip_count_validity | Rows where `trip_count > 0` | 100% | Every pipeline run |
| non_negative_amount_validity | Rows where amount/revenue are non-negative | >= 99% | Every pipeline run |
| data_freshness_months | Months since latest `trip_month` | <= 6 months if current files are downloaded | Every pipeline run |

The calculated output is stored in:

```text
quality/data_quality_metrics.csv
```

## Data Product Contract

The contract is stored in:

```text
data_product_contract.yaml
```

It describes:

- product name and owner
- purpose and business problem
- data sources
- schema and column descriptions
- refresh frequency
- access method
- quality metrics
- known limitations
- SQL and Python usage examples


```powershell
$env:NYC_TAXI_START_YEAR="2018"
$env:NYC_TAXI_START_MONTH="1"
$env:NYC_TAXI_END_YEAR="2018"
$env:NYC_TAXI_END_MONTH="7"
python scripts/download_data.py
```

After download, run:

```powershell
python scripts/main.py --mode full
python scripts/export_gold_dataset.py
```


## Last successful local run - metrics for submission

The latest provided run finished successfully and produced the following Data Product Quality metrics:

| Metric | Current value | Threshold | Status |
|---|---:|---:|---|
| Gold row count | 3312 | > 0 | PASS |
| Gold key completeness | 100.0% | >= 95% | PASS |
| Positive trip count validity | 100.0% | 100% | PASS |
| Non-negative amount validity | 100.0% | >= 99% | PASS |
| Data freshness months | 74 | <= 6 months if current source files are downloaded | WARN |

Freshness is WARN because the downloaded TLC source files are historical. This is expected for historical analysis and is documented in the Data Product Card and data_product_contract.yaml.
