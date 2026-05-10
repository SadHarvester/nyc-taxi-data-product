# Assignment Requirements Mapping

## Assignment Scope

This document explains how the project fulfills the requirements of the **Processing and Data Orchestration** assignment.

The assignment required:

1. **An orchestrated pipeline with a scalable processing engine**
2. **A runnable Spark or another Big Data processing engine script/notebook with a single entry point**
3. **A pipeline architecture diagram visible in Git**
4. **Idempotency and maintainable project structure**
5. **Meaningful naming of files, branches, commits, and variables**

This project was prepared as an extension of the original **NYC Yellow Taxi medallion architecture** assignment.

---

## What Was Already Available Before This Extension

The previous version of the project already included:

- ingestion of NYC Yellow Taxi source data
- a **Bronze / Raw** layer
- a **Silver / Cleaned** layer
- a **Gold / Curated** layer
- SQL transformations for each layer
- a local analytical database file (`warehouse.duckdb`)
- data quality checks
- a medallion-style repository structure

That version solved the earlier task focused on loading and transforming data through Bronze, Silver, and Gold.

---

## What Was Added in This Version

To satisfy the new assignment, the project was extended with the following elements.

### 1. Single Entry Point
A new script was added:

```bash
python scripts/main.py --mode full
python scripts/main.py --mode incremental
```

This means the pipeline can now be started from one central place, with a selected execution mode.

### 2. Two Execution Modes
Two separate operational modes were implemented:

- **Full mode** – runs the complete pipeline from source ingestion to Gold layer rebuild
- **Incremental mode** – runs through a dedicated incremental path, tracks execution state, skips already downloaded files, and is prepared for further partition-based optimization

Supporting scripts added:

- `scripts/main.py`
- `scripts/run_full.py`
- `scripts/run_incremental.py`

### 3. Orchestration Layer
A Dagster orchestration layer was added in:

- `orchestration/dagster_pipeline.py`

Two Dagster jobs are available:

- `full_pipeline_job`
- `incremental_pipeline_job`

This makes the project an **orchestrated pipeline**, not just a collection of manually run scripts.

### 4. Pipeline State Tracking
A state file was added:

- `state/pipeline_state.json`

This file is used to record pipeline execution metadata and support idempotent behavior.

### 5. Improved Repository Structure
The project structure was expanded so the repository is easier to understand and maintain.

---

## How Assignment Requirements Were Fulfilled

### Requirement 1 – Orchestrated Pipeline with a Processing Engine
Fulfilled by:
- using **DuckDB** as the analytical processing engine
- organizing execution through **Python scripts**
- orchestrating pipeline jobs with **Dagster**

### Requirement 2 – Runnable Script / Notebook with Single Entry Point
Fulfilled by:
- introducing `scripts/main.py`
- supporting:
  - `--mode full`
  - `--mode incremental`

### Requirement 3 – Pipeline Architecture Diagram
Fulfilled by:
- preparing a dedicated pipeline architecture diagram for the repository
- clearly separating ingestion, Bronze, Silver, Gold, orchestration, and execution control

### Requirement 4 – Idempotency
Fulfilled by:
- skipping already downloaded files
- separating execution modes
- storing execution metadata in `state/pipeline_state.json`
- using a repeatable script-based structure

### Requirement 5 – Maintainable Project Structure
Fulfilled by:
- introducing clearer folders for orchestration and state
- separating execution logic from SQL transformations
- using meaningful file names and explicit operational paths

---

## Final Outcome

As a result, the project now includes:

- a medallion architecture based on Bronze, Silver, and Gold layers
- a single entry point for execution
- full and incremental pipeline modes
- orchestration with Dagster
- state tracking for safer repeated execution
- a clearer and more maintainable repository structure

This means the solution goes beyond basic SQL execution and becomes a more complete, reusable, and easier-to-maintain data pipeline.
