from dagster import Definitions, job, op
import subprocess
import sys


@op
def run_full_pipeline():
    print("Running FULL pipeline via main entry point...")
    subprocess.run(
        [sys.executable, "scripts/main.py", "--mode", "full"],
        check=True
    )


@op
def run_incremental_pipeline():
    print("Running INCREMENTAL pipeline via main entry point...")
    subprocess.run(
        [sys.executable, "scripts/main.py", "--mode", "incremental"],
        check=True
    )


@job
def full_pipeline_job():
    run_full_pipeline()


@job
def incremental_pipeline_job():
    run_incremental_pipeline()


defs = Definitions(
    jobs=[full_pipeline_job, incremental_pipeline_job]
)