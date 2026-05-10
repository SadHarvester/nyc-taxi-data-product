import json
import subprocess
import sys
from pathlib import Path


STATE_FILE = Path("state/pipeline_state.json")


def load_state():
    if not STATE_FILE.exists():
        return {}
    with open(STATE_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def save_state(state):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


def main():
    state = load_state()

    print("STEP 1: Downloading only new data if available...")
    subprocess.run([sys.executable, "scripts/download_data.py"], check=True)

    print("STEP 2: Running incremental pipeline...")
    print("Current version: re-running medallion pipeline with state tracking.")

    subprocess.run([sys.executable, "scripts/run_pipeline.py"], check=True)

    print("STEP 3: Calculating data product quality metrics...")
    subprocess.run([sys.executable, "scripts/generate_data_product_metrics.py"], check=True)

    state["last_run_mode"] = "incremental"
    save_state(state)

    print("INCREMENTAL pipeline finished successfully.")


if __name__ == "__main__":
    main()