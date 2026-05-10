import subprocess
import sys


def main():
    print("STEP 1: Downloading data...")
    subprocess.run([sys.executable, "scripts/download_data.py"], check=True)

    print("STEP 2: Running full medallion pipeline...")
    subprocess.run([sys.executable, "scripts/run_pipeline.py"], check=True)

    print("STEP 3: Calculating data product quality metrics...")
    subprocess.run([sys.executable, "scripts/generate_data_product_metrics.py"], check=True)

    print("FULL pipeline finished successfully.")


if __name__ == "__main__":
    main()