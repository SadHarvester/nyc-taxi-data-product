import argparse
import subprocess
import sys


def run_script(script_path: str):
    result = subprocess.run([sys.executable, script_path], check=True)
    return result.returncode


def main():
    parser = argparse.ArgumentParser(description="NYC Taxi Medallion Pipeline")
    parser.add_argument(
        "--mode",
        choices=["full", "incremental"],
        required=True,
        help="Pipeline execution mode"
    )

    args = parser.parse_args()

    if args.mode == "full":
        print("Running FULL pipeline...")
        run_script("scripts/run_full.py")
    elif args.mode == "incremental":
        print("Running INCREMENTAL pipeline...")
        run_script("scripts/run_incremental.py")


if __name__ == "__main__":
    main()