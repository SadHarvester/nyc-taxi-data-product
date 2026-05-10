from pathlib import Path
import duckdb

DB_PATH = Path("warehouse.duckdb")
EXPORT_DIR = Path("export")


def main():
    if not DB_PATH.exists():
        raise FileNotFoundError(
            "warehouse.duckdb was not found. Run `python scripts/main.py --mode full` first."
        )

    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))

    con.execute("""
        COPY gold_monthly_borough_stats
        TO 'export/gold_monthly_borough_stats.csv'
        (HEADER, DELIMITER ',');
    """)

    con.execute("""
        COPY gold_monthly_borough_stats
        TO 'export/gold_monthly_borough_stats.parquet'
        (FORMAT PARQUET);
    """)

    print("Exported:")
    print("- export/gold_monthly_borough_stats.csv")
    print("- export/gold_monthly_borough_stats.parquet")
    con.close()


if __name__ == "__main__":
    main()
