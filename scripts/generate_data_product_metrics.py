from pathlib import Path
import duckdb

DB_PATH = Path("warehouse.duckdb")
SQL_PATH = Path("sql/05_data_product_quality_metrics.sql")
OUT_DIR = Path("quality")
OUT_CSV = OUT_DIR / "data_quality_metrics.csv"


def main():
    if not DB_PATH.exists():
        raise FileNotFoundError(
            "warehouse.duckdb was not found. Run `python scripts/main.py --mode full` first."
        )

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))

    sql = SQL_PATH.read_text(encoding="utf-8")
    con.execute(sql)

    df = con.sql("SELECT * FROM data_product_quality_metrics ORDER BY metric_name").df()
    df.to_csv(OUT_CSV, index=False)

    print("=== DATA PRODUCT QUALITY METRICS ===")
    print(df.to_string(index=False))
    print(f"\nSaved metrics to: {OUT_CSV}")

    con.close()


if __name__ == "__main__":
    main()
