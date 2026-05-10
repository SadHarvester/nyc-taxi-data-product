from __future__ import annotations

from pathlib import Path

import duckdb

DB_PATH = "warehouse.duckdb"
RAW_YELLOW_PATTERN = "data/raw/yellow/yellow_tripdata_*.parquet"
LOOKUP_PATH = Path("data/raw/lookups/taxi_zone_lookup.csv")
SQL_FILES = [
    "sql/02_silver.sql",
    "sql/03_gold.sql",
]

# Canonical schema used by the Silver layer. Older TLC files may not contain
# every optional column, so the Bronze loader creates missing fields as NULL.
EXPECTED_YELLOW_COLUMNS = [
    ("VendorID", "BIGINT", ["VendorID", "vendorid", "vendor_id"]),
    ("tpep_pickup_datetime", "TIMESTAMP", ["tpep_pickup_datetime"]),
    ("tpep_dropoff_datetime", "TIMESTAMP", ["tpep_dropoff_datetime"]),
    ("passenger_count", "DOUBLE", ["passenger_count"]),
    ("trip_distance", "DOUBLE", ["trip_distance"]),
    ("RatecodeID", "DOUBLE", ["RatecodeID", "ratecodeid", "rate_code_id"]),
    ("store_and_fwd_flag", "VARCHAR", ["store_and_fwd_flag"]),
    ("PULocationID", "BIGINT", ["PULocationID", "pulocationid", "pickup_location_id"]),
    ("DOLocationID", "BIGINT", ["DOLocationID", "dolocationid", "dropoff_location_id"]),
    ("payment_type", "BIGINT", ["payment_type"]),
    ("fare_amount", "DOUBLE", ["fare_amount"]),
    ("extra", "DOUBLE", ["extra"]),
    ("mta_tax", "DOUBLE", ["mta_tax"]),
    ("tip_amount", "DOUBLE", ["tip_amount"]),
    ("tolls_amount", "DOUBLE", ["tolls_amount"]),
    ("improvement_surcharge", "DOUBLE", ["improvement_surcharge"]),
    ("total_amount", "DOUBLE", ["total_amount"]),
    ("congestion_surcharge", "DOUBLE", ["congestion_surcharge"]),
    ("airport_fee", "DOUBLE", ["airport_fee", "Airport_fee"]),
]


def quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def build_safe_select_list(available_columns: list[str]) -> str:
    available_by_lower = {column.lower(): column for column in available_columns}
    expressions: list[str] = []

    for canonical_name, data_type, aliases in EXPECTED_YELLOW_COLUMNS:
        source_column = None
        for alias in aliases:
            source_column = available_by_lower.get(alias.lower())
            if source_column:
                break

        if source_column:
            expression = (
                f"TRY_CAST({quote_identifier(source_column)} AS {data_type}) "
                f"AS {quote_identifier(canonical_name)}"
            )
        else:
            expression = f"CAST(NULL AS {data_type}) AS {quote_identifier(canonical_name)}"

        expressions.append(expression)

    return ",\n    ".join(expressions)


def create_bronze_layer(con: duckdb.DuckDBPyConnection) -> None:
    raw_files = sorted(Path("data/raw/yellow").glob("yellow_tripdata_*.parquet"))
    if not raw_files:
        raise FileNotFoundError(
            "No raw Yellow Taxi parquet files found in data/raw/yellow. "
            "Run `python scripts/download_data.py` first."
        )

    if not LOOKUP_PATH.exists():
        raise FileNotFoundError(
            "Taxi zone lookup file not found at data/raw/lookups/taxi_zone_lookup.csv. "
            "Run `python scripts/download_data.py` first."
        )

    print(f"Creating bronze_yellow_trips from {len(raw_files)} parquet files...")
    con.execute(
        f"""
        CREATE OR REPLACE VIEW raw_yellow_source AS
        SELECT *
        FROM read_parquet('{RAW_YELLOW_PATTERN}', union_by_name = true);
        """
    )

    available_columns = [row[0] for row in con.execute("DESCRIBE raw_yellow_source").fetchall()]
    select_list = build_safe_select_list(available_columns)

    con.execute(
        f"""
        CREATE OR REPLACE TABLE bronze_yellow_trips AS
        SELECT
            {select_list}
        FROM raw_yellow_source;
        """
    )

    print("Creating bronze_taxi_zones from lookup CSV...")
    con.execute(
        """
        CREATE OR REPLACE TABLE bronze_taxi_zones AS
        SELECT *
        FROM read_csv_auto('data/raw/lookups/taxi_zone_lookup.csv');
        """
    )


def main() -> None:
    con = duckdb.connect(DB_PATH)

    try:
        print("Running dynamic Bronze loader...")
        create_bronze_layer(con)

        for sql_file in SQL_FILES:
            print(f"Running {sql_file}...")
            sql = Path(sql_file).read_text(encoding="utf-8-sig")
            con.execute(sql)

        print("Pipeline finished.")
    finally:
        con.close()


if __name__ == "__main__":
    main()
