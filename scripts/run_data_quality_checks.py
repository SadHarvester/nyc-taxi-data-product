import duckdb

con = duckdb.connect("warehouse.duckdb")

checks = {
    "invalid_time_order": """
        SELECT COUNT(*)
        FROM bronze_yellow_trips
        WHERE tpep_dropoff_datetime < tpep_pickup_datetime
    """,
    "invalid_distance": """
        SELECT COUNT(*)
        FROM bronze_yellow_trips
        WHERE trip_distance <= 0 OR trip_distance IS NULL
    """,
    "missing_location_ids": """
        SELECT COUNT(*)
        FROM bronze_yellow_trips
        WHERE PULocationID IS NULL OR DOLocationID IS NULL
    """
}

print("=== DATA QUALITY CHECKS ===")
for name, query in checks.items():
    value = con.sql(query).fetchone()[0]
    print(f"{name}: {value}")

con.close()
