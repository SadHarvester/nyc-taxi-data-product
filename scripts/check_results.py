import duckdb

con = duckdb.connect("warehouse.duckdb")

print("=== TABLES ===")
print(con.sql("SHOW TABLES").df())

print()
print("=== COUNTS ===")
print(con.sql("""
SELECT 'bronze_yellow_trips' AS table_name, COUNT(*) AS row_count FROM bronze_yellow_trips
UNION ALL
SELECT 'bronze_taxi_zones', COUNT(*) FROM bronze_taxi_zones
UNION ALL
SELECT 'silver_yellow_trips', COUNT(*) FROM silver_yellow_trips
UNION ALL
SELECT 'gold_monthly_borough_stats', COUNT(*) FROM gold_monthly_borough_stats
""").df())

print()
print("=== GOLD SAMPLE ===")
print(con.sql("""
SELECT *
FROM gold_monthly_borough_stats
ORDER BY trip_month, trip_count DESC
LIMIT 20
""").df())

con.close()
