CREATE OR REPLACE TABLE gold_monthly_borough_stats AS
SELECT
    DATE_TRUNC('month', t.pickup_ts) AS trip_month,
    pu.Borough AS pickup_borough,
    doo.Borough AS dropoff_borough,
    COUNT(*) AS trip_count,
    ROUND(AVG(t.trip_distance), 2) AS avg_trip_distance,
    ROUND(AVG(t.total_amount), 2) AS avg_total_amount,
    ROUND(SUM(t.total_amount), 2) AS total_revenue
FROM silver_yellow_trips t
LEFT JOIN bronze_taxi_zones pu
    ON t.PULocationID = pu.LocationID
LEFT JOIN bronze_taxi_zones doo
    ON t.DOLocationID = doo.LocationID
GROUP BY 1, 2, 3;
