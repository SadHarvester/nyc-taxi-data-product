CREATE OR REPLACE TABLE silver_yellow_trips AS
SELECT
    VendorID,
    CAST(tpep_pickup_datetime AS TIMESTAMP) AS pickup_ts,
    CAST(tpep_dropoff_datetime AS TIMESTAMP) AS dropoff_ts,
    passenger_count,
    trip_distance,
    RatecodeID,
    store_and_fwd_flag,
    PULocationID,
    DOLocationID,
    payment_type,
    COALESCE(fare_amount, 0.0) AS fare_amount,
    COALESCE(extra, 0.0) AS extra,
    COALESCE(mta_tax, 0.0) AS mta_tax,
    COALESCE(tip_amount, 0.0) AS tip_amount,
    COALESCE(tolls_amount, 0.0) AS tolls_amount,
    COALESCE(improvement_surcharge, 0.0) AS improvement_surcharge,
    COALESCE(total_amount, 0.0) AS total_amount,
    COALESCE(congestion_surcharge, 0.0) AS congestion_surcharge,
    COALESCE(airport_fee, 0.0) AS airport_fee
FROM bronze_yellow_trips
WHERE tpep_pickup_datetime IS NOT NULL
  AND tpep_dropoff_datetime IS NOT NULL
  AND tpep_dropoff_datetime >= tpep_pickup_datetime
  AND trip_distance > 0
  AND total_amount >= 0
  AND PULocationID IS NOT NULL
  AND DOLocationID IS NOT NULL
  AND tpep_pickup_datetime >= TIMESTAMP '2014-01-01 00:00:00'
  AND tpep_pickup_datetime <  TIMESTAMP '2026-01-01 00:00:00'
  AND tpep_dropoff_datetime >= TIMESTAMP '2014-01-01 00:00:00'
  AND tpep_dropoff_datetime <  TIMESTAMP '2026-01-01 00:00:00';
