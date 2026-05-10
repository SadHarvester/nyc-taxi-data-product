-- 1. Records where dropoff is earlier than pickup
SELECT COUNT(*) AS invalid_time_order
FROM bronze_yellow_trips
WHERE tpep_dropoff_datetime < tpep_pickup_datetime;

-- 2. Records with invalid distance
SELECT COUNT(*) AS invalid_distance
FROM bronze_yellow_trips
WHERE trip_distance <= 0 OR trip_distance IS NULL;

-- 3. Records missing pickup/dropoff locations
SELECT COUNT(*) AS missing_location_ids
FROM bronze_yellow_trips
WHERE PULocationID IS NULL OR DOLocationID IS NULL;
