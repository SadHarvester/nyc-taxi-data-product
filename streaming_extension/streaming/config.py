from __future__ import annotations

import os
from pathlib import Path

try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    # dotenv is optional at runtime; environment variables still work without it.
    pass


PROJECT_ROOT = Path(__file__).resolve().parents[1]

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "nyc_taxi_raw")
KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "nyc_taxi_bronze_loader")

DUCKDB_PATH = os.getenv("DUCKDB_PATH", str(PROJECT_ROOT / "warehouse.duckdb"))

NYC_TRIP_DATA_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
NYC_TAXI_ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
DEFAULT_NYC_TAXI_MONTH = os.getenv("NYC_TAXI_MONTH", "2024-01")

DEFAULT_MAX_MESSAGES = int(os.getenv("STREAMING_MAX_MESSAGES", "5000"))
DEFAULT_SLEEP_SECONDS = float(os.getenv("STREAMING_SLEEP_SECONDS", "0.0"))

RAW_YELLOW_DIR = PROJECT_ROOT / "data" / "raw" / "yellow"
RAW_LOOKUPS_DIR = PROJECT_ROOT / "data" / "raw" / "lookups"
