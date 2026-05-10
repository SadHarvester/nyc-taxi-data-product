from __future__ import annotations

import argparse
import hashlib
import json
import time
from pathlib import Path
from typing import Any

import pandas as pd
from kafka import KafkaProducer

from scripts.download_data import download_yellow_tripdata
from streaming.config import (
    DEFAULT_MAX_MESSAGES,
    DEFAULT_NYC_TAXI_MONTH,
    DEFAULT_SLEEP_SECONDS,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    RAW_YELLOW_DIR,
)
from streaming.kafka_utils import ensure_topic

REQUIRED_COLUMNS = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "RatecodeID",
    "store_and_fwd_flag",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
]


def _json_default(value: Any) -> str:
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return str(value)


def build_message_id(record: dict[str, Any], row_number: int, source_file: str) -> str:
    natural_key = "|".join(
        [
            source_file,
            str(row_number),
            str(record.get("VendorID")),
            str(record.get("tpep_pickup_datetime")),
            str(record.get("tpep_dropoff_datetime")),
            str(record.get("PULocationID")),
            str(record.get("DOLocationID")),
        ]
    )
    return hashlib.sha256(natural_key.encode("utf-8")).hexdigest()


def prepare_payload(record: dict[str, Any], row_number: int, source_file: str) -> dict[str, Any]:
    payload = {}
    for column in REQUIRED_COLUMNS:
        value = record.get(column)
        if pd.isna(value):
            payload[column] = None
        elif isinstance(value, pd.Timestamp):
            payload[column] = value.isoformat()
        else:
            payload[column] = value

    payload["message_id"] = build_message_id(payload, row_number, source_file)
    payload["source_file"] = source_file
    payload["source_row_number"] = row_number
    payload["published_at"] = pd.Timestamp.utcnow().isoformat()
    return payload


def resolve_source_file(month: str, source_file: str | None) -> Path:
    if source_file:
        path = Path(source_file)
        if not path.exists():
            raise FileNotFoundError(f"Source file does not exist: {path}")
        return path

    path = RAW_YELLOW_DIR / f"yellow_tripdata_{month}.parquet"
    if not path.exists():
        path = download_yellow_tripdata(month)
    return path


def publish_records(
    source_file: Path,
    topic: str,
    bootstrap_servers: str,
    max_messages: int,
    sleep_seconds: float,
) -> int:
    ensure_topic(topic, bootstrap_servers=bootstrap_servers)

    print(f"[producer] reading source parquet: {source_file}")
    df = pd.read_parquet(source_file, columns=REQUIRED_COLUMNS)
    if max_messages > 0:
        df = df.head(max_messages)

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        key_serializer=lambda value: value.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value, default=_json_default).encode("utf-8"),
        linger_ms=50,
    )

    produced = 0
    source_name = source_file.name
    for row_number, record in enumerate(df.to_dict(orient="records"), start=1):
        payload = prepare_payload(record, row_number=row_number, source_file=source_name)
        producer.send(topic, key=payload["message_id"], value=payload)
        produced += 1

        if produced % 1000 == 0:
            producer.flush()
            print(f"[producer] published {produced} records")

        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

    producer.flush()
    producer.close()
    print(f"[producer] finished. Published records: {produced}")
    return produced


def main() -> None:
    parser = argparse.ArgumentParser(description="Read NYC Taxi data and publish records to Kafka/Redpanda.")
    parser.add_argument("--month", default=DEFAULT_NYC_TAXI_MONTH)
    parser.add_argument("--source-file", default=None)
    parser.add_argument("--topic", default=KAFKA_TOPIC)
    parser.add_argument("--bootstrap-servers", default=KAFKA_BOOTSTRAP_SERVERS)
    parser.add_argument("--max-messages", type=int, default=DEFAULT_MAX_MESSAGES)
    parser.add_argument("--sleep-seconds", type=float, default=DEFAULT_SLEEP_SECONDS)
    args = parser.parse_args()

    source_file = resolve_source_file(args.month, args.source_file)
    publish_records(
        source_file=source_file,
        topic=args.topic,
        bootstrap_servers=args.bootstrap_servers,
        max_messages=args.max_messages,
        sleep_seconds=args.sleep_seconds,
    )


if __name__ == "__main__":
    main()
