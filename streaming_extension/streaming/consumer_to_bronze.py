from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
from kafka import KafkaConsumer

from streaming.config import (
    DEFAULT_MAX_MESSAGES,
    DUCKDB_PATH,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_CONSUMER_GROUP,
    KAFKA_TOPIC,
    PROJECT_ROOT,
)
from streaming.kafka_utils import ensure_topic

BRONZE_COLUMNS = [
    "message_id",
    "source_file",
    "source_row_number",
    "published_at",
    "ingested_at",
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

TIMESTAMP_COLUMNS = ["published_at", "ingested_at", "tpep_pickup_datetime", "tpep_dropoff_datetime"]


def initialize_bronze_table(con: duckdb.DuckDBPyConnection) -> None:
    sql_path = PROJECT_ROOT / "sql" / "00_create_streaming_bronze.sql"
    con.execute(sql_path.read_text(encoding="utf-8"))


def normalize_payload(payload: dict[str, Any]) -> dict[str, Any]:
    payload = dict(payload)
    payload["ingested_at"] = pd.Timestamp.utcnow().isoformat()

    row = {column: payload.get(column) for column in BRONZE_COLUMNS}
    for column in TIMESTAMP_COLUMNS:
        if row.get(column):
            row[column] = pd.to_datetime(row[column], errors="coerce")
        else:
            row[column] = pd.NaT
    return row


def insert_batch(con: duckdb.DuckDBPyConnection, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0

    df = pd.DataFrame(rows, columns=BRONZE_COLUMNS)
    con.register("incoming_bronze_batch", df)
    column_list = ", ".join(BRONZE_COLUMNS)
    con.execute(
        f"""
        INSERT INTO bronze_yellow_trips_streaming ({column_list})
        SELECT {column_list}
        FROM incoming_bronze_batch incoming
        WHERE NOT EXISTS (
            SELECT 1
            FROM bronze_yellow_trips_streaming existing
            WHERE existing.message_id = incoming.message_id
        )
        """
    )
    con.unregister("incoming_bronze_batch")
    return len(rows)


def consume_to_bronze(
    topic: str,
    bootstrap_servers: str,
    group_id: str,
    db_path: str,
    max_messages: int,
    batch_size: int,
    idle_timeout_seconds: int,
    reset_offsets: bool,
) -> int:
    ensure_topic(topic, bootstrap_servers=bootstrap_servers)

    con = duckdb.connect(db_path)
    initialize_bronze_table(con)

    auto_offset_reset = "earliest" if reset_offsets else "latest"
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=True,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        consumer_timeout_ms=1000,
    )

    print(
        f"[consumer] consuming topic={topic}, group_id={group_id}, "
        f"offset_reset={auto_offset_reset}, db={db_path}"
    )

    consumed = 0
    buffered_rows: list[dict[str, Any]] = []
    last_message_at = time.time()

    try:
        while True:
            for message in consumer:
                buffered_rows.append(normalize_payload(message.value))
                consumed += 1
                last_message_at = time.time()

                if len(buffered_rows) >= batch_size:
                    insert_batch(con, buffered_rows)
                    print(f"[consumer] inserted batch. Total consumed: {consumed}")
                    buffered_rows.clear()

                if max_messages > 0 and consumed >= max_messages:
                    break

            if max_messages > 0 and consumed >= max_messages:
                break

            if time.time() - last_message_at >= idle_timeout_seconds:
                print(f"[consumer] idle timeout after {idle_timeout_seconds}s")
                break

        if buffered_rows:
            insert_batch(con, buffered_rows)
            print(f"[consumer] inserted final batch. Total consumed: {consumed}")

    finally:
        consumer.close()
        con.close()

    print(f"[consumer] finished. Consumed messages: {consumed}")
    return consumed


def main() -> None:
    parser = argparse.ArgumentParser(description="Consume Kafka/Redpanda messages and append them to DuckDB Bronze.")
    parser.add_argument("--topic", default=KAFKA_TOPIC)
    parser.add_argument("--bootstrap-servers", default=KAFKA_BOOTSTRAP_SERVERS)
    parser.add_argument("--group-id", default=KAFKA_CONSUMER_GROUP)
    parser.add_argument("--db-path", default=DUCKDB_PATH)
    parser.add_argument("--max-messages", type=int, default=DEFAULT_MAX_MESSAGES)
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--idle-timeout-seconds", type=int, default=10)
    parser.add_argument(
        "--reset-offsets",
        action="store_true",
        help="Use earliest offset for this consumer group. Useful in local demos.",
    )
    args = parser.parse_args()

    consume_to_bronze(
        topic=args.topic,
        bootstrap_servers=args.bootstrap_servers,
        group_id=args.group_id,
        db_path=args.db_path,
        max_messages=args.max_messages,
        batch_size=args.batch_size,
        idle_timeout_seconds=args.idle_timeout_seconds,
        reset_offsets=args.reset_offsets,
    )


if __name__ == "__main__":
    main()
