from __future__ import annotations

import time
from typing import Iterable

from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable

from streaming.config import KAFKA_BOOTSTRAP_SERVERS


def wait_for_kafka(
    bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
    timeout_seconds: int = 60,
) -> None:
    """Block until the Kafka-compatible broker is reachable."""
    deadline = time.time() + timeout_seconds
    last_error: Exception | None = None

    while time.time() < deadline:
        try:
            client = KafkaAdminClient(
                bootstrap_servers=bootstrap_servers,
                client_id="nyc-taxi-waiter",
                request_timeout_ms=5000,
            )
            client.close()
            return
        except Exception as exc:  # Kafka startup can fail in several transient ways.
            last_error = exc
            time.sleep(2)

    raise RuntimeError(
        f"Kafka/Redpanda is not reachable at {bootstrap_servers}. "
        "Start it with: docker compose up -d"
    ) from last_error


def ensure_topic(
    topic_name: str,
    bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
    num_partitions: int = 1,
    replication_factor: int = 1,
) -> None:
    """Create a topic if it does not exist yet."""
    wait_for_kafka(bootstrap_servers=bootstrap_servers)

    admin = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
        client_id="nyc-taxi-topic-admin",
    )
    existing_topics = set(admin.list_topics())
    if topic_name not in existing_topics:
        topic = NewTopic(
            name=topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
        )
        try:
            admin.create_topics([topic], validate_only=False)
            print(f"[topic] created: {topic_name}")
        except TopicAlreadyExistsError:
            print(f"[topic] already exists: {topic_name}")
    else:
        print(f"[topic] already exists: {topic_name}")
    admin.close()
