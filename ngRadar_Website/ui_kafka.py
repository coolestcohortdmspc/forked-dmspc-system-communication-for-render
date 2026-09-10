import json
import logging
import os
import threading
from ngRadar_Website.enums import Message
from confluent_kafka import Consumer, KafkaError

from .sse import sse_broker


logger = logging.getLogger(__name__)

_consumer_started = False
_consumer_lock = threading.Lock()


def consume_ui_events():
    topics = os.getenv(
        "UI_KAFKA_TOPICS",
        "GBT_notif,VLBA_notif,DSOC_notif",
    ).split(",")

    topics = [
        topic.strip()
        for topic in topics
        if topic.strip()
    ]

    consumer_config = {
        "bootstrap.servers": os.getenv(
            "BOOTSTRAP_SERVER",
            "kafka-broker:29092",
        ),
        "group.id": os.getenv(
            "UI_KAFKA_GROUP_ID",
            "ngradar-ui-sse",
        ),
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    }

    consumer = Consumer(consumer_config)

    consumer.subscribe(topics)

    logger.info(
        "UI Kafka consumer subscribed to %s",
        topics,
    )

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if (
                    msg.error().code()
                    == KafkaError._PARTITION_EOF
                ):
                    continue

                logger.error(
                    "UI Kafka consumer error: %s",
                    msg.error(),
                )
                continue

            incoming_key = int(
                msg.key().decode("utf-8")
            )

            if incoming_key != Message.DB_COMMITTED.value:
                continue

            try:
                payload = json.loads(
                    msg.value().decode("utf-8")
                )
            except (
                UnicodeDecodeError,
                json.JSONDecodeError,
            ):
                logger.exception(
                    "Invalid DB committed message"
                )
                continue

            event_type = payload.get(
                "ui_event_type"
            )

            if not event_type:
                logger.warning(
                    "DB committed event missing ui_event_type"
                )
                continue

            sse_broker.publish(
                {
                    "type": event_type,
                    "data": payload.get(
                        "data",
                        {},
                    ),
                }
            )

    finally:
        consumer.close()


def start_ui_kafka_consumer():
    global _consumer_started

    with _consumer_lock:
        if _consumer_started:
            return

        _consumer_started = True

    thread = threading.Thread(
        target=consume_ui_events,
        name="ui-kafka-consumer",
        daemon=True,
    )

    thread.start()

    logger.info(
        "UI Kafka consumer thread started"
    )