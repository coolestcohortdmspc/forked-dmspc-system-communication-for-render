import json
from ngRadar_Website.enums import Message
from ngRadar_Website.models.models import ObservatoryEvent
from ngRadar_Website.utils import (
    consume,
    produce,
)
from django.db import transaction

TOPIC_TO_UI_EVENT = {
    "GBT_notif": "gbt_changed",
    "VLBA_notif": "vlba_changed",
    "DSOC_notif": "dsoc_changed",
}

def process_msg(
    msg,
    producer_topic,
    producer_config,
):
    try:
        incoming_key = int(
            msg.key().decode("utf-8")
        )

        if incoming_key == Message.DB_COMMITTED.value:
            return True

        topic = msg.topic()

        payload = json.loads(
            msg.value().decode("utf-8")
        )

        ui_event_type = TOPIC_TO_UI_EVENT.get(
            topic
        )

        if ui_event_type is None:
            return True

        with transaction.atomic():
            record_obs_event(payload)

            transaction.on_commit(
                lambda: publish_db_committed(
                    topic=topic,
                    producer_config=producer_config,
                    payload=payload,
                    ui_event_type=ui_event_type,
                )
            )

        return True

    except json.JSONDecodeError as error:
        print(
            f"DB consumer received invalid JSON: "
            f"{error}"
        )
        return False

    except (KeyError, TypeError, ValueError) as error:
        print(
            f"DB consumer received invalid payload: "
            f"{error}"
        )
        return False

    except Exception as error:
        print(
            f"DB consumer failed to process message: "
            f"{error}"
        )
        return False


def record_obs_event(payload):
    obs_event, created = (
        ObservatoryEvent.objects.update_or_create(
            uuid=payload["event_uuid"],
            defaults={
                "gbt_uuid": payload.get(
                    "gbt_uuid"
                ),
                "transfer_uuid": payload.get(
                    "transfer_uuid"
                ),
                "object_id": payload.get(
                    "object_id"
                ),
                "target": payload.get(
                    "target"
                ),
                "tx_waveform": payload.get(
                    "tx_waveform"
                ),
                "rec_waveform": payload.get(
                    "rec_waveform"
                ),
                "product_type": payload.get(
                    "product_type"
                ),
                "product_id": payload.get(
                    "product_id"
                ),
                "station": payload.get(
                    "station"
                ),
                "event_time": payload[
                    "event_time"
                ],
                "xmit_station": payload.get(
                    "xmit_station"
                ),
                "rcvr_station": payload.get(
                    "rcvr_station"
                ),
                "image_key": payload.get(
                    "image_key"
                ),
                "num_bytes": payload.get(
                    "num_bytes"
                ),
                "latency_ms": payload.get(
                    "latency_ms",
                    0.0,
                ),
                "status": payload.get(
                    "status"
                ),
                "message": payload.get(
                    "message",
                    "",
                ),
            },
        )
    )

    return obs_event, created


def publish_db_committed(
    *,
    topic,
    producer_config,
    payload,
    ui_event_type,
):
    notification = {
        "event_type": "db_committed",
        "ui_event_type": ui_event_type,
        "data": payload,
    }

    produce(
        topic,
        producer_config,
        str(Message.DB_COMMITTED.value),
        json.dumps(notification),
    )