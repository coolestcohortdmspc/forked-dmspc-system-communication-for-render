from datetime import datetime, timezone

import json
import time
import uuid

from django.core.management.base import BaseCommand

from ngRadar_Website.enums import Stations, Message
from ngRadar_Website.utils import (
    latency_calc,
    bootstrap,
    consume,
    produce,
)


def set_payload_dict(
    waveform,
    ui_event_time,
):
    return {
        "object_id": "30104",
        "target": "Moretus",
        "tx_waveform": waveform,
        "rec_waveform": waveform,
        "event_time": datetime.now(
            timezone.utc
        ),
        "latency_ms": latency_calc(
            ui_event_time,
            Stations.GBT,
        ),
    }


def turn_off_transmitter():
    print("GBT transmitter OFF")
    time.sleep(5)


def process_msg(
    msg,
    producer_topic,
    producer_config,
):
    incoming_key = int(
        msg.key().decode("utf-8")
    )

    if incoming_key != Message.UI_EVENT.value:
        return True

    payload = json.loads(
        msg.value().decode("utf-8")
    )

    waveform = payload["tx_waveform"]

    ui_event_time = datetime.fromisoformat(
        payload["event_time"]
    )

    print(
        f"GBT received waveform request: "
        f"{waveform}"
    )

    turn_off_transmitter()

    gbt_payload = set_payload_dict(
        waveform,
        ui_event_time,
    )

    # Correlates the whole observation sequence.
    gbt_uuid = uuid.uuid4()

    # Uniquely identifies this specific
    # ObservatoryEvent.
    event_uuid = uuid.uuid4()

    kafka_payload = {
        "event_uuid": str(event_uuid),

        "gbt_uuid": str(gbt_uuid),

        "transfer_uuid": None,

        "station": int(Stations.GBT),
        "station_name": Stations.GBT.label,

        "object_id": gbt_payload[
            "object_id"
        ],
        "target": gbt_payload[
            "target"
        ],

        "tx_waveform": gbt_payload[
            "tx_waveform"
        ],
        "rec_waveform": gbt_payload[
            "rec_waveform"
        ],

        "product_type": None,
        "product_id": None,

        "status": None,

        "xmit_station": int(
            Stations.GBT
        ),
        "rcvr_station": None,

        "image_key": None,
        "num_bytes": 0,

        "latency_ms": gbt_payload[
            "latency_ms"
        ],

        "message": (
            f"GBT transmitting waveform "
            f"{waveform}."
        ),

        "event_time": (
            gbt_payload["event_time"]
            .isoformat()
        ),
        "gbt_event_time": (
            gbt_payload["event_time"]
            .isoformat()
        ),
    }

    produce(
        producer_topic,
        producer_config,
        str(Message.GBT_TX.value),
        json.dumps(kafka_payload),
    )

    print(
        f"GBT published event "
        f"{event_uuid}"
    )

    return True


class Command(BaseCommand):
    help = "Runs the GBT simulator"

    def handle(
        self,
        *args,
        **options,
    ):
        print("Starting GBT simulator")

        (
            producer_topic,
            producer_config,
            consumer_topic,
            consumer_config,
        ) = bootstrap(Stations.GBT)

        consume(
            consumer_topic,
            consumer_config,
            process_msg,
            producer_topic=producer_topic,
            producer_config=producer_config,
        )