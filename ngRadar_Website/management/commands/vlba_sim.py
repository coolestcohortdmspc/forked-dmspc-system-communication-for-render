import json
import subprocess
import time
import uuid

from pathlib import Path
from threading import Thread

from django.core.management.base import BaseCommand

from ngRadar_Website.enums import (
    Stations,
    Status,
    Message,
)

from ngRadar_Website.utils import (
    bootstrap,
    consume,
    create_file,
    watch_for_file,
    send_kafka_message,
    etc_send,
    wait_for_etd,
    delete_observation_data,
    ETD_MAX_CONN_RETRY,
    ETD_RETRY_CONN_DELAY,
)


"""
VLBA Simulator

This simulator:

- Consumes GBT_TX events from Kafka.
- Generates/stages VLBA observation data.
- Requests DSOC storage availability.
- Sends the data to DSOC using e-transfer.
- Publishes VLBA state changes to Kafka.
- Does NOT write ObservatoryEvent directly.

The db_consumer is responsible for consuming these Kafka
messages and persisting them to ObservatoryEvent.

For now this simulator represents the Hancock VLBA station.
"""


FAILURE_REASONS = {
    -9: "The e-transfer process was terminated",
    -6: "The connection to the e-transfer daemon was lost",
}

MAX_RESUME_ATTEMPTS = 5

VLBA_STATION = Stations.HN


def process_msg(
    msg,
    producer_topic,
    producer_config,
):
    incoming_key = int(
        msg.key().decode("utf-8")
    )

    raw_data_path = Path("/raw_data")

    # ---------------------------------------------------------
    # DB_COMMITTED messages are acknowledgements intended for
    # the website/UI consumer.
    #
    # Workflow consumers must ignore them.
    # ---------------------------------------------------------
    if incoming_key == Message.DB_COMMITTED.value:
        return True

    # Generic status-only events do not instruct VLBA
    # to perform any workflow action.
    if incoming_key == Message.STATUS_UPDATE.value:
        return True

    # =========================================================
    # GBT -> VLBA
    # =========================================================
    if incoming_key == Message.GBT_TX.value:
        print(
            "Received Kafka message from GBT."
        )

        payload = json.loads(
            msg.value().decode("utf-8")
        )

        # An observation-wide correlation ID.
        gbt_uuid = payload["gbt_uuid"]

        # Preserve the original GBT timestamp for
        # end-to-end latency calculation at DSOC.
        gbt_event_time = payload.get(
            "gbt_event_time",
            payload["event_time"],
        )

        # Observation context inherited from GBT.
        object_id = payload.get(
            "object_id"
        )

        target = payload.get(
            "target"
        )

        tx_waveform = payload.get(
            "tx_waveform"
        )

        rec_waveform = payload.get(
            "rec_waveform"
        )

        # One transfer UUID identifies this entire
        # VLBA -> DSOC e-transfer lifecycle.
        transfer_uuid = uuid.uuid4()

        frame_path = (
            raw_data_path
            / f"{transfer_uuid}.bin"
        )

        Thread(
            target=create_file,
            args=(frame_path,),
            daemon=True,
        ).start()

        watch_for_file(
            frame_path
        )

        # -----------------------------------------------------
        # Raw data file successfully created
        # -----------------------------------------------------
        if frame_path.is_file():
            num_bytes = (
                frame_path.stat().st_size
            )

            # This ONE Kafka message:
            #
            # 1. tells DSOC to check storage
            # 2. describes the VLBA READY state
            # 3. is consumed by db_consumer and saved
            #    as an ObservatoryEvent
            send_kafka_message(
                producer_topic=producer_topic,
                producer_config=producer_config,
                message_type=(
                    Message.VLBA_REQUEST_STORAGE
                ),
                transfer_uuid=transfer_uuid,
                gbt_uuid=gbt_uuid,
                gbt_event_time=gbt_event_time,
                station=VLBA_STATION,
                status=Status.READY,
                object_id=object_id,
                target=target,
                tx_waveform=tx_waveform,
                rec_waveform=rec_waveform,
                num_bytes=num_bytes,
                filename=frame_path.name,
                xmit_station=VLBA_STATION,
                rcvr_station=Stations.DSOC,
                message=(
                    "VLBA requesting DSOC "
                    "storage availability."
                ),
            )

            print(
                "VLBA requesting DSOC "
                "check storage..."
            )

        # -----------------------------------------------------
        # Raw data file creation failed
        # -----------------------------------------------------
        else:
            send_kafka_message(
                producer_topic=producer_topic,
                producer_config=producer_config,
                message_type=(
                    Message.VLBA_FAILED
                ),
                transfer_uuid=transfer_uuid,
                gbt_uuid=gbt_uuid,
                gbt_event_time=gbt_event_time,
                station=VLBA_STATION,
                status=Status.FAILED,
                object_id=object_id,
                target=target,
                tx_waveform=tx_waveform,
                rec_waveform=rec_waveform,
                num_bytes=0,
                filename=frame_path.name,
                xmit_station=VLBA_STATION,
                rcvr_station=Stations.DSOC,
                message=(
                    "VLBA source file "
                    "does not exist."
                ),
            )

            print(
                "Source file does not exist."
            )

            return True

    # =========================================================
    # DSOC -> VLBA
    #
    # DSOC responded to our storage request.
    # =========================================================
    elif (
        incoming_key
        == Message.DSOC_RESPOND_STORAGE.value
    ):
        print(
            "Received DSOC's storage "
            "check response!"
        )

        payload = json.loads(
            msg.value().decode("utf-8")
        )

        transfer_uuid = payload[
            "transfer_uuid"
        ]

        gbt_uuid = payload[
            "gbt_uuid"
        ]

        gbt_event_time = payload.get(
            "gbt_event_time"
        )

        object_id = payload.get(
            "object_id"
        )

        target = payload.get(
            "target"
        )

        tx_waveform = payload.get(
            "tx_waveform"
        )

        rec_waveform = payload.get(
            "rec_waveform"
        )

        num_bytes = int(
            payload.get(
                "num_bytes",
                0,
            )
        )

        filename = payload.get(
            "filename"
        )

        retry_count = int(
            payload.get(
                "retry_count",
                0,
            )
        )

        # =====================================================
        # DSOC storage check permanently failed
        # =====================================================
        if (
            payload["status"]
            == Status.FAILED.value
        ):
            print(
                "DSOC storage request "
                f"failed: {payload['message']}"
            )

            return True

        # =====================================================
        # DSOC HAS STORAGE
        # =====================================================
        elif payload["message"] == "Yes":
            attempts = 0

            while True:
                try:
                    # -----------------------------------------
                    # VLBA is beginning the actual transfer.
                    #
                    # This Kafka message:
                    #
                    # - tells DSOC that transmission started
                    # - records VLBA TRANSFERRING through
                    #   db_consumer
                    # -----------------------------------------
                    send_kafka_message(
                        producer_topic=(
                            producer_topic
                        ),
                        producer_config=(
                            producer_config
                        ),
                        message_type=(
                            Message.VLBA_TRANSFERRING
                        ),
                        transfer_uuid=(
                            transfer_uuid
                        ),
                        gbt_uuid=gbt_uuid,
                        gbt_event_time=(
                            gbt_event_time
                        ),
                        station=VLBA_STATION,
                        status=(
                            Status.TRANSFERRING
                        ),
                        object_id=object_id,
                        target=target,
                        tx_waveform=tx_waveform,
                        rec_waveform=(
                            rec_waveform
                        ),
                        num_bytes=num_bytes,
                        filename=filename,
                        xmit_station=(
                            VLBA_STATION
                        ),
                        rcvr_station=(
                            Stations.DSOC
                        ),
                        message=(
                            "Hancock VLBA has "
                            "started sending the "
                            "data file to DSOC "
                            "via e-transfer."
                        ),
                    )

                    frame_path = (
                        raw_data_path
                        / f"{transfer_uuid}.bin"
                    )

                    print(
                        "DSOC responded "
                        "affirmative to storage "
                        "check. Initiating "
                        "e-transfer..."
                    )

                    etc_send(
                        frame_path
                    )

                    # -----------------------------------------
                    # etc_send returned successfully.
                    #
                    # VLBA now knows that its side of the
                    # transfer completed successfully.
                    # -----------------------------------------
                    send_kafka_message(
                        producer_topic=(
                            producer_topic
                        ),
                        producer_config=(
                            producer_config
                        ),
                        message_type=(
                            Message.STATUS_UPDATE
                        ),
                        transfer_uuid=(
                            transfer_uuid
                        ),
                        gbt_uuid=gbt_uuid,
                        gbt_event_time=(
                            gbt_event_time
                        ),
                        station=VLBA_STATION,
                        status=(
                            Status.TRANSFERRED
                        ),
                        object_id=object_id,
                        target=target,
                        tx_waveform=tx_waveform,
                        rec_waveform=(
                            rec_waveform
                        ),
                        num_bytes=num_bytes,
                        filename=filename,
                        xmit_station=(
                            VLBA_STATION
                        ),
                        rcvr_station=(
                            Stations.DSOC
                        ),
                        message=(
                            "Hancock VLBA "
                            "completed the "
                            "e-transfer to DSOC."
                        ),
                    )

                    break

                # =============================================
                # Known ETC/e-transfer process failure
                # =============================================
                except subprocess.CalledProcessError as exc:
                    failure_reason = (
                        FAILURE_REASONS.get(
                            exc.returncode,
                            "The e-transfer failed",
                        )
                    )

                    failure_message = (
                        f"{failure_reason} "
                        "mid-transfer. "
                        "Transfer interrupted. "
                        f"(return code: "
                        f"{exc.returncode})"
                    )

                    print(
                        "E-transfer failed with "
                        "return code: "
                        f"{exc.returncode}"
                    )

                    send_kafka_message(
                        producer_topic=(
                            producer_topic
                        ),
                        producer_config=(
                            producer_config
                        ),
                        message_type=(
                            Message.VLBA_FAILED
                        ),
                        transfer_uuid=(
                            transfer_uuid
                        ),
                        gbt_uuid=gbt_uuid,
                        gbt_event_time=(
                            gbt_event_time
                        ),
                        station=VLBA_STATION,
                        status=Status.FAILED,
                        object_id=object_id,
                        target=target,
                        tx_waveform=tx_waveform,
                        rec_waveform=(
                            rec_waveform
                        ),
                        num_bytes=num_bytes,
                        filename=filename,
                        xmit_station=(
                            VLBA_STATION
                        ),
                        rcvr_station=(
                            Stations.DSOC
                        ),
                        message=(
                            failure_message
                        ),
                    )

                    attempts += 1

                    if (
                        attempts
                        >= MAX_RESUME_ATTEMPTS
                    ):
                        print(
                            "E-transfer failed "
                            f"{attempts} times. "
                            "Giving up."
                        )

                        return False

                    print(
                        "Waiting for the "
                        "e-transfer daemon to "
                        "come back..."
                    )

                    if not wait_for_etd():
                        print(
                            "E-transfer daemon "
                            "never came back. "
                            "Giving up."
                        )

                        return False

                    print(
                        "E-transfer daemon is "
                        "back. Resuming the "
                        "transfer..."
                    )

                # =============================================
                # Unexpected transfer failure
                # =============================================
                except Exception as exc:
                    print(
                        "Unexpected e-transfer "
                        f"failure: {exc}"
                    )

                    send_kafka_message(
                        producer_topic=(
                            producer_topic
                        ),
                        producer_config=(
                            producer_config
                        ),
                        message_type=(
                            Message.VLBA_FAILED
                        ),
                        transfer_uuid=(
                            transfer_uuid
                        ),
                        gbt_uuid=gbt_uuid,
                        gbt_event_time=(
                            gbt_event_time
                        ),
                        station=VLBA_STATION,
                        status=Status.FAILED,
                        object_id=object_id,
                        target=target,
                        tx_waveform=tx_waveform,
                        rec_waveform=(
                            rec_waveform
                        ),
                        num_bytes=num_bytes,
                        filename=filename,
                        xmit_station=(
                            VLBA_STATION
                        ),
                        rcvr_station=(
                            Stations.DSOC
                        ),
                        message=(
                            "The e-transfer "
                            "failed unexpectedly "
                            "mid-transfer. "
                            "Transfer interrupted. "
                            f"({exc})"
                        ),
                    )

                    return False

        # =====================================================
        # DSOC DOES NOT HAVE STORAGE
        # =====================================================
        else:
            print(
                "DSOC responded negative to "
                "storage check. Will ask "
                "again in 5 seconds..."
            )

            time.sleep(5)

            # VLBA remains BLOCKED but performs the
            # same workflow action again:
            #
            # VLBA_REQUEST_STORAGE
            #
            # Message type = action
            # Status       = current state
            send_kafka_message(
                producer_topic=producer_topic,
                producer_config=producer_config,
                message_type=(
                    Message.VLBA_REQUEST_STORAGE
                ),
                retry_count=retry_count,
                transfer_uuid=transfer_uuid,
                gbt_uuid=gbt_uuid,
                gbt_event_time=gbt_event_time,
                station=VLBA_STATION,
                status=Status.BLOCKED,
                object_id=object_id,
                target=target,
                tx_waveform=tx_waveform,
                rec_waveform=rec_waveform,
                num_bytes=num_bytes,
                filename=filename,
                xmit_station=VLBA_STATION,
                rcvr_station=Stations.DSOC,
                message=(
                    "Waiting for DSOC "
                    "storage availability."
                ),
            )

    # =========================================================
    # DSOC -> VLBA
    #
    # DSOC says the VLBA raw file can be deleted.
    # =========================================================
    elif (
        incoming_key
        == Message.VLBA_DELETE.value
    ):
        payload = json.loads(
            msg.value().decode("utf-8")
        )

        file_name = payload[
            "filename"
        ]

        delete_observation_data(
            file_name
        )

        print(
            f"Deleted VLBA raw data "
            f"{file_name}."
        )

    # =========================================================
    # Unknown workflow message
    # =========================================================
    else:
        print(
            "NOT A VALID KAFKA "
            "MESSAGE VALUE!"
        )

    return True


class Command(BaseCommand):
    help = "Runs the VLBA simulator"

    def handle(
        self,
        *args,
        **options,
    ):
        print(
            "Starting VLBA simulator"
        )

        (
            producer_topic,
            producer_config,
            consumer_topic,
            consumer_config,
        ) = bootstrap(
            VLBA_STATION
        )

        # process_msg can remain blocked while
        # wait_for_etd() waits for the daemon to
        # return.
        #
        # Increase Kafka's allowed poll interval
        # accordingly.
        consumer_config[
            "max.poll.interval.ms"
        ] = (
            (
                ETD_MAX_CONN_RETRY
                * ETD_RETRY_CONN_DELAY
            )
            + 300
        ) * 1000

        consume(
            consumer_topic,
            consumer_config,
            process_msg,
            producer_topic=producer_topic,
            producer_config=producer_config,
            manual_commit=True,
        )