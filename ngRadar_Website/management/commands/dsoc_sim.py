from datetime import datetime

import io
import json
import os
import time
import uuid

from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
import numpy as np

from django.core.management.base import BaseCommand

from ngRadar_Website.enums import (
    Stations,
    Status,
    Message,
)

from ngRadar_Website.utils import (
    bootstrap,
    consume,
    consumer_group_has_members,
    create_s3_client,
    get_folder_size,
    latency_calc,
    send_kafka_message,
    upload_seaweedfs,
    write_transfer_progress,
)


"""
DSOC Simulator

This simulator:

- Consumes VLBA workflow events from Kafka.
- Checks DSOC storage availability.
- Monitors the incoming e-transfer.
- Verifies the completed transfer.
- Generates the simulated DDM product.
- Stores the DDM image in SeaweedFS.
- Sends DSOC state/workflow events to Kafka.

This simulator does NOT write directly to:

- gbtEvent
- dsocEvent
- ETransferEvent
- ObservatoryEvent

The db_consumer is solely responsible for persisting
Kafka events to ObservatoryEvent.
"""


STALL_TIMEOUT_SECONDS = 15

MAX_STORAGE_RETRIES = 15

VLBA_STATION = Stations.HN


# =============================================================
# DDM IMAGE GENERATION
# =============================================================

def create_img(tx_waveform):
    """
    Generate a simulated DSOC DDM product.
    """

    matplotlib.use("Agg")

    x_data = np.random.uniform(
        -30,
        30,
        40,
    )

    y_data = np.random.uniform(
        -300,
        300,
        40,
    )

    plt.scatter(
        x_data,
        y_data,
        color="red",
    )

    plt.axhline(
        0,
        color="black",
        linewidth=0.5,
    )

    plt.axvline(
        0,
        color="black",
        linewidth=0.5,
    )

    plt.title(
        f"DDM for {tx_waveform}",
        size=20,
    )

    plt.xlabel(
        "Doppler Freq (Hz)"
    )

    plt.ylabel(
        "Range (km)"
    )

    plt.grid(True)

    byte_buffer = io.BytesIO()

    plt.savefig(
        byte_buffer,
        format="png",
    )

    byte_buffer.seek(0)

    image_file = (
        byte_buffer.getvalue()
    )

    plt.close()

    num_bytes = len(
        image_file
    )

    return (
        image_file,
        num_bytes,
    )


# =============================================================
# SEAWEEDFS
# =============================================================

def save_image_to_seaweedfs(
    target,
    image_file,
    product_uuid,
):
    """
    Save the generated DDM image to SeaweedFS.

    Any error is raised back to process_msg(), which will send
    a FAILED Kafka event. This function does not write to the DB.
    """

    image_key = (
        f"ddm/{target}/"
        f"{product_uuid}.png"
    )

    try:
        s3 = create_s3_client()

        image_key = upload_seaweedfs(
            s3,
            image_key,
            image_file,
        )

        print(
            "Success: Image saved to "
            f"SeaweedFS at {image_key}"
        )

        return image_key

    except Exception as exc:
        raise RuntimeError(
            "Failed to save DDM image "
            "to SeaweedFS."
        ) from exc


# =============================================================
# TRANSFER VERIFICATION
# =============================================================

def verify_incoming_transfer(
    *,
    incoming_file,
    expected_num_bytes,
    attempts=10,
    delay_seconds=0.5,
):
    """
    Verify that the incoming file exists and matches the
    expected byte count supplied by VLBA.
    """

    expected_num_bytes = int(
        expected_num_bytes
    )

    for _ in range(attempts):
        if incoming_file.is_file():
            actual_num_bytes = (
                incoming_file
                .stat()
                .st_size
            )

            if (
                actual_num_bytes
                == expected_num_bytes
            ):
                return actual_num_bytes

        time.sleep(
            delay_seconds
        )

    raise RuntimeError(
        "Transfer verification failed for "
        f"{incoming_file}. Expected "
        f"{expected_num_bytes} bytes."
    )


# =============================================================
# E-TRANSFER PROGRESS
# =============================================================

def track_etransfer_progress(
    payload,
    incoming_file: Path,
):
    """
    Track bytes arriving from VLBA.

    This no longer checks ETransferEvent to decide whether the
    transfer should continue. Kafka/workflow state and the actual
    incoming file are now the source of truth.
    """

    transfer_uuid = payload[
        "transfer_uuid"
    ]

    num_bytes = int(
        payload["num_bytes"]
    )

    if num_bytes <= 0:
        raise ValueError(
            "Expected transfer size must "
            "be greater than zero."
        )

    received_bytes = 0

    # Reset the progress display.
    write_transfer_progress(
        received_bytes=0,
        total_bytes=num_bytes,
        percent=0,
        transfer_id=str(
            transfer_uuid
        ),
    )

    print(
        "Transfer in progress..."
    )

    last_progress_at = (
        time.monotonic()
    )

    while True:

        if incoming_file.exists():
            current_bytes = (
                incoming_file
                .stat()
                .st_size
            )

            if (
                current_bytes
                > received_bytes
            ):
                last_progress_at = (
                    time.monotonic()
                )

            received_bytes = (
                current_bytes
            )

            percent = (
                received_bytes
                / num_bytes
                * 100
            )

            write_transfer_progress(
                received_bytes=(
                    received_bytes
                ),
                total_bytes=num_bytes,
                percent=f"{percent:.1f}",
                transfer_id=str(
                    transfer_uuid
                ),
            )

        if (
            received_bytes
            >= num_bytes
        ):
            print(
                "Transfer of "
                f"<{transfer_uuid}.bin> "
                "COMPLETE."
            )

            break

        # -----------------------------------------------------
        # No bytes have arrived recently.
        #
        # Ask Kafka whether the VLBA consumer is still alive.
        # -----------------------------------------------------
        if (
            time.monotonic()
            - last_progress_at
            > STALL_TIMEOUT_SECONDS
        ):
            vlba_consumer_group = (
                f"{VLBA_STATION.name.lower()}"
                "-consumer-group"
            )

            if consumer_group_has_members(
                vlba_consumer_group
            ):
                # VLBA is alive. The transfer may
                # simply be slow.
                last_progress_at = (
                    time.monotonic()
                )

            else:
                raise RuntimeError(
                    f"{VLBA_STATION.label} "
                    "went offline "
                    "mid-transfer. "
                    "Transfer interrupted."
                )

        time.sleep(0.5)

    if (
        received_bytes
        != num_bytes
    ):
        raise ValueError(
            "Transfer progress halted "
            "before all expected bytes "
            "were received."
        )

    return received_bytes


# =============================================================
# KAFKA PROCESSING
# =============================================================

def process_msg(
    msg,
    producer_topic,
    producer_config,
):
    incoming_key = int(
        msg.key().decode("utf-8")
    )

    # DB_COMMITTED is intended for the website/UI consumer.
    if (
        incoming_key
        == Message.DB_COMMITTED.value
    ):
        return True

    # Generic status events do not instruct DSOC
    # to perform workflow actions.
    if (
        incoming_key
        == Message.STATUS_UPDATE.value
    ):
        return True

    payload = json.loads(
        msg.value().decode("utf-8")
    )

    volume_folder = Path(
        "/dsoc/incoming"
    )

    # ---------------------------------------------------------
    # Context propagated from GBT -> VLBA -> DSOC
    # ---------------------------------------------------------

    transfer_uuid = payload.get(
        "transfer_uuid"
    )

    gbt_uuid = payload.get(
        "gbt_uuid"
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

    gbt_event_time = payload.get(
        "gbt_event_time"
    )

    filename = payload.get(
        "filename"
    )

    num_bytes = int(
        payload.get(
            "num_bytes",
            0,
        )
    )

    retry_count = int(
        payload.get(
            "retry_count",
            0,
        )
    )

    # =========================================================
    # VLBA -> DSOC
    #
    # VLBA requests a storage check.
    # =========================================================

    if (
        incoming_key
        == Message.VLBA_REQUEST_STORAGE.value
    ):
        expected_num_bytes = (
            num_bytes
        )

        storage_limit = (
            int(
                os.environ[
                    "DSOC_VOLUME_SIZE"
                ]
            )
            * 1_000_000_000
        )

        print(
            "DSOC has "
            f"{storage_limit / 1_000_000_000:0.2f}"
            "GB of storage total."
        )

        storage_used = int(
            get_folder_size(
                volume_folder
            )
        )

        space_remaining = (
            storage_limit
            - storage_used
        )

        print(
            "DSOC has "
            f"{space_remaining / 1_000_000_000:0.2f}"
            "GB of storage remaining."
        )

        # -----------------------------------------------------
        # NOT ENOUGH STORAGE
        # -----------------------------------------------------

        if (
            storage_used
            + expected_num_bytes
            >= storage_limit
        ):
            next_retry_count = (
                retry_count + 1
            )

            # -------------------------------------------------
            # Maximum retries reached
            # -------------------------------------------------

            if (
                next_retry_count
                >= MAX_STORAGE_RETRIES
            ):
                send_kafka_message(
                    producer_topic=(
                        producer_topic
                    ),
                    producer_config=(
                        producer_config
                    ),

                    message_type=(
                        Message.DSOC_RESPOND_STORAGE
                    ),

                    transfer_uuid=(
                        transfer_uuid
                    ),
                    gbt_uuid=gbt_uuid,

                    gbt_event_time=(
                        gbt_event_time
                    ),

                    station=Stations.DSOC,
                    status=Status.FAILED,

                    object_id=object_id,
                    target=target,

                    tx_waveform=(
                        tx_waveform
                    ),
                    rec_waveform=(
                        rec_waveform
                    ),

                    num_bytes=(
                        expected_num_bytes
                    ),

                    filename=filename,

                    retry_count=(
                        next_retry_count
                    ),

                    xmit_station=(
                        Stations.DSOC
                    ),
                    rcvr_station=(
                        VLBA_STATION
                    ),

                    message=(
                        "DSOC does not have "
                        "enough storage. "
                        f"Failed after "
                        f"{next_retry_count} "
                        "storage checks."
                    ),
                )

                print(
                    "DSOC failed to clear "
                    "enough storage after "
                    f"{next_retry_count} "
                    "tries."
                )

                return True

            # -------------------------------------------------
            # Ask VLBA to retry later.
            #
            # This message is also persisted by db_consumer
            # as a DSOC RETRYING event.
            # -------------------------------------------------

            send_kafka_message(
                producer_topic=(
                    producer_topic
                ),
                producer_config=(
                    producer_config
                ),

                message_type=(
                    Message.DSOC_RESPOND_STORAGE
                ),

                transfer_uuid=(
                    transfer_uuid
                ),
                gbt_uuid=gbt_uuid,

                gbt_event_time=(
                    gbt_event_time
                ),

                station=Stations.DSOC,
                status=Status.RETRYING,

                object_id=object_id,
                target=target,

                tx_waveform=tx_waveform,
                rec_waveform=(
                    rec_waveform
                ),

                num_bytes=(
                    expected_num_bytes
                ),

                filename=filename,

                retry_count=(
                    next_retry_count
                ),

                xmit_station=(
                    Stations.DSOC
                ),
                rcvr_station=(
                    VLBA_STATION
                ),

                message="No",
            )

            print(
                "DSOC does not have enough "
                "storage to accept the "
                "transfer request. "
                "Remaining disk space: "
                f"{space_remaining / 1_000_000_000:0.2f}"
                "GB. Incoming data: "
                f"{expected_num_bytes / 1_000_000_000:0.2f}"
                "GB."
            )

        # -----------------------------------------------------
        # ENOUGH STORAGE
        # -----------------------------------------------------

        else:
            send_kafka_message(
                producer_topic=(
                    producer_topic
                ),
                producer_config=(
                    producer_config
                ),

                message_type=(
                    Message.DSOC_RESPOND_STORAGE
                ),

                transfer_uuid=(
                    transfer_uuid
                ),
                gbt_uuid=gbt_uuid,

                gbt_event_time=(
                    gbt_event_time
                ),

                station=Stations.DSOC,
                status=Status.READY,

                object_id=object_id,
                target=target,

                tx_waveform=tx_waveform,
                rec_waveform=(
                    rec_waveform
                ),

                num_bytes=(
                    expected_num_bytes
                ),

                filename=filename,

                retry_count=(
                    retry_count
                ),

                xmit_station=(
                    Stations.DSOC
                ),
                rcvr_station=(
                    VLBA_STATION
                ),

                message="Yes",
            )

            print(
                "DSOC has enough storage "
                "to accept the incoming "
                "data. Awaiting "
                "e-transfer..."
            )

    # =========================================================
    # VLBA -> DSOC
    #
    # VLBA has started the e-transfer.
    # =========================================================

    elif (
        incoming_key
        == Message.VLBA_TRANSFERRING.value
    ):
        incoming_file = (
            volume_folder
            / f"{transfer_uuid}.bin"
        )

        # -----------------------------------------------------
        # Track incoming bytes
        # -----------------------------------------------------

        try:
            track_etransfer_progress(
                payload,
                incoming_file,
            )

        except Exception as exc:
            print(
                "Incoming data progress "
                f"interrupted: {exc}"
            )

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

                station=Stations.DSOC,
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

                message=str(exc),
            )

            return True

        # -----------------------------------------------------
        # DSOC begins verification
        # -----------------------------------------------------

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

            station=Stations.DSOC,
            status=Status.VERIFYING,

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
                f"Verifying "
                f"{filename}."
            ),
        )

        # -----------------------------------------------------
        # Verify incoming file
        # -----------------------------------------------------

        try:
            actual_num_bytes = (
                verify_incoming_transfer(
                    incoming_file=(
                        incoming_file
                    ),
                    expected_num_bytes=(
                        num_bytes
                    ),
                )
            )

        except Exception as exc:
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

                station=Stations.DSOC,
                status=Status.FAILED,

                object_id=object_id,
                target=target,

                tx_waveform=tx_waveform,
                rec_waveform=(
                    rec_waveform
                ),

                num_bytes=0,
                filename=filename,

                xmit_station=(
                    VLBA_STATION
                ),
                rcvr_station=(
                    Stations.DSOC
                ),

                message=str(exc),
            )

            return True

        # -----------------------------------------------------
        # Generate and store DDM
        # -----------------------------------------------------

        try:
            if not gbt_event_time:
                raise ValueError(
                    "GBT event time is "
                    "missing from the "
                    "Kafka payload."
                )

            original_gbt_time = (
                datetime.fromisoformat(
                    gbt_event_time
                )
            )

            dsoc_latency = (
                latency_calc(
                    original_gbt_time,
                    Stations.DSOC,
                )
            )

            image_file, image_num_bytes = (
                create_img(
                    tx_waveform
                )
            )

            product_uuid = (
                uuid.uuid4()
            )

            image_key = (
                save_image_to_seaweedfs(
                    target,
                    image_file,
                    product_uuid,
                )
            )

        except Exception as exc:
            print(
                "DSOC image processing "
                f"failed: {exc}"
            )

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

                station=Stations.DSOC,
                status=Status.FAILED,

                object_id=object_id,
                target=target,

                tx_waveform=tx_waveform,
                rec_waveform=(
                    rec_waveform
                ),

                num_bytes=(
                    actual_num_bytes
                ),

                filename=filename,

                xmit_station=(
                    VLBA_STATION
                ),
                rcvr_station=(
                    Stations.DSOC
                ),

                message=(
                    "DSOC image processing "
                    f"failed: {exc}"
                ),
            )

            return True

        # -----------------------------------------------------
        # COMPLETE
        #
        # This single event:
        #
        # 1. records DSOC COMPLETED
        # 2. includes the generated DDM product
        # 3. tells VLBA to delete its raw data
        # -----------------------------------------------------

        send_kafka_message(
            producer_topic=(
                producer_topic
            ),
            producer_config=(
                producer_config
            ),

            message_type=(
                Message.VLBA_DELETE
            ),

            transfer_uuid=(
                transfer_uuid
            ),
            gbt_uuid=gbt_uuid,

            gbt_event_time=(
                gbt_event_time
            ),

            station=Stations.DSOC,
            status=Status.COMPLETED,

            object_id=object_id,
            target=target,

            tx_waveform=tx_waveform,
            rec_waveform=(
                rec_waveform
            ),

            product_type="DDM",
            product_id=str(
                product_uuid
            ),

            image_key=image_key,

            # The final DSOC product row describes
            # the generated DDM product size.
            num_bytes=(
                image_num_bytes
            ),

            filename=filename,

            latency_ms=(
                dsoc_latency
            ),

            xmit_station=(
                VLBA_STATION
            ),
            rcvr_station=(
                Stations.DSOC
            ),

            message=(
                "DSOC verified the "
                "e-transfer, generated "
                "the DDM image, stored "
                "the image, and completed "
                "processing. VLBA may "
                "delete its raw data."
            ),
        )

        print(
            "DSOC processing COMPLETE."
        )

    else:
        print(
            "Invalid Kafka Message Key!"
        )

    return True


class Command(BaseCommand):
    help = "Runs the DSOC simulator"

    def handle(
        self,
        *args,
        **options,
    ):
        print(
            "Starting DSOC simulator"
        )

        (
            producer_topic,
            producer_config,
            consumer_topic,
            consumer_config,
        ) = bootstrap(
            Stations.DSOC
        )

        consume(
            consumer_topic,
            consumer_config,
            process_msg,
            producer_topic=(
                producer_topic
            ),
            producer_config=(
                producer_config
            ),
        )