from datetime import datetime, timezone
from pathlib import Path

import json
import os
import random
import re
import select
import subprocess
import time
import uuid

import boto3

from botocore.config import Config
from botocore.exceptions import (
    ClientError,
    ConnectionError,
    EndpointConnectionError,
)

from confluent_kafka import (
    Consumer,
    KafkaError,
    Producer,
)

from confluent_kafka.admin import AdminClient

from dotenv import load_dotenv

from ngRadar_Website.enums import (
    Stations,
    Status,
)


# =============================================================
# CONSTANTS
# =============================================================

SESSION_TIMEOUT_MS = 10_000
MAX_BYTES = 8_388_608

ETD_MAX_CONN_RETRY = 90
ETD_RETRY_CONN_DELAY = 10


# =============================================================
# REGEX PATTERNS
# =============================================================

# Matches progress output from the etc CLI.
PROGRESS_RE = re.compile(
    r"\]\s+"
    r"(?P<percent>\d+(?:\.\d+)?)%\s+"
    r"(?P<received>\d+(?:\.\d+)?)\s+"
    r"(?P<received_unit>\S+)\s+/\s+"
    r"(?P<total>\d+(?:\.\d+)?)\s+"
    r"(?P<total_unit>\S+)"
)

# Removes terminal escape sequences such as ESC[K.
ANSI_RE = re.compile(
    r"\x1b\[[0-9;]*[A-Za-z]"
)


# =============================================================
# GENERAL HELPERS
# =============================================================

def latency_calc(
    event_time,
    sim=None,
    current_time=None,
):
    """
    Calculate latency in milliseconds between event_time and now.

    For GBT, the historical implementation subtracts the
    simulator's 5-second delay.
    """

    if current_time is None:
        current_time = datetime.now(
            timezone.utc
        )

    if sim == Stations.GBT:
        if event_time == -1:
            # Historical behavior for the first GBT payload.
            return 0

        latency = (
            current_time
            - event_time
        )

        return (
            latency.total_seconds()
            * 1000
            - 5000
        )

    latency = (
        current_time
        - event_time
    )

    return (
        latency.total_seconds()
        * 1000
    )


# =============================================================
# KAFKA CONFIGURATION
# =============================================================

def config_func(
    sim,
    bootstrap_server,
):
    """
    Generate Kafka topics and client configuration for a station.

    Domain topics:
        GBT_notif
        VLBA_notif
        DSOC_notif

    GBT:
        consumes GBT_notif
        produces GBT_notif

    VLBA:
        consumes GBT_notif + DSOC_notif
        produces VLBA_notif

    DSOC:
        consumes VLBA_notif
        produces DSOC_notif

    UI:
        produces GBT_notif
    """

    if sim == Stations.GBT:
        producer_topic = "GBT_notif"

        consumer_topic = [
            "GBT_notif",
        ]

    elif sim == Stations.HN:
        producer_topic = "VLBA_notif"

        consumer_topic = [
            "GBT_notif",
            "DSOC_notif",
        ]

    elif sim == Stations.DSOC:
        producer_topic = "DSOC_notif"

        consumer_topic = [
            "VLBA_notif",
        ]

    elif sim == Stations.UI:
        producer_topic = "GBT_notif"

        producer_config = {
            "bootstrap.servers": (
                bootstrap_server
            ),
            "message.max.bytes": (
                MAX_BYTES
            ),
            "message.timeout.ms": 2000,
            "client.id": (
                f"{sim.name.lower()}"
                "-producer"
            ),
        }

        return (
            producer_topic,
            producer_config,
        )

    else:
        raise ValueError(
            f"Unsupported station: {sim}"
        )

    producer_config = {
        "bootstrap.servers": (
            bootstrap_server
        ),
        "message.max.bytes": (
            MAX_BYTES
        ),
        "message.timeout.ms": 2000,
        "client.id": (
            f"{sim.name.lower()}"
            "-producer"
        ),
    }

    consumer_config = {
        "bootstrap.servers": (
            bootstrap_server
        ),
        "fetch.max.bytes": (
            MAX_BYTES
        ),
        "session.timeout.ms": (
            SESSION_TIMEOUT_MS
        ),
        "client.id": (
            f"{sim.name.lower()}"
            "-consumer"
        ),
        "group.id": (
            f"{sim.name.lower()}"
            "-consumer-group"
        ),
        "auto.offset.reset": (
            "earliest"
        ),
    }

    return (
        producer_topic,
        producer_config,
        consumer_topic,
        consumer_config,
    )

def bootstrap(sim):
    """
    Load Kafka bootstrap configuration from environment
    and return station-specific topics/configuration.
    """

    load_dotenv()

    bootstrap_server = os.getenv(
        "BOOTSTRAP_SERVER",
        "kafka-broker:29092",
    )

    return config_func(
        sim,
        bootstrap_server,
    )


# =============================================================
# KAFKA PRODUCER / CONSUMER
# =============================================================

def produce(
    topic,
    config,
    key,
    value,
    report_failure=True,
):
    """
    Produce one Kafka message.

    Returns:
        True  - message delivered successfully
        False - delivery failed
    """

    delivery_error = None

    def delivery_report(
        err,
        msg,
    ):
        nonlocal delivery_error

        if err is not None:
            delivery_error = err

    try:
        producer = Producer(
            config
        )

        producer.produce(
            topic,
            key=key,
            value=value,
            callback=delivery_report,
        )

        remaining = (
            producer.flush(2)
        )

        if delivery_error is not None:
            print(
                "Failed to produce message "
                f"to {topic}: "
                f"{delivery_error}"
            )

            return False

        if remaining > 0:
            print(
                "Kafka broker did not "
                "respond while publishing "
                f"to {topic}."
            )

            return False

        print(
            "Produced message to topic "
            f"{topic} with key {key}."
        )

        return True

    except Exception as exc:
        print(
            "Failed to send Kafka message "
            f"to {topic}: {exc}"
        )

        return False


def consume(
    topic,
    config,
    process_msg,
    producer_topic=None,
    producer_config=None,
    manual_commit=False,
):
    """
    Consume Kafka messages and pass them to process_msg().

    If manual_commit=True, a message is committed only when
    process_msg() returns True.
    """

    if manual_commit:
        config = {
            **config,
            "enable.auto.commit": False,
        }

    consumer = Consumer(
        config
    )

    consumer.subscribe(
        topic
    )

    try:
        while True:
            msg = consumer.poll(
                1.0
            )

            if msg is None:
                continue

            if msg.error():
                error = msg.error()

                if (
                    error.code()
                    == KafkaError._PARTITION_EOF
                ):
                    print(
                        "Consumer reached "
                        "partition EOF."
                    )
                    continue

                print(
                    "Consumer error:",
                    error,
                )

                break

            succeeded = process_msg(
                msg,
                producer_topic,
                producer_config,
            )

            if (
                manual_commit
                and succeeded
            ):
                consumer.commit(
                    msg
                )

    finally:
        consumer.close()


# =============================================================
# KAFKA DOMAIN MESSAGE HELPERS
# =============================================================

def send_kafka_message(
    *,
    message_type,
    producer_topic,
    producer_config,
    station,
    gbt_uuid=None,
    gbt_event_time=None,
    transfer_uuid=None,
    retry_count=0,
    status=None,
    object_id=None,
    target=None,
    tx_waveform=None,
    rec_waveform=None,
    product_type=None,
    product_id=None,
    num_bytes=0,
    latency_ms=0.0,
    message="",
    xmit_station=None,
    rcvr_station=None,
    image_key=None,
    filename=None,
):
    """
    Build and send one canonical ngRadar domain event to Kafka.

    This function does NOT write to the database.

    The db_consumer is responsible for persisting the event
    to ObservatoryEvent.
    """

    event_uuid = (
        uuid.uuid4()
    )

    payload = {
        "event_uuid": (
            str(event_uuid)
        ),

        "gbt_uuid": (
            str(gbt_uuid)
            if gbt_uuid
            else None
        ),

        "gbt_event_time": (
            gbt_event_time
            if gbt_event_time
            else None
        ),

        "transfer_uuid": (
            str(transfer_uuid)
            if transfer_uuid
            else None
        ),

        "retry_count": (
            int(retry_count)
        ),

        "object_id": (
            object_id
        ),

        "target": (
            target
        ),

        "tx_waveform": (
            tx_waveform
        ),

        "rec_waveform": (
            rec_waveform
        ),

        "product_type": (
            product_type
        ),

        "product_id": (
            str(product_id)
            if product_id is not None
            else None
        ),

        "station": (
            int(station)
        ),

        "station_name": (
            station.label
        ),

        "status": (
            int(status)
            if status is not None
            else None
        ),

        "xmit_station": (
            int(xmit_station)
            if xmit_station is not None
            else None
        ),

        "rcvr_station": (
            int(rcvr_station)
            if rcvr_station is not None
            else None
        ),

        "image_key": (
            image_key
        ),

        "filename": (
            filename
        ),

        "num_bytes": (
            int(num_bytes)
        ),

        "latency_ms": (
            float(latency_ms)
        ),

        "message": (
            message
        ),

        "event_time": (
            datetime.now(
                timezone.utc
            ).isoformat()
        ),
    }

    success = produce(
        producer_topic,
        producer_config,
        str(
            message_type.value
        ),
        json.dumps(
            payload
        ),
    )

    if not success:
        return None

    return event_uuid


def consumer_group_has_members(
    group_id,
):
    """
    Ask Kafka whether a consumer group currently has
    any active members.
    """

    bootstrap_server = os.getenv(
        "BOOTSTRAP_SERVER",
        "kafka-broker:29092",
    )

    admin = AdminClient(
        {
            "bootstrap.servers": (
                bootstrap_server
            )
        }
    )

    group = (
        admin
        .describe_consumer_groups(
            [group_id]
        )[group_id]
        .result()
    )

    return (
        len(group.members)
        > 0
    )


# =============================================================
# SEAWEEDFS / S3
# =============================================================

def create_s3_client():
    """
    Create a boto3 S3 client and wait for the SeaweedFS
    S3 gateway to become available.
    """

    endpoint = os.environ[
        "WEED_S3_ENDPOINT"
    ]

    print(
        "Connecting to:",
        endpoint,
    )

    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=(
            os.environ[
                "WEED_S3_ACCESS_KEY"
            ]
        ),
        aws_secret_access_key=(
            os.environ[
                "WEED_S3_SECRET_KEY"
            ]
        ),
        region_name="us-east-1",
        config=Config(
            signature_version="s3v4",
            s3={
                "addressing_style": (
                    "path"
                )
            },
        ),
    )

    for attempt in range(3):
        try:
            s3.list_buckets()

            print(
                "SeaweedFS S3 is ready."
            )

            break

        except (
            EndpointConnectionError,
            ConnectionError,
        ):
            print(
                "Waiting for SeaweedFS... "
                f"({attempt + 1}/3)"
            )

            time.sleep(1)

        except ClientError as exc:
            print(
                "SeaweedFS responded: "
                f"{exc.response['Error']['Code']}"
            )

            break

    else:
        raise RuntimeError(
            "SeaweedFS S3 never "
            "became ready."
        )

    ensure_bucket_exists(
        s3
    )

    return s3


def ensure_bucket_exists(s3):
    """
    Ensure the configured SeaweedFS bucket exists.
    """

    bucket = os.environ[
        "WEED_S3_BUCKET"
    ]

    try:
        s3.head_bucket(
            Bucket=bucket
        )

        print(
            f"Bucket '{bucket}' exists."
        )

        return

    except ClientError as exc:
        status_code = (
            exc.response[
                "ResponseMetadata"
            ][
                "HTTPStatusCode"
            ]
        )

        if status_code != 404:
            raise

    print(
        f"Creating bucket '{bucket}'..."
    )

    s3.create_bucket(
        Bucket=bucket
    )

    print(
        "Bucket created."
    )


def upload_seaweedfs(
    s3,
    image_key,
    file_data,
):
    """
    Upload PNG data to SeaweedFS via its S3 API.
    """

    bucket = os.environ[
        "WEED_S3_BUCKET"
    ]

    s3.put_object(
        Bucket=bucket,
        Key=image_key,
        Body=file_data,
        ContentType="image/png",
    )

    print(
        f"Success: {image_key}"
    )

    return image_key


# =============================================================
# E-TRANSFER PROGRESS
# =============================================================

def write_transfer_progress(
    *,
    received_bytes,
    total_bytes,
    percent,
    transfer_id,
):
    """
    Atomically update progress.json for the website's
    progress SSE endpoint.
    """

    progress_path = (
        "/service/mock_assets/"
        "progress.json"
    )

    temp_path = (
        progress_path
        + ".tmp"
    )

    progress_data = {
        "received_bytes": (
            received_bytes
        ),
        "total_bytes": (
            total_bytes
        ),
        "percent": (
            percent
        ),
        "transfer_id": (
            transfer_id
        ),
    }

    with open(
        temp_path,
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            progress_data,
            file,
        )

    os.replace(
        temp_path,
        progress_path,
    )


def parse_etc_progress(
    line,
    *,
    expected_num_bytes,
    transfer_id,
):
    """
    Parse one line of etc CLI progress output.
    """

    clean_line = (
        ANSI_RE.sub(
            "",
            line,
        )
    )

    match = (
        PROGRESS_RE.search(
            clean_line
        )
    )

    if not match:
        return

    percent = float(
        match.group(
            "percent"
        )
    )

    received_bytes = round(
        expected_num_bytes
        * (
            percent
            / 100.0
        )
    )

    if percent >= 100.0:
        received_bytes = (
            expected_num_bytes
        )

    print(
        "Transfer progress: "
        f"{received_bytes}/"
        f"{expected_num_bytes} "
        "bytes "
        f"({percent:.1f}%)"
    )

    # Progress currently also gets measured from
    # the receiving DSOC side.
    #
    # Re-enable this later if etc should become
    # the authoritative progress source.
    #
    # write_transfer_progress(
    #     received_bytes=received_bytes,
    #     total_bytes=expected_num_bytes,
    #     percent=percent,
    #     transfer_id=transfer_id,
    # )


# =============================================================
# E-TRANSFER CONNECTION / COMMANDS
# =============================================================

def wait_for_etd():
    """
    Wait for the e-transfer daemon to become reachable again.

    Returns:
        True  - daemon responded
        False - retry limit exhausted
    """

    result = subprocess.run(
        [
            "etc",
            "--list",
            os.environ[
                "ETD_DESTINATION"
            ],
            "--max-conn-retry",
            str(
                ETD_MAX_CONN_RETRY
            ),
            "--retry-conn-delay",
            str(
                ETD_RETRY_CONN_DELAY
            ),
        ],
        capture_output=True,
    )

    return (
        result.returncode
        == 0
    )


def etc_send(frame_path):
    """
    Send one raw-data file from VLBA to DSOC using e-transfer.

    Uses --resume so an interrupted transfer can continue
    using the partially received destination file.
    """

    expected_num_bytes = (
        frame_path
        .stat()
        .st_size
    )

    transfer_id = str(
        uuid.uuid4()
    )

    master_fd, slave_fd = (
        os.openpty()
    )

    etd_host = os.environ[
        "ETD_HOST"
    ]

    etd_command_port = (
        os.environ.get(
            "ETD_COMMAND_PORT",
            "4004",
        )
    )

    etd_destination = (
        f"tcp://{etd_host}"
        f"#{etd_command_port}"
        ":/dsoc/incoming/"
    )

    process = subprocess.Popen(
        [
            "etc",
            str(frame_path),
            etd_destination,
            "--resume",
        ],
        stdin=slave_fd,
        stdout=slave_fd,
        stderr=slave_fd,
        close_fds=True,
    )

    os.close(
        slave_fd
    )

    buffer = ""

    try:
        while (
            process.poll()
            is None
        ):
            readable, _, _ = (
                select.select(
                    [master_fd],
                    [],
                    [],
                    0.5,
                )
            )

            if not readable:
                continue

            try:
                terminal_output = (
                    os.read(
                        master_fd,
                        4096,
                    ).decode(
                        "utf-8",
                        errors="replace",
                    )
                )

            except OSError:
                break

            print(
                terminal_output,
                end="",
                flush=True,
            )

            buffer += (
                terminal_output
            )

            parts = re.split(
                r"[\r\n]",
                buffer,
            )

            buffer = (
                parts.pop()
            )

            for line in parts:
                parse_etc_progress(
                    line,
                    expected_num_bytes=(
                        expected_num_bytes
                    ),
                    transfer_id=(
                        transfer_id
                    ),
                )

        if buffer:
            parse_etc_progress(
                buffer,
                expected_num_bytes=(
                    expected_num_bytes
                ),
                transfer_id=(
                    transfer_id
                ),
            )

    finally:
        os.close(
            master_fd
        )

    return_code = (
        process.wait()
    )

    if return_code != 0:
        raise (
            subprocess
            .CalledProcessError(
                return_code,
                process.args,
            )
        )


# =============================================================
# FILE / STORAGE HELPERS
# =============================================================

def create_file(
    file_path,
    file_mb=200,
):
    """
    Create a random binary file for simulated VLBA data.
    """

    file_size_bytes = (
        file_mb
        * 1024
        * 1024
    )

    num_buffers = 100

    buffer_size = (
        file_size_bytes
        // num_buffers
    )

    remainder = (
        file_size_bytes
        % num_buffers
    )

    with open(
        file_path,
        "wb",
    ) as file:
        for index in range(
            num_buffers
        ):
            size = (
                buffer_size
                + (
                    1
                    if index
                    < remainder
                    else 0
                )
            )

            buffer = (
                random.randbytes(
                    size
                )
            )

            file.write(
                buffer
            )

    print(
        "Successfully created a "
        f"{file_mb}MB random binary "
        f"file at {file_path}"
    )


def watch_for_file(
    file_path,
):
    """
    Wait until no process has the file open.
    """

    while True:
        result = subprocess.run(
            [
                "lsof",
                file_path,
            ],
            capture_output=True,
            text=True,
        )

        output = (
            result.stdout
        )

        if output.strip():
            print(
                "Output:\n",
                output,
            )

        else:
            break

        time.sleep(1)


def delete_observation_data(
    file_name,
    directory="/raw_data",
):
    """
    Delete one raw VLBA observation file.
    """

    file_path = (
        Path(directory)
        / file_name
    )

    if file_path.exists():
        file_path.unlink()

        print(
            "Successfully deleted "
            f"{file_name}"
        )

    else:
        print(
            f"File {file_name} "
            "does not exist."
        )


def get_folder_size(
    folder_path: Path,
):
    """
    Return total size of all files beneath folder_path.
    """

    if not folder_path.exists():
        raise FileNotFoundError(
            folder_path
        )

    return sum(
        path.stat().st_size
        for path
        in folder_path.rglob("*")
        if path.is_file()
    )