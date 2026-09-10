import asyncio
import json
import logging
import os
import time

from datetime import datetime, timezone

from django.contrib import messages
from django.contrib.auth import authenticate, login, logout
from django.contrib.auth.decorators import login_not_required
from django.core.cache import cache
from django.db.models import Avg
from django.http import (
    HttpResponse,
    HttpResponseNotFound,
    JsonResponse,
    StreamingHttpResponse,
)
from django.shortcuts import (
    get_object_or_404,
    redirect,
    render,
)
from django.views.decorators.cache import cache_control
from django.views.decorators.http import require_GET, require_POST

from ngRadar_Website.enums import Message, Stations, Status
from ngRadar_Website.models.models import ObservatoryEvent
from ngRadar_Website.sse import sse_broker
from ngRadar_Website.utils import (
    bootstrap,
    create_s3_client,
    send_kafka_message,
    write_transfer_progress,
)


logger = logging.getLogger(__name__)


# ============================================================
# Constants
# ============================================================

RECORDS_TO_DISPLAY = 30
LAST_RECORDS = 5

PROGRESS_JSON_PATH = "/service/mock_assets/progress.json"


# ============================================================
# ObservatoryEvent query helpers
# ============================================================

def get_latest_station_event(station):
    """
    Return the latest persisted ObservatoryEvent for a station.
    Used only to establish initial page state.
    """

    return (
        ObservatoryEvent.objects
        .filter(station=station)
        .order_by("-event_time", "-uuid")
        .first()
    )


def get_latest_image_event():
    """
    Return the most recent event with a SeaweedFS image.
    """

    return (
        ObservatoryEvent.objects
        .exclude(image_key__isnull=True)
        .exclude(image_key="")
        .order_by("-event_time", "-uuid")
        .first()
    )


def get_current_waveform():
    """
    Return the most recently submitted waveform.

    UI_EVENT messages are now persisted to ObservatoryEvent
    by db_consumer, so there is no longer a uiEvent table.
    """

    event = (
        ObservatoryEvent.objects
        .filter(station=Stations.UI)
        .exclude(tx_waveform__isnull=True)
        .order_by("-event_time", "-uuid")
        .first()
    )

    return event.tx_waveform if event else None


def get_home_context():
    """
    Initial/fallback state for home.html.

    After initial page load, live operational updates should
    arrive through Kafka -> UI consumer -> SSE.
    """

    latest_event = (
        ObservatoryEvent.objects
        .order_by("-event_time", "-uuid")
        .first()
    )

    return {
        "latest_event": latest_event,
        "ui_event": get_latest_station_event(Stations.UI),
        "gbt_event": get_latest_station_event(Stations.GBT),
        "vlba_event": get_latest_station_event(Stations.HN),
        "dsoc_event": get_latest_station_event(Stations.DSOC),
        "current_waveform": get_current_waveform(),
        "latest_image_event": get_latest_image_event(),
    }


def get_dashboard_context():
    """
    Persisted history for dashboard.html.

    ObservatoryEvent is the only source of truth here.
    """

    latest_events = list(
        ObservatoryEvent.objects
        .order_by("-event_time", "-uuid")
        [:RECORDS_TO_DISPLAY]
    )

    avg_latency = (
        ObservatoryEvent.objects
        .exclude(latency_ms=0)
        .aggregate(avg=Avg("latency_ms"))
        ["avg"]
        or 0
    )

    latest_event = (
        latest_events[0]
        if latest_events
        else None
    )

    current_transfer_uuid = (
        latest_event.transfer_uuid
        if latest_event
        else None
    )

    transfer_events = []

    if current_transfer_uuid:
        transfer_events = list(
            ObservatoryEvent.objects
            .filter(
                transfer_uuid=current_transfer_uuid
            )
            .order_by("-event_time", "-uuid")
        )

    transferring_count = (
        ObservatoryEvent.objects
        .filter(
            transfer_uuid=current_transfer_uuid,
            status=Status.TRANSFERRING,
        )
        .count()
        if current_transfer_uuid
        else 0
    )

    return {
        "latest_events": latest_events,
        "latest_event": latest_event,
        "avg_latency": round(avg_latency, 2),
        "current_waveform": get_current_waveform(),
        "latest_image_event": get_latest_image_event(),
        "transfer_events": transfer_events,
        "transfer_resumed": transferring_count > 1,
    }


# ============================================================
# Main Kafka -> browser SSE stream
# ============================================================

async def sse_stream(request):
    """
    Single browser SSE connection.

    Events are published here by the website Kafka consumer.

    Examples:
        gbt_changed
        vlba_changed
        dsoc_changed
        status_changed
        observatory_event_created
        heartbeat
    """

    subscriber = sse_broker.subscribe()
    _, queue = subscriber

    async def event_generator():
        try:
            # Browser should reconnect after 3 seconds
            # if connection is interrupted.
            yield "retry: 3000\n\n"

            while True:
                try:
                    event = await asyncio.wait_for(
                        queue.get(),
                        timeout=15,
                    )

                    event_type = event["type"]
                    event_data = event.get(
                        "data",
                        {},
                    )

                    yield (
                        f"event: {event_type}\n"
                        f"data: {json.dumps(event_data)}\n\n"
                    )

                except asyncio.TimeoutError:
                    heartbeat = {
                        "timestamp": (
                            datetime.now(timezone.utc)
                            .isoformat()
                        )
                    }

                    yield (
                        "event: heartbeat\n"
                        f"data: {json.dumps(heartbeat)}\n\n"
                    )

        except asyncio.CancelledError:
            # Browser navigated away, refreshed,
            # or closed the connection.
            raise

        finally:
            sse_broker.unsubscribe(
                subscriber
            )

    response = StreamingHttpResponse(
        event_generator(),
        content_type="text/event-stream",
    )

    response["Cache-Control"] = "no-cache"
    response["X-Accel-Buffering"] = "no"

    return response


# ============================================================
# Latency data
# ============================================================

@require_GET
def latency_data(request):
    """
    Return persisted latency history.

    This does NOT need its own SSE connection.

    dashboard.html can request this endpoint whenever it receives
    observatory_event_created over the main SSE connection.
    """

    events = list(
        ObservatoryEvent.objects
        .exclude(tx_waveform="Tx_OFF")
        .exclude(latency_ms=0)
        .order_by("-event_time")
        [:RECORDS_TO_DISPLAY]
    )

    # Graph should read oldest -> newest.
    events.reverse()

    data = {
        "latency_array": [],
        "event_source_array": [],
        "event_metadata_array": [],
    }

    for event in events:
        station_short = (
            Stations(event.station).name
            if event.station is not None
            else "Unknown"
        )

        station_full = (
            event.get_station_display()
            if event.station is not None
            else "Unknown"
        )

        data["latency_array"].append(
            round(event.latency_ms, 3)
        )

        data["event_source_array"].append(
            station_short
        )

        data["event_metadata_array"].append({
            "station": station_full,
            "status": (
                event.get_status_display()
                if event.status is not None
                else "-"
            ),
            "time": (
                event.event_time.strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
                if event.event_time
                else "-"
            ),
            "object_id": (
                event.object_id
                or "-"
            ),
            "target": (
                event.target
                or "-"
            ),
        })

    return JsonResponse(data)


# ============================================================
# SeaweedFS image serving
# ============================================================

def serve_image(request, uuid):
    event = get_object_or_404(
        ObservatoryEvent,
        uuid=uuid,
    )

    if not event.image_key:
        return HttpResponseNotFound(
            "Image not available."
        )

    try:
        bucket = os.environ[
            "WEED_S3_BUCKET"
        ]

        s3 = create_s3_client()

        obj = s3.get_object(
            Bucket=bucket,
            Key=event.image_key,
        )

        return HttpResponse(
            obj["Body"].read(),
            content_type=obj.get(
                "ContentType",
                "image/png",
            ),
        )

    except Exception as exc:
        logger.exception(
            "Failed to retrieve image "
            "from SeaweedFS."
        )

        (
            producer_topic,
            producer_config,
            _,
            _,
        ) = bootstrap(
            Stations.DSOC
        )

        send_kafka_message(
            message_type=Message.STATUS_UPDATE,
            producer_topic=producer_topic,
            producer_config=producer_config,

            station=Stations.DSOC,
            status=Status.FAILED,

            gbt_uuid=event.gbt_uuid,
            transfer_uuid=event.transfer_uuid,

            object_id=event.object_id,
            target=event.target,

            tx_waveform=event.tx_waveform,
            rec_waveform=event.rec_waveform,

            product_type=event.product_type,
            product_id=event.product_id,

            image_key=event.image_key,

            xmit_station=event.xmit_station,
            rcvr_station=event.rcvr_station,

            message=(
                "Failed to retrieve DDM image "
                f"from SeaweedFS: {exc}"
            ),
        )

        return HttpResponse(
            "Unable to retrieve image.",
            status=503,
        )


# ============================================================
# Waveform submission lock
# ============================================================

@require_GET
def lock_status(request):
    """
    Submission lock fallback endpoint.

    A submission remains locked until a DSOC COMPLETED
    ObservatoryEvent occurs after the lock was created.

    The browser can also unlock immediately from live SSE.
    """

    try:
        lock_time = cache.get(
            "submit_locked"
        )

        if lock_time is None:
            return JsonResponse({
                "locked": False,
                "error": False,
            })

        observation_completed = (
            ObservatoryEvent.objects
            .filter(
                station=Stations.DSOC,
                status=Status.COMPLETED,
                event_time__gt=lock_time,
            )
            .exists()
        )

        if observation_completed:
            cache.delete(
                "submit_locked"
            )

            return JsonResponse({
                "locked": False,
                "error": False,
            })

        return JsonResponse({
            "locked": True,
            "error": False,
        })

    except Exception as exc:
        logger.exception(
            "Unable to determine "
            "waveform submission lock."
        )

        return JsonResponse(
            {
                "locked": True,
                "error": True,
                "message": (
                    "Unable to determine "
                    "lock status."
                ),
            },
            status=503,
        )


# ============================================================
# UI -> Kafka waveform submission
# ============================================================

@require_POST
def submit_waveform(request):
    """
    User waveform submission.

    No uiEvent database write occurs here.

    UI -> GBT_notif -> GBT
    """

    waveform = request.POST.get(
        "waveform"
    )

    if not waveform:
        messages.error(
            request,
            "Waveform is required.",
        )

        return redirect("home")

    producer_topic, producer_config = (
        bootstrap(Stations.UI)
    )

    event_uuid = send_kafka_message(
        message_type=Message.UI_EVENT,

        producer_topic=producer_topic,
        producer_config=producer_config,

        station=Stations.UI,

        tx_waveform=waveform,
        rec_waveform=waveform,

        message=(
            f"User submitted waveform "
            f"{waveform}."
        ),
    )

    if event_uuid is None:
        messages.error(
            request,
            "Unable to submit waveform.",
        )

        return redirect("home")

    # Existing e-transfer progress implementation.
    # This can eventually move to Kafka too.
    write_transfer_progress(
        received_bytes=0,
        total_bytes=0,
        percent=0.0,
        transfer_id=0,
    )

    cache.set(
        "submit_locked",
        datetime.now(timezone.utc),
    )

    return redirect("home")


# ============================================================
# Authentication
# ============================================================

@login_not_required
@cache_control(
    no_cache=True,
    must_revalidate=True,
    no_store=True,
    max_age=0,
)
def login_view(request):
    if request.user.is_authenticated:
        return redirect("home")

    if request.method == "POST":
        username_input = request.POST[
            "username"
        ]

        password_input = request.POST[
            "password"
        ]

        user = authenticate(
            request,
            username=username_input,
            password=password_input,
        )

        if user is not None:
            login(
                request,
                user,
            )

            return redirect("home")

        messages.error(
            request,
            "Invalid username or password.",
        )

    return render(
        request,
        "registration/login.html",
    )


@require_POST
def logout_view(request):
    logout(request)

    return redirect("login")


# ============================================================
# Full-page views
# ============================================================

@cache_control(
    no_cache=True,
    must_revalidate=True,
    no_store=True,
    max_age=0,
)
def home_view(request):
    """
    Initial Home render.

    DB provides initial/fallback state.
    Kafka/SSE provides live updates after page load.
    """

    return render(
        request,
        "ngRadar_Website/home.html",
        get_home_context(),
    )


@cache_control(
    no_cache=True,
    must_revalidate=True,
    no_store=True,
    max_age=0,
)
def dashboard_view(request):
    """
    Dashboard represents persisted ObservatoryEvent history.
    """

    return render(
        request,
        "ngRadar_Website/dashboard.html",
        get_dashboard_context(),
    )


# ============================================================
# HTMX partials
# ============================================================

@require_GET
def event_table_partial(request):
    """
    Refresh after observatory_event_created SSE event.

    This reads committed ObservatoryEvent rows only.
    """

    return render(
        request,
        (
            "ngRadar_Website/"
            "partials/dashboard_updates.html"
        ),
        get_dashboard_context(),
    )



# ============================================================
# e-transfer progress SSE
# ============================================================

@require_GET
def progress_sse(request):
    """
    Existing file-based e-transfer progress stream.

    This is independent of the main Kafka/SSE event stream.

    Future improvement:
        publish transfer progress through Kafka and remove
        progress.json + this second SSE connection.
    """

    if not os.path.exists(
        PROGRESS_JSON_PATH
    ):
        return HttpResponseNotFound(
            "Progress file not found"
        )

    def format_sse(
        event=None,
        data=None,
    ):
        output = ""

        if event:
            output += (
                f"event: {event}\n"
            )

        if data is not None:
            output += (
                f"data: {data}\n"
            )

        return output + "\n"

    def event_generator():
        last_seen = None
        completed_transfer_id = None

        while True:
            if not os.path.exists(
                PROGRESS_JSON_PATH
            ):
                time.sleep(0.5)
                continue

            try:
                with open(
                    PROGRESS_JSON_PATH,
                    "r",
                    encoding="utf-8",
                ) as progress_file:
                    payload = json.load(
                        progress_file
                    )

                received = payload.get(
                    "received_bytes",
                    0,
                )

                total = payload.get(
                    "total_bytes",
                    0,
                )

                percent = payload.get(
                    "percent",
                    0.0,
                )

                transfer_id = payload.get(
                    "transfer_id",
                    0,
                )

                if payload != last_seen:
                    last_seen = payload

                    yield format_sse(
                        data=json.dumps({
                            "received": received,
                            "total": total,
                            "percent": percent,
                            "transfer_id": (
                                transfer_id
                            ),
                        })
                    )

                if (
                    total > 0
                    and received >= total
                    and transfer_id
                    != completed_transfer_id
                ):
                    completed_transfer_id = (
                        transfer_id
                    )

                    yield format_sse(
                        event="done",
                        data=json.dumps({
                            "transfer_id": (
                                transfer_id
                            ),
                            "percent": percent,
                        }),
                    )

            except Exception as exc:
                logger.exception(
                    "Unable to read "
                    "e-transfer progress."
                )

                yield format_sse(
                    event="progress_error",
                    data=json.dumps({
                        "message": str(exc)
                    }),
                )

            time.sleep(0.2)

    response = StreamingHttpResponse(
        event_generator(),
        content_type="text/event-stream",
    )

    response["Cache-Control"] = "no-cache"
    response["X-Accel-Buffering"] = "no"

    return response


# ============================================================
# latency graph 
# ============================================================
@require_GET
def latency_data(request):
    database_events = (
        ObservatoryEvent.objects
        .exclude(tx_waveform="Tx_OFF")
        .order_by("-event_time")
        [:RECORDS_TO_DISPLAY]
    )

    latest_events = list(
        reversed(database_events)
    )

    latency_array = []
    event_source_array = []
    event_metadata_array = []

    for event in latest_events:
        latency_array.append(
            round(event.latency_ms, 3)
        )

        station_short = (
            Stations(event.station).name
            if event.station is not None
            else "Unknown"
        )

        station_full = (
            event.get_station_display()
            if event.station is not None
            else "Unknown"
        )

        event_source_array.append(
            station_short
        )

        event_metadata_array.append({
            "station": station_full,
            "status": (
                event.get_status_display()
                if event.status is not None
                else "-"
            ),
            "time": (
                event.event_time.strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
            ),
            "object_id": (
                event.object_id or "-"
            ),
            "target": (
                event.target or "-"
            ),
        })

    return JsonResponse({
        "latency_array": latency_array,
        "event_source_array":
            event_source_array,
        "event_metadata_array":
            event_metadata_array,
    })