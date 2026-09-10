const eventSource = window.eventSource;


function dispatchRadarEvent(
    browserEventName,
    event
) {
    let data = {};

    try {
        data = JSON.parse(
            event.data || "{}"
        );

    } catch (error) {
        console.error(
            `[SSE] Could not parse ${event.type}:`,
            error
        );

        return;
    }

    console.log(
        `[SSE] ${event.type}:`,
        data
    );

    document.body.dispatchEvent(
        new CustomEvent(
            browserEventName,
            {
                detail: data
            }
        )
    );
}


const HOME_EVENTS = {
    status_changed:
        "statusChanged",

    gbt_changed:
        "gbtChanged",

    vlba_changed:
        "vlbaChanged",

    dsoc_changed:
        "dsocChanged",

    transfer_changed:
        "transferChanged",

    latency_changed:
        "latencyChanged",

    image_ready:
        "imageReady",
};


for (
    const [
        sseEvent,
        browserEvent
    ] of Object.entries(HOME_EVENTS)
) {
    eventSource.addEventListener(
        sseEvent,
        (event) => {
            dispatchRadarEvent(
                browserEvent,
                event
            );
        }
    );
}