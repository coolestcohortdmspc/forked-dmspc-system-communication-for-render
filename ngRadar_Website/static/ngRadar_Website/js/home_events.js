const eventSource = window.eventSource;


function dispatchRadarEvent(
    browserEventName,
    event
) {
    let data = {};

    if (event.data) {
        try {
            data = JSON.parse(
                event.data
            );
        } catch (error) {
            console.error(
                `[SSE] Could not parse ${event.type} payload:`,
                error
            );
        }
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

eventSource.addEventListener(
    "status_changed",
    (event) => {
        dispatchRadarEvent(
            "statusChanged",
            event
        );
    }
);

eventSource.addEventListener(
    "gbt_changed",
    (event) => {
        dispatchRadarEvent(
            "gbtChanged",
            event
        );
    }
);

eventSource.addEventListener(
    "vlba_changed",
    (event) => {
        dispatchRadarEvent(
            "vlbaChanged",
            event
        );
    }
);

eventSource.addEventListener(
    "dsoc_changed",
    (event) => {
        dispatchRadarEvent(
            "dsocChanged",
            event
        );
    }
);

eventSource.addEventListener(
    "transfer_changed",
    (event) => {
        dispatchRadarEvent(
            "transferChanged",
            event
        );
    }
);

eventSource.addEventListener(
    "latency_changed",
    (event) => {
        dispatchRadarEvent(
            "latencyChanged",
            event
        );
    }
);

eventSource.addEventListener(
    "image_ready",
    (event) => {
        dispatchRadarEvent(
            "imageReady",
            event
        );
    }
);