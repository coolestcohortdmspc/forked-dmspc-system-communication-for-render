// =========================================================
// Generic DOM helpers
// =========================================================

function setText(
    id,
    value,
    fallback = "-"
) {
    const element =
        document.getElementById(id);

    if (!element) {
        return;
    }

    element.textContent =
        value ?? fallback;
}


function setHidden(
    id,
    hidden
) {
    const element =
        document.getElementById(id);

    if (!element) {
        return;
    }

    element.classList.toggle(
        "d-none",
        hidden
    );
}


// =========================================================
// Global system status
// =========================================================

function updateSystemStatus(data) {
    const label =
        data.status_label
        ?? data.status_name
        ?? data.status
        ?? "Active";

    const message =
        data.message
        ?? "";

    setText(
        "system-status-label",
        label,
        ""
    );

    setText(
        "system-status-message",
        message,
        ""
    );
}


// =========================================================
// GBT panel
// =========================================================

function updateGbtPanel(event) {
    const data = event.detail;

    setText(
        "gbt-status",
        data.status_label
            ?? data.status_name
            ?? data.status
    );

    setText(
        "gbt-target",
        data.target
    );

    setText(
        "gbt-object-id",
        data.object_id
    );

    setText(
        "gbt-waveform",
        data.tx_waveform
    );

    setText(
        "gbt-recorded-waveform",
        data.rec_waveform
    );

    setText(
        "gbt-message",
        data.message
    );

    updateSystemStatus(
        data
    );
}


// =========================================================
// VLBA state
// =========================================================

function updateVlbaState(event) {
    const data = event.detail;

    // There is currently no dedicated VLBA panel
    // on home.html, but VLBA state still changes
    // the overall system status.
    updateSystemStatus(
        data
    );
}


// =========================================================
// DSOC panel
// =========================================================

function updateDsocPanel(event) {
    const data = event.detail;

    setText(
        "dsoc-message",
        data.message,
        ""
    );

    updateSystemStatus(
        data
    );

    // If this DSOC event represents a newly available
    // image, show it immediately.
    if (
        data.event_uuid
        && data.image_key
    ) {
        showDsocImage(
            data
        );
    }
}


function showDsocImage(data) {
    const image =
        document.getElementById(
            "dsoc-image"
        );

    if (!image) {
        return;
    }

    image.src =
        `/home/image/${data.event_uuid}/`;

    image.classList.remove(
        "d-none"
    );

    setHidden(
        "dsoc-image-empty",
        true
    );

    const heading =
        document.getElementById(
            "dsoc-image-heading"
        );

    if (heading) {
        heading.textContent =
            "DDM updated from DSOC";
    }
}


// =========================================================
// Waveform submission button
// =========================================================

function lockSubmitButton() {
    const button =
        document.getElementById(
            "waveform-submit-btn"
        );

    if (!button) {
        return;
    }

    button.disabled = true;
    button.textContent =
        "Generating ...";
}


function unlockSubmitButton() {
    const button =
        document.getElementById(
            "waveform-submit-btn"
        );

    if (!button) {
        return;
    }

    button.disabled = false;
    button.textContent =
        "Submit";
}

async function initializeSubmitLock() {
    const button =
        document.getElementById(
            "waveform-submit-btn"
        );

    if (!button) {
        return;
    }

    try {
        const response = await fetch(
            "/home/lock-status/"
        );

        if (!response.ok) {
            throw new Error(
                `HTTP ${response.status}`
            );
        }

        const data =
            await response.json();

        if (data.locked) {
            lockSubmitButton();
        } else {
            unlockSubmitButton();
        }

    } catch (error) {
        console.error(
            "[Home] Could not determine "
            + "initial submit lock:",
            error
        );

        button.disabled = true;
        button.textContent =
            "Lock unavailable";
    }
}


initializeSubmitLock();


// =========================================================
// Browser event listeners
// =========================================================

document.body.addEventListener(
    "gbtChanged",
    updateGbtPanel
);


document.body.addEventListener(
    "vlbaChanged",
    updateVlbaState
);


document.body.addEventListener(
    "dsocChanged",
    updateDsocPanel
);


document.body.addEventListener(
    "statusChanged",
    (event) => {
        updateSystemStatus(
            event.detail
        );
    }
);


document.body.addEventListener(
    "imageReady",
    (event) => {
        showDsocImage(
            event.detail
        );
    }
);


// A waveform submission starts a new operation.
// GBT activity confirms that processing has begun.

document.body.addEventListener(
    "gbtChanged",
    () => {
        lockSubmitButton();
    }
);


// Unlock once DSOC reports completion or failure.

document.body.addEventListener(
    "dsocChanged",
    (event) => {
        const data =
            event.detail;

        if (
            data.status_name === "COMPLETED"
            || data.status_name === "FAILED"
            || data.status === 8
            || data.status === 7
        ) {
            unlockSubmitButton();
        }
    }
);