"use strict";

/* =========================================================
   APPLICATION STATE
   ========================================================= */

let chain = [];
let snapshot = null;
let exposureChart = null;

let activeView = "gex";

let playbackTimer = null;
let playbackRunning = false;
let chainLoading = false;

const PLAYBACK_DELAY_MS = 1200;
const MARKET_OPEN_MINUTES = 9 * 60 + 15;
const MARKET_CLOSE_MINUTES = 15 * 60 + 30;


/* =========================================================
   DOM AND VALUE HELPERS
   ========================================================= */

const $ = (id) => document.getElementById(id);

const num = (value) => {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : 0;
};

const nullableNumber = (value) => {
    if (
        value === null ||
        value === undefined ||
        value === ""
    ) {
        return null;
    }

    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
};

const numbersEqual = (left, right) => {
    const leftNumber = nullableNumber(left);
    const rightNumber = nullableNumber(right);

    return (
        leftNumber !== null &&
        rightNumber !== null &&
        leftNumber === rightNumber
    );
};

const fmt = (value) => {
    const parsed = nullableNumber(value);

    if (parsed === null) {
        return "-";
    }

    return parsed.toLocaleString("en-IN", {
        maximumFractionDigits: 2,
    });
};

const oiL = (value) => {
    const parsed = nullableNumber(value);

    if (parsed === null) {
        return "-";
    }

    return (parsed / 100000).toFixed(2);
};

const signed = (value) => {
    const parsed = nullableNumber(value);

    if (parsed === null) {
        return "-";
    }

    const prefix = parsed > 0 ? "+" : "";

    return `${prefix}${(parsed / 100000).toFixed(2)}`;
};

const setText = (id, value) => {
    const element = $(id);

    if (element) {
        element.textContent = value;
    }
};

const setDisplay = (id, displayValue) => {
    const element = $(id);

    if (element) {
        element.style.display = displayValue;
    }
};


/* =========================================================
   GREEK FORMATTER
   ========================================================= */

function metricValue(row, side) {
    const metricSelect = $("greekMetric");

    if (!metricSelect) {
        return "-";
    }

    const metric = metricSelect.value;
    const key = `${side}_${metric}`;
    const value = nullableNumber(row?.[key]);

    if (value === null) {
        return "-";
    }

    if (metric === "gamma") {
        return value.toFixed(8);
    }

    return value.toFixed(4);
}


/* =========================================================
   API HELPER
   ========================================================= */

async function fetchJson(url, options = {}) {
    const response = await fetch(url, {
        cache: "no-store",
        ...options,
    });

    const contentType =
        response.headers.get("content-type") || "";

    let data;

    if (contentType.includes("application/json")) {
        data = await response.json();
    } else {
        const responseText = await response.text();

        throw new Error(
            responseText ||
            `Server returned HTTP ${response.status}`
        );
    }

    if (!response.ok || data?.ok === false) {
        throw new Error(
            data?.error ||
            data?.message ||
            `Request failed with HTTP ${response.status}`
        );
    }

    return data;
}


/* =========================================================
   DEFAULT VALUES
   ========================================================= */

async function loadDefaults() {
    const symbolElement = $("symbol");

    if (!symbolElement) {
        return;
    }

    const query = new URLSearchParams({
        dataset: symbolElement.value,
        _: String(Date.now()),
    });

    const data = await fetchJson(`/api/defaults?${query}`);

    const dateInput = $("queryDate");
    const timeInput = $("queryTime");

    if (dateInput && !dateInput.value) {
        dateInput.value = data.query_date || "";
    }

    if (timeInput && !timeInput.value) {
        timeInput.value = data.query_time || "09:30";
    }
}


/* =========================================================
   OPTION CHAIN LOADING
   ========================================================= */

function getChainRequestParameters() {
    return new URLSearchParams({
        dataset: $("symbol")?.value || "NIFTY",
        date: $("queryDate")?.value || "",
        time: $("queryTime")?.value || "09:30",
        interval: $("interval")?.value || "5",
        strike_count: $("strikeCount")?.value || "14",
        expiry: $("expirySelect")?.value || "",
        compute_greeks: "true",
        _: String(Date.now()),
    });
}

async function loadChain() {
    if (chainLoading) {
        return;
    }

    chainLoading = true;

    const refreshButton = $("refreshBtn");

    try {
        if (refreshButton) {
            refreshButton.disabled = true;
        }

        setPlaybackButtonsDisabled(true);

        const query = getChainRequestParameters();

        snapshot = await fetchJson(`/api/chain?${query}`);

        chain = Array.isArray(snapshot?.rows)
            ? snapshot.rows
            : [];

        renderHeader();
        renderExpiry();
        renderChain();
        renderLevels();
        renderExposure();
    } catch (error) {
        console.error("Unable to load option chain:", error);

        stopPlayback();

        alert(
            error instanceof Error
                ? error.message
                : "Unable to load option chain."
        );
    } finally {
        if (refreshButton) {
            refreshButton.disabled = false;
        }

        chainLoading = false;
        setPlaybackButtonsDisabled(false);
    }
}


/* =========================================================
   PLAYBACK CONTROLS
   ========================================================= */

function setPlaybackButtonsDisabled(disabled) {
    [
        "prevIntervalBtn",
        "nextIntervalBtn",
    ].forEach((id) => {
        const element = $(id);

        if (element) {
            element.disabled = disabled;
        }
    });
}

function timeToMinutes(value) {
    const [hoursText, minutesText] =
        String(value || "09:15").split(":");

    const hours = Number(hoursText);
    const minutes = Number(minutesText);

    return (
        (Number.isFinite(hours) ? hours : 9) * 60 +
        (Number.isFinite(minutes) ? minutes : 15)
    );
}

function minutesToTime(totalMinutes) {
    const safeMinutes = Math.max(
        0,
        Math.min(23 * 60 + 59, totalMinutes)
    );

    const hours = Math.floor(safeMinutes / 60);
    const minutes = safeMinutes % 60;

    return (
        `${String(hours).padStart(2, "0")}:` +
        `${String(minutes).padStart(2, "0")}`
    );
}

async function moveInterval(direction) {
    if (chainLoading) {
        return false;
    }

    const timeInput = $("queryTime");

    if (!timeInput) {
        return false;
    }

    const intervalValue = num($("interval")?.value);
    const step = Math.max(1, intervalValue);

    const currentMinutes =
        timeToMinutes(timeInput.value);

    const targetMinutes =
        currentMinutes + direction * step;

    if (targetMinutes < MARKET_OPEN_MINUTES) {
        timeInput.value =
            minutesToTime(MARKET_OPEN_MINUTES);

        return false;
    }

    if (targetMinutes > MARKET_CLOSE_MINUTES) {
        timeInput.value =
            minutesToTime(MARKET_CLOSE_MINUTES);

        stopPlayback();
        return false;
    }

    timeInput.value =
        minutesToTime(targetMinutes);

    await loadChain();

    return targetMinutes < MARKET_CLOSE_MINUTES;
}

function updatePlayButton() {
    const button = $("playPauseBtn");

    if (!button) {
        return;
    }

    button.textContent =
        playbackRunning ? "❚❚" : "▶";

    button.classList.toggle(
        "playing",
        playbackRunning
    );

    button.title = playbackRunning
        ? "Pause simulation"
        : "Play simulation";

    button.setAttribute(
        "aria-label",
        button.title
    );
}

function stopPlayback() {
    playbackRunning = false;

    if (playbackTimer) {
        clearTimeout(playbackTimer);
        playbackTimer = null;
    }

    updatePlayButton();
}

async function playbackTick() {
    if (!playbackRunning) {
        return;
    }

    try {
        const canContinue = await moveInterval(1);

        if (playbackRunning && canContinue) {
            playbackTimer = setTimeout(
                playbackTick,
                PLAYBACK_DELAY_MS
            );
        } else {
            stopPlayback();
        }
    } catch (error) {
        console.error("Playback failed:", error);
        stopPlayback();
    }
}

function togglePlayback() {
    if (playbackRunning) {
        stopPlayback();
        return;
    }

    playbackRunning = true;
    updatePlayButton();

    playbackTimer = setTimeout(
        playbackTick,
        150
    );
}


/* =========================================================
   HEADER
   ========================================================= */

function renderHeader() {
    if (!snapshot) {
        return;
    }

    setText("spotValue", fmt(snapshot.spot));
    setText("vixValue", fmt(snapshot.india_vix));
    setText("dteValue", fmt(snapshot.dte));
}


/* =========================================================
   EXPIRY DROPDOWN
   ========================================================= */

function renderExpiry() {
    const select = $("expirySelect");

    if (!select || !snapshot) {
        return;
    }

    const previousValue = select.value;
    const expiryList = Array.isArray(
        snapshot.available_expiries
    )
        ? snapshot.available_expiries
        : [];

    select.innerHTML = "";

    expiryList.forEach((expiryItem) => {
        const option =
            document.createElement("option");

        option.value = expiryItem.value;
        option.textContent = expiryItem.label;

        if (
            expiryItem.value ===
            (previousValue || snapshot.expiry)
        ) {
            option.selected = true;
        }

        select.appendChild(option);
    });

    if (
        !select.value &&
        snapshot.expiry
    ) {
        select.value = snapshot.expiry;
    }
}


/* =========================================================
   STRIKE LEVEL HELPERS
   ========================================================= */

function getStrikeLevel(row) {
    const levels = snapshot?.levels || {};
    const strike = nullableNumber(row?.strike);

    if (strike === null) {
        return {
            label: "",
            cellClass: "",
            badgeClass: "",
        };
    }

    if (numbersEqual(strike, levels.r2)) {
        return {
            label: "R2",
            cellClass: "resistance-level",
            badgeClass: "resistance-level",
        };
    }

    if (numbersEqual(strike, levels.r1)) {
        return {
            label: "R1",
            cellClass: "resistance-level",
            badgeClass: "resistance-level",
        };
    }

    if (
        row?.atm === true ||
        numbersEqual(strike, snapshot?.atm)
    ) {
        return {
            label: "ATM",
            cellClass: "atm-level",
            badgeClass: "atm-level",
        };
    }

    if (numbersEqual(strike, levels.s1)) {
        return {
            label: "S1",
            cellClass: "support-level",
            badgeClass: "support-level",
        };
    }

    if (numbersEqual(strike, levels.s2)) {
        return {
            label: "S2",
            cellClass: "support-level",
            badgeClass: "support-level",
        };
    }

    return {
        label: "",
        cellClass: "",
        badgeClass: "",
    };
}

function buildStrikeCell(row) {
    const level = getStrikeLevel(row);

    const badgeHtml = level.label
        ? `
            <span class="strike-level-badge ${level.badgeClass}">
                ${level.label}
            </span>
        `
        : `
            <span class="strike-level-placeholder"></span>
        `;

    return `
        <td class="strike-cell ${level.cellClass}">
            <div class="strike-cell-content">
                ${badgeHtml}

                <span class="strike-price">
                    ${fmt(row.strike)}
                </span>
            </div>
        </td>
    `;
}


/* =========================================================
   OPTION CHAIN TABLE
   ========================================================= */

function renderChain() {
    const body = $("chainBody");

    if (!body) {
        return;
    }

    body.innerHTML = "";

    const metric =
        $("greekMetric")?.value || "delta";

    setText(
        "ceGreekHead",
        metric.toUpperCase()
    );

    setText(
        "peGreekHead",
        metric.toUpperCase()
    );

    let totalCallOi = 0;
    let totalPutOi = 0;

    const fragment =
        document.createDocumentFragment();

    chain.forEach((row) => {
        totalCallOi += num(row.ce_oi);
        totalPutOi += num(row.pe_oi);

        const tableRow =
            document.createElement("tr");

        const isAtm =
            row.atm === true ||
            numbersEqual(
                row.strike,
                snapshot?.atm
            );

        if (isAtm) {
            tableRow.classList.add("atm");
        }

        const callChangeClass =
            num(row.ce_change_oi) >= 0
                ? "positive"
                : "negative";

        const putChangeClass =
            num(row.pe_change_oi) >= 0
                ? "positive"
                : "negative";

        tableRow.innerHTML = `
            <td>
                ${oiL(row.ce_oi)}
            </td>

            <td class="${callChangeClass}">
                ${signed(row.ce_change_oi)}
            </td>

            <td>
                ${fmt(row.ce_ltp)}
            </td>

            <td>
                ${
                    nullableNumber(row.ce_iv) === null
                        ? "-"
                        : fmt(num(row.ce_iv) * 100)
                }
            </td>

            <td class="positive">
                ${metricValue(row, "ce")}
            </td>

            ${buildStrikeCell(row)}

            <td class="negative">
                ${metricValue(row, "pe")}
            </td>

            <td>
                ${
                    nullableNumber(row.pe_iv) === null
                        ? "-"
                        : fmt(num(row.pe_iv) * 100)
                }
            </td>

            <td>
                ${fmt(row.pe_ltp)}
            </td>

            <td class="${putChangeClass}">
                ${signed(row.pe_change_oi)}
            </td>

            <td>
                ${oiL(row.pe_oi)}
            </td>
        `;

        fragment.appendChild(tableRow);
    });

    body.appendChild(fragment);

    setText(
        "callTotals",
        `Total Call OI: ${oiL(totalCallOi)} L`
    );

    setText(
        "putTotals",
        `Total Put OI: ${oiL(totalPutOi)} L`
    );
}


/* =========================================================
   LEVELS AND GEX SUMMARY

   R1, R2, ATM, S1 and S2 are now displayed inside the
   strike column. The external Spot marker is hidden.
   ========================================================= */

function renderLevels() {
    if (!snapshot) {
        return;
    }

    const levels = snapshot.levels || {};

    /*
     * Hide the separate right-side Spot marker.
     * Spot remains available in snapshot.spot for calculations.
     */
    setDisplay("spotMarker", "none");

    /*
     * Hide the separate Spot value in the levels summary,
     * when that element exists.
     */
    setDisplay("spotLevel", "none");

    /*
     * Hide the old external R/S markers as the labels are now
     * rendered directly inside the Strike column.
     *
     * Remove these four lines if you still want external
     * R1/R2/S1/S2 markers in addition to the strike labels.
     */
    setDisplay("r2Marker", "none");
    setDisplay("r1Marker", "none");
    setDisplay("s1Marker", "none");
    setDisplay("s2Marker", "none");

    setText("r2Value", fmt(levels.r2));
    setText("r1Value", fmt(levels.r1));
    setText("s1Value", fmt(levels.s1));
    setText("s2Value", fmt(levels.s2));

    setText(
        "gammaFlip",
        fmt(levels.gamma_flip)
    );

    setText(
        "zeroGamma",
        fmt(levels.zero_gamma)
    );

    setText(
        "maxPositive",
        fmt(levels.max_positive_gex)
    );

    setText(
        "maxNegative",
        fmt(levels.max_negative_gex)
    );

    setText(
        "callWall",
        fmt(
            levels.call_wall ??
            snapshot.call_wall
        )
    );

    setText(
        "putWall",
        fmt(
            levels.put_wall ??
            snapshot.put_wall
        )
    );

    const totalGex =
        levels.total_gex ??
        snapshot.total_net_gex ??
        snapshot.total_gex;

    setText("totalGex", fmt(totalGex));

    setText(
        "marketBias",
        num(totalGex) >= 0
            ? "Bullish / stabilising"
            : "Bearish / unstable"
    );
}


/* =========================================================
   EXPOSURE CALCULATIONS
   ========================================================= */

function exposures() {
    if (!snapshot) {
        return [];
    }

    const lotSize = num(snapshot.lot_size);
    const spot = num(snapshot.spot);

    return chain.map((row) => {
        let value = 0;

        if (activeView === "gex") {
            value = num(row.net_gex);
        }

        if (activeView === "dex") {
            value = (
                num(row.ce_delta) *
                num(row.ce_oi) +
                num(row.pe_delta) *
                num(row.pe_oi)
            ) * lotSize * spot;
        }

        if (activeView === "vex") {
            value = (
                num(row.ce_vega) *
                num(row.ce_oi) +
                num(row.pe_vega) *
                num(row.pe_oi)
            ) * lotSize;
        }

        if (activeView === "tex") {
            value = (
                num(row.ce_theta) *
                num(row.ce_oi) +
                num(row.pe_theta) *
                num(row.pe_oi)
            ) * lotSize;
        }

        return {
            strike: row.strike,
            value,
        };
    });
}


/* =========================================================
   EXPOSURE CHART
   ========================================================= */

function renderExposure() {
    const canvas = $("exposureChart");

    if (!canvas || typeof Chart === "undefined") {
        return;
    }

    const titles = {
        gex: "Gamma Exposure (GEX)",
        dex: "Delta Exposure (DEX)",
        vex: "Vega Exposure (VEX)",
        tex: "Theta Exposure (TEX)",
    };

    const currentTitle =
        titles[activeView] || titles.gex;

    setText("chartTitle", currentTitle);

    const rows = exposures();

    if (exposureChart) {
        exposureChart.destroy();
        exposureChart = null;
    }

    exposureChart = new Chart(canvas, {
        type: "bar",

        data: {
            labels: rows.map(
                (row) => row.strike
            ),

            datasets: [
                {
                    label: currentTitle,

                    data: rows.map(
                        (row) => row.value
                    ),

                    backgroundColor: rows.map(
                        (row) =>
                            row.value >= 0
                                ? "rgba(13,160,77,.9)"
                                : "rgba(240,40,60,.9)"
                    ),

                    borderWidth: 0,
                },
            ],
        },

        options: {
            responsive: true,
            maintainAspectRatio: false,

            animation: {
                duration: 250,
            },

            plugins: {
                legend: {
                    display: false,
                },

                tooltip: {
                    callbacks: {
                        label: (context) =>
                            `${currentTitle}: ${fmt(context.raw)}`,
                    },
                },
            },

            scales: {
                x: {
                    title: {
                        display: true,
                        text: "Strike Price",
                    },

                    grid: {
                        display: false,
                    },

                    ticks: {
                        autoSkip: true,
                        maxRotation: 45,
                        minRotation: 0,
                    },
                },

                y: {
                    title: {
                        display: true,
                        text: "Exposure",
                    },

                    grid: {
                        color: "#e8edf3",
                    },
                },
            },
        },
    });
}


/* =========================================================
   EVENT LISTENERS
   ========================================================= */

function registerEventListeners() {
    $("refreshBtn")?.addEventListener(
        "click",
        () => {
            stopPlayback();
            loadChain();
        }
    );

    $("prevIntervalBtn")?.addEventListener(
        "click",
        () => {
            stopPlayback();
            moveInterval(-1);
        }
    );

    $("nextIntervalBtn")?.addEventListener(
        "click",
        () => {
            stopPlayback();
            moveInterval(1);
        }
    );

    $("playPauseBtn")?.addEventListener(
        "click",
        togglePlayback
    );

    $("queryTime")?.addEventListener(
        "change",
        () => {
            stopPlayback();
            loadChain();
        }
    );

    $("queryDate")?.addEventListener(
        "change",
        () => {
            stopPlayback();
            loadChain();
        }
    );

    $("interval")?.addEventListener(
        "change",
        () => {
            stopPlayback();
            loadChain();
        }
    );

    $("strikeCount")?.addEventListener(
        "change",
        () => {
            stopPlayback();
            loadChain();
        }
    );

    $("greekMetric")?.addEventListener(
        "change",
        renderChain
    );

    $("expirySelect")?.addEventListener(
        "change",
        () => {
            stopPlayback();
            loadChain();
        }
    );

    $("symbol")?.addEventListener(
        "change",
        async () => {
            stopPlayback();

            try {
                const expirySelect =
                    $("expirySelect");

                if (expirySelect) {
                    expirySelect.value = "";
                }

                await loadDefaults();
                await loadChain();
            } catch (error) {
                console.error(
                    "Symbol change failed:",
                    error
                );

                alert(
                    error instanceof Error
                        ? error.message
                        : "Unable to change instrument."
                );
            }
        }
    );

    document
        .querySelectorAll(
            ".exposure-tabs button"
        )
        .forEach((button) => {
            button.addEventListener(
                "click",
                () => {
                    document
                        .querySelectorAll(
                            ".exposure-tabs button"
                        )
                        .forEach((item) => {
                            item.classList.remove(
                                "active"
                            );
                        });

                    button.classList.add("active");

                    activeView =
                        button.dataset.view || "gex";

                    renderExposure();
                }
            );
        });
}


/* =========================================================
   APPLICATION INITIALIZATION
   ========================================================= */

async function initializeApplication() {
    registerEventListeners();
    updatePlayButton();

    try {
        await loadDefaults();
        await loadChain();
    } catch (error) {
        console.error(
            "Application initialization failed:",
            error
        );

        alert(
            error instanceof Error
                ? error.message
                : "Unable to initialize the simulator."
        );
    }
}

document.addEventListener(
    "DOMContentLoaded",
    initializeApplication
);
