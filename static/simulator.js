let chain = [];
let legs = [];
let chart = null;

let appDefaults = null;

let fullscreenChart = null;
let fullscreenSeries = null;

let futureChart = null;
let futureSeries = null;

let vixChart = null;
let vixSeries = null;
let volSmileChart = null;
let ivSurfaceChart = null;
let pinnedIvPoint = null;
let ivSurfaceRequestGeneration = 0;


let activeIndicatorChart = null;

const advancedCharts = {
  index: {
    chart: null,
    candleSeries: null,
    rows: [],
    indicators: []
  },

  future: {
    chart: null,
    candleSeries: null,
    rows: [],
    indicators: []
  },

  vix: {
    chart: null,
    candleSeries: null,
    rows: [],
    indicators: []
  },

  option: {
    chart: null,
    candleSeries: null,
    rows: [],
    indicators: []
  }
};

const DEFAULT_DATASET = "NIFTY";
const DEFAULT_TIME = "09:15";
const DEFAULT_INTERVAL = 1;
const DEFAULT_STRIKE_COUNT = 10;

function fmtMoney(x) {
  if (typeof x === "string") return x;
  return "₹" + Number(x || 0).toLocaleString("en-IN", {
    maximumFractionDigits: 0
  });
}

function getEl(id) {
  return document.getElementById(id);
}

function getDataset() {
  const select = getEl("dataset") || getEl("underlying") || getEl("symbol");
  const value = select ? select.value : DEFAULT_DATASET;
  const text = String(value || DEFAULT_DATASET).toUpperCase();

  if (text === "BSE" || text === "SENSEX") return "SENSEX";
  if (text === "BANKNIFTY" || text === "BANK NIFTY") return "BANKNIFTY";

  return "NIFTY";
}

function getFixedQty() {
  return getDataset() === "SENSEX" ? 20 : 65;
}

function getSpot() {
  return Number(getEl("spot")?.value || 0);
}

function getIV() {
  return Number(getEl("iv")?.value || 20);
}

function getDays() {
  return Number(getEl("days")?.value || 1);
}

function getQueryDate() {
  const dateInput = getEl("queryDate") || getEl("date") || getEl("query_date");
  if (dateInput && dateInput.value) return dateInput.value;
  if (appDefaults && appDefaults.query_date) return appDefaults.query_date;
  return "2026-03-30";
}

function getQueryTime() {
  const timeInput = getEl("queryTime") || getEl("time") || getEl("query_time");
  if (timeInput && timeInput.value) return timeInput.value;
  if (appDefaults && appDefaults.query_time) return appDefaults.query_time;
  return DEFAULT_TIME;
}


function getInterval() {
  const intervalInput = getEl("interval") || getEl("candleInterval");
  const value = Number(intervalInput?.value || DEFAULT_INTERVAL);
  return value > 0 ? value : DEFAULT_INTERVAL;
}
function getStrikeCount() {
  const input = getEl("strikeCount");
  const value = Number(input?.value || DEFAULT_STRIKE_COUNT);
  return value > 0 ? Math.floor(value) : DEFAULT_STRIKE_COUNT;
}

function parseExpiryValue(expiryValue) {
  const value = String(expiryValue || "").trim();

  if (!/^\d{6}$/.test(value)) {
    return null;
  }

  const year = 2000 + Number(value.slice(0, 2));
  const month = Number(value.slice(2, 4)) - 1;
  const day = Number(value.slice(4, 6));

  const expiryDate = new Date(year, month, day);

  return Number.isNaN(expiryDate.getTime())
    ? null
    : expiryDate;
}


function isNiftyMonthlyExpiry(expiryValue) {
  const selectedDate = parseExpiryValue(expiryValue);

  if (!selectedDate) {
    return false;
  }

  const expirySelect = getEl("expirySelect");

  if (!expirySelect) {
    return false;
  }

  const sameMonthExpiries = Array.from(expirySelect.options)
    .map(option => ({
      value: option.value,
      date: parseExpiryValue(option.value)
    }))
    .filter(item =>
      item.date &&
      item.date.getFullYear() === selectedDate.getFullYear() &&
      item.date.getMonth() === selectedDate.getMonth()
    )
    .sort((a, b) => a.date - b.date);

  if (!sameMonthExpiries.length) {
    return false;
  }

  const monthlyExpiry =
    sameMonthExpiries[sameMonthExpiries.length - 1].value;

  return String(expiryValue) === String(monthlyExpiry);
}


function getStrikeStep(expiryValue = null) {
  const dataset = getDataset();

  if (dataset === "BANKNIFTY" || dataset === "SENSEX") {
    return 100;
  }

  if (
    dataset === "NIFTY" &&
    isNiftyMonthlyExpiry(expiryValue)
  ) {
    return 100;
  }

  return 50;
}


function normalizeStrike(strike, expiryValue = null) {
  const value = Number(strike);

  if (!Number.isFinite(value)) {
    return 0;
  }

  const step = getStrikeStep(expiryValue);

  return Math.round(value / step) * step;
}

function setSpot(value) {
  const spotInput = getEl("spot");
  if (spotInput && Number.isFinite(Number(value))) {
    spotInput.value = Number(value).toFixed(0);
  }
}

function addMinutesToTime(timeValue, minutesToAdd) {
  const [hh, mm] = String(timeValue || DEFAULT_TIME).split(":").map(Number);
  const d = new Date();

  d.setHours(Number.isFinite(hh) ? hh : 9, Number.isFinite(mm) ? mm : 30, 0, 0);
  d.setMinutes(d.getMinutes() + minutesToAdd);

  return `${String(d.getHours()).padStart(2, "0")}:${String(d.getMinutes()).padStart(2, "0")}`;
}

function moveNextInterval() {
  const timeInput = getEl("queryTime");
  if (!timeInput) return;

  timeInput.value = addMinutesToTime(
    timeInput.value || DEFAULT_TIME,
    getInterval()
  );

  loadChain(true);
}

async function movePreviousInterval() {
  const btn = getEl("prevIntervalBtn");
  const timeInput = getEl("queryTime");
  const dateInput = getEl("queryDate");

  if (!timeInput || !dateInput) return;
  if (btn) btn.disabled = true;

  try {
    const currentTime = timeInput.value || DEFAULT_TIME;
    const nextTime = addMinutesToTime(currentTime, -getInterval());

    if (nextTime >= "09:15") {
      timeInput.value = nextTime;
      await loadChain(true);
      return;
    }

    const params = new URLSearchParams({
      dataset: getDataset(),
      date: getEl("indexChartStartDate")?.value || getQueryDate(),
      end_date: getEl("indexChartEndDate")?.value || getQueryDate(),
      time: getEl("indexChartEndTime")?.value || getQueryTime(),
      interval: getEl("indexChartInterval")?.value || String(getInterval()),
      _: String(Date.now())
    });

    const data = await fetchJson(
      `/api/previous-trading-session?${params.toString()}`
    );

    dateInput.value = data.date;
    timeInput.value = data.time || "15:30";

    legs = [];
    renderLegs();
    calculate();

    await loadChain(true);

  } catch (err) {
    alert(err.message || "Previous trading day not found");
  } finally {
    if (btn) btn.disabled = false;
  }
}

async function fetchJson(url, options = {}) {
  const res = await fetch(url, options);
  const data = await res.json().catch(() => null);

  if (!res.ok) {
    throw new Error(data?.error || `Request failed: ${res.status}`);
  }

  return data;
}

function ensureExpiryWarning() {
  const simPanel = document.querySelector(".sim-panel");
  const tabs = document.querySelector(".sim-panel .tabs");

  if (!simPanel || !tabs || getEl("expiryWarning")) return;

  const warning = document.createElement("div");
  warning.id = "expiryWarning";
  warning.className = "expiry-warning";
  warning.textContent =
    "⚠ Payoff analysis is for same-expiry strategies only and is valid only at the time of expiry only. Calendar and diagonal spreads are not supported.";

  tabs.insertAdjacentElement("afterend", warning);
}

function ensureMetricsCards() {
  const metrics = document.querySelector(".metrics");
  if (!metrics) return;

  if (!getEl("tradeCost")) {
    const div = document.createElement("div");
    div.innerHTML = `<span>Charges</span><b id="tradeCost">₹0</b>`;
    metrics.insertBefore(div, getEl("maxProfit")?.closest("div") || null);
  }

  if (!getEl("netProfitAfterCost")) {
    const div = document.createElement("div");
    div.innerHTML = `<span>Net P&L After Charges</span><b id="netProfitAfterCost">₹0</b>`;
    metrics.insertBefore(div, getEl("maxProfit")?.closest("div") || null);
  }

  if (!getEl("marginRequired")) {
    const div = document.createElement("div");
    div.innerHTML = `<span>Margin</span><b id="marginRequired">₹0</b>`;
    metrics.insertBefore(div, getEl("maxProfit")?.closest("div") || null);
  }
}

function ensurePositionsHeaders() {
  const legsBody = getEl("legsBody");
  if (!legsBody) return;

  const table = legsBody.closest("table");
  if (!table) return;

  const headerRow = table.querySelector("thead tr");
  if (!headerRow) return;

  headerRow.innerHTML = `
    <th>Side</th>
    <th>Type</th>
    <th>Strike</th>
    <th>Expiry</th>
    <th>Entry Time</th>
    <th>Premium</th>
    <th>Lots</th>
    <th>Qty</th>
    <th></th>
  `;
}

async function loadDefaults() {
  ensureExpiryWarning();
  ensureMetricsCards();
  ensurePositionsHeaders();

  const data = await fetchJson(`/api/defaults?dataset=${encodeURIComponent(getDataset())}`);

  if (!data.ok) {
    throw new Error(data.error || "Could not load defaults");
  }

  appDefaults = data;

  const dateInput = getEl("queryDate");
  if (dateInput && !dateInput.value) dateInput.value = data.query_date;

  const timeInput = getEl("queryTime");
  if (timeInput && !timeInput.value) timeInput.value = data.query_time || DEFAULT_TIME;

  return data;
}

function setRefreshLoading(isLoading) {
  const refreshBtn = getEl("refreshBtn");

  if (!refreshBtn) return;

  if (!refreshBtn.dataset.originalText) {
    refreshBtn.dataset.originalText =
      refreshBtn.innerHTML || "Refresh";
  }

  refreshBtn.disabled = Boolean(isLoading);

  if (isLoading) {
    refreshBtn.classList.add("is-loading");
    refreshBtn.innerHTML = `
      <span class="button-spinner"></span>
      Loading...
    `;
  } else {
    refreshBtn.classList.remove("is-loading");
    refreshBtn.innerHTML =
      refreshBtn.dataset.originalText || "Refresh";
  }
}

async function loadChain(
  forceReload = false,
  selectNearestExpiry = false
) {
  try {
    ensureExpiryWarning();
    ensureMetricsCards();
    ensurePositionsHeaders();

    const expirySelect = getEl("expirySelect");

    /*
     * When selectNearestExpiry is true, do not send the
     * previously selected expiry. This allows the backend
     * to select the nearest valid expiry for the new date.
     */
    const requestedExpiry = selectNearestExpiry
      ? ""
      : expirySelect?.value || "";

    const params = new URLSearchParams({
      dataset: getDataset(),
      date: getQueryDate(),
      time: getQueryTime(),
      interval: String(getInterval()),
      strike_count: String(getStrikeCount()),
      expiry_rule: "current expiry",
      expiry: requestedExpiry,
      _: String(Date.now())
    });

    const data = await fetchJson(
      `/api/chain?${params.toString()}`
    );

    if (!data.ok) {
      throw new Error(
        data.error || "Option chain failed"
      );
    }

    chain = Array.isArray(data.rows)
      ? data.rows
      : [];

    const warning = getEl("expiryWarning");

    if (
      warning &&
      data.query_date &&
      data.expiry_label
    ) {
      const expiryType =
        getDataset() === "BANKNIFTY"
          ? "Monthly expiry"
          : "Selected expiry";

      warning.textContent =
        `⚠ ${expiryType} payoff is calculated for ` +
        `${data.expiry_label}. Payoff is valid only at ` +
        `expiry. Calendar and diagonal spreads are not supported.`;
    }

    if (Number.isFinite(Number(data.spot))) {
      setSpot(data.spot);
    }

    if (
      Number.isFinite(
        Number(data.india_vix)
      )
    ) {
      const ivInput = getEl("iv");

      if (ivInput) {
        ivInput.value =
          Number(data.india_vix).toFixed(1);
      }
    }

    const dteInput = getEl("days");

    if (
      dteInput &&
      Number.isFinite(Number(data.dte))
    ) {
      dteInput.value = Number(data.dte);
    }

    const expiryInput =
      getEl("expiryLabel");

    if (expiryInput) {
      expiryInput.value =
        data.expiry_label || "";
    }

    /*
     * Update expiry dropdown.
     *
     * Normal refresh:
     *   preserve the user's selected expiry when it is
     *   still available.
     *
     * Date change:
     *   select the expiry returned by the backend,
     *   which should be the nearest valid expiry.
     */
    if (
      expirySelect &&
      Array.isArray(data.available_expiries)
    ) {
      const previousExpiry =
        expirySelect.value || "";

      expirySelect.innerHTML = "";

      data.available_expiries.forEach(
        expiry => {
          const option =
            document.createElement("option");

          option.value =
            String(expiry.value || "");

          option.textContent =
            expiry.label ||
            expiry.value ||
            "";

          expirySelect.appendChild(option);
        }
      );

      const availableValues =
        Array.from(expirySelect.options)
          .map(option => option.value);

      if (
        !selectNearestExpiry &&
        previousExpiry &&
        availableValues.includes(
          previousExpiry
        )
      ) {
        /*
         * Manual refresh:
         * keep the previously selected expiry.
         */
        expirySelect.value =
          previousExpiry;
      } else if (
        data.expiry &&
        availableValues.includes(
          String(data.expiry)
        )
      ) {
        /*
         * Date changed:
         * select the nearest expiry chosen by backend.
         */
        expirySelect.value =
          String(data.expiry);
      } else if (
        expirySelect.options.length > 0
      ) {
        /*
         * Fallback:
         * first available expiry.
         */
        expirySelect.selectedIndex = 0;
      }
    }

    /*
     * Keep the existing positions, but update the fixed
     * quantity according to the currently selected dataset.
     */
    legs = legs.map(leg => ({
      ...leg,
      qty: getFixedQty()
    }));

    renderChain();
    renderVolSmile();
    renderLegs();

    await calculate();

    return data;

  } catch (err) {
    console.error(
      "loadChain error:",
      err
    );

    chain = [];

    renderChain();

    alert(
      err.message ||
      "Failed to load option chain"
    );

    return null;
  }
}


let optionMetricChart = null;
let optionMetricSeries = null;

async function openOptionMetricChart(event, strike, metric) {
  event.preventDefault();

  const modal = getEl("chartModal");
  const container = getEl("fullscreenIndexChart");

  if (!modal || !container || typeof LightweightCharts === "undefined") {
    alert("Chart modal not found");
    return;
  }

    modal.style.display = "block";

    const startDate = getEl("indexChartStartDate");
    const startTime = getEl("indexChartStartTime");
    const endDate = getEl("indexChartEndDate");
    const endTime = getEl("indexChartEndTime");
    const intervalSelect = getEl("indexChartInterval");

    const applyBtn = getEl("indexChartApplyBtn");

    if (applyBtn) {
      applyBtn.dataset.chart = "option";
      applyBtn.dataset.strike = String(strike);
      applyBtn.dataset.metric = metric;
    }

  if (startDate && !startDate.value) startDate.value = getQueryDate();
  if (startTime && !startTime.value) startTime.value = "09:15";

  if (endDate && !endDate.dataset.userChanged) endDate.value = getQueryDate();
  if (endTime && !endTime.dataset.userChanged) endTime.value = getQueryTime();

  if (startTime && !startTime.dataset.changeAttached) {
    startTime.addEventListener("change", () => {
      startTime.dataset.userChanged = "true";
    });
    startTime.dataset.changeAttached = "true";
  }

  if (endDate && !endDate.dataset.changeAttached) {
    endDate.addEventListener("change", () => {
      endDate.dataset.userChanged = "true";
    });
    endDate.dataset.changeAttached = "true";
  }

  if (endTime && !endTime.dataset.changeAttached) {
    endTime.addEventListener("change", () => {
      endTime.dataset.userChanged = "true";
    });
    endTime.dataset.changeAttached = "true";
  }

  const isLtp = metric === "ce_ltp" || metric === "pe_ltp";
  const side = metric === "ce_ltp" ? "CE" : "PE";

  const chartTitle = modal.querySelector("h2");
  const expiryText =
    getEl("expirySelect")?.selectedOptions?.[0]?.textContent ||
    getEl("expirySelect")?.value ||
    "";

  if (chartTitle) {
    if (metric === "ce_ltp") {
      chartTitle.textContent = `CE ${strike} ${expiryText} LTP Chart`;
    } else if (metric === "pe_ltp") {
      chartTitle.textContent = `PE ${strike} ${expiryText} LTP Chart`;
    } else {
      chartTitle.textContent = `${String(metric).toUpperCase()} ${strike} Chart`;
    }
  }

  let data;

  if (isLtp) {
    const candleParams = new URLSearchParams({
      dataset: getDataset(),
      date: startDate?.value || getQueryDate(),
      start_time: startTime?.value || "09:15",
      end_date: endDate?.value || getQueryDate(),
      end_time: endTime?.value || getQueryTime(),
      interval: intervalSelect?.value || String(getInterval()),
      strike: String(strike),
      side: side,
      expiry: getEl("expirySelect")?.value || "",
      _: String(Date.now())
    });

    data = await fetchJson(`/api/option-ltp-candle-chart?${candleParams.toString()}`);
  } else {
    const params = new URLSearchParams({
      dataset: getDataset(),
      date: startDate?.value || getQueryDate(),
      start_time: startTime?.value || "09:15",
      end_date: endDate?.value || getQueryDate(),
      end_time: endTime?.value || getQueryTime(),
      interval: intervalSelect?.value || String(getInterval()),
      strike: String(strike),
      metric: metric,
      expiry: getEl("expirySelect")?.value || "",
      _: String(Date.now())
    });

    data = await fetchJson(`/api/option-metric-chart?${params.toString()}`);
  }

  if (!data.ok || !data.rows || !data.rows.length) {
    alert("No chart data found");
    return;
  }

  if (fullscreenChart) {
    fullscreenChart.remove();
    fullscreenChart = null;
    fullscreenSeries = null;
  }

  container.innerHTML = "";

  fullscreenChart = LightweightCharts.createChart(container, {
    width: container.clientWidth,
    height: container.clientHeight,
    layout: {
      background: { color: "#ffffff" },
      textColor: "#222"
    },
    grid: {
      vertLines: { color: "#eef2f7" },
      horzLines: { color: "#eef2f7" }
    },
    crosshair: {
      mode: LightweightCharts.CrosshairMode.Normal
    },
    timeScale: {
      timeVisible: true,
      secondsVisible: false
    }
  });

  if (isLtp) {
    fullscreenSeries = fullscreenChart.addCandlestickSeries({
      upColor: "#16a34a",
      downColor: "#dc2626",
      borderUpColor: "#16a34a",
      borderDownColor: "#dc2626",
      wickUpColor: "#16a34a",
      wickDownColor: "#dc2626"
    });

    fullscreenSeries.setData(data.rows || []);

    advancedCharts.option.chart = fullscreenChart;
    advancedCharts.option.candleSeries = fullscreenSeries;
    advancedCharts.option.rows = data.rows || [];

    if (!Array.isArray(advancedCharts.option.indicators)) {
      advancedCharts.option.indicators = [];
    }
    advancedCharts.option.indicators.forEach(indicator => { indicator.series = []; });
    redrawChartIndicators("option");

    attachOhlcBox(
      fullscreenChart,
      fullscreenSeries,
      container,
      "optionOhlcBox"
    );
  } else {
    fullscreenSeries = fullscreenChart.addLineSeries({
      lineWidth: 2
    });

    fullscreenSeries.setData(
      data.rows.map(r => ({
        time: r.time,
        value: r.value
      }))
    );
  }

  fullscreenChart.timeScale().fitContent();
}
function renderChain() {
  const body = getEl("chainBody");
  if (!body) return;

  body.innerHTML = "";

  if (!chain.length) {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td colspan="7" style="text-align:center;padding:14px;color:#777;">
        No option chain data loaded
      </td>
    `;
    body.appendChild(tr);
    return;
  }

  for (const row of chain) {
    const ceLtp = Number(row.ce_ltp || 0);
    const peLtp = Number(row.pe_ltp || 0);

    const tr = document.createElement("tr");
    if (row.atm) tr.classList.add("atm");

    tr.innerHTML = `
  <td class="clickable"
      onclick="addLeg('BUY','CE',${row.strike},${ceLtp})"
      oncontextmenu="openOptionMetricChart(event, ${row.strike}, 'ce_ltp')">
      ${row.ce_ltp ?? "-"}
  </td>

  <td class="clickable"
      oncontextmenu="openOptionMetricChart(event, ${row.strike}, 'ce_delta')">
      ${row.ce_delta ?? "-"}
  </td>

  <td class="clickable"
      oncontextmenu="openOptionMetricChart(event, ${row.strike}, 'ce_iv')">
      ${row.ce_iv ?? "-"}
  </td>

  <td>
    <b>${row.strike}</b>
    ${row.atm ? '<span class="atm-badge">ATM</span>' : ""}
  </td>

  <td class="clickable"
      oncontextmenu="openOptionMetricChart(event, ${row.strike}, 'pe_iv')">
      ${row.pe_iv ?? "-"}
  </td>

  <td class="clickable"
      oncontextmenu="openOptionMetricChart(event, ${row.strike}, 'pe_delta')">
      ${row.pe_delta ?? "-"}
  </td>

  <td class="clickable"
      onclick="addLeg('BUY','PE',${row.strike},${peLtp})"
      oncontextmenu="openOptionMetricChart(event, ${row.strike}, 'pe_ltp')">
      ${row.pe_ltp ?? "-"}
  </td>
`;

    body.appendChild(tr);
  }
}

function renderVolSmile() {
  const canvas = getEl("volSmileChart");
  const body = getEl("volSmileBody");

  if (!canvas || !body || typeof Chart === "undefined") return;

  const normalizeIv = value => {
    const n = Number(value);
    if (!Number.isFinite(n)) return null;

    // Backend may send IV as 0.1925 or 19.25
    return n <= 1 ? n * 100 : n;
  };

  const atmFromChain = chain.find(r => r.atm);
  const atmStrike = atmFromChain ? Number(atmFromChain.strike) : null;

  const getSmileIv = row => {
    const ce = normalizeIv(row.ce_iv);
    const pe = normalizeIv(row.pe_iv);
    const strike = Number(row.strike);

    if (!atmStrike || !Number.isFinite(strike)) return null;

    // OTM put side
    if (strike < atmStrike) {
      return pe;
    }

    // OTM call side
    if (strike > atmStrike) {
      return ce;
    }

    // ATM = average of CE IV and PE IV
    if (ce !== null && pe !== null) {
      return (ce + pe) / 2;
    }

    return ce ?? pe;
  };

  const rows = chain
    .filter(r => r.strike && getSmileIv(r) !== null)
    .sort((a, b) => Number(a.strike) - Number(b.strike));

  body.innerHTML = "";

  rows.forEach(row => {
    const cePct = normalizeIv(row.ce_iv);
    const pePct = normalizeIv(row.pe_iv);
    const smilePct = getSmileIv(row);

    const tr = document.createElement("tr");
    if (row.atm) tr.classList.add("atm");

    tr.innerHTML = `
      <td><b>${row.strike}</b>${row.atm ? ' <span class="atm-badge">ATM</span>' : ""}</td>
      <td>${cePct !== null ? cePct.toFixed(2) : "-"}</td>
      <td>${pePct !== null ? pePct.toFixed(2) : "-"}</td>
      <td>${smilePct !== null ? smilePct.toFixed(2) : "-"}</td>
    `;

    body.appendChild(tr);
  });

  if (volSmileChart) volSmileChart.destroy();

  const atmLinePlugin = {
    id: "atmLine",
    afterDraw(chart) {
      if (!atmStrike) return;

      const xScale = chart.scales.x;
      const yScale = chart.scales.y;

      const atmIndex = rows.findIndex(
        r => Number(r.strike) === atmStrike
      );

      if (atmIndex < 0) return;

      const x = xScale.getPixelForValue(atmIndex);
      const ctx = chart.ctx;

      ctx.save();

      ctx.beginPath();
      ctx.strokeStyle = "#111827";
      ctx.lineWidth = 2.5;
      ctx.setLineDash([]);

      ctx.moveTo(x, yScale.top);
      ctx.lineTo(x, yScale.bottom);
      ctx.stroke();

      ctx.fillStyle = "#111827";
      ctx.font = "bold 12px Arial";
      ctx.textAlign = "left";
      ctx.fillText(`ATM ${atmStrike}`, x + 8, yScale.top + 16);

      ctx.restore();
    }
  };

  volSmileChart = new Chart(canvas.getContext("2d"), {
    type: "line",
    plugins: [atmLinePlugin],
    data: {
      labels: rows.map(r => r.strike),
      datasets: [
        {
          label: "OTM IV Smile",
          data: rows.map(r => getSmileIv(r)),
          borderWidth: 3,
          tension: 0.35,
          spanGaps: true,
          pointRadius: 4,
          pointHoverRadius: 6
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      spanGaps: true,
      plugins: {
        legend: {
          position: "top",
          align: "start"
        }
      },
      scales: {
        y: {
          title: {
            display: true,
            text: "Implied Volatility (%)"
          }
        },
        x: {
          title: {
            display: true,
            text: "Strike"
          }
        }
      }
    }
  });
}

async function renderIvSurface() {
  const container = getEl("ivSurfaceChart");

  if (!container) {
    console.error("IV Surface container not found: #ivSurfaceChart");
    return;
  }

  if (typeof Plotly === "undefined") {
    container.innerHTML = "<p>Plotly library is not loaded.</p>";
    console.error("Plotly is not loaded.");
    return;
  }

  /*
   * Every invocation gets its own generation number. If the user changes the
   * date/time and starts another request, an older response is ignored instead
   * of replacing the newer chart.
   */
  const requestGeneration = ++ivSurfaceRequestGeneration;
  const isCurrentRequest = () =>
    requestGeneration === ivSurfaceRequestGeneration;

  const sleep = milliseconds =>
    new Promise(resolve => setTimeout(resolve, milliseconds));

  const readyStatuses = new Set([
    "ready",
    "ready_with_errors"
  ]);

  const pendingStatuses = new Set([
    "queued",
    "processing",
    "pending",
    "running"
  ]);

  const renderStatus = message => {
    if (isCurrentRequest()) {
      container.innerHTML = `<p>${message}</p>`;
    }
  };

  try {
    const params = new URLSearchParams({
      dataset: getDataset(),
      date: getQueryDate(),
      time: getQueryTime(),
      interval: String(getInterval()),
      strike_count: String(getStrikeCount()),
      max_months: "4",
      _: String(Date.now())
    });

    renderStatus("Building IV surface...");

    let finalData = await fetchJson(
      `/api/iv-surface?${params.toString()}`
    );

    if (!isCurrentRequest()) return;

    /*
     * The ThreadPool backend normally returns:
     *   status = "queued" on the initial request,
     *   status = "processing" while calculating,
     *   status = "ready" or "ready_with_errors" when complete.
     *
     * The old code only polled when the initial status was "processing".
     * Therefore a normal "queued" response was treated as the final response,
     * causing the false "No IV surface data returned" error.
     */
    if (
      pendingStatuses.has(String(finalData?.status || "").toLowerCase()) &&
      finalData?.job_id
    ) {
      const jobId = encodeURIComponent(String(finalData.job_id));
      const maximumAttempts = 120;
      const pollIntervalMs = 1000;

      let completed = false;

      for (let attempt = 1; attempt <= maximumAttempts; attempt += 1) {
        if (!isCurrentRequest()) return;

        renderStatus(
          `Building IV surface... (${attempt}/${maximumAttempts})`
        );

        await sleep(pollIntervalMs);

        if (!isCurrentRequest()) return;

        const statusData = await fetchJson(
          `/api/iv-surface/status/${jobId}?_=${Date.now()}`
        );

        if (!isCurrentRequest()) return;

        const status = String(statusData?.status || "").toLowerCase();

        if (readyStatuses.has(status)) {
          finalData = statusData;
          completed = true;
          break;
        }

        if (status === "failed") {
          throw new Error(
            statusData?.error ||
            statusData?.detail ||
            "IV surface job failed."
          );
        }

        /*
         * A cached result may be returned without a status transition that the
         * frontend recognizes. Treat a complete payload as ready.
         */
        if (
          statusData?.ok &&
          Array.isArray(statusData?.rows) &&
          statusData.rows.length > 0 &&
          Array.isArray(statusData?.expiries) &&
          statusData.expiries.length > 0
        ) {
          finalData = statusData;
          completed = true;
          break;
        }

        if (!pendingStatuses.has(status) && status !== "done") {
          throw new Error(
            statusData?.error ||
            `Unexpected IV surface job status: ${status || "unknown"}`
          );
        }
      }

      if (!completed) {
        throw new Error(
          "IV surface calculation timed out. Please try again."
        );
      }
    }

    if (!isCurrentRequest()) return;

    const hasSurfaceData =
      finalData &&
      finalData.ok &&
      Array.isArray(finalData.strikes) &&
      finalData.strikes.length > 0 &&
      Array.isArray(finalData.expiries) &&
      finalData.expiries.length > 0 &&
      Array.isArray(finalData.rows) &&
      finalData.rows.length > 0;

    if (!hasSurfaceData) {
      console.error("IV Surface response:", finalData);
      throw new Error(
        finalData?.error ||
        finalData?.detail ||
        "No IV surface data returned from backend."
      );
    }

    const expiries = finalData.expiries
      .filter(expiry => expiry && (expiry.value || expiry.label))
      .map(expiry => ({
        value: String(expiry.value || ""),
        label: String(expiry.label || expiry.value || "")
      }));

    const normalizedStrikes = [...new Set(
      finalData.strikes
        .map(strike => Number(strike))
        .filter(Number.isFinite)
    )].sort((left, right) => left - right);

    const rows = finalData.rows.filter(row =>
      row &&
      row.error === undefined &&
      row.iv !== undefined &&
      row.iv !== null &&
      row.strike !== undefined &&
      Number.isFinite(Number(row.iv)) &&
      Number.isFinite(Number(row.strike))
    );

    if (
      !normalizedStrikes.length ||
      !expiries.length ||
      !rows.length
    ) {
      renderStatus("No IV surface data found.");
      return;
    }

    /*
     * Build an O(1) lookup map instead of repeatedly scanning all rows for
     * every expiry/strike grid point.
     */
    const rowLookup = new Map();

    rows.forEach(row => {
      const expiry = String(row.expiry || "");
      const strike = Number(row.strike);
      const iv = Number(row.iv);

      if (!expiry || !Number.isFinite(strike) || !Number.isFinite(iv)) {
        return;
      }

      rowLookup.set(`${expiry}|${strike}`, {
        ...row,
        expiry,
        strike,
        iv
      });
    });

    const expiryLabels = expiries.map(expiry => expiry.label);

    const z = expiries.map(expiry =>
      normalizedStrikes.map(strike => {
        const row = rowLookup.get(`${expiry.value}|${strike}`);
        return row ? row.iv : null;
      })
    );

    const validGridPoints = z.reduce(
      (count, values) =>
        count + values.filter(value => Number.isFinite(Number(value))).length,
      0
    );

    if (!validGridPoints) {
      renderStatus("No valid IV values were available for plotting.");
      return;
    }

    const hoverText = expiries.map((expiry, expiryIndex) =>
      normalizedStrikes.map((strike, strikeIndex) => {
        const iv = z[expiryIndex][strikeIndex];

        if (!Number.isFinite(Number(iv))) {
          return "";
        }

        return (
          `Strike: ${strike}<br>` +
          `Expiry: ${expiry.label}<br>` +
          `IV: ${Number(iv).toFixed(2)}%`
        );
      })
    );

    const surfaceTrace = {
      type: "surface",
      name: "IV Surface",
      x: normalizedStrikes,
      y: expiryLabels,
      z,
      text: hoverText,
      hoverinfo: "text",
      colorscale: [
        [0, "#440154"],
        [0.25, "#31688e"],
        [0.5, "#35b779"],
        [0.75, "#fde725"],
        [1, "#ffff00"]
      ],
      opacity: 0.78,
      showscale: true,
      colorbar: {
        title: "IV %",
        thickness: 18,
        len: 0.75
      },
      connectgaps: true
    };

    const lineTraces = expiries
      .map((expiry, expiryIndex) => {
        const lineX = [];
        const lineY = [];
        const lineZ = [];
        const lineText = [];

        normalizedStrikes.forEach((strike, strikeIndex) => {
          const iv = z[expiryIndex][strikeIndex];

          if (Number.isFinite(Number(iv))) {
            lineX.push(strike);
            lineY.push(expiry.label);
            lineZ.push(Number(iv) + 0.05);
            lineText.push(
              `Strike: ${strike}<br>` +
              `Expiry: ${expiry.label}<br>` +
              `IV: ${Number(iv).toFixed(2)}%`
            );
          }
        });

        if (!lineX.length) return null;

        return {
          type: "scatter3d",
          mode: "lines+markers",
          name: expiry.label,
          x: lineX,
          y: lineY,
          z: lineZ,
          text: lineText,
          hoverinfo: "text",
          line: { width: 10 },
          marker: { size: 4 }
        };
      })
      .filter(Boolean);

    let pinnedTrace = null;

    if (pinnedIvPoint) {
      const matched = rows.find(row =>
        Number(row.strike) === Number(pinnedIvPoint.strike) &&
        (
          String(row.expiry_label || "") ===
            String(pinnedIvPoint.expiryLabel || "") ||
          String(row.expiry || "") ===
            String(pinnedIvPoint.expiry || "")
        )
      );

      if (matched) {
        const expiryLabel =
          matched.expiry_label ||
          expiries.find(
            expiry => String(expiry.value) === String(matched.expiry)
          )?.label ||
          matched.expiry ||
          pinnedIvPoint.expiryLabel;

        pinnedIvPoint = {
          strike: Number(matched.strike),
          expiry: String(matched.expiry || ""),
          expiryLabel: String(expiryLabel),
          iv: Number(matched.iv)
        };

        pinnedTrace = {
          type: "scatter3d",
          mode: "markers+text",
          name: "Pinned Point",
          x: [Number(matched.strike)],
          y: [String(expiryLabel)],
          z: [Number(matched.iv) + 0.35],
          text: [
            `Tracking<br>${matched.strike}<br>` +
            `${Number(matched.iv).toFixed(2)}%`
          ],
          textposition: "top center",
          marker: {
            size: 9,
            color: "black",
            symbol: "circle"
          },
          showlegend: false,
          hoverinfo: "text"
        };
      } else {
        pinnedIvPoint = null;
      }
    }

    const layout = {
      autosize: true,
      height: 420,
      hovermode: "closest",
      showlegend: true,
      legend: {
        orientation: "h",
        x: 0,
        y: 1.15,
        xanchor: "left",
        yanchor: "bottom"
      },
      margin: {
        l: 0,
        r: 0,
        b: 0,
        t: 50
      },
      scene: {
        xaxis: { title: "Strike" },
        yaxis: {
          title: "Expiry",
          type: "category"
        },
        zaxis: { title: "IV (%)" },
        camera: {
          eye: {
            x: 1.7,
            y: 1.7,
            z: 1.1
          }
        }
      }
    };

    try {
      if (container.data) {
        Plotly.purge(container);
      }
    } catch (purgeError) {
      console.warn("Plotly purge warning:", purgeError);
    }

    if (!isCurrentRequest()) return;

    container.innerHTML = "";
    container.style.position = "relative";

    await Plotly.newPlot(
      container,
      [
        surfaceTrace,
        ...lineTraces,
        ...(pinnedTrace ? [pinnedTrace] : [])
      ],
      layout,
      {
        responsive: true,
        displaylogo: false
      }
    );

    if (!isCurrentRequest()) {
      try {
        Plotly.purge(container);
      } catch (error) {
        console.warn("Plotly stale-request purge warning:", error);
      }
      return;
    }

    let pinnedBox = container.querySelector(".iv-pinned-box");

    if (!pinnedBox) {
      pinnedBox = document.createElement("div");
      pinnedBox.className = "iv-pinned-box";
      pinnedBox.style.position = "absolute";
      pinnedBox.style.left = "14px";
      pinnedBox.style.top = "14px";
      pinnedBox.style.zIndex = "100";
      pinnedBox.style.background = "#ffffff";
      pinnedBox.style.border = "1px solid #111827";
      pinnedBox.style.borderRadius = "6px";
      pinnedBox.style.padding = "8px 10px";
      pinnedBox.style.fontSize = "12px";
      pinnedBox.style.fontWeight = "600";
      pinnedBox.style.boxShadow = "0 4px 12px rgba(0,0,0,0.15)";
      pinnedBox.style.display = "none";
      pinnedBox.style.pointerEvents = "none";
      container.appendChild(pinnedBox);
    }

    if (pinnedIvPoint) {
      pinnedBox.innerHTML =
        `Tracking<br>` +
        `Strike: ${pinnedIvPoint.strike}<br>` +
        `Expiry: ${pinnedIvPoint.expiryLabel}<br>` +
        `IV: ${Number(pinnedIvPoint.iv).toFixed(2)}%`;

      pinnedBox.style.display = "block";
    }

    let lastIvClickTime = 0;

    container.removeAllListeners?.("plotly_click");

    container.on("plotly_click", clickData => {
      try {
        if (
          !clickData ||
          !Array.isArray(clickData.points) ||
          !clickData.points.length
        ) {
          return;
        }

        const point = clickData.points[0];
        const clickedStrike = Number(point.x);
        const clickedExpiryLabel = String(point.y || "");

        const matchingExpiry = expiries.find(
          expiry =>
            expiry.label === clickedExpiryLabel ||
            expiry.value === clickedExpiryLabel
        );

        const clicked =
          rowLookup.get(
            `${matchingExpiry?.value || ""}|${clickedStrike}`
          ) ||
          rows.find(row =>
            Number(row.strike) === clickedStrike &&
            (
              String(row.expiry_label || "") === clickedExpiryLabel ||
              String(row.expiry || "") === clickedExpiryLabel
            )
          );

        if (!clicked) return;

        const expiryLabel =
          clicked.expiry_label ||
          matchingExpiry?.label ||
          clickedExpiryLabel ||
          clicked.expiry;

        const ivValue = Number(clicked.iv);

        pinnedBox.innerHTML =
          `Point Details<br>` +
          `Strike: ${clicked.strike}<br>` +
          `Expiry: ${expiryLabel}<br>` +
          `IV: ${ivValue.toFixed(2)}%`;

        pinnedBox.style.display = "block";

        const now = Date.now();

        if (now - lastIvClickTime < 350) {
          pinnedIvPoint = {
            strike: Number(clicked.strike),
            expiry: String(clicked.expiry || ""),
            expiryLabel: String(expiryLabel),
            iv: ivValue
          };

          renderIvSurface();
        }

        lastIvClickTime = now;
      } catch (clickError) {
        console.error(
          "IV surface click handler error:",
          clickError
        );
      }
    });

    container.oncontextmenu = event => {
      event.preventDefault();

      pinnedIvPoint = null;

      const box = container.querySelector(".iv-pinned-box");
      if (box) {
        box.style.display = "none";
      }

      renderIvSurface();
      return false;
    };

  } catch (error) {
    if (!isCurrentRequest()) return;

    console.error("renderIvSurface error:", error);
    container.innerHTML =
      "<p>Failed to load IV surface. Check console for details.</p>";

    alert(error.message || "Failed to load IV surface");
  }
}

function getCurrentLtpForLeg(leg) {
  const currentTime = getQueryTime();
  const entryTime = leg.entry_time || currentTime;

  if (entryTime === currentTime) {
    return Number(leg.premium || 0);
  }

  const strike = Number(leg.strike);
  const type = String(leg.type || "").toUpperCase();
  const row = chain.find(r => Number(r.strike) === strike);

  if (!row) return Number(leg.premium || 0);

  if (type === "CE") return Number(row.ce_ltp ?? leg.premium ?? 0);
  if (type === "PE") return Number(row.pe_ltp ?? leg.premium ?? 0);

  return Number(leg.premium || 0);
}

function getGreeksForLeg(type, strike) {
  const row = chain.find(r => Number(r.strike) === Number(strike));
  const optType = String(type || "").toUpperCase();

  return {
    delta: optType === "CE" ? Number(row?.ce_delta || 0) : Number(row?.pe_delta || 0),
    gamma: optType === "CE" ? Number(row?.ce_gamma || 0) : Number(row?.pe_gamma || 0),
    vega: optType === "CE" ? Number(row?.ce_vega || 0) : Number(row?.pe_vega || 0),
    theta: optType === "CE" ? Number(row?.ce_theta || 0) : Number(row?.pe_theta || 0)
  };
}

function createLeg(side, type, strike, premium, lots = 1) {
  const expiry =
    getEl("expirySelect")?.value || "current expiry";

  const normalizedStrike = normalizeStrike(
    strike,
    expiry
  );

  const g = getGreeksForLeg(
    type,
    normalizedStrike
  );

  return {
    side,
    type,
    strike: normalizedStrike,
    expiry,
    entry_time: getQueryTime(),
    premium: Number(premium || 0),
    lots: Number(lots || 1),
    qty: getFixedQty(),

    entry_delta: g.delta,
    entry_gamma: g.gamma,
    entry_vega: g.vega,
    entry_theta: g.theta
  };
}

function addLeg(side, type, strike, premium) {
  legs.push(createLeg(side, type, strike, premium, 1));
  renderLegs();
  calculate();
}

function addEmptyLeg() {
  const expiry =
    getEl("expirySelect")?.value || "current expiry";

  const atmRow = chain.find(row => row.atm);

  const atm = atmRow
    ? normalizeStrike(atmRow.strike, expiry)
    : normalizeStrike(getSpot(), expiry);

  addLeg("BUY", "CE", atm, 100);
}

function removeLeg(index) {
  legs.splice(index, 1);

  renderLegs();
  calculate();
}

function updateLeg(index, field, value) {
  if (!legs[index]) return;

  if (field === "qty") {
    legs[index].qty = getFixedQty();
  }

  else if (field === "strike") {
    legs[index].strike = normalizeStrike(
      value,
      legs[index].expiry
    );

    // Show the corrected strike immediately
    renderLegs();
  }

  else if (field === "expiry") {
    legs[index].expiry = value;

    legs[index].strike = normalizeStrike(
      legs[index].strike,
      value
    );

    renderLegs();
  }

  else {
    legs[index][field] = value;
  }

  calculate();
}

function renderLegs() {
  ensurePositionsHeaders();

  const body = getEl("legsBody");
  if (!body) return;

  body.innerHTML = "";

  legs.forEach((leg, i) => {
    if (!leg.expiry) {
      leg.expiry =
        getEl("expirySelect")?.value || "current expiry";
    }

    if (!leg.entry_time) {
      leg.entry_time = getQueryTime();
    }

    leg.qty = getFixedQty();

    const tr = document.createElement("tr");

    tr.innerHTML = `
      <td>
        <select
          onchange="updateLeg(${i}, 'side', this.value)"
        >
          <option
            value="BUY"
            ${leg.side === "BUY" ? "selected" : ""}
          >
            BUY
          </option>

          <option
            value="SELL"
            ${leg.side === "SELL" ? "selected" : ""}
          >
            SELL
          </option>
        </select>
      </td>

      <td>
        <select
          onchange="updateLeg(${i}, 'type', this.value)"
        >
          <option
            value="CE"
            ${leg.type === "CE" ? "selected" : ""}
          >
            CE
          </option>

          <option
            value="PE"
            ${leg.type === "PE" ? "selected" : ""}
          >
            PE
          </option>
        </select>
      </td>

      <td>
        <input
          class="leg-input"
          type="number"
          value="${leg.strike}"
          step="${getStrikeStep(leg.expiry)}"
          onchange="updateLeg(
            ${i},
            'strike',
            Number(this.value)
          )"
        >
      </td>

      <td>
        <select
          class="leg-input"
          onchange="updateLeg(
            ${i},
            'expiry',
            this.value
          )"
        >
          ${
            getEl("expirySelect")
              ? Array.from(
                  getEl("expirySelect").options
                )
                  .map(
                    option => `
                      <option
                        value="${option.value}"
                        ${
                          leg.expiry === option.value
                            ? "selected"
                            : ""
                        }
                      >
                        ${option.textContent}
                      </option>
                    `
                  )
                  .join("")
              : `
                  <option value="${leg.expiry}">
                    ${leg.expiry}
                  </option>
                `
          }
        </select>
      </td>

      <td>
        <input
          class="leg-input leg-entry-time"
          type="time"
          value="${leg.entry_time}"
          onchange="updateLeg(
            ${i},
            'entry_time',
            this.value
          )"
        >
      </td>

      <td>
        <input
          class="leg-input"
          type="number"
          value="${leg.premium}"
          step="0.05"
          min="0"
          onchange="updateLeg(
            ${i},
            'premium',
            Number(this.value)
          )"
        >
      </td>

      <td>
        <input
          class="leg-input"
          type="number"
          value="${leg.lots}"
          step="1"
          min="1"
          onchange="updateLeg(
            ${i},
            'lots',
            Number(this.value)
          )"
        >
      </td>

      <td>
        <input
          class="leg-input"
          type="number"
          value="${getFixedQty()}"
          readonly
        >
      </td>

      <td>
        <button
          class="delete-leg-btn"
          type="button"
          onclick="removeLeg(${i})"
        >
          Delete
        </button>
      </td>
    `;

    body.appendChild(tr);
  });
}

function loadTemplate(name) {
  const expiry =
    getEl("expirySelect")?.value || "current expiry";

  const atmRow = chain.find(row => row.atm);

  const atm = atmRow
    ? normalizeStrike(atmRow.strike, expiry)
    : normalizeStrike(getSpot(), expiry);

  const find = (strike, type) => {
    const row = chain.find(
      item => Number(item.strike) === Number(strike)
    );

    if (!row) return 100;

    return Number(
      (type === "CE" ? row.ce_ltp : row.pe_ltp) || 100
    );
  };

  if (name === "short_straddle") {
    legs = [
      createLeg("SELL", "CE", atm, find(atm, "CE")),
      createLeg("SELL", "PE", atm, find(atm, "PE"))
    ];
  } else if (name === "short_strangle") {
    legs = [
      createLeg("SELL", "CE", atm + 200, find(atm + 200, "CE")),
      createLeg("SELL", "PE", atm - 200, find(atm - 200, "PE"))
    ];
  } else if (name === "iron_condor") {
    legs = [
      createLeg("BUY", "PE", atm - 400, find(atm - 400, "PE")),
      createLeg("SELL", "PE", atm - 200, find(atm - 200, "PE")),
      createLeg("SELL", "CE", atm + 200, find(atm + 200, "CE")),
      createLeg("BUY", "CE", atm + 400, find(atm + 400, "CE"))
    ];
  } else {
    console.warn(`Unknown strategy template: ${name}`);
    return;
  }

  renderLegs();
  calculate();
}

function calculateLivePositionGreeks() {
  let liveDelta = 0;
  let liveGamma = 0;
  let liveVega = 0;
  let liveTheta = 0;

  let entryDelta = 0;
  let entryGamma = 0;
  let entryVega = 0;
  let entryTheta = 0;

  for (const leg of legs) {
    const row = chain.find(r => Number(r.strike) === Number(leg.strike));
    if (!row) continue;

    const type = String(leg.type || "").toUpperCase();
    const side = String(leg.side || "").toUpperCase();
    const lots = Number(leg.lots || 1);
    const sign = side === "SELL" ? -1 : 1;

    const d = type === "CE" ? Number(row.ce_delta || 0) : Number(row.pe_delta || 0);
    const g = type === "CE" ? Number(row.ce_gamma || 0) : Number(row.pe_gamma || 0);
    const v = type === "CE" ? Number(row.ce_vega || 0) : Number(row.pe_vega || 0);
    const t = type === "CE" ? Number(row.ce_theta || 0) : Number(row.pe_theta || 0);

    liveDelta += sign * d * lots;
    liveGamma += sign * g * lots;
    liveVega += sign * v * lots;
    liveTheta += sign * t * lots;

    entryDelta += sign * Number(leg.entry_delta || 0) * lots;
    entryGamma += sign * Number(leg.entry_gamma || 0) * lots;
    entryVega += sign * Number(leg.entry_vega || 0) * lots;
    entryTheta += sign * Number(leg.entry_theta || 0) * lots;
  }

  const set = (id, value, decimals) => {
    const el = getEl(id);
    if (el) el.textContent = Number(value || 0).toFixed(decimals);
  };

  set("entryDelta", entryDelta, 2);
  set("liveDelta", liveDelta, 2);
  set("entryGamma", entryGamma, 4);
  set("liveGamma", liveGamma, 4);
  set("entryVega", entryVega, 2);
  set("liveVega", liveVega, 2);
  set("entryTheta", entryTheta, 2);
  set("liveTheta", liveTheta, 2);
}

async function calculate() {
  try {
    ensureMetricsCards();
    const uniqueExpiries = [...new Set(legs.map(l => l.expiry).filter(Boolean))];
    const warning = getEl("expiryWarning");

    if (uniqueExpiries.length > 1) {
      renderChart([], [], []);

      if (warning) {
        warning.textContent =
          "⚠ Diagonal / calendar strategy payoff is not possible because legs have different expiries.";
      }

      return;
    }

    if (warning) {
      warning.textContent =
        "⚠ Payoff analysis is for same-expiry strategies only and is valid only at the time of expiry only. Calendar and diagonal spreads are not supported.";
    }

    const fixedQty = getFixedQty();

    const payloadLegs = legs.map(leg => ({
      ...leg,
      qty: fixedQty,
      current_price: getCurrentLtpForLeg(leg)
    }));

    const res = await fetch("/api/calculate", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        dataset: getDataset(),
        spot: getSpot(),
        iv: getIV(),
        days: getDays(),
        legs: payloadLegs
      })
    });

    const data = await res.json();

    if (!res.ok || data.ok === false) {
      throw new Error(data.error || "Calculation failed");
    }

    const s = data.summary || {};
    const charges = Number(s.charges || s.trade_cost || 0);
    const pnlNow = Number(s.pnl_now || 0);

    getEl("netCredit").textContent = fmtMoney(s.net_credit);
    getEl("pnlNow").textContent = fmtMoney(pnlNow);
    getEl("tradeCost").textContent = fmtMoney(charges);
    getEl("netProfitAfterCost").textContent = fmtMoney(pnlNow - charges);
    getEl("marginRequired").textContent = fmtMoney(s.margin_required || s.margin || 0);
    getEl("maxProfit").textContent = fmtMoney(s.max_profit);
    getEl("maxLoss").textContent = fmtMoney(s.max_loss);
    getEl("breakevens").textContent = s.breakevens?.length ? s.breakevens.join(", ") : "-";

    calculateLivePositionGreeks();

    renderChart(data.spots || [], data.payoff || [], data.current || []);

  } catch (err) {
    console.error("calculate error:", err);
  }
}

function renderChart(labels, payoff, current) {
  const canvas = getEl("payoffChart");

  if (!canvas || typeof Chart === "undefined") {
    return;
  }

  const ctx = canvas.getContext("2d");

  if (chart) {
    chart.destroy();
  }

  const currentSpotPlugin = {
    id: "currentSpotPlugin",

    afterDatasetsDraw(chartInstance) {
      const currentSpot = Number(getSpot());

      if (
        !Number.isFinite(currentSpot) ||
        !Array.isArray(labels) ||
        labels.length === 0
      ) {
        return;
      }

      const numericLabels = labels.map(Number);
      const xScale = chartInstance.scales.x;
      const yScale = chartInstance.scales.y;

      if (!xScale || !yScale) {
        return;
      }

      let xPixel = null;
      const exactIndex = numericLabels.findIndex(
        value => value === currentSpot
      );

      if (exactIndex >= 0) {
        xPixel = xScale.getPixelForValue(exactIndex);
      } else {
        for (let index = 1; index < numericLabels.length; index += 1) {
          const previousValue = numericLabels[index - 1];
          const nextValue = numericLabels[index];

          if (
            currentSpot >= previousValue &&
            currentSpot <= nextValue &&
            nextValue !== previousValue
          ) {
            const previousPixel = xScale.getPixelForValue(index - 1);
            const nextPixel = xScale.getPixelForValue(index);
            const ratio =
              (currentSpot - previousValue) /
              (nextValue - previousValue);

            xPixel =
              previousPixel +
              ratio * (nextPixel - previousPixel);
            break;
          }
        }
      }

      if (!Number.isFinite(xPixel)) {
        return;
      }

      const chartContext = chartInstance.ctx;
      const label = `${getDataset()} Spot : ${currentSpot.toFixed(2)}`;

      chartContext.save();

      // Current-spot vertical line.
      chartContext.beginPath();
      chartContext.setLineDash([]);
      chartContext.strokeStyle = "#16a34a";
      chartContext.lineWidth = 2;
      chartContext.moveTo(xPixel, yScale.top);
      chartContext.lineTo(xPixel, yScale.bottom);
      chartContext.stroke();

      // Label is placed in the reserved space above the plotting area.
      chartContext.font = "bold 12px Arial";
      chartContext.textAlign = "center";
      chartContext.textBaseline = "bottom";
      chartContext.fillStyle = "#111827";
      chartContext.fillText(
        label,
        xPixel,
        yScale.top - 8
      );

      chartContext.restore();
    }
  };

  chart = new Chart(ctx, {
    type: "line",
    plugins: [currentSpotPlugin],

    data: {
      labels,
      datasets: [
        {
          // The payoff curve remains, but its legend/title is hidden.
          label: "",
          data: payoff,
          borderWidth: 2,
          pointRadius: 0,
          pointHoverRadius: 4,
          tension: 0,
          spanGaps: true,
          segment: {
            borderColor: context => {
              const y0 = Number(context.p0?.parsed?.y);
              const y1 = Number(context.p1?.parsed?.y);
              return y0 >= 0 && y1 >= 0 ? "#16a34a" : "#ef4444";
            }
          },
          fill: {
            target: "origin",
            above: "rgba(22, 163, 74, 0.18)",
            below: "rgba(239, 68, 68, 0.14)"
          }
        }
      ]
    },

    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: {
        mode: "index",
        intersect: false
      },
      layout: {
        padding: {
          top: 36,
          right: 10,
          bottom: 4,
          left: 4
        }
      },
      plugins: {
        legend: {
          display: false
        },
        tooltip: {
          enabled: true
        }
      },
      scales: {
        y: {
          grace: "8%",
          title: {
            display: true,
            text: "P&L"
          },
          ticks: {
            maxTicksLimit: 7
          },
          grid: {
            color: "rgba(148, 163, 184, 0.28)"
          }
        },
        x: {
          offset: false,
          title: {
            display: true,
            text: "Spot"
          },
          ticks: {
            autoSkip: true,
            maxTicksLimit: 12,
            maxRotation: 45,
            minRotation: 45
          },
          grid: {
            color: "rgba(148, 163, 184, 0.22)"
          }
        }
      }
    }
  });
}

function attachOhlcBox(chartInstance, series, container, boxId) {
  let box = document.getElementById(boxId);

  if (!box) {
    box = document.createElement("div");
    box.id = boxId;

    box.style.position = "absolute";
    box.style.top = "10px";
    box.style.left = "10px";
    box.style.zIndex = "100";
    box.style.fontSize = "14px";
    box.style.fontWeight = "600";
    box.style.background = "transparent";
    box.style.pointerEvents = "none";

    container.style.position = "relative";
    container.appendChild(box);
  }

  chartInstance.subscribeCrosshairMove(param => {
    if (!param?.time) return;

    const candle = param.seriesData.get(series);
    if (!candle) return;

    if (
      candle.open === undefined ||
      candle.high === undefined ||
      candle.low === undefined ||
      candle.close === undefined
    ) {
      return;
    }

    box.innerHTML =
      `O ${Number(candle.open).toFixed(2)} ` +
      `H ${Number(candle.high).toFixed(2)} ` +
      `L ${Number(candle.low).toFixed(2)} ` +
      `C ${Number(candle.close).toFixed(2)}`;
  });
}

async function openChartPopup() {
  try {
    const modal = getEl("chartModal");
    const container = getEl("fullscreenIndexChart");

    if (
      !modal ||
      !container ||
      typeof LightweightCharts === "undefined"
    ) {
      throw new Error("Index chart modal or chart library is missing");
    }

    const startDate = getEl("indexChartStartDate");
    const startTime = getEl("indexChartStartTime");
    const endDate = getEl("indexChartEndDate");
    const endTime = getEl("indexChartEndTime");
    const intervalSelect = getEl("indexChartInterval");
    const applyBtn = getEl("indexChartApplyBtn");

    modal.style.display = "block";

    const chartTitle = modal.querySelector("h2");

    if (chartTitle) {
      chartTitle.textContent = `${getDataset()} Index Chart`;
    }

    if (applyBtn) {
      applyBtn.dataset.chart = "index";
      delete applyBtn.dataset.strike;
      delete applyBtn.dataset.metric;
    }

    if (startDate && !startDate.value) {
      startDate.value = getQueryDate();
    }

    if (startTime && !startTime.value) {
      startTime.value = "09:15";
    }

    if (endDate && !endDate.dataset.userChanged) {
      endDate.value = getQueryDate();
    }

    if (endTime && !endTime.dataset.userChanged) {
      endTime.value = getQueryTime();
    }

    const params = new URLSearchParams({
      dataset: getDataset(),
      date: startDate?.value || getQueryDate(),
      start_time: startTime?.value || "09:15",
      end_date: endDate?.value || getQueryDate(),
      end_time: endTime?.value || getQueryTime(),
      interval: intervalSelect?.value || String(getInterval()),
      _: String(Date.now())
    });

    const data = await fetchJson(
      `/api/index-chart?${params.toString()}`
    );

    if (!data.ok || !Array.isArray(data.rows) || !data.rows.length) {
      throw new Error("No index chart data found");
    }

    if (fullscreenChart) {
      fullscreenChart.remove();
      fullscreenChart = null;
      fullscreenSeries = null;
    }

    container.innerHTML = "";

    fullscreenChart = LightweightCharts.createChart(container, {
      width: container.clientWidth,
      height: container.clientHeight || 700,

      layout: {
        background: { color: "#ffffff" },
        textColor: "#222222"
      },

      grid: {
        vertLines: { color: "#eef2f7" },
        horzLines: { color: "#eef2f7" }
      },

      crosshair: {
        mode: LightweightCharts.CrosshairMode.Normal
      },

      timeScale: {
        timeVisible: true,
        secondsVisible: false,
        borderColor: "#dddddd"
      },

      rightPriceScale: {
        borderColor: "#dddddd"
      }
    });

    fullscreenSeries = fullscreenChart.addCandlestickSeries({
      upColor: "#16a34a",
      downColor: "#dc2626",
      borderUpColor: "#16a34a",
      borderDownColor: "#dc2626",
      wickUpColor: "#16a34a",
      wickDownColor: "#dc2626"
    });

    fullscreenSeries.setData(data.rows);

    advancedCharts.index.chart = fullscreenChart;
    advancedCharts.index.candleSeries = fullscreenSeries;
    advancedCharts.index.rows = data.rows;

    if (!Array.isArray(advancedCharts.index.indicators)) {
      advancedCharts.index.indicators = [];
    }

    advancedCharts.index.indicators.forEach(indicator => {
      indicator.series = [];
    });

    redrawChartIndicators("index");

    attachOhlcBox(
      fullscreenChart,
      fullscreenSeries,
      container,
      "indexOhlcBox"
    );

    fullscreenChart.timeScale().fitContent();

    const resizeHandler = () => {
      if (!fullscreenChart || !container) return;

      fullscreenChart.applyOptions({
        width: container.clientWidth,
        height: container.clientHeight || 700
      });
    };

    window.addEventListener("resize", resizeHandler, {
      once: true
    });

  } catch (err) {
    console.error("openChartPopup error:", err);
    alert(err.message || "Failed to load index chart");
  }
}

function closeChartPopup() {
  const modal = getEl("chartModal");

  if (modal) {
    modal.style.display = "none";
  }

  if (fullscreenChart) {
    fullscreenChart.remove();
    fullscreenChart = null;
    fullscreenSeries = null;
  }

  const container = getEl("fullscreenIndexChart");
  if (container) {
    container.innerHTML = "";
  }
}

async function openFutureChartPopup() {
  try {
    const modal = getEl("futureChartModal");
    const container = getEl("fullscreenFutureChart");
    const monthSelect = getEl("futureMonthSelect");

    const startDate = getEl("futureChartStartDate");
    const startTime = getEl("futureChartStartTime");
    const endDate = getEl("futureChartEndDate");
    const endTime = getEl("futureChartEndTime");
    const intervalSelect = getEl("futureChartInterval");

    if (!modal || !container || typeof LightweightCharts === "undefined") return;

    modal.style.display = "block";

    if (startDate && !startDate.value) startDate.value = getQueryDate();
    if (startTime && !startTime.value) startTime.value = "09:15";

    if (endDate && !endDate.dataset.userChanged) endDate.value = getQueryDate();
    if (endTime && !endTime.dataset.userChanged) endTime.value = getQueryTime();

    if (startTime && !startTime.dataset.changeAttached) {
      startTime.addEventListener("change", () => {
        startTime.dataset.userChanged = "true";
      });
      startTime.dataset.changeAttached = "true";
    }

    if (endDate && !endDate.dataset.changeAttached) {
      endDate.addEventListener("change", () => {
        endDate.dataset.userChanged = "true";
      });
      endDate.dataset.changeAttached = "true";
    }

    if (endTime && !endTime.dataset.changeAttached) {
      endTime.addEventListener("change", () => {
        endTime.dataset.userChanged = "true";
      });
      endTime.dataset.changeAttached = "true";
    }

    const params = new URLSearchParams({
      dataset: getDataset(),
      date: startDate?.value || getQueryDate(),
      start_time: startTime?.value || "09:15",
      end_date: endDate?.value || getQueryDate(),
      end_time: endTime?.value || getQueryTime(),
      interval: intervalSelect?.value || String(getInterval()),
      month: monthSelect ? monthSelect.value : "current",
      _: String(Date.now())
    });

    const data = await fetchJson(`/api/future-chart?${params.toString()}`);

    if (!data.ok || !data.rows || !data.rows.length) {
      alert("No future chart data found");
      return;
    }

    if (!futureChart) {
      futureChart = LightweightCharts.createChart(container, {
        width: container.clientWidth,
        height: container.clientHeight,
        layout: {
          background: { color: "#ffffff" },
          textColor: "#222"
        },
        grid: {
          vertLines: { color: "#eef2f7" },
          horzLines: { color: "#eef2f7" }
        },
        crosshair: {
          mode: LightweightCharts.CrosshairMode.Normal
        },
        timeScale: {
          timeVisible: true,
          secondsVisible: false,
          borderColor: "#ddd"
        },
        rightPriceScale: {
          borderColor: "#ddd"
        }
      });

      futureSeries = futureChart.addCandlestickSeries({
        upColor: "#16a34a",
        downColor: "#dc2626",
        borderUpColor: "#16a34a",
        borderDownColor: "#dc2626",
        wickUpColor: "#16a34a",
        wickDownColor: "#dc2626"
      });
    }

    futureSeries.setData(data.rows || []);

    advancedCharts.future.chart = futureChart;
    advancedCharts.future.candleSeries = futureSeries;
    advancedCharts.future.rows = data.rows || [];

    if (!Array.isArray(advancedCharts.future.indicators)) {
      advancedCharts.future.indicators = [];
    }
    redrawChartIndicators("future");

    attachOhlcBox(
      futureChart,
      futureSeries,
      container,
      "futureOhlcBox"
    );

    futureChart.timeScale().fitContent();

  } catch (err) {
    console.error("openFutureChartPopup error:", err);
    alert(err.message || "Failed to load future chart");
  }
}

function closeFutureChartPopup() {
  const modal = getEl("futureChartModal");
  if (modal) modal.style.display = "none";
}

async function openIndiaVixChartPopup() {
  try {
    const modal = getEl("vixChartModal");
    const container = getEl("fullscreenVixChart");

    const startDate = getEl("vixChartStartDate");
    const startTime = getEl("vixChartStartTime");
    const endDate = getEl("vixChartEndDate");
    const endTime = getEl("vixChartEndTime");
    const intervalSelect = getEl("vixChartInterval");

    if (!modal || !container || typeof LightweightCharts === "undefined") return;

    modal.style.display = "block";

    if (startDate && !startDate.value) startDate.value = getQueryDate();
    if (startTime && !startTime.value) startTime.value = "09:15";

    if (endDate && !endDate.dataset.userChanged) endDate.value = getQueryDate();
    if (endTime && !endTime.dataset.userChanged) endTime.value = getQueryTime();

    if (startTime && !startTime.dataset.changeAttached) {
      startTime.addEventListener("change", () => {
        startTime.dataset.userChanged = "true";
      });
      startTime.dataset.changeAttached = "true";
    }

    if (endDate && !endDate.dataset.changeAttached) {
      endDate.addEventListener("change", () => {
        endDate.dataset.userChanged = "true";
      });
      endDate.dataset.changeAttached = "true";
    }

    if (endTime && !endTime.dataset.changeAttached) {
      endTime.addEventListener("change", () => {
        endTime.dataset.userChanged = "true";
      });
      endTime.dataset.changeAttached = "true";
    }

    const params = new URLSearchParams({
      dataset: getDataset(),
      date: startDate?.value || getQueryDate(),
      start_time: startTime?.value || "09:15",
      end_date: endDate?.value || getQueryDate(),
      end_time: endTime?.value || getQueryTime(),
      interval: intervalSelect?.value || String(getInterval()),
      _: String(Date.now())
    });

    const data = await fetchJson(`/api/india-vix-chart?${params.toString()}`);

    if (!data.ok || !data.rows || !data.rows.length) {
      alert("No India VIX chart data found");
      return;
    }

    if (!vixChart) {
      vixChart = LightweightCharts.createChart(container, {
        width: container.clientWidth,
        height: container.clientHeight,
        layout: {
          background: { color: "#ffffff" },
          textColor: "#222"
        },
        grid: {
          vertLines: { color: "#eef2f7" },
          horzLines: { color: "#eef2f7" }
        },
        crosshair: {
          mode: LightweightCharts.CrosshairMode.Normal
        },
        timeScale: {
          timeVisible: true,
          secondsVisible: false,
          borderColor: "#ddd"
        },
        rightPriceScale: {
          borderColor: "#ddd"
        }
      });

      vixSeries = vixChart.addCandlestickSeries({
        upColor: "#16a34a",
        downColor: "#dc2626",
        borderUpColor: "#16a34a",
        borderDownColor: "#dc2626",
        wickUpColor: "#16a34a",
        wickDownColor: "#dc2626"
      });
    }

    vixSeries.setData(data.rows || []);

    advancedCharts.vix.chart = vixChart;
    advancedCharts.vix.candleSeries = vixSeries;
    advancedCharts.vix.rows = data.rows || [];

    if (!Array.isArray(advancedCharts.vix.indicators)) {
      advancedCharts.vix.indicators = [];
    }
    redrawChartIndicators("vix");

    attachOhlcBox(
      vixChart,
      vixSeries,
      container,
      "vixOhlcBox"
    );

    vixChart.timeScale().fitContent();

  } catch (err) {
    console.error("openIndiaVixChartPopup error:", err);
    alert(err.message || "Failed to load India VIX chart");
  }
}

function closeIndiaVixChartPopup() {
  const modal = getEl("vixChartModal");
  if (modal) modal.style.display = "none";
}

function openGreeksPopup() {
  const modal = getEl("greeksModal");
  const body = getEl("greeksBody");

  if (!modal || !body) return;

  body.innerHTML = "";

  if (!chain.length) {
    body.innerHTML = `
      <tr>
        <td colspan="13" style="padding:20px;text-align:center;">
          No Greeks data available
        </td>
      </tr>
    `;
  } else {
    chain.forEach(row => {
      const tr = document.createElement("tr");
      if (row.atm) tr.classList.add("atm");

      tr.innerHTML = `
        <td>
          <b>${row.strike}</b>
          ${row.atm ? '<span class="atm-badge">ATM</span>' : ""}
        </td>
        <td>${row.ce_ltp ?? "-"}</td>
        <td>${row.ce_delta ?? "-"}</td>
        <td>${row.ce_gamma ?? "-"}</td>
        <td>${row.ce_vega ?? "-"}</td>
        <td>${row.ce_theta ?? "-"}</td>
        <td>${row.ce_iv ?? "-"}</td>
        <td>${row.pe_iv ?? "-"}</td>
        <td>${row.pe_theta ?? "-"}</td>
        <td>${row.pe_vega ?? "-"}</td>
        <td>${row.pe_gamma ?? "-"}</td>
        <td>${row.pe_delta ?? "-"}</td>
        <td>${row.pe_ltp ?? "-"}</td>
      `;

      body.appendChild(tr);
    });
  }

  modal.style.display = "block";
}

function closeGreeksPopup() {
  const modal = getEl("greeksModal");
  if (modal) modal.style.display = "none";
}

/* =========================================================
   INDICATOR MODAL
========================================================= */

function openIndicatorModal(chartName) {
  const modal = getEl("indicatorModal");

  if (!modal) {
    console.error("Indicator modal not found: #indicatorModal");
    return;
  }

  activeIndicatorChart = chartName;

  modal.classList.add("open");
  modal.setAttribute("aria-hidden", "false");

  refreshIndicatorModalState();

  const searchInput = getEl("indicatorSearchInput");

  if (searchInput) {
    searchInput.value = "";
    filterIndicatorList();
    searchInput.focus();
  }
}

function closeIndicatorModal() {
  const modal = getEl("indicatorModal");

  if (!modal) return;

  modal.classList.remove("open");
  modal.setAttribute("aria-hidden", "true");
}

function refreshIndicatorModalState() {
  const chartState = advancedCharts[activeIndicatorChart];

  document.querySelectorAll(".indicator-list-item").forEach(button => {
    const indicatorType = button.dataset.indicator;
    const status = button.querySelector(".indicator-status");

    let isAdded = false;

    if (chartState && chartState.indicators) {
      if (Array.isArray(chartState.indicators)) {
        isAdded = chartState.indicators.some(
          indicator => indicator.type === indicatorType
        );
      } else {
        isAdded = Boolean(chartState.indicators[indicatorType]);
      }
    }

    button.classList.toggle("selected", isAdded);

    if (status) {
      status.textContent = isAdded ? "Added" : "Add";
    }
  });
}

function filterIndicatorList() {
  const searchInput = getEl("indicatorSearchInput");
  const searchText = String(searchInput?.value || "")
    .trim()
    .toLowerCase();

  const activeCategory =
    document.querySelector(".indicator-category.active")?.dataset.category ||
    "all";

  document.querySelectorAll(".indicator-list-item").forEach(button => {
    const buttonCategory = button.dataset.category || "";
    const buttonText = button.textContent.toLowerCase();

    const matchesSearch =
      !searchText || buttonText.includes(searchText);

    const matchesCategory =
      activeCategory === "all" || buttonCategory === activeCategory;

    button.hidden = !(matchesSearch && matchesCategory);
  });
}

function initialiseIndicatorModal() {
  const modal = getEl("indicatorModal");
  const closeButton = getEl("closeIndicatorModal");
  const searchInput = getEl("indicatorSearchInput");

  if (!modal) {
    console.error("Cannot initialise indicators: #indicatorModal not found");
    return;
  }

  /*
   * Connect the indicator button from every chart toolbar.
   */
  document
    .querySelectorAll('.chart-toolbar button[data-tool="indicators"]')
    .forEach(button => {
      if (button.dataset.indicatorClickAttached === "true") {
        return;
      }

      button.addEventListener("click", event => {
        event.preventDefault();
        event.stopPropagation();

        const toolbar = button.closest(".chart-toolbar");
        const chartName = toolbar?.dataset.chart;

        if (!chartName) {
          console.error(
            "The chart toolbar does not contain a data-chart value"
          );
          return;
        }

        openIndicatorModal(chartName);
      });

      button.dataset.indicatorClickAttached = "true";
    });

  /*
   * Close button.
   */
  if (closeButton && closeButton.dataset.clickAttached !== "true") {
    closeButton.addEventListener("click", closeIndicatorModal);
    closeButton.dataset.clickAttached = "true";
  }

  /*
   * Close when clicking the background.
   */
  if (modal.dataset.backgroundClickAttached !== "true") {
    modal.addEventListener("click", event => {
      if (event.target === modal) {
        closeIndicatorModal();
      }
    });

    modal.dataset.backgroundClickAttached = "true";
  }

  /*
   * Indicator search.
   */
  if (searchInput && searchInput.dataset.inputAttached !== "true") {
    searchInput.addEventListener("input", filterIndicatorList);
    searchInput.dataset.inputAttached = "true";
  }

  /*
   * Category buttons: All, Trend, Momentum, etc.
   */
  document.querySelectorAll(".indicator-category").forEach(button => {
    if (button.dataset.clickAttached === "true") return;

    button.addEventListener("click", () => {
      document.querySelectorAll(".indicator-category").forEach(category => {
        category.classList.remove("active");
      });

      button.classList.add("active");
      filterIndicatorList();
    });

    button.dataset.clickAttached = "true";
  });

  /*
   * Indicator list buttons.
   * For now, this opens the settings modal.
   */
  document.querySelectorAll(".indicator-list-item").forEach(button => {
    if (button.dataset.clickAttached === "true") return;

    button.addEventListener("click", () => {
      const indicatorType = button.dataset.indicator;

      if (!indicatorType) {
        console.error("Indicator type is missing");
        return;
      }

      const chartState = advancedCharts[activeIndicatorChart];
      const existing = Array.isArray(chartState?.indicators)
        ? chartState.indicators.find(indicator => indicator.type === indicatorType)
        : null;
      openIndicatorSettings(indicatorType, existing?.id || null);
    });

    button.dataset.clickAttached = "true";
  });

  /*
   * Escape key closes the modal.
   */
  if (document.body.dataset.indicatorEscapeAttached !== "true") {
    document.addEventListener("keydown", event => {
      if (event.key === "Escape") {
        closeIndicatorModal();
        closeIndicatorSettings();
      }
    });

    document.body.dataset.indicatorEscapeAttached = "true";
  }
}

/* =========================================================
   INDICATOR SETTINGS MODAL
========================================================= */

let currentIndicatorType = null;
let currentIndicatorId = null;

const INDICATOR_DEFAULTS = {
  ema: {
    length: 20,
    source: "close",
    color: "#2563eb",
    secondaryColor: "#dc2626",
    lineWidth: 2,
    visible: true
  },

  sma: {
    length: 20,
    source: "close",
    color: "#7c3aed",
    secondaryColor: "#dc2626",
    lineWidth: 2,
    visible: true
  },

  vwap: {
    source: "hlc3",
    color: "#f59e0b",
    secondaryColor: "#dc2626",
    lineWidth: 2,
    visible: true
  },

  supertrend: {
    atrLength: 10,
    multiplier: 3,
    color: "#16a34a",
    secondaryColor: "#dc2626",
    lineWidth: 2,
    visible: true
  },

  bollinger: {
    length: 20,
    source: "close",
    multiplier: 2,
    color: "#2563eb",
    secondaryColor: "#94a3b8",
    lineWidth: 2,
    visible: true
  },

  rsi: {
    length: 14,
    source: "close",
    color: "#7c3aed",
    secondaryColor: "#dc2626",
    lineWidth: 2,
    visible: true
  },

  macd: {
    fastLength: 12,
    slowLength: 26,
    signalLength: 9,
    source: "close",
    color: "#2563eb",
    secondaryColor: "#dc2626",
    lineWidth: 2,
    visible: true
  },

  atr: {
    length: 14,
    color: "#f97316",
    secondaryColor: "#dc2626",
    lineWidth: 2,
    visible: true
  },

  volume: {
    color: "#16a34a",
    secondaryColor: "#dc2626",
    lineWidth: 1,
    visible: true
  }
};

function createIndicatorId(type) {
  return `${type}-${Date.now()}-${Math.random()
    .toString(36)
    .slice(2, 8)}`;
}

function getIndicatorDefaultSettings(type) {
  return {
    ...(INDICATOR_DEFAULTS[type] || {})
  };
}

function setIndicatorFieldVisibility(type) {
  const supportedFields = {
    ema: ["length", "source"],
    sma: ["length", "source"],
    vwap: ["source"],
    supertrend: ["atrLength", "multiplier"],
    bollinger: ["length", "source", "multiplier"],
    rsi: ["length", "source"],
    macd: [
      "source",
      "fastLength",
      "slowLength",
      "signalLength"
    ],
    atr: ["length"],
    volume: []
  };

  const visibleFields = supportedFields[type] || [];

  document
    .querySelectorAll("[data-setting-field]")
    .forEach(row => {
      const fieldName = row.dataset.settingField;
      row.hidden = !visibleFields.includes(fieldName);
    });
}

function fillIndicatorSettingsForm(settings) {
  const setValue = (id, value) => {
    const element = getEl(id);

    if (element && value !== undefined && value !== null) {
      element.value = value;
    }
  };

  setValue("indicatorLength", settings.length ?? 20);
  setValue("indicatorSource", settings.source ?? "close");
  setValue("indicatorMultiplier", settings.multiplier ?? 2);
  setValue("indicatorAtrLength", settings.atrLength ?? 10);
  setValue("indicatorFastLength", settings.fastLength ?? 12);
  setValue("indicatorSlowLength", settings.slowLength ?? 26);
  setValue("indicatorSignalLength", settings.signalLength ?? 9);
  setValue("indicatorColor", settings.color ?? "#2563eb");
  setValue(
    "indicatorSecondaryColor",
    settings.secondaryColor ?? "#dc2626"
  );
  setValue("indicatorLineWidth", settings.lineWidth ?? 2);

  const visibleInput = getEl("indicatorVisible");

  if (visibleInput) {
    visibleInput.checked = settings.visible !== false;
  }
}

function readIndicatorSettingsForm() {
  return {
    length: Math.max(
      1,
      Number(getEl("indicatorLength")?.value || 20)
    ),

    source: getEl("indicatorSource")?.value || "close",

    multiplier: Math.max(
      0.1,
      Number(getEl("indicatorMultiplier")?.value || 2)
    ),

    atrLength: Math.max(
      1,
      Number(getEl("indicatorAtrLength")?.value || 10)
    ),

    fastLength: Math.max(
      1,
      Number(getEl("indicatorFastLength")?.value || 12)
    ),

    slowLength: Math.max(
      1,
      Number(getEl("indicatorSlowLength")?.value || 26)
    ),

    signalLength: Math.max(
      1,
      Number(getEl("indicatorSignalLength")?.value || 9)
    ),

    color: getEl("indicatorColor")?.value || "#2563eb",

    secondaryColor:
      getEl("indicatorSecondaryColor")?.value || "#dc2626",

    lineWidth: Math.max(
      1,
      Number(getEl("indicatorLineWidth")?.value || 2)
    ),

    visible: Boolean(getEl("indicatorVisible")?.checked)
  };
}

function openIndicatorSettings(indicatorType, indicatorId = null) {
  const modal = getEl("indicatorSettingsModal");

  if (!modal) {
    console.error(
      "Indicator settings modal not found: #indicatorSettingsModal"
    );
    return;
  }

  currentIndicatorType = indicatorType;
  currentIndicatorId = indicatorId;

  let settings = getIndicatorDefaultSettings(indicatorType);

  const chartState = advancedCharts[activeIndicatorChart];

  if (
    chartState &&
    Array.isArray(chartState.indicators) &&
    indicatorId
  ) {
    const existingIndicator = chartState.indicators.find(
      indicator => indicator.id === indicatorId
    );

    if (existingIndicator) {
      settings = {
        ...settings,
        ...existingIndicator.settings
      };
    }
  }

  const title = getEl("indicatorSettingsTitle");

  if (title) {
    title.textContent =
      `Indicator settings — ${indicatorType.toUpperCase()}`;
  }

  setIndicatorFieldVisibility(indicatorType);
  fillIndicatorSettingsForm(settings);

  modal.classList.add("open");
  modal.setAttribute("aria-hidden", "false");

  showIndicatorSettingsTab("inputs");
}

function closeIndicatorSettings() {
  const modal = getEl("indicatorSettingsModal");

  if (!modal) return;

  modal.classList.remove("open");
  modal.setAttribute("aria-hidden", "true");

  currentIndicatorType = null;
  currentIndicatorId = null;
}

function showIndicatorSettingsTab(tabName) {
  const inputsPanel = getEl("indicatorInputsPanel");
  const stylePanel = getEl("indicatorStylePanel");

  document
    .querySelectorAll(".indicator-settings-tab")
    .forEach(button => {
      button.classList.toggle(
        "active",
        button.dataset.settingsTab === tabName
      );
    });

  if (inputsPanel) {
    inputsPanel.hidden = tabName !== "inputs";
  }

  if (stylePanel) {
    stylePanel.hidden = tabName !== "style";
  }
}

function saveIndicatorSettings() {
  if (!activeIndicatorChart || !currentIndicatorType) {
    console.error("No active chart or indicator selected");
    return;
  }

  const chartState = advancedCharts[activeIndicatorChart];

  if (!chartState) {
    console.error(`Unknown chart: ${activeIndicatorChart}`);
    return;
  }

  /*
   * Convert old object state into an array.
   */
  if (!Array.isArray(chartState.indicators)) {
    chartState.indicators = [];
  }

  const settings = readIndicatorSettingsForm();

  if (currentIndicatorId) {
    const existingIndex = chartState.indicators.findIndex(
      indicator => indicator.id === currentIndicatorId
    );

    if (existingIndex >= 0) {
      chartState.indicators[existingIndex] = {
        ...chartState.indicators[existingIndex],
        type: currentIndicatorType,
        settings
      };
    }
  } else {
    chartState.indicators.push({
      id: createIndicatorId(currentIndicatorType),
      type: currentIndicatorType,
      settings,
      series: []
    });
  }

  closeIndicatorSettings();
  closeIndicatorModal();
  refreshIndicatorModalState();

  console.log(
    `Saved ${currentIndicatorType} for ${activeIndicatorChart}`,
    settings
  );

  /*
   * The actual indicator drawing function will be called here.
   */
  redrawChartIndicators(activeIndicatorChart);
}

function removeCurrentIndicator() {
  if (
    !activeIndicatorChart ||
    !currentIndicatorId
  ) {
    closeIndicatorSettings();
    return;
  }

  const chartState = advancedCharts[activeIndicatorChart];

  if (!chartState || !Array.isArray(chartState.indicators)) {
    closeIndicatorSettings();
    return;
  }

  chartState.indicators = chartState.indicators.filter(
    indicator => indicator.id !== currentIndicatorId
  );

  closeIndicatorSettings();
  refreshIndicatorModalState();
  redrawChartIndicators(activeIndicatorChart);
}

function initialiseIndicatorSettingsModal() {
  const modal = getEl("indicatorSettingsModal");
  const closeButton = getEl("closeIndicatorSettings");
  const cancelButton = getEl("cancelIndicatorSettings");
  const saveButton = getEl("saveIndicatorSettings");
  const removeButton = getEl("removeIndicatorSettings");

  if (!modal) {
    console.error(
      "Cannot initialise indicator settings: modal not found"
    );
    return;
  }

  closeButton?.addEventListener(
    "click",
    closeIndicatorSettings
  );

  cancelButton?.addEventListener(
    "click",
    closeIndicatorSettings
  );

  saveButton?.addEventListener(
    "click",
    saveIndicatorSettings
  );

  removeButton?.addEventListener(
    "click",
    removeCurrentIndicator
  );

  document
    .querySelectorAll(".indicator-settings-tab")
    .forEach(button => {
      button.addEventListener("click", () => {
        showIndicatorSettingsTab(
          button.dataset.settingsTab || "inputs"
        );
      });
    });

  modal.addEventListener("click", event => {
    if (event.target === modal) {
      closeIndicatorSettings();
    }
  });
}

function getIndicatorSourceValue(row, source = "close") {
  const open = Number(row?.open);
  const high = Number(row?.high);
  const low = Number(row?.low);
  const close = Number(row?.close);

  if (![open, high, low, close].every(Number.isFinite)) return null;

  switch (source) {
    case "open": return open;
    case "high": return high;
    case "low": return low;
    case "hl2": return (high + low) / 2;
    case "hlc3": return (high + low + close) / 3;
    case "ohlc4": return (open + high + low + close) / 4;
    default: return close;
  }
}

function calculateSMA(rows, period, source = "close") {
  const result = [];
  const window = [];
  let sum = 0;

  rows.forEach(row => {
    const value = getIndicatorSourceValue(row, source);
    if (!Number.isFinite(value)) return;

    window.push(value);
    sum += value;

    if (window.length > period) sum -= window.shift();
    if (window.length === period) result.push({ time: row.time, value: sum / period });
  });

  return result;
}

function calculateEMA(rows, period, source = "close") {
  const values = rows
    .map(row => ({ row, value: getIndicatorSourceValue(row, source) }))
    .filter(item => Number.isFinite(item.value));

  if (values.length < period) return [];

  const result = [];
  const multiplier = 2 / (period + 1);
  let ema = values.slice(0, period).reduce((sum, item) => sum + item.value, 0) / period;
  result.push({ time: values[period - 1].row.time, value: ema });

  for (let i = period; i < values.length; i += 1) {
    ema = ((values[i].value - ema) * multiplier) + ema;
    result.push({ time: values[i].row.time, value: ema });
  }

  return result;
}

function calculateVWAP(rows, source = "hlc3") {
  const result = [];
  let cumulativePV = 0;
  let cumulativeVolume = 0;

  rows.forEach(row => {
    const price = getIndicatorSourceValue(row, source);
    const volume = Number(row?.volume ?? row?.qty ?? 0);
    if (!Number.isFinite(price) || !Number.isFinite(volume) || volume <= 0) return;

    cumulativePV += price * volume;
    cumulativeVolume += volume;
    result.push({ time: row.time, value: cumulativePV / cumulativeVolume });
  });

  return result;
}

function calculateBollingerBands(rows, period, multiplier, source = "close") {
  const middle = [];
  const upper = [];
  const lower = [];
  const window = [];

  rows.forEach(row => {
    const value = getIndicatorSourceValue(row, source);
    if (!Number.isFinite(value)) return;

    window.push(value);
    if (window.length > period) window.shift();
    if (window.length !== period) return;

    const mean = window.reduce((sum, item) => sum + item, 0) / period;
    const variance = window.reduce((sum, item) => sum + ((item - mean) ** 2), 0) / period;
    const deviation = Math.sqrt(variance) * multiplier;

    middle.push({ time: row.time, value: mean });
    upper.push({ time: row.time, value: mean + deviation });
    lower.push({ time: row.time, value: mean - deviation });
  });

  return { middle, upper, lower };
}

function calculateATR(rows, period) {
  const trueRanges = [];
  const result = [];
  let previousClose = null;
  let atr = null;

  rows.forEach(row => {
    const high = Number(row?.high);
    const low = Number(row?.low);
    const close = Number(row?.close);
    if (![high, low, close].every(Number.isFinite)) return;

    const tr = previousClose === null
      ? high - low
      : Math.max(high - low, Math.abs(high - previousClose), Math.abs(low - previousClose));
    trueRanges.push({ time: row.time, value: tr });
    previousClose = close;
  });

  if (trueRanges.length < period) return [];
  atr = trueRanges.slice(0, period).reduce((sum, item) => sum + item.value, 0) / period;
  result.push({ time: trueRanges[period - 1].time, value: atr });

  for (let i = period; i < trueRanges.length; i += 1) {
    atr = ((atr * (period - 1)) + trueRanges[i].value) / period;
    result.push({ time: trueRanges[i].time, value: atr });
  }

  return result;
}

function calculateRSI(rows, period, source = "close") {
  const values = rows
    .map(row => ({ time: row.time, value: getIndicatorSourceValue(row, source) }))
    .filter(item => Number.isFinite(item.value));
  if (values.length <= period) return [];

  let gains = 0;
  let losses = 0;
  for (let i = 1; i <= period; i += 1) {
    const change = values[i].value - values[i - 1].value;
    gains += Math.max(change, 0);
    losses += Math.max(-change, 0);
  }

  let avgGain = gains / period;
  let avgLoss = losses / period;
  const result = [];
  const toRsi = () => avgLoss === 0 ? 100 : 100 - (100 / (1 + (avgGain / avgLoss)));
  result.push({ time: values[period].time, value: toRsi() });

  for (let i = period + 1; i < values.length; i += 1) {
    const change = values[i].value - values[i - 1].value;
    avgGain = ((avgGain * (period - 1)) + Math.max(change, 0)) / period;
    avgLoss = ((avgLoss * (period - 1)) + Math.max(-change, 0)) / period;
    result.push({ time: values[i].time, value: toRsi() });
  }

  return result;
}

function calculateMACD(rows, fastLength, slowLength, signalLength, source = "close") {
  const fast = calculateEMA(rows, fastLength, source);
  const slow = calculateEMA(rows, slowLength, source);
  const slowMap = new Map(slow.map(item => [item.time, item.value]));
  const macd = fast
    .filter(item => slowMap.has(item.time))
    .map(item => ({ time: item.time, value: item.value - slowMap.get(item.time) }));

  const signalRows = macd.map(item => ({
    time: item.time,
    open: item.value,
    high: item.value,
    low: item.value,
    close: item.value
  }));
  const signal = calculateEMA(signalRows, signalLength, "close");
  const signalMap = new Map(signal.map(item => [item.time, item.value]));
  const histogram = macd
    .filter(item => signalMap.has(item.time))
    .map(item => ({
      time: item.time,
      value: item.value - signalMap.get(item.time),
      color: item.value - signalMap.get(item.time) >= 0 ? "rgba(22,163,74,0.65)" : "rgba(220,38,38,0.65)"
    }));

  return { macd, signal, histogram };
}

function calculateSupertrend(rows, atrLength, multiplier) {
  const atr = calculateATR(rows, atrLength);
  const atrMap = new Map(atr.map(item => [item.time, item.value]));
  const result = [];
  let finalUpper = null;
  let finalLower = null;
  let supertrend = null;
  let previousClose = null;

  rows.forEach(row => {
    const high = Number(row?.high);
    const low = Number(row?.low);
    const close = Number(row?.close);
    const atrValue = atrMap.get(row.time);
    if (![high, low, close, atrValue].every(Number.isFinite)) {
      previousClose = Number.isFinite(close) ? close : previousClose;
      return;
    }

    const hl2 = (high + low) / 2;
    const basicUpper = hl2 + (multiplier * atrValue);
    const basicLower = hl2 - (multiplier * atrValue);

    finalUpper = finalUpper === null || basicUpper < finalUpper || previousClose > finalUpper
      ? basicUpper
      : finalUpper;
    finalLower = finalLower === null || basicLower > finalLower || previousClose < finalLower
      ? basicLower
      : finalLower;

    if (supertrend === null) supertrend = finalUpper;
    else if (supertrend === finalUpper) supertrend = close <= finalUpper ? finalUpper : finalLower;
    else supertrend = close >= finalLower ? finalLower : finalUpper;

    result.push({
      time: row.time,
      value: supertrend,
      color: close >= supertrend ? "#16a34a" : "#dc2626"
    });
    previousClose = close;
  });

  return result;
}

function safeRemoveSeries(chart, series) {
  if (!chart || !series) return;
  try { chart.removeSeries(series); } catch (error) { console.warn("Indicator series removal warning:", error); }
}

function clearIndicatorSeries(chartState, indicator) {
  if (!Array.isArray(indicator.series)) indicator.series = [];
  indicator.series.forEach(series => safeRemoveSeries(chartState.chart, series));
  indicator.series = [];
}

function createLineIndicatorSeries(chartState, data, settings, options = {}) {
  const series = chartState.chart.addLineSeries({
    color: options.color || settings.color,
    lineWidth: Number(settings.lineWidth || 2),
    visible: settings.visible !== false,
    priceScaleId: options.priceScaleId || "right",
    lastValueVisible: options.lastValueVisible ?? true,
    priceLineVisible: options.priceLineVisible ?? false
  });
  series.setData(data);
  return series;
}

function redrawChartIndicators(chartName) {
  const chartState = advancedCharts[chartName];
  if (!chartState?.chart || !Array.isArray(chartState.rows)) return;
  if (!Array.isArray(chartState.indicators)) chartState.indicators = [];

  chartState.indicators.forEach(indicator => {
    clearIndicatorSeries(chartState, indicator);
    const settings = indicator.settings || {};
    const rows = chartState.rows;

    try {
      if (indicator.type === "ema") {
        indicator.series.push(createLineIndicatorSeries(chartState, calculateEMA(rows, settings.length, settings.source), settings));
      } else if (indicator.type === "sma") {
        indicator.series.push(createLineIndicatorSeries(chartState, calculateSMA(rows, settings.length, settings.source), settings));
      } else if (indicator.type === "vwap") {
        indicator.series.push(createLineIndicatorSeries(chartState, calculateVWAP(rows, settings.source), settings));
      } else if (indicator.type === "bollinger") {
        const bands = calculateBollingerBands(rows, settings.length, settings.multiplier, settings.source);
        indicator.series.push(createLineIndicatorSeries(chartState, bands.middle, settings));
        indicator.series.push(createLineIndicatorSeries(chartState, bands.upper, settings, { color: settings.secondaryColor }));
        indicator.series.push(createLineIndicatorSeries(chartState, bands.lower, settings, { color: settings.secondaryColor }));
      } else if (indicator.type === "supertrend") {
        const data = calculateSupertrend(rows, settings.atrLength, settings.multiplier);
        const series = chartState.chart.addLineSeries({
          color: settings.color,
          lineWidth: Number(settings.lineWidth || 2),
          visible: settings.visible !== false,
          priceLineVisible: false,
          lastValueVisible: true
        });
        series.setData(data);
        indicator.series.push(series);
      } else if (indicator.type === "rsi") {
        chartState.chart.priceScale("rsi").applyOptions({ scaleMargins: { top: 0.72, bottom: 0.02 } });
        indicator.series.push(createLineIndicatorSeries(chartState, calculateRSI(rows, settings.length, settings.source), settings, { priceScaleId: "rsi" }));
      } else if (indicator.type === "atr") {
        chartState.chart.priceScale("atr").applyOptions({ scaleMargins: { top: 0.72, bottom: 0.02 } });
        indicator.series.push(createLineIndicatorSeries(chartState, calculateATR(rows, settings.length), settings, { priceScaleId: "atr" }));
      } else if (indicator.type === "macd") {
        chartState.chart.priceScale("macd").applyOptions({ scaleMargins: { top: 0.72, bottom: 0.02 } });
        const macd = calculateMACD(rows, settings.fastLength, settings.slowLength, settings.signalLength, settings.source);
        indicator.series.push(createLineIndicatorSeries(chartState, macd.macd, settings, { priceScaleId: "macd" }));
        indicator.series.push(createLineIndicatorSeries(chartState, macd.signal, settings, { priceScaleId: "macd", color: settings.secondaryColor }));
        const histogram = chartState.chart.addHistogramSeries({
          priceScaleId: "macd",
          priceFormat: { type: "price" },
          visible: settings.visible !== false,
          priceLineVisible: false,
          lastValueVisible: false
        });
        histogram.setData(macd.histogram);
        indicator.series.push(histogram);
      } else if (indicator.type === "volume") {
        const volumeData = rows
          .map(row => ({
            time: row.time,
            value: Number(row?.volume ?? row?.qty ?? 0),
            color: Number(row?.close) >= Number(row?.open)
              ? "rgba(22,163,74,0.55)"
              : "rgba(220,38,38,0.55)"
          }))
          .filter(item => Number.isFinite(item.value) && item.value > 0);

        const histogram = chartState.chart.addHistogramSeries({
          priceScaleId: "volume",
          priceFormat: { type: "volume" },
          visible: settings.visible !== false,
          priceLineVisible: false,
          lastValueVisible: false
        });
        chartState.chart.priceScale("volume").applyOptions({ scaleMargins: { top: 0.78, bottom: 0 } });
        histogram.setData(volumeData);
        indicator.series.push(histogram);
      }
    } catch (error) {
      console.error(`Failed to draw ${indicator.type} on ${chartName}:`, error);
    }
  });
}

/* =========================================================
   CHART DRAWING TOOLS
========================================================= */

const chartDrawings = {
  index: [],
  future: [],
  vix: []
};

const drawingState = {
  chartName: null,
  tool: null,
  canvas: null,
  context: null,
  startPoint: null,
  previewPoint: null,
  drawing: false
};

function getDrawingCanvas(chartName) {
  const ids = {
    index: "indexDrawingCanvas",
    future: "futureDrawingCanvas",
    vix: "vixDrawingCanvas"
  };

  return getEl(ids[chartName]);
}

function resizeDrawingCanvas(chartName) {
  const canvas = getDrawingCanvas(chartName);

  if (!canvas) return;

  const rect = canvas.parentElement.getBoundingClientRect();
  const ratio = window.devicePixelRatio || 1;

  canvas.width = Math.max(1, Math.round(rect.width * ratio));
  canvas.height = Math.max(1, Math.round(rect.height * ratio));

  canvas.style.width = `${rect.width}px`;
  canvas.style.height = `${rect.height}px`;

  const context = canvas.getContext("2d");

  context.setTransform(ratio, 0, 0, ratio, 0, 0);

  redrawSavedDrawings(chartName);
}

function getCanvasPoint(canvas, event) {
  const rect = canvas.getBoundingClientRect();

  return {
    x: event.clientX - rect.left,
    y: event.clientY - rect.top
  };
}

function drawTrendLine(context, start, end) {
  context.beginPath();
  context.moveTo(start.x, start.y);
  context.lineTo(end.x, end.y);
  context.stroke();
}

function drawHorizontalLine(context, point, width) {
  context.beginPath();
  context.moveTo(0, point.y);
  context.lineTo(width, point.y);
  context.stroke();
}

function drawRectangle(context, start, end) {
  const x = Math.min(start.x, end.x);
  const y = Math.min(start.y, end.y);
  const width = Math.abs(end.x - start.x);
  const height = Math.abs(end.y - start.y);

  context.strokeRect(x, y, width, height);
}

function drawTextAnnotation(context, point, text) {
  context.save();
  context.font = "bold 14px Arial";
  context.fillStyle = "#111827";
  context.fillText(text, point.x, point.y);
  context.restore();
}

function applyDrawingStyle(context) {
  context.strokeStyle = "#2563eb";
  context.fillStyle = "#2563eb";
  context.lineWidth = 2;
  context.lineCap = "round";
  context.lineJoin = "round";
}

function renderDrawing(context, canvas, drawing) {
  applyDrawingStyle(context);

  if (drawing.tool === "trend") {
    drawTrendLine(context, drawing.start, drawing.end);
  }

  if (drawing.tool === "horizontal") {
    drawHorizontalLine(
      context,
      drawing.start,
      canvas.getBoundingClientRect().width
    );
  }

  if (drawing.tool === "rectangle") {
    drawRectangle(context, drawing.start, drawing.end);
  }

  if (drawing.tool === "text") {
    drawTextAnnotation(
      context,
      drawing.start,
      drawing.text || "Text"
    );
  }
}

function redrawSavedDrawings(chartName, previewDrawing = null) {
  const canvas = getDrawingCanvas(chartName);

  if (!canvas) return;

  const context = canvas.getContext("2d");
  const rect = canvas.getBoundingClientRect();

  context.clearRect(0, 0, rect.width, rect.height);

  const drawings = chartDrawings[chartName] || [];

  drawings.forEach(drawing => {
    renderDrawing(context, canvas, drawing);
  });

  if (previewDrawing) {
    renderDrawing(context, canvas, previewDrawing);
  }
}

function deactivateDrawingTool() {
  if (drawingState.canvas) {
    drawingState.canvas.classList.remove("active");
  }

  document
    .querySelectorAll(
      '.chart-toolbar button[data-tool="trend"], ' +
      '.chart-toolbar button[data-tool="horizontal"], ' +
      '.chart-toolbar button[data-tool="rectangle"], ' +
      '.chart-toolbar button[data-tool="text"]'
    )
    .forEach(button => {
      button.classList.remove("active");
    });

  drawingState.chartName = null;
  drawingState.tool = null;
  drawingState.canvas = null;
  drawingState.context = null;
  drawingState.startPoint = null;
  drawingState.previewPoint = null;
  drawingState.drawing = false;
}

function activateDrawingTool(chartName, tool, button) {
  deactivateDrawingTool();

  const canvas = getDrawingCanvas(chartName);

  if (!canvas) {
    console.error(`Drawing canvas missing for ${chartName}`);
    return;
  }

  resizeDrawingCanvas(chartName);

  drawingState.chartName = chartName;
  drawingState.tool = tool;
  drawingState.canvas = canvas;
  drawingState.context = canvas.getContext("2d");

  canvas.classList.add("active");
  button.classList.add("active");
}

function clearChartDrawings(chartName) {
  if (!chartDrawings[chartName]) return;

  chartDrawings[chartName] = [];
  redrawSavedDrawings(chartName);
}

function handleDrawingMouseDown(event) {
  if (!drawingState.canvas || !drawingState.tool) return;

  const point = getCanvasPoint(drawingState.canvas, event);

  if (drawingState.tool === "horizontal") {
    chartDrawings[drawingState.chartName].push({
      tool: "horizontal",
      start: point
    });

    redrawSavedDrawings(drawingState.chartName);
    return;
  }

  if (drawingState.tool === "text") {
    const text = window.prompt("Enter annotation text:");

    if (text && text.trim()) {
      chartDrawings[drawingState.chartName].push({
        tool: "text",
        start: point,
        text: text.trim()
      });

      redrawSavedDrawings(drawingState.chartName);
    }

    return;
  }

  drawingState.startPoint = point;
  drawingState.previewPoint = point;
  drawingState.drawing = true;
}

function handleDrawingMouseMove(event) {
  if (
    !drawingState.drawing ||
    !drawingState.canvas ||
    !drawingState.startPoint
  ) {
    return;
  }

  drawingState.previewPoint = getCanvasPoint(
    drawingState.canvas,
    event
  );

  redrawSavedDrawings(drawingState.chartName, {
    tool: drawingState.tool,
    start: drawingState.startPoint,
    end: drawingState.previewPoint
  });
}

function handleDrawingMouseUp(event) {
  if (
    !drawingState.drawing ||
    !drawingState.canvas ||
    !drawingState.startPoint
  ) {
    return;
  }

  const endPoint = getCanvasPoint(drawingState.canvas, event);

  chartDrawings[drawingState.chartName].push({
    tool: drawingState.tool,
    start: drawingState.startPoint,
    end: endPoint
  });

  drawingState.startPoint = null;
  drawingState.previewPoint = null;
  drawingState.drawing = false;

  redrawSavedDrawings(drawingState.chartName);
}

function initialiseDrawingCanvas(chartName) {
  const canvas = getDrawingCanvas(chartName);

  if (!canvas || canvas.dataset.drawingInitialised === "true") {
    return;
  }

  canvas.addEventListener("mousedown", handleDrawingMouseDown);
  canvas.addEventListener("mousemove", handleDrawingMouseMove);
  canvas.addEventListener("mouseup", handleDrawingMouseUp);
  canvas.addEventListener("mouseleave", event => {
    if (drawingState.drawing) {
      handleDrawingMouseUp(event);
    }
  });

  canvas.dataset.drawingInitialised = "true";

  resizeDrawingCanvas(chartName);
}

function initialiseChartDrawingTools() {
  ["index", "future", "vix"].forEach(initialiseDrawingCanvas);

  document
    .querySelectorAll(".chart-toolbar button[data-tool]")
    .forEach(button => {
      if (button.dataset.drawingClickAttached === "true") {
        return;
      }

      const tool = button.dataset.tool;

      if (tool === "indicators") {
        return;
      }

      button.addEventListener("click", event => {
        event.preventDefault();

        const toolbar = button.closest(".chart-toolbar");
        const chartName = toolbar?.dataset.chart;
        const chartState = advancedCharts[chartName];

        if (!chartName) return;

        if (tool === "fit") {
          chartState?.chart?.timeScale().fitContent();
          resizeDrawingCanvas(chartName);
          return;
        }

        if (tool === "clear") {
          clearChartDrawings(chartName);
          deactivateDrawingTool();
          return;
        }

        if (tool === "crosshair") {
          deactivateDrawingTool();
          return;
        }

        if (
          tool === "trend" ||
          tool === "horizontal" ||
          tool === "rectangle" ||
          tool === "text"
        ) {
          activateDrawingTool(chartName, tool, button);
        }
      });

      button.dataset.drawingClickAttached = "true";
    });

  window.addEventListener("resize", () => {
    ["index", "future", "vix"].forEach(resizeDrawingCanvas);
  });
}

async function initSimulator() {
  try {
    ensureExpiryWarning();
    ensureMetricsCards();
    ensurePositionsHeaders();

    await loadDefaults();
    await loadChain();

    if (chain.length) {
      loadTemplate("short_straddle");
    }
  } catch (err) {
    console.error("initSimulator error:", err);
    alert(err.message || "Simulator failed to initialize");
  }
}

window.addEventListener("DOMContentLoaded", () => {
  ensureExpiryWarning();
  ensureMetricsCards();
  ensurePositionsHeaders();

  initialiseIndicatorModal();
  initialiseIndicatorSettingsModal();
  initialiseChartDrawingTools();

  // =====================================================
  // REFRESH BUTTON WITH LOADING SIGNAL
  // =====================================================

  const refreshBtn = getEl("refreshBtn");

  if (refreshBtn) {
    refreshBtn.onclick = async () => {
      if (refreshBtn.disabled) return;

      try {
        setRefreshLoading(true);

        /*
         * Manual refresh:
         * preserve the expiry currently selected by the user.
         */
        await loadChain(true, false);

      } catch (error) {
        console.error(
          "Refresh error:",
          error
        );

        alert(
          error.message ||
          "Failed to refresh option chain"
        );

      } finally {
        setRefreshLoading(false);
      }
    };
  }

  // =====================================================
  // DATE CHANGE
  // AUTOMATICALLY SELECT NEAREST VALID EXPIRY
  // =====================================================

  const queryDateInput =
    getEl("queryDate") ||
    getEl("date") ||
    getEl("query_date");

  if (queryDateInput) {
    queryDateInput.addEventListener(
      "change",
      async () => {
        try {
          setRefreshLoading(true);

          /*
           * Clear all existing strategy positions because
           * premiums and Greeks belong to the old date.
           */
          legs = [];
          renderLegs();
          calculateLivePositionGreeks();

          /*
           * The second argument is true.
           *
           * It prevents the previous expiry from being sent
           * to the backend. The backend therefore selects the
           * nearest valid expiry for the newly selected date.
           */
          await loadChain(true, true);

        } catch (error) {
          console.error(
            "Date-change error:",
            error
          );

          alert(
            error.message ||
            "Failed to load data for the selected date"
          );

        } finally {
          setRefreshLoading(false);
        }
      }
    );
  }

  // =====================================================
  // PREVIOUS AND NEXT INTERVAL BUTTONS
  // =====================================================

  const prevBtn = getEl("prevIntervalBtn");

  if (prevBtn) {
    prevBtn.onclick = movePreviousInterval;
  }

  const nextBtn = getEl("nextIntervalBtn");

  if (nextBtn) {
    nextBtn.onclick = moveNextInterval;
  }

  // =====================================================
  // GREEKS MODAL
  // =====================================================

  const openGreeksBtn = getEl("openGreeksBtn");

  if (openGreeksBtn) {
    openGreeksBtn.addEventListener(
      "click",
      openGreeksPopup
    );
  }

  const closeGreeksBtn = getEl("closeGreeksBtn");

  if (closeGreeksBtn) {
    closeGreeksBtn.addEventListener(
      "click",
      closeGreeksPopup
    );
  }

  // =====================================================
  // INDEX CHART
  // =====================================================

  const chartOpenBtn = getEl("openChartPopup");

  if (chartOpenBtn) {
    chartOpenBtn.addEventListener(
      "click",
      openChartPopup
    );
  }

  const chartCloseBtn = getEl("closeChartPopup");

  if (chartCloseBtn) {
    chartCloseBtn.addEventListener(
      "click",
      closeChartPopup
    );
  }

  // =====================================================
  // FUTURE CHART
  // =====================================================

  const futureOpenBtn =
    getEl("openFutureChartPopup");

  if (futureOpenBtn) {
    futureOpenBtn.addEventListener(
      "click",
      openFutureChartPopup
    );
  }

  const futureCloseBtn =
    getEl("closeFutureChartPopup");

  if (futureCloseBtn) {
    futureCloseBtn.addEventListener(
      "click",
      closeFutureChartPopup
    );
  }

  const futureMonthSelect =
    getEl("futureMonthSelect");

  if (futureMonthSelect) {
    futureMonthSelect.addEventListener(
      "change",
      openFutureChartPopup
    );
  }

  /*
   * These variables may be used elsewhere in your existing
   * chart code. Keep them if they are referenced later.
   */
  const futureChartStartDate =
    getEl("futureChartStartDate");

  const futureChartEndDate =
    getEl("futureChartEndDate");

  const futureChartEndTime =
    getEl("futureChartEndTime");

  const futureChartInterval =
    getEl("futureChartInterval");

  // =====================================================
  // INDIA VIX CHART
  // =====================================================

  const vixOpenBtn =
    getEl("openVixChartPopup");

  if (vixOpenBtn) {
    vixOpenBtn.addEventListener(
      "click",
      openIndiaVixChartPopup
    );
  }

  const vixCloseBtn =
    getEl("closeVixChartPopup");

  if (vixCloseBtn) {
    vixCloseBtn.addEventListener(
      "click",
      closeIndiaVixChartPopup
    );
  }

  // =====================================================
  // CHART APPLY BUTTONS
  // =====================================================

  document
    .querySelectorAll(".chart-apply-btn")
    .forEach(btn => {
      if (
        btn.dataset.clickAttached === "true"
      ) {
        return;
      }

      btn.addEventListener(
        "click",
        () => {
          if (
            btn.dataset.chart === "future"
          ) {
            openFutureChartPopup();
          }

          if (
            btn.dataset.chart === "index"
          ) {
            openChartPopup();
          }

          if (
            btn.dataset.chart === "vix"
          ) {
            openIndiaVixChartPopup();
          }

          if (
            btn.dataset.chart === "option"
          ) {
            openOptionMetricChart(
              {
                preventDefault: () => {}
              },
              Number(btn.dataset.strike),
              btn.dataset.metric
            );
          }
        }
      );

      btn.dataset.clickAttached = "true";
    });

  // =====================================================
  // DATASET CHANGE
  // NIFTY / BANKNIFTY / SENSEX
  // =====================================================

  const datasetSelect =
    getEl("dataset") ||
    getEl("underlying") ||
    getEl("symbol");

  if (datasetSelect) {
    datasetSelect.addEventListener(
      "change",
      async () => {
        try {
          setRefreshLoading(true);

          legs = [];
          renderLegs();
          calculateLivePositionGreeks();

          /*
           * Dataset changed, so the previous expiry must not
           * be preserved.
           *
           * BANKNIFTY will therefore receive its nearest
           * monthly expiry from the backend.
           */
          await loadChain(true, true);

        } catch (error) {
          console.error(
            "Dataset-change error:",
            error
          );

          alert(
            error.message ||
            "Failed to change dataset"
          );

        } finally {
          setRefreshLoading(false);
        }
      }
    );
  }

  // =====================================================
  // EXPIRY CHANGE
  // =====================================================

  const expirySelect =
    getEl("expirySelect");

  if (expirySelect) {
    expirySelect.addEventListener(
      "change",
      async () => {
        try {
          setRefreshLoading(true);

          legs = [];
          renderLegs();
          calculateLivePositionGreeks();

          /*
           * The user manually selected an expiry.
           * Preserve and send that selected value.
           */
          await loadChain(true, false);

        } catch (error) {
          console.error(
            "Expiry-change error:",
            error
          );

          alert(
            error.message ||
            "Failed to load selected expiry"
          );

        } finally {
          setRefreshLoading(false);
        }
      }
    );
  }

  // =====================================================
  // RESPONSIVE CHART RESIZING
  // =====================================================

  window.addEventListener(
    "resize",
    () => {
      const indexContainer =
        getEl("fullscreenIndexChart");

      if (
        fullscreenChart &&
        indexContainer
      ) {
        fullscreenChart.applyOptions({
          width: indexContainer.clientWidth,
          height: indexContainer.clientHeight
        });
      }

      const futureContainer =
        getEl("fullscreenFutureChart");

      if (
        futureChart &&
        futureContainer
      ) {
        futureChart.applyOptions({
          width: futureContainer.clientWidth,
          height: futureContainer.clientHeight
        });
      }

      const vixContainer =
        getEl("fullscreenVixChart");

      if (
        vixChart &&
        vixContainer
      ) {
        vixChart.applyOptions({
          width: vixContainer.clientWidth,
          height: vixContainer.clientHeight
        });
      }
    }
  );

  // =====================================================
  // EXISTING TAB CODE CONTINUES BELOW
  // =====================================================

  const payoffTab = getEl("payoffTab");
  const volSmileTab = getEl("volSmileTab");
  const ivSurfaceTab = getEl("ivSurfaceTab");

  const payoffPanel = getEl("payoffPanel");
  const volSmilePanel = getEl("volSmilePanel");
  const ivSurfacePanel = getEl("ivSurfacePanel");

  /*
   * Keep the remainder of your existing DOMContentLoaded
   * code below this point.
   */

function setMainPanel(activeTab, activePanel) {
  [payoffTab, volSmileTab, ivSurfaceTab].forEach(tab => {
    if (tab) tab.classList.remove("active");
  });

  [payoffPanel, volSmilePanel, ivSurfacePanel].forEach(panel => {
    if (panel) panel.style.display = "none";
  });

  if (activeTab) activeTab.classList.add("active");
  if (activePanel) activePanel.style.display = "block";
}

if (payoffTab && payoffPanel) {
  payoffTab.addEventListener("click", () => {
    setMainPanel(payoffTab, payoffPanel);
  });
}

if (volSmileTab && volSmilePanel) {
  volSmileTab.addEventListener("click", () => {
    setMainPanel(volSmileTab, volSmilePanel);
    renderVolSmile();
  });
}

if (ivSurfaceTab && ivSurfacePanel) {
  ivSurfaceTab.addEventListener("click", () => {
    setMainPanel(ivSurfaceTab, ivSurfacePanel);
    renderIvSurface();
  });
}



  initSimulator();
});
