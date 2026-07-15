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

async function loadChain(forceReload = false) {
  try {
    ensureExpiryWarning();
    ensureMetricsCards();
    ensurePositionsHeaders();

    const params = new URLSearchParams({
              dataset: getDataset(),
              date: getQueryDate(),
              time: getQueryTime(),
              interval: String(getInterval()),
              strike_count: String(getStrikeCount()),
              expiry_rule: "current expiry",
              expiry: getEl("expirySelect")?.value || "",
              _: String(Date.now())
            });

    const data = await fetchJson(`/api/chain?${params.toString()}`);

    if (!data.ok) {
      throw new Error(data.error || "Option chain failed");
    }

    chain = Array.isArray(data.rows) ? data.rows : [];

    const warning = getEl("expiryWarning");

    if (warning && data.query_date && data.expiry_label) {
      warning.textContent =
        `⚠ Weekly expiry payoff is done for ${data.expiry_label}. ` +
        `Payoff is valid only at expiry. Calendar and diagonal spreads are not supported.`;
    }

    if (Number.isFinite(Number(data.spot))) {
      setSpot(data.spot);
    }

    if (Number.isFinite(Number(data.india_vix))) {
      const ivInput = getEl("iv");
      if (ivInput) ivInput.value = Number(data.india_vix).toFixed(1);
    }

    const dteInput = getEl("days");
    if (dteInput && Number.isFinite(Number(data.dte))) {
      dteInput.value = Number(data.dte);
}

    const expiryInput = getEl("expiryLabel");
    if (expiryInput) expiryInput.value = data.expiry_label || "";

    const expirySelect = getEl("expirySelect");

    if (expirySelect && Array.isArray(data.available_expiries)) {
      const currentValue = expirySelect.value || data.expiry;

      expirySelect.innerHTML = "";

      data.available_expiries.forEach(exp => {
        const opt = document.createElement("option");
        opt.value = exp.value;
        opt.textContent = exp.label;

        if (exp.value === currentValue || exp.value === data.expiry) {
          opt.selected = true;
        }

        expirySelect.appendChild(opt);
      });
    }

    legs = legs.map(leg => ({
      ...leg,
      qty: getFixedQty()
    }));

    renderChain();
    renderVolSmile();
    renderLegs();
    await calculate();


  } catch (err) {
    console.error("loadChain error:", err);
    chain = [];
    renderChain();
    alert(err.message || "Failed to load option chain");
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
          position: "top"
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

  try {
    const params = new URLSearchParams({
      dataset: getDataset(),
      date: getQueryDate(),
      time: getQueryTime(),
      interval: String(getInterval()),
      strike_count: String(getStrikeCount()),
      max_months: "2",
      _: String(Date.now())
    });

    container.innerHTML = "<p>Building IV surface...</p>";

    const data = await fetchJson(`/api/iv-surface?${params.toString()}`);
    let finalData = data;

    if (data.status === "processing" && data.job_id) {
      for (let i = 0; i < 60; i++) {
        await new Promise(resolve => setTimeout(resolve, 1000));

        const statusData = await fetchJson(`/api/iv-surface/status/${data.job_id}`);

        if (statusData.status === "ready") {
          finalData = statusData;
          break;
        }

        if (statusData.status === "failed") {
          throw new Error(statusData.error || "IV surface job failed");
        }
      }
    }

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
      throw new Error(finalData?.error || "No IV surface data returned from backend.");
    }

    const strikes = finalData.strikes;
    const expiries = finalData.expiries;

    const rows = finalData.rows.filter(row =>
      row &&
      row.error === undefined &&
      row.iv !== undefined &&
      row.iv !== null &&
      row.strike !== undefined &&
      Number.isFinite(Number(row.iv)) &&
      Number.isFinite(Number(row.strike))
    );

    if (!strikes.length || !expiries.length || !rows.length) {
      container.innerHTML = "<p>No IV surface data found.</p>";
      return;
    }

    const expiryLabels = expiries.map(exp => String(exp.label || exp.value || ""));
    const normalizedStrikes = strikes.map(strike => Number(strike));

    const z = expiries.map(exp =>
      normalizedStrikes.map(strike => {
        const found = rows.find(row =>
          String(row.expiry) === String(exp.value) &&
          Number(row.strike) === Number(strike)
        );

        return found ? Number(found.iv) : null;
      })
    );

    const hoverText = expiries.map((exp, expiryIndex) =>
      normalizedStrikes.map((strike, strikeIndex) => {
        const iv = z[expiryIndex][strikeIndex];

        if (iv === null || iv === undefined || !Number.isFinite(Number(iv))) {
          return "";
        }

        return (
          `Strike: ${strike}<br>` +
          `Expiry: ${exp.label || exp.value}<br>` +
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
      opacity: 0.75,
      showscale: true,
      colorbar: {
        title: "IV %",
        thickness: 18,
        len: 0.75
      },
      connectgaps: true
    };

    const lineTraces = expiries
      .map((exp, expiryIndex) => {
        const lineX = [];
        const lineY = [];
        const lineZ = [];
        const lineText = [];

        normalizedStrikes.forEach((strike, strikeIndex) => {
          const iv = z[expiryIndex][strikeIndex];

          if (iv !== null && iv !== undefined && Number.isFinite(Number(iv))) {
            lineX.push(Number(strike));
            lineY.push(String(exp.label || exp.value || ""));
            lineZ.push(Number(iv) + 0.05);
            lineText.push(
              `Strike: ${strike}<br>` +
              `Expiry: ${exp.label || exp.value}<br>` +
              `IV: ${Number(iv).toFixed(2)}%`
            );
          }
        });

        if (!lineX.length) return null;

        return {
          type: "scatter3d",
          mode: "lines+markers",
          name: exp.label || exp.value,
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
          String(row.expiry_label || "") === String(pinnedIvPoint.expiryLabel || "") ||
          String(row.expiry || "") === String(pinnedIvPoint.expiry || "")
        )
      );

      if (matched) {
        const expiryLabel =
          matched.expiry_label ||
          expiries.find(e => String(e.value) === String(matched.expiry))?.label ||
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
          text: [`Tracking<br>${matched.strike}<br>${Number(matched.iv).toFixed(2)}%`],
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
          title: "",
          showticklabels: false
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
    } catch (purgeErr) {
      console.warn("Plotly purge warning:", purgeErr);
    }

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

    container.on("plotly_click", function(clickData) {
      try {
        if (!clickData || !clickData.points || !clickData.points.length) return;

        const point = clickData.points[0];
        const clickedStrike = Number(point.x);
        const clickedExpiryLabel = String(point.y || "");

        const clicked = rows.find(row =>
          Number(row.strike) === clickedStrike &&
          (
            String(row.expiry_label || "") === clickedExpiryLabel ||
            String(row.expiry || "") === clickedExpiryLabel ||
            expiryLabels.includes(clickedExpiryLabel)
          )
        );

        if (!clicked) return;

        const expiryLabel = clicked.expiry_label || clickedExpiryLabel || clicked.expiry;
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
      } catch (clickErr) {
        console.error("IV surface click handler error:", clickErr);
      }
    });

    container.oncontextmenu = function(event) {
      event.preventDefault();

      pinnedIvPoint = null;

      const box = container.querySelector(".iv-pinned-box");
      if (box) {
        box.style.display = "none";
      }

      renderIvSurface();
      return false;
    };

  } catch (err) {
    console.error("renderIvSurface error:", err);
    container.innerHTML = "<p>Failed to load IV surface. Check console for details.</p>";
    alert(err.message || "Failed to load IV surface");
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
  const g = getGreeksForLeg(type, strike);

  return {
    side,
    type,
    strike: Number(strike),
    expiry: getEl("expirySelect")?.value || "current expiry",
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
  const atmRow = chain.find(row => row.atm);
  const atm = atmRow ? Number(atmRow.strike) : Math.round(getSpot() / 50) * 50;
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
  } else {
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
    if (!leg.expiry) leg.expiry = "current expiry";
    if (!leg.entry_time) leg.entry_time = getQueryTime();

    leg.qty = getFixedQty();

    const tr = document.createElement("tr");

    tr.innerHTML = `
      <td>
        <select onchange="updateLeg(${i}, 'side', this.value)">
          <option value="BUY" ${leg.side === "BUY" ? "selected" : ""}>BUY</option>
          <option value="SELL" ${leg.side === "SELL" ? "selected" : ""}>SELL</option>
        </select>
      </td>

      <td>
        <select onchange="updateLeg(${i}, 'type', this.value)">
          <option value="CE" ${leg.type === "CE" ? "selected" : ""}>CE</option>
          <option value="PE" ${leg.type === "PE" ? "selected" : ""}>PE</option>
        </select>
      </td>

      <td>
        <input class="leg-input" type="number" value="${leg.strike}"
          onchange="updateLeg(${i}, 'strike', Number(this.value))">
      </td>

      <td>
  <select class="leg-input" onchange="updateLeg(${i}, 'expiry', this.value)">
    ${
      getEl("expirySelect")
        ? Array.from(getEl("expirySelect").options)
            .map(
              opt => `
                <option value="${opt.value}"
                  ${leg.expiry === opt.value ? "selected" : ""}>
                  ${opt.textContent}
                </option>
              `
            )
            .join("")
        : `<option value="${leg.expiry}">${leg.expiry}</option>`
    }
  </select>
</td>

      <td>
        <input class="leg-input leg-entry-time" type="time" value="${leg.entry_time}"
          onchange="updateLeg(${i}, 'entry_time', this.value)">
      </td>

      <td>
        <input class="leg-input" type="number" value="${leg.premium}" step="0.05"
          onchange="updateLeg(${i}, 'premium', Number(this.value))">
      </td>

      <td>
        <input class="leg-input" type="number" value="${leg.lots}"
          onchange="updateLeg(${i}, 'lots', Number(this.value))">
      </td>

      <td>
        <input class="leg-input" type="number" value="${getFixedQty()}" readonly>
      </td>

      <td>
        <button class="delete-leg-btn" type="button" onclick="removeLeg(${i})">
          Delete
        </button>
      </td>
    `;

    body.appendChild(tr);
  });
}

function loadTemplate(name) {
  const atmRow = chain.find(row => row.atm);
  const atm = atmRow ? Number(atmRow.strike) : Math.round(getSpot() / 50) * 50;

  const find = (strike, type) => {
    const row = chain.find(x => Number(x.strike) === Number(strike));
    if (!row) return 100;
    return Number((type === "CE" ? row.ce_ltp : row.pe_ltp) || 100);
  };

  if (name === "short_straddle") {
    legs = [
      createLeg("SELL", "CE", atm, find(atm, "CE")),
      createLeg("SELL", "PE", atm, find(atm, "PE"))
    ];
  }

  if (name === "short_strangle") {
    legs = [
      createLeg("SELL", "CE", atm + 200, find(atm + 200, "CE")),
      createLeg("SELL", "PE", atm - 200, find(atm - 200, "PE"))
    ];
  }

  if (name === "iron_condor") {
    legs = [
      createLeg("BUY", "PE", atm - 400, find(atm - 400, "PE")),
      createLeg("SELL", "PE", atm - 200, find(atm - 200, "PE")),
      createLeg("SELL", "CE", atm + 200, find(atm + 200, "CE")),
      createLeg("BUY", "CE", atm + 400, find(atm + 400, "CE"))
    ];
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
  if (!canvas || typeof Chart === "undefined") return;

  const ctx = canvas.getContext("2d");
  if (chart) chart.destroy();

  chart = new Chart(ctx, {
    type: "line",
    data: {
      labels,
      datasets: [
        {
          label: "Expiry Payoff",
          data: payoff,
          borderWidth: 2,
          fill: {
            target: "origin",
            above: "rgba(0, 180, 90, 0.18)",
            below: "rgba(230, 80, 80, 0.18)"
          },
          tension: 0.15
        },
        {
          label: "Current MTM",
          data: current,
          borderWidth: 2,
          borderDash: [6, 4],
          tension: 0.15
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { position: "top" }
      },
      scales: {
        y: { title: { display: true, text: "P&L" } },
        x: { title: { display: true, text: "Spot" } }
      }
    }
  });
}

function attachOhlcBox(chart, series, container, boxId) {
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

  chart.subscribeCrosshairMove(param => {
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
      `O ${candle.open.toFixed(2)} ` +
      `H ${candle.high.toFixed(2)} ` +
      `L ${candle.low.toFixed(2)} ` +
      `C ${candle.close.toFixed(2)}`;
  });
}

async function openChartPopup() {
  try {
    const modal = getEl("chartModal");
    const container = getEl("fullscreenIndexChart");

    if (!modal || !container || typeof LightweightCharts === "undefined") return;

    modal.style.display = "block";

    const startDate = getEl("indexChartStartDate");
    const startTime = getEl("indexChartStartTime");
    const endDate = getEl("indexChartEndDate");
    const endTime = getEl("indexChartEndTime");
    const intervalSelect = getEl("indexChartInterval");

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

    const data = await fetchJson(`/api/index-chart?${params.toString()}`);

    if (!data.ok || !data.rows || !data.rows.length) {
      alert("No index chart data found");
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
        secondsVisible: false,
        borderColor: "#ddd"
      },
      rightPriceScale: {
        borderColor: "#ddd"
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

    fullscreenSeries.setData(data.rows || []);

    attachOhlcBox(
      fullscreenChart,
      fullscreenSeries,
      container,
      "indexOhlcBox"
    );

    fullscreenChart.timeScale().fitContent();

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

  const refreshBtn = getEl("refreshBtn");
  if (refreshBtn) refreshBtn.onclick = () => loadChain(true);

  const prevBtn = getEl("prevIntervalBtn");
  if (prevBtn) prevBtn.onclick = movePreviousInterval;

  const nextBtn = getEl("nextIntervalBtn");
  if (nextBtn) nextBtn.onclick = moveNextInterval;

  const openGreeksBtn = getEl("openGreeksBtn");
  if (openGreeksBtn) openGreeksBtn.addEventListener("click", openGreeksPopup);

  const closeGreeksBtn = getEl("closeGreeksBtn");
  if (closeGreeksBtn) closeGreeksBtn.addEventListener("click", closeGreeksPopup);

  const chartOpenBtn = getEl("openChartPopup");
  if (chartOpenBtn) chartOpenBtn.addEventListener("click", openChartPopup);

  const chartCloseBtn = getEl("closeChartPopup");
  if (chartCloseBtn) chartCloseBtn.addEventListener("click", closeChartPopup);

  const futureOpenBtn = getEl("openFutureChartPopup");
  if (futureOpenBtn) futureOpenBtn.addEventListener("click", openFutureChartPopup);

  const futureCloseBtn = getEl("closeFutureChartPopup");
  if (futureCloseBtn) futureCloseBtn.addEventListener("click", closeFutureChartPopup);

  const futureMonthSelect = getEl("futureMonthSelect");
  if (futureMonthSelect) {
    futureMonthSelect.addEventListener("change", openFutureChartPopup);
  }

  // Reload only when Apply button is clicked

    const futureChartStartDate = getEl("futureChartStartDate");
    const futureChartEndDate = getEl("futureChartEndDate");
    const futureChartEndTime = getEl("futureChartEndTime");
    const futureChartInterval = getEl("futureChartInterval");

  const vixOpenBtn = getEl("openVixChartPopup");
  if (vixOpenBtn) vixOpenBtn.addEventListener("click", openIndiaVixChartPopup);

  const vixCloseBtn = getEl("closeVixChartPopup");
  if (vixCloseBtn) vixCloseBtn.addEventListener("click", closeIndiaVixChartPopup);

  document.querySelectorAll(".chart-apply-btn").forEach(btn => {
    if (btn.dataset.clickAttached) return;

    btn.addEventListener("click", () => {
  if (btn.dataset.chart === "future") openFutureChartPopup();
  if (btn.dataset.chart === "index") openChartPopup();
  if (btn.dataset.chart === "vix") openIndiaVixChartPopup();

  if (btn.dataset.chart === "option") {
    openOptionMetricChart(
      { preventDefault: () => {} },
      Number(btn.dataset.strike),
      btn.dataset.metric
    );
  }
});

    btn.dataset.clickAttached = "true";
  });

  const datasetSelect = getEl("dataset") || getEl("underlying") || getEl("symbol");

  if (datasetSelect) {
    datasetSelect.addEventListener("change", () => {
      legs = [];
      renderLegs();
      calculateLivePositionGreeks();
      loadChain(true);
    });
  }

  const expirySelect = getEl("expirySelect");

  if (expirySelect) {
    expirySelect.addEventListener("change", () => {
      legs = [];
      renderLegs();
      calculateLivePositionGreeks();
      loadChain(true);
    });
  }

  window.addEventListener("resize", () => {
    const indexContainer = getEl("fullscreenIndexChart");
    if (fullscreenChart && indexContainer) {
      fullscreenChart.applyOptions({
        width: indexContainer.clientWidth,
        height: indexContainer.clientHeight
      });
    }

    const futureContainer = getEl("fullscreenFutureChart");
    if (futureChart && futureContainer) {
      futureChart.applyOptions({
        width: futureContainer.clientWidth,
        height: futureContainer.clientHeight
      });
    }

    const vixContainer = getEl("fullscreenVixChart");
    if (vixChart && vixContainer) {
      vixChart.applyOptions({
        width: vixContainer.clientWidth,
        height: vixContainer.clientHeight
      });
    }
  });

  const payoffTab = getEl("payoffTab");
const volSmileTab = getEl("volSmileTab");
const ivSurfaceTab = getEl("ivSurfaceTab");

const payoffPanel = getEl("payoffPanel");
const volSmilePanel = getEl("volSmilePanel");
const ivSurfacePanel = getEl("ivSurfacePanel");

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