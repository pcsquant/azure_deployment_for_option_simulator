# Options Strategy Simulator

A StockMock-style options simulator built with **Flask + Chart.js**, backed by a
historical **tick-level parquet data store**, with an asynchronous **IV-surface**
pipeline on **Kafka + Redis**.

The app reconstructs an option chain at any historical date/time from tick data,
prices implied volatility and Greeks with Black-Scholes, and renders payoff,
metric, and volatility-surface charts.

---

## Features

- Option chain reconstructed at any historical timestamp from tick data
- Add/edit option legs — BUY/SELL CE/PE
- Payoff chart, current MTM line, net credit/debit
- Max profit/loss, breakevens, portfolio Greeks
- Per-strike metric charts (IV / Delta / LTP over the day)
- Implied-volatility **surface** across strikes × expiries (async)
- Strategy templates: Short Straddle, Short Strangle, Iron Condor
- Vectorized Black-Scholes IV solver and Greeks
- Consolidated per-day/per-expiry chain store for fast snapshots

---

## Data Architecture

This project has **no SQL database**. Persistence is split across three layers,
each chosen for its access pattern:

| Layer | Technology | Holds | Lifetime |
|-------|-----------|-------|----------|
| **Cold store** | Parquet files on disk (`PARQUET_BASE_PATH`) | Raw tick data + consolidated chains | Permanent |
| **Hot cache** | In-process LRU dicts (per worker) | Normalized frames, candles, chains | Process lifetime |
| **Shared cache / queue** | Redis + Kafka | IV-surface results, job state, job queue | TTL / transient |

### 1. Cold store — parquet on disk

All historical market data lives under `PARQUET_BASE_PATH` (env var; defaults to
`C:\Users\admin\Documents\shamil\agent\parquet data`, mounted to `/app/data`
inside Docker). The tree is organized by **week folder → per-day tick folders**:

```
PARQUET_BASE_PATH/
└── <week_no> - PARQUET/                    # folder name starts with an int (week_start..week_end)
    ├── NSE_IDX_TICK_YYYYMMDD/              # index / spot ticks for the day
    │   ├── NIFTYYYYYMMDD.parquet           #   underlying spot
    │   └── INDIAVIX.parquet                #   India VIX
    │
    ├── NSE_OPT_TICK_YYYYMMDD/              # one file PER option contract
    │   ├── NIFTY<expiry><strike>CE.parquet
    │   ├── NIFTY<expiry><strike>PE.parquet
    │   └── ...                             #   hundreds of files per day
    │
    ├── NSE_FUT_TICK_YYYYMMDD/
    │   └── Contract Futures/
    │       ├── NIFTY26MARFUT.parquet       #   <symbol><yy><MON>FUT
    │       └── ...
    │
    └── NSE_OPT_CONSOLIDATED_YYYYMMDD/      # derived store (built offline) — see §"Consolidated store"
        └── NIFTY_YYYYMMDD_<expiry>.parquet
```

**Naming conventions**

| Token | Meaning | Example |
|-------|---------|---------|
| `<symbol>` | Instrument | `NIFTY`, `SENSEX` |
| `<expiry>` | Expiry, `yymmdd` (6 digits) | `260430` = 2026-04-30 |
| `<strike>` | Strike price (integer) | `22000` |
| `YYYYMMDD` | Trading date | `20260415` |
| `<week_no>` | Sequential week bucket | `100` |

Per-instrument settings (`strike_step`, expiry calendar, week range) live in
`config_for_simulation.py::get_dataset_config`. NIFTY uses `strike_step=50`;
SENSEX uses `100`. Expiry calendars (`NIFTY_COMBINED_EXPIRY`, `BSE_COMBINED_EXPIRY`)
are hardcoded weekly+monthly date lists used to resolve "current expiry".

**Raw file schemas** (column names are auto-detected case-insensitively, then
normalized by `data_engine_for_simulation._read_parquet_normalized`):

| Mode | Source columns accepted | Normalized output |
|------|------------------------|-------------------|
| `spot` (index/VIX) | `datetime` \| (`date`+`time`); `value`\|`ltp`\|`price`\|`close` | `datetime` (IST, tz-aware), `value` |
| `option`/`future` | `datetime` \| (`date`+`time`); `price`\|`ltp`\|`value`\|`close`; `volume`\|`qty` | `datetime` (IST), `price`, `volume` |

All timestamps are localized/converted to **Asia/Kolkata (IST)** and filtered to
the trading session **09:15–15:30** on load.

### 2. Consolidated option-chain store (derived, fast path)

Because the raw layout is **one file per contract**, reconstructing one option
chain snapshot means opening `~2 × (2·strike_count + 1)` files (e.g. ~42 reads
for ±10 strikes), and an IV surface multiplies that across expiries (~200+
reads). To remove that fan-out, an offline builder pre-aggregates each
`(date, expiry)` into a single **long-format** file:

```
NSE_OPT_CONSOLIDATED_YYYYMMDD/NIFTY_YYYYMMDD_<expiry>.parquet
```

| Column | Type | Meaning |
|--------|------|---------|
| `timestamp` | datetime (1-min, IST wall-clock, tz-naive on disk) | Minute bucket |
| `strike` | int | Strike price |
| `ce` | float | 1-minute **close** (LTP) of the CE contract; `NaN` if no trade |
| `pe` | float | 1-minute **close** (LTP) of the PE contract; `NaN` if no trade |

Rows are sorted by `(timestamp, strike)`. 1-minute closes resample exactly to any
N-minute interval the UI requests (`.last()`), and since the snapshot only ever
consumes the candle **close**, this representation is lossless for this path.

A snapshot now does **one columnar read per expiry** + a vectorized
"LTP-for-all-strikes-at-once" computation, with a transparent **fallback** to the
per-contract reader when no consolidated file exists.

**Building the store** (`build_consolidated_option_chain.py`):

```bash
# Build everything for the configured instrument / week folders
python build_consolidated_option_chain.py --instrument NIFTY

# One date (all expiries) / one expiry / rebuild existing
python build_consolidated_option_chain.py --date 20260415
python build_consolidated_option_chain.py --date 20260415 --expiry 260430
python build_consolidated_option_chain.py --date 20260415 --overwrite

# Parallelism (default = min(8, cpu_count); 1 = serial for debugging)
python build_consolidated_option_chain.py --workers 8
```

The builder fans contract reads out across **worker processes** (the per-minute
resample is CPU-bound), and uses a lean reader that projects only the
datetime/price columns and bypasses the app's RAM cache. It is **incremental**
(skips existing files unless `--overwrite`). Run it after each day's tick
ingestion. The read path picks it up automatically — no app config change needed.

### 3. In-process hot caches (per worker)

LRU dicts in `data_engine_for_simulation` and `simulator` avoid re-reading and
re-deriving data within a process:

| Cache | Module | Key | Limit (env) |
|-------|--------|-----|-------------|
| `PARQUET_FILE_PATH_CACHE` | data_engine | (folder, filename) → path | unbounded |
| `RAW_PARQUET_CACHE` | data_engine | (path, mode) → normalized frame | `MAX_RAW_PARQUET_CACHE_SIZE` (1000) |
| `SPOT_CANDLE_CACHE` | simulator | (dataset, date, interval, folder) → candles | `SPOT_CANDLE_CACHE_SIZE` (50) |
| `OPTION_WINDOW_CACHE` | simulator | (…strike, side, interval, ts) → candle window | `OPTION_WINDOW_CACHE_SIZE` (500) |
| `CONSOLIDATED_CHAIN_CACHE` | simulator | (dataset, date, expiry, folder) → chain frame | `CONSOLIDATED_CHAIN_CACHE_SIZE` (32) |

These are **per-process** — each Flask/worker replica warms independently.

### 4. Redis + Kafka (shared cache & job queue)

Used only by the asynchronous **IV-surface** pipeline:

- **Kafka** (`IV_SURFACE_TOPIC`, default `iv_surface_requests`): the Flask route
  enqueues a job; `iv_surface_worker.py` consumers (a Kafka consumer group)
  process them. Scale throughput by adding worker replicas / topic partitions.
- **Redis**: caches completed surface payloads (`IV_SURFACE_CACHE_TTL_SECONDS`,
  default 1h), tracks in-flight jobs to de-duplicate requests
  (`job_lookup:<cache_key>`), and stores per-job status (`job:iv_surface:<id>`).

---

## Request / processing flow

### Option chain snapshot (`/api/chain`)

```
request(date, time, expiry, strike_count, interval)
  → resolve week folder + date_str           (_resolve_week_folder_for_date)
  → spot candles for the day                  (SPOT_CANDLE_CACHE)
  → nearest candle at/before target time → spot, ATM
  → load consolidated chain for (date, expiry)         ── present? ──┐
        ├─ YES: vectorized LTP for all strikes at once  (fast path)  │
        └─ NO : per-strike CE/PE read + candle window   (fallback)   │
  → append_black_scholes_iv(chain)            (vectorized IV + Greeks)
  → JSON: rows[strike] = {ce_ltp, pe_ltp, iv, greeks, ...}
```

### IV surface (`/api/iv-surface`, async)

```
GET /api/iv-surface ──► Redis cache hit? ──► return payload
        │ miss
        ▼
   create job → Redis (job + lookup) → Kafka(IV_SURFACE_TOPIC)
        │
        └─► 202 {status: processing, job_id}

iv_surface_worker (Kafka consumer group)
        │ consume job
        ▼
   build_iv_surface_payload
        ├─ get_available_expiries_for_date (≤ IV_SURFACE_MAX_EXPIRIES)
        └─ ThreadPool over expiries:
              build_option_chain_snapshot(compute_greeks=False)   # surface needs IV only
              → smile IV per strike (OTM side: PE<ATM, CE>ATM)
        ▼
   write result → Redis(cache_key, TTL) + job status = ready

GET /api/iv-surface/status/<job_id> ──► Redis ──► payload when ready
```

---

## Implied volatility & Greeks

`black_scholes_iv_for_simulation.py`:

- **TTE**: trading-time to expiry — whole days + intraday fraction
  (`minutes_left / 375`), annualized by `/252`.
- **IV solver** (`_vectorized_iv`): vectorized **bisection** on the monotonic
  Black-Scholes price across whole arrays — robust in the zero-vega flat regions
  that trap Newton for deep ITM/OTM. Returns `NaN` for invalid inputs (price
  below intrinsic, above bound, or with negligible extrinsic value), matching the
  prior `py_vollib` semantics.
- **Greeks** (`_compute_greeks_for_side`): fully vectorized Delta/Gamma/Vega/
  Theta/Rho. Gated by `compute_greeks` — the IV-surface path skips them.
- Constants: `RISK_FREE_RATE = 0.0`, `DIVIDEND_YIELD = 0.0`,
  `TRADING_MINUTES_PER_DAY = 375`, `TRADING_DAYS_PER_YEAR = 252`.

---

## Running

### Local (app only)

```bash
pip install -r requirements.txt
python simulator.py        # serves http://localhost:8000
```

> The IV-surface endpoints additionally require Redis and Kafka (see Docker).

### Docker Compose (full stack)

```bash
docker compose up --build
```

Brings up:

| Service | Port | Role |
|---------|------|------|
| `redis` | 6379 | result/job cache |
| `kafka` | 9092 | IV-surface job queue |
| `options-simulator` | 8001 → 8000 | Flask app |
| `iv-surface-worker` | — | Kafka consumer building surfaces |

Mount your real data by editing the `volumes:` entry
(`<host parquet path>:/app/data`) in `docker-compose.yml`, then build the
consolidated store (the builder shares the worker image deps).

---

## Configuration (environment variables)

| Variable | Default | Purpose |
|----------|---------|---------|
| `PARQUET_BASE_PATH` | `…\parquet data` / `/app/data` | Cold-store root |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis connection |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka brokers |
| `IV_SURFACE_TOPIC` | `iv_surface_requests` | Surface job topic |
| `IV_SURFACE_MAX_EXPIRIES` | `5` | Expiries per surface |
| `IV_SURFACE_MAX_WORKERS` | `4` | ThreadPool size per surface job |
| `IV_SURFACE_CACHE_TTL_SECONDS` | `3600` | Redis result TTL |
| `MAX_RAW_PARQUET_CACHE_SIZE` | `1000` | Raw frame cache |
| `OPTION_WINDOW_CACHE_SIZE` | `500` | Option window cache |
| `SPOT_CANDLE_CACHE_SIZE` | `50` | Spot candle cache |
| `CONSOLIDATED_CHAIN_CACHE_SIZE` | `32` | Consolidated chain cache |

---

## Code map

| File | Responsibility |
|------|----------------|
| `simulator.py` | Flask app, routes, snapshot builder, RAM caches |
| `data_engine_for_simulation.py` | Parquet discovery/readers, candles, consolidated loader |
| `black_scholes_iv_for_simulation.py` | TTE, vectorized IV solver, Greeks |
| `iv_surface_async.py` | Surface payload builder, Flask routes, Kafka helpers |
| `iv_surface_worker.py` | Standalone Kafka consumer for surface jobs |
| `build_consolidated_option_chain.py` | Offline builder for the consolidated store |
| `config_for_simulation.py` | Datasets, strike steps, expiry calendars, paths |
| `chart.py` | Chart payload assembly |
| `to_parquet*.py` | One-time CSV/zip → parquet converters |
| `templates/`, `static/` | UI (HTML, JS, CSS) |

---

## API reference (summary)

| Endpoint | Description |
|----------|-------------|
| `GET /api/chain` | Option chain snapshot at a timestamp |
| `GET /api/iv-surface` | Request an IV surface (async; returns `job_id`) |
| `GET /api/iv-surface/status/<job_id>` | Poll surface job / fetch result |
| `GET /api/option-metric-chart` | IV/Delta/LTP for one strike over the day |
| `GET /api/option-ltp-candle-chart` | Option LTP candles |
| `GET /api/index-chart` / `future-chart` / `india-vix-chart` | Underlying charts |
| `POST /api/calculate` | Strategy payoff / Greeks |
| `GET /api/defaults` / `previous-trading-session` | UI bootstrap helpers |
| `GET /api/cache/status` · `POST /api/cache/clear` | RAM cache introspection |
| `GET /api/health` | Health check |

---

This simulator runs on historical recorded ticks. For live use, replace the
parquet cold store with a live NSE/BSE/broker feed behind the same
`data_engine_for_simulation` interface.
