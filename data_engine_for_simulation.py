"""
StockMock-style Options Simulator Flask app.

Place this file at:
    agent_for_production/app.py

This version adds:
- interval-aware RAM window caching
- only 10 candles before + current + 10 candles after are kept per option leg
- works for 1m, 2m, 3m, 5m, 10m, 15m, etc.
- keeps existing API contract unchanged
- uses Redis + ThreadPoolExecutor for asynchronous IV-surface jobs

It uses your existing modules:
    agent_for_data/config_for_simulation.py
    agent_for_data/data_engine_for_simulation.py
    agent_for_data/black_scholes_iv.py

Frontend files expected:
    templates/simulator.html
    static/simulator.js
    static/style.css
"""

from __future__ import annotations

import copy
import json
import logging
import math
import os
import threading
import time
import uuid
from collections import OrderedDict
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from flask import Flask, g, jsonify, render_template, request
from flask.json.provider import JSONProvider
from werkzeug.middleware.proxy_fix import ProxyFix

try:
    import redis
except ImportError:  # pragma: no cover - dependency validation happens at startup
    redis = None


try:
    import orjson
except ImportError:  # pragma: no cover
    orjson = None

from black_scholes_iv_for_simulation import append_black_scholes_iv

try:
    from black_scholes_iv_for_simulation import clear_iv_cache, iv_cache_stats
except ImportError:  # Backward compatibility with an older IV module.
    def clear_iv_cache() -> None:
        return None

    def iv_cache_stats() -> dict:
        return {"enabled": False}
from chart import build_chart_payload
from config_for_simulation import (
    CANDLE_INTERVAL_MINUTES,
    DATA_LAYOUT,
    IST,
    STORAGE_MODE,
    get_dataset_config,
    log_configuration,
    validate_configuration,
)


LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}



class _OrjsonProvider(JSONProvider):
    """Fast Flask JSON provider with safe fallback-compatible behavior."""

    def dumps(self, obj: Any, **kwargs) -> str:
        if orjson is None:
            return json.dumps(obj, default=str, separators=(",", ":"))
        return orjson.dumps(
            obj,
            option=orjson.OPT_NON_STR_KEYS | orjson.OPT_SERIALIZE_NUMPY,
            default=str,
        ).decode("utf-8")

    def loads(self, value: Any, **kwargs) -> Any:
        if orjson is None:
            return json.loads(value)
        return orjson.loads(value)


def create_app() -> Flask:
    flask_app = Flask(__name__)
    flask_app.json = _OrjsonProvider(flask_app)
    flask_app.config.update(
        JSON_SORT_KEYS=False,
        MAX_CONTENT_LENGTH=int(os.getenv("MAX_CONTENT_LENGTH", str(2 * 1024 * 1024))),
        PROPAGATE_EXCEPTIONS=False,
    )
    if _env_bool("TRUST_PROXY_HEADERS", True):
        flask_app.wsgi_app = ProxyFix(
            flask_app.wsgi_app,
            x_for=1,
            x_proto=1,
            x_host=1,
            x_port=1,
        )
    return flask_app


app = create_app()

# Validate static configuration at process startup. Historical paths are checked
# lazily because Blob mode does not require local data directories.
validate_configuration(require_data_paths=False)
log_configuration()
logger.info(
    "Options Simulator startup: storage_mode=%s data_layout=%s",
    STORAGE_MODE,
    DATA_LAYOUT,
)


@app.before_request
def _before_request():
    g.request_started_at = time.monotonic()
    g.request_id = request.headers.get("X-Request-ID") or uuid.uuid4().hex


@app.after_request
def add_response_headers(response):
    response.headers["X-Request-ID"] = getattr(g, "request_id", uuid.uuid4().hex)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "SAMEORIGIN"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
    if request.path.startswith("/api/"):
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    started = getattr(g, "request_started_at", None)
    if started is not None:
        response.headers["Server-Timing"] = f'app;dur={(time.monotonic() - started) * 1000:.2f}'
    return response


@app.errorhandler(404)
def _not_found(_error):
    if request.path.startswith("/api/"):
        return jsonify({"ok": False, "error": "Endpoint not found."}), 404
    return render_template("simulator.html"), 404


@app.errorhandler(500)
def _internal_error(error):
    logger.exception("Unhandled application error", exc_info=error)
    return jsonify({"ok": False, "error": "Internal server error."}), 500




def _slice_consolidated_option_data(chain, strike):
    """Return the standard CE/PE option-data shape for one strike.

    The consolidated chain stores timestamp/strike/ce/pe columns.  Downstream
    code expects datetime/price/volume frames, so normalize the fast-path
    result to exactly the same interface as the per-contract fallback loader.
    """
    empty = pd.DataFrame(columns=["datetime", "price", "volume"])

    if chain is None or chain.empty:
        return {"CE": empty.copy(), "PE": empty.copy()}

    required = {"timestamp", "strike"}
    if not required.issubset(chain.columns):
        return {"CE": empty.copy(), "PE": empty.copy()}

    strike_value = int(strike)
    strike_rows = chain.loc[
        pd.to_numeric(chain["strike"], errors="coerce") == strike_value
    ]

    result = {}
    for side, price_column in (("CE", "ce"), ("PE", "pe")):
        if price_column not in strike_rows.columns:
            result[side] = empty.copy()
            continue

        frame = strike_rows[["timestamp", price_column]].rename(
            columns={"timestamp": "datetime", price_column: "price"}
        ).copy()
        frame["datetime"] = pd.to_datetime(frame["datetime"], errors="coerce")
        frame["price"] = pd.to_numeric(frame["price"], errors="coerce")
        frame["volume"] = 0
        frame = (
            frame.dropna(subset=["datetime", "price"])
            .sort_values("datetime")
            .reset_index(drop=True)
        )
        result[side] = frame[["datetime", "price", "volume"]]

    return result


def _load_option_data_fast(folder, date_str, expiry_str, strike, dataset, chain=None):
    """Use the consolidated chain first and fall back to contract files.

    Passing ``chain`` lets callers processing many strikes load the consolidated
    file only once.  A missing consolidated file preserves the existing
    per-contract behavior.
    """
    consolidated = chain
    if consolidated is None:
        consolidated = load_consolidated_option_chain(
            folder=folder,
            date_str=date_str,
            expiry_str=expiry_str,
            instrument=dataset,
        )

    if consolidated is not None:
        return _slice_consolidated_option_data(consolidated, strike)

    return load_required_option_data_for_date(
        folder=folder,
        date_str=date_str,
        expiry_str=expiry_str,
        strike=int(strike),
        instrument=dataset,
    )


LOT_SIZE_BY_INSTRUMENT = {
    "NIFTY": 65,
    "SENSEX": 20,
}

DEFAULT_QUERY_TIME = "09:30"


MAX_ALLOWED_QUERY_DATE = date.max

# Number of candles to keep before and after selected candle.
WINDOW_CANDLES_BEFORE = max(0, int(os.getenv("WINDOW_CANDLES_BEFORE", "10")))
WINDOW_CANDLES_AFTER = max(0, int(os.getenv("WINDOW_CANDLES_AFTER", "10")))

# Max option-window entries kept in RAM.
# One entry is for one instrument/date/expiry/strike/CE-or-PE/interval/target-time.
MAX_OPTION_WINDOW_CACHE_SIZE = max(1, int(os.getenv("OPTION_WINDOW_CACHE_SIZE", "500")))

# Max spot candle-day entries kept in RAM.
MAX_SPOT_CANDLE_CACHE_SIZE = max(1, int(os.getenv("SPOT_CANDLE_CACHE_SIZE", "50")))

OPTION_WINDOW_CACHE: "OrderedDict[Tuple[Any, ...], pd.DataFrame]" = OrderedDict()
SPOT_CANDLE_CACHE: "OrderedDict[Tuple[Any, ...], pd.DataFrame]" = OrderedDict()
OPTION_WINDOW_CACHE_LOCK = threading.RLock()
SPOT_CANDLE_CACHE_LOCK = threading.RLock()

# Complete option-chain payload cache. This avoids repeating spot resolution,
# chain lookup, IV solving and JSON preparation for identical historical requests.
MAX_CHAIN_SNAPSHOT_CACHE_SIZE = max(
    1,
    int(os.getenv("CHAIN_SNAPSHOT_CACHE_SIZE", "256")),
)
CHAIN_SNAPSHOT_CACHE_TTL_SECONDS = max(
    0.0,
    float(os.getenv("CHAIN_SNAPSHOT_CACHE_TTL_SECONDS", "900")),
)
CHAIN_SNAPSHOT_CACHE: "OrderedDict[Tuple[Any, ...], Tuple[float, Dict[str, Any]]]" = OrderedDict()
CHAIN_SNAPSHOT_CACHE_LOCK = threading.RLock()


def _get_chain_snapshot_cache(key: Tuple[Any, ...]) -> Optional[Dict[str, Any]]:
    now = time.monotonic()
    with CHAIN_SNAPSHOT_CACHE_LOCK:
        item = CHAIN_SNAPSHOT_CACHE.get(key)
        if item is None:
            return None

        created_at, payload = item
        if (
            CHAIN_SNAPSHOT_CACHE_TTL_SECONDS > 0
            and now - created_at >= CHAIN_SNAPSHOT_CACHE_TTL_SECONDS
        ):
            CHAIN_SNAPSHOT_CACHE.pop(key, None)
            return None

        CHAIN_SNAPSHOT_CACHE.move_to_end(key)
        return copy.deepcopy(payload)


def _put_chain_snapshot_cache(
    key: Tuple[Any, ...],
    payload: Dict[str, Any],
) -> None:
    with CHAIN_SNAPSHOT_CACHE_LOCK:
        CHAIN_SNAPSHOT_CACHE[key] = (time.monotonic(), copy.deepcopy(payload))
        CHAIN_SNAPSHOT_CACHE.move_to_end(key)
        while len(CHAIN_SNAPSHOT_CACHE) > MAX_CHAIN_SNAPSHOT_CACHE_SIZE:
            CHAIN_SNAPSHOT_CACHE.popitem(last=False)


def _clear_chain_snapshot_cache() -> None:
    with CHAIN_SNAPSHOT_CACHE_LOCK:
        CHAIN_SNAPSHOT_CACHE.clear()


# Completed option-metric chart payloads are cached separately because a chart
# request can involve full-session resampling plus IV/Greek calculation.
MAX_METRIC_CHART_CACHE_SIZE = max(
    1,
    int(os.getenv("METRIC_CHART_CACHE_SIZE", "256")),
)
METRIC_CHART_CACHE_TTL_SECONDS = max(
    0.0,
    float(os.getenv("METRIC_CHART_CACHE_TTL_SECONDS", "900")),
)
METRIC_CHART_CACHE: "OrderedDict[Tuple[Any, ...], Tuple[float, Dict[str, Any]]]" = OrderedDict()
METRIC_CHART_CACHE_LOCK = threading.RLock()


def _get_metric_chart_cache(key: Tuple[Any, ...]) -> Optional[Dict[str, Any]]:
    now = time.monotonic()
    with METRIC_CHART_CACHE_LOCK:
        item = METRIC_CHART_CACHE.get(key)
        if item is None:
            return None
        created_at, payload = item
        if (
            METRIC_CHART_CACHE_TTL_SECONDS > 0
            and now - created_at >= METRIC_CHART_CACHE_TTL_SECONDS
        ):
            METRIC_CHART_CACHE.pop(key, None)
            return None
        METRIC_CHART_CACHE.move_to_end(key)
        return copy.deepcopy(payload)


def _put_metric_chart_cache(key: Tuple[Any, ...], payload: Dict[str, Any]) -> None:
    with METRIC_CHART_CACHE_LOCK:
        METRIC_CHART_CACHE[key] = (time.monotonic(), copy.deepcopy(payload))
        METRIC_CHART_CACHE.move_to_end(key)
        while len(METRIC_CHART_CACHE) > MAX_METRIC_CHART_CACHE_SIZE:
            METRIC_CHART_CACHE.popitem(last=False)


def _clear_metric_chart_cache() -> None:
    with METRIC_CHART_CACHE_LOCK:
        METRIC_CHART_CACHE.clear()



# Speed mode: IV/Greeks calculation is expensive.
# Default OFF for faster simulation. Set ENABLE_IV_CALC=true to enable it.
ENABLE_IV_CALC = _env_bool("ENABLE_IV_CALC", True)

# Redis configuration

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
REDIS_REQUIRED = _env_bool("REDIS_REQUIRED", False)

redis_client = None
if redis is not None:
    try:
        redis_client = redis.Redis.from_url(
            REDIS_URL,
            decode_responses=True,
            socket_connect_timeout=float(
                os.getenv("REDIS_CONNECT_TIMEOUT_SECONDS", "2")
            ),
            socket_timeout=float(
                os.getenv("REDIS_SOCKET_TIMEOUT_SECONDS", "3")
            ),
            health_check_interval=30,
            retry_on_timeout=True,
        )
        redis_client.ping()
        logger.info("Redis connection established.")
    except Exception as exc:
        redis_client = None
        if REDIS_REQUIRED:
            raise RuntimeError(
                f"Redis is required but unavailable: {exc}"
            ) from exc
        logger.warning(
            "Redis unavailable; async IV-surface routes will return 503: %s",
            exc,
        )
elif REDIS_REQUIRED:
    raise RuntimeError("redis package is required but not installed.")



# =========================================================
# BASIC HELPERS
# =========================================================

def _json_error(message: str, status: int = 400, **extra):
    payload = {"ok": False, "error": message}
    payload.update(extra)
    return jsonify(payload), status


def _normalize_dataset(value: Optional[str]) -> str:
    value = str(value or "NIFTY").strip().upper()
    if value in {"BSE", "SENSEX"}:
        return "SENSEX"
    return "NIFTY"


def _normalize_date(value: Optional[str]) -> str:
    if not value:
        raise ValueError("date/query_date is required.")

    text = str(value).strip()

    formats = [
        "%Y-%m-%d",
        "%Y%m%d",
        "%d-%b-%Y",
        "%d %b %Y",
        "%d-%B-%Y",
        "%d %B %Y",
        "%d/%m/%Y",
        "%d-%m-%Y",
    ]

    for fmt in formats:
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass

    raise ValueError(f"Unsupported date format: {value}")


def _normalize_time(value: Optional[str], default: str = DEFAULT_QUERY_TIME) -> str:
    if not value:
        return default

    text = str(value).strip().lower()

    formats = [
        "%H:%M",
        "%H:%M:%S",
        "%H",
        "%I:%M%p",
        "%I:%M %p",
        "%I%p",
        "%I %p",
    ]

    for fmt in formats:
        try:
            return datetime.strptime(text, fmt).strftime("%H:%M")
        except ValueError:
            pass

    raise ValueError(f"Unsupported time format: {value}")


def _safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if value is None or pd.isna(value):
            return default
        value = float(value)
        if not np.isfinite(value):
            return default
        return value
    except Exception:
        return default


def _safe_int(value: Any, default: Optional[int] = None) -> Optional[int]:
    try:
        if value is None or pd.isna(value):
            return default
        return int(float(value))
    except Exception:
        return default


def _round_or_none(value: Any, digits: int = 2):
    value = _safe_float(value)
    if value is None:
        return None
    return round(value, digits)


def _fmt_expiry_label(expiry_yyMMdd: str) -> str:
    try:
        return datetime.strptime(str(expiry_yyMMdd), "%y%m%d").strftime("%d %b %Y")
    except Exception:
        return str(expiry_yyMMdd)


def _to_ist_timestamp(value: Any) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        return ts.tz_localize(IST)
    return ts.tz_convert(IST)


def _cache_lock(cache: OrderedDict) -> threading.RLock:
    return OPTION_WINDOW_CACHE_LOCK if cache is OPTION_WINDOW_CACHE else SPOT_CANDLE_CACHE_LOCK


def _touch_cache(cache: OrderedDict, key: Tuple[Any, ...], value: pd.DataFrame, max_size: int) -> None:
    with _cache_lock(cache):
        cache[key] = value
        cache.move_to_end(key)
        while len(cache) > max_size:
            cache.popitem(last=False)


def _get_cache(cache: OrderedDict, key: Tuple[Any, ...]) -> Optional[pd.DataFrame]:
    with _cache_lock(cache):
        value = cache.get(key)
        if value is not None:
            cache.move_to_end(key)
        return value


# =========================================================
# DATA RESOLUTION
# =========================================================

def _resolve_week_folder_for_date(
    query_date: str,
    dataset: str,
    week_number: Optional[int] = None,
) -> Tuple[int, str, str]:
    query_date = _normalize_date(query_date)
    target_date = datetime.strptime(query_date, "%Y-%m-%d").date()

    if target_date > MAX_ALLOWED_QUERY_DATE:
        raise ValueError(
            f"Selected date {query_date} is after allowed cutoff "
            f"{MAX_ALLOWED_QUERY_DATE.strftime('%Y-%m-%d')}."
        )

    for num, folder in get_week_folders(instrument=dataset):
        if week_number is not None and int(num) != int(week_number):
            continue

        available_dates = get_dates_for_week_folder(
            num,
            folder,
            instrument=dataset,
        )

        for date_str in available_dates:
            current_date = datetime.strptime(date_str, "%Y%m%d").date()

            if current_date > MAX_ALLOWED_QUERY_DATE:
                continue

            if current_date == target_date:
                return int(num), folder, date_str

    if week_number is not None:
        raise ValueError(f"{dataset} date {query_date} not found in week {week_number}.")

    raise ValueError(f"{dataset} date {query_date} not found in historical data folders.")


def _resolve_default_week_date(dataset: str) -> Tuple[int, str, str, str]:
    latest_item = None

    for num, folder in get_week_folders(instrument=dataset):
        dates = get_dates_for_week_folder(
            num,
            folder,
            instrument=dataset,
        )

        for date_str in dates:
            try:
                dt = datetime.strptime(date_str, "%Y%m%d").date()
            except Exception:
                continue

            # Do not choose any default date after the allowed cutoff.
            if dt > MAX_ALLOWED_QUERY_DATE:
                continue

            item = (dt, int(num), folder, date_str)

            if latest_item is None or item[0] > latest_item[0]:
                latest_item = item

    if latest_item is None:
        raise ValueError(
            f"No available historical dates found for {dataset} on or before "
            f"{MAX_ALLOWED_QUERY_DATE.strftime('%Y-%m-%d')}."
        )

    dt, week_number, folder, date_str = latest_item

    return (
        week_number,
        folder,
        date_str,
        dt.strftime("%Y-%m-%d"),
    )
def get_available_expiries_for_date(query_date, dataset="NIFTY", max_months=4):
    cfg = get_dataset_config(dataset)
    expiries = pd.to_datetime(cfg["combined_expiry"])

    q = pd.Timestamp(query_date).normalize()
    max_date = q + pd.DateOffset(months=max_months)

    valid = expiries[(expiries >= q) & (expiries <= max_date)]

    return [
        {
            "value": pd.Timestamp(x).strftime("%y%m%d"),
            "label": pd.Timestamp(x).strftime("%d %b %Y"),
        }
        for x in valid
    ]

# =========================================================
# WINDOW / RAM CACHE HELPERS
# =========================================================

def _window_time_bounds(
    target_ts: pd.Timestamp,
    candle_interval_minutes: int,
    before: int = WINDOW_CANDLES_BEFORE,
    after: int = WINDOW_CANDLES_AFTER,
) -> Tuple[pd.Timestamp, pd.Timestamp]:
    """
    Returns a time range wide enough to build:
        before candles + target candle + after candles.

    We add a small buffer of one interval on both sides so resampling has enough ticks.
    """
    target_ts = _to_ist_timestamp(target_ts)
    interval = int(candle_interval_minutes)

    start_ts = target_ts - timedelta(minutes=(before + 1) * interval)
    end_ts = target_ts + timedelta(minutes=(after + 1) * interval)

    return start_ts, end_ts


def _prepare_tick_df(raw_df: pd.DataFrame) -> pd.DataFrame:
    if raw_df is None or raw_df.empty:
        return pd.DataFrame()

    if "datetime" not in raw_df.columns or "price" not in raw_df.columns:
        return pd.DataFrame()

    df = raw_df[["datetime", "price"]].copy()
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df = df.dropna(subset=["datetime", "price"])

    if df.empty:
        return pd.DataFrame()

    df = df.sort_values("datetime")

    if getattr(df["datetime"].dt, "tz", None) is None:
        df["datetime"] = df["datetime"].dt.tz_localize(IST)
    else:
        df["datetime"] = df["datetime"].dt.tz_convert(IST)

    return df


def _build_option_candle_window(
    raw_df: pd.DataFrame,
    target_ts: pd.Timestamp,
    candle_interval_minutes: int,
    before: int = WINDOW_CANDLES_BEFORE,
    after: int = WINDOW_CANDLES_AFTER,
) -> pd.DataFrame:
    """
    Converts raw option ticks into a small OHLC candle window only.

    Output keeps at most:
        before + current + after candles
    around target_ts.
    """
    df = _prepare_tick_df(raw_df)
    if df.empty:
        return pd.DataFrame()

    target_ts = _to_ist_timestamp(target_ts)
    start_ts, end_ts = _window_time_bounds(
        target_ts=target_ts,
        candle_interval_minutes=candle_interval_minutes,
        before=before,
        after=after,
    )

    df = df[(df["datetime"] >= start_ts) & (df["datetime"] <= end_ts)]
    if df.empty:
        return pd.DataFrame()

    df = df.set_index("datetime")

    ohlc = df["price"].resample(f"{int(candle_interval_minutes)}min").ohlc()
    ohlc.columns = ["open", "high", "low", "close"]
    ohlc = ohlc.dropna(subset=["close"])

    if ohlc.empty:
        return pd.DataFrame()

    # Select nearest candle index at or before target timestamp.
    earlier = ohlc[ohlc.index <= target_ts]
    if earlier.empty:
        center_pos = 0
    else:
        center_label = earlier.index[-1]
        center_pos = int(ohlc.index.get_loc(center_label))

    start_pos = max(center_pos - before, 0)
    end_pos = min(center_pos + after + 1, len(ohlc))

    window = ohlc.iloc[start_pos:end_pos].copy()
    window["timestamp"] = window.index
    return window


def _option_ltp_from_window(
    window_df: pd.DataFrame,
    target_ts: pd.Timestamp,
) -> Optional[float]:
    if window_df is None or window_df.empty:
        return None

    target_ts = _to_ist_timestamp(target_ts)

    df = window_df.copy()
    if not isinstance(df.index, pd.DatetimeIndex):
        if "timestamp" not in df.columns:
            return None
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        df = df.dropna(subset=["timestamp"])
        df = df.set_index("timestamp")

    if getattr(df.index, "tz", None) is None:
        df.index = df.index.tz_localize(IST)
    else:
        df.index = df.index.tz_convert(IST)

    earlier = df[df.index <= target_ts]
    if earlier.empty:
        return None

    return _safe_float(earlier.iloc[-1].get("close"))


def _option_ltp_at_time_cached(
    raw_df: pd.DataFrame,
    target_ts: pd.Timestamp,
    candle_interval_minutes: int,
    cache_key: Tuple[Any, ...],
) -> Optional[float]:
    cached_window = _get_cache(OPTION_WINDOW_CACHE, cache_key)

    if cached_window is None:
        cached_window = _build_option_candle_window(
            raw_df=raw_df,
            target_ts=target_ts,
            candle_interval_minutes=candle_interval_minutes,
            before=WINDOW_CANDLES_BEFORE,
            after=WINDOW_CANDLES_AFTER,
        )
        _touch_cache(
            OPTION_WINDOW_CACHE,
            cache_key,
            cached_window,
            MAX_OPTION_WINDOW_CACHE_SIZE,
        )

    return _option_ltp_from_window(cached_window, target_ts)


# =========================================================
# SPOT CANDLES
# =========================================================

def _get_spot_candles_for_day(
    folder: str,
    date_str: str,
    dataset: str,
    candle_interval_minutes: int,
) -> pd.DataFrame:
    """
    Load and cache spot/index candles for a trading day.

    The resolved week folder must be passed directly to load_tick_data().
    load_tick_data() internally resolves the IDX_TICK directory and the
    instrument Parquet file for both local and Azure Blob storage.
    """

    folder = str(folder).strip()
    date_str = str(date_str).strip()
    dataset = str(dataset).strip().upper()
    candle_interval_minutes = int(candle_interval_minutes)

    if not folder:
        raise ValueError("folder cannot be empty")

    if not date_str:
        raise ValueError("date_str cannot be empty")

    if not dataset:
        raise ValueError("dataset cannot be empty")

    if candle_interval_minutes <= 0:
        raise ValueError(
            "candle_interval_minutes must be greater than zero"
        )

    cache_key = (
        "spot",
        dataset,
        date_str,
        candle_interval_minutes,
        folder,
    )

    cached = _get_cache(SPOT_CANDLE_CACHE, cache_key)

    if cached is not None:
        return cached.copy()

    try:
        # Date-aware loader supports the production layout:
        #
        #   <week_folder>/IDX_TICK/<YYYYMMDD>/<SYMBOL>.parquet
        #
        # It also preserves compatibility with older flat layouts through the
        # path-resolution logic inside data_engine_for_simulation.py.
        tick_df = load_index_data_by_symbol(
            folder=folder,
            date_str=date_str,
            symbol_name=dataset,
        )

    except Exception:
        logger.exception(
            "Failed to load spot tick data: "
            "dataset=%s date=%s folder=%s",
            dataset,
            date_str,
            folder,
        )
        return pd.DataFrame()

    if tick_df is None or tick_df.empty:
        logger.warning(
            "No spot tick data found: "
            "dataset=%s date=%s folder=%s",
            dataset,
            date_str,
            folder,
        )
        return pd.DataFrame()

    try:
        candles = create_candles(
            tick_df,
            candle_interval_minutes,
        )

    except Exception:
        logger.exception(
            "Failed to create spot candles: "
            "dataset=%s date=%s interval=%s",
            dataset,
            date_str,
            candle_interval_minutes,
        )
        return pd.DataFrame()

    if candles is None or candles.empty:
        logger.warning(
            "Spot candle creation returned no rows: "
            "dataset=%s date=%s interval=%s",
            dataset,
            date_str,
            candle_interval_minutes,
        )
        return pd.DataFrame()

    candles = candles.copy()

    _touch_cache(
        SPOT_CANDLE_CACHE,
        cache_key,
        candles,
        MAX_SPOT_CANDLE_CACHE_SIZE,
    )

    return candles.copy()

def _nearest_candle_row(
    candles: pd.DataFrame,
    date_str: str,
    query_time: str,
):
    target_ts = IST.localize(
        datetime.strptime(
            f"{date_str} {query_time}:00",
            "%Y%m%d %H:%M:%S",
        )
    )

    if candles.empty:
        raise ValueError("No spot candles available.")

    candles = candles.copy()
    if getattr(candles.index, "tz", None) is None:
        candles.index = candles.index.tz_localize(IST)
    else:
        candles.index = candles.index.tz_convert(IST)

    if target_ts in candles.index:
        return target_ts, candles.loc[target_ts]

    earlier = candles[candles.index <= target_ts]

    if earlier.empty:
        raise ValueError(f"No spot candle found at or before {query_time}.")

    ts = earlier.index[-1]
    return ts, candles.loc[ts]


def _resample_option_close_for_session(
    raw_df: pd.DataFrame,
    interval_minutes: int,
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
) -> pd.Series:
    """Return a full-session option close series aligned to session candles.

    Unlike the small UI window helper, this function does not truncate the
    morning session. That truncation was the reason early times such as 09:48
    produced an empty PE_DELTA chart when the window was centered at 15:30.
    """
    if raw_df is None or raw_df.empty:
        return pd.Series(dtype="float64")
    if "datetime" not in raw_df.columns or "price" not in raw_df.columns:
        return pd.Series(dtype="float64")

    frame = raw_df.loc[:, ["datetime", "price"]].copy()
    frame["datetime"] = pd.to_datetime(frame["datetime"], errors="coerce")
    frame["price"] = pd.to_numeric(frame["price"], errors="coerce")
    frame = frame.dropna(subset=["datetime", "price"])
    if frame.empty:
        return pd.Series(dtype="float64")

    if getattr(frame["datetime"].dt, "tz", None) is None:
        frame["datetime"] = frame["datetime"].dt.tz_localize(
            IST, ambiguous="NaT", nonexistent="NaT"
        )
    else:
        frame["datetime"] = frame["datetime"].dt.tz_convert(IST)
    frame = frame.dropna(subset=["datetime"])
    frame = frame[
        (frame["datetime"] >= _to_ist_timestamp(start_ts))
        & (frame["datetime"] <= _to_ist_timestamp(end_ts))
    ]
    if frame.empty:
        return pd.Series(dtype="float64")

    session_offset = pd.Timedelta(hours=9, minutes=15)
    return (
        frame.set_index("datetime")["price"]
        .sort_index()
        .resample(
            f"{int(interval_minutes)}min",
            origin="start_day",
            offset=session_offset,
            label="left",
            closed="left",
        )
        .last()
        .dropna()
    )


# =========================================================
# OPTION CHAIN BUILDER
# =========================================================

def build_option_chain_snapshot(
    query_date: Optional[str] = None,
    query_time: str = DEFAULT_QUERY_TIME,
    dataset: str = "NIFTY",
    week_number: Optional[int] = None,
    expiry_rule: str = "current expiry",
    strike_count_each_side: int = 14,
    candle_interval_minutes: int = CANDLE_INTERVAL_MINUTES,
    spot_price_field: str = "close",
    selected_expiry: Optional[str] = None,
    compute_greeks: bool = True,
) -> Dict[str, Any]:
    """Build one historical option-chain snapshot with profiling and caching."""

    total_started = time.perf_counter()
    timings: Dict[str, float] = {}

    dataset = _normalize_dataset(dataset)
    query_time = _normalize_time(query_time)
    candle_interval_minutes = int(candle_interval_minutes)
    strike_count_each_side = int(strike_count_each_side)

    if candle_interval_minutes <= 0:
        raise ValueError("interval must be greater than zero.")
    if strike_count_each_side < 0 or strike_count_each_side > 100:
        raise ValueError("strike_count_each_side must be between 0 and 100.")

    cfg = get_dataset_config(dataset)
    strike_step = int(cfg["strike_step"])

    stage = time.perf_counter()
    if query_date:
        query_date = _normalize_date(query_date)
        resolved_week, folder, date_str = _resolve_week_folder_for_date(
            query_date=query_date,
            dataset=dataset,
            week_number=week_number,
        )
    else:
        resolved_week, folder, date_str, query_date = _resolve_default_week_date(
            dataset=dataset,
        )
    timings["resolve_data_ms"] = round((time.perf_counter() - stage) * 1000, 3)

    expiry_str = selected_expiry or get_upcoming_expiry_np(
        datetime.strptime(date_str, "%Y%m%d").date(),
        instrument=dataset,
        expiry_rule=expiry_rule,
    )
    if expiry_str is None:
        raise ValueError("No upcoming expiry found.")
    expiry_str = str(expiry_str).strip()

    cache_key = (
        dataset,
        date_str,
        expiry_str,
        query_time,
        candle_interval_minutes,
        strike_count_each_side,
        spot_price_field,
        bool(compute_greeks),
    )
    cached_payload = _get_chain_snapshot_cache(cache_key)
    if cached_payload is not None:
        cached_payload["cache_hit"] = True
        cached_payload["performance"] = {
            "cache_lookup_ms": round((time.perf_counter() - total_started) * 1000, 3),
            "total_ms": round((time.perf_counter() - total_started) * 1000, 3),
        }
        return cached_payload

    stage = time.perf_counter()
    spot_candles = _get_spot_candles_for_day(
        folder=folder,
        date_str=date_str,
        dataset=dataset,
        candle_interval_minutes=candle_interval_minutes,
    )
    timings["spot_candles_ms"] = round((time.perf_counter() - stage) * 1000, 3)

    if spot_candles.empty:
        raise ValueError("No spot candles found for selected date.")

    target_ts, spot_row = _nearest_candle_row(
        candles=spot_candles,
        date_str=date_str,
        query_time=query_time,
    )
    target_ts = _to_ist_timestamp(target_ts)

    if spot_price_field not in {"open", "high", "low", "close"}:
        spot_price_field = "close"

    spot = _safe_float(spot_row.get(spot_price_field))
    if spot is None:
        raise ValueError(f"Spot candle field '{spot_price_field}' is missing or invalid.")

    day_open = _safe_float(spot_candles.iloc[0].get("open"), spot)
    atm = int(
        get_nearest_strike(
            spot,
            instrument=dataset,
            expiry_rule=expiry_rule,
        )
    )

    strikes = [
        atm + offset * strike_step
        for offset in range(-strike_count_each_side, strike_count_each_side + 1)
    ]

    # India VIX is best-effort and must not fail the main chain request.
    stage = time.perf_counter()
    india_vix_value = None
    try:
        vix_df = load_index_data_by_symbol(
            folder=folder,
            date_str=date_str,
            symbol_name="INDIAVIX",
        )
        if vix_df is not None and not vix_df.empty:
            vix_ts = pd.to_datetime(vix_df["datetime"], errors="coerce")
            vix_values = pd.to_numeric(vix_df["value"], errors="coerce")
            vix_work = pd.DataFrame({"datetime": vix_ts, "value": vix_values}).dropna()
            if not vix_work.empty:
                if getattr(vix_work["datetime"].dt, "tz", None) is None:
                    vix_work["datetime"] = vix_work["datetime"].dt.tz_localize(
                        IST,
                        ambiguous="NaT",
                        nonexistent="NaT",
                    )
                else:
                    vix_work["datetime"] = vix_work["datetime"].dt.tz_convert(IST)
                values_before = vix_work.loc[vix_work["datetime"] <= target_ts, "value"]
                if not values_before.empty:
                    india_vix_value = _safe_float(values_before.iloc[-1])
    except Exception as exc:
        logger.warning("INDIA VIX load error: %s", exc)
    timings["vix_ms"] = round((time.perf_counter() - stage) * 1000, 3)

    # Fast path: data_engine performs one indexed lookup and binary-searches the
    # latest timestamp at or before target_ts. This avoids scanning the full chain
    # once for every strike.
    stage = time.perf_counter()
    snapshot = get_option_chain_snapshot(
        folder=folder,
        date_str=date_str,
        expiry_str=expiry_str,
        target_timestamp=target_ts,
        instrument=dataset,
    )
    timings["chain_snapshot_ms"] = round((time.perf_counter() - stage) * 1000, 3)

    rows: List[Dict[str, Any]] = []
    chain_source = "consolidated"

    if snapshot is not None and not snapshot.empty:
        snapshot = snapshot.loc[:, ["timestamp", "strike", "ce", "pe"]].copy(deep=False)
        strike_values = pd.to_numeric(snapshot["strike"], errors="coerce")
        snapshot = snapshot.loc[strike_values.notna()].copy()
        snapshot["strike"] = strike_values.loc[strike_values.notna()].astype(int)
        by_strike = snapshot.drop_duplicates("strike", keep="last").set_index("strike")

        for strike in strikes:
            ce_ltp = pe_ltp = None
            if int(strike) in by_strike.index:
                strike_row = by_strike.loc[int(strike)]
                if isinstance(strike_row, pd.DataFrame):
                    strike_row = strike_row.iloc[-1]
                ce_ltp = _safe_float(strike_row.get("ce"))
                pe_ltp = _safe_float(strike_row.get("pe"))

            rows.append({
                "timestamp": target_ts.tz_convert(IST).tz_localize(None),
                "trade_date": datetime.strptime(date_str, "%Y%m%d").date(),
                "instrument": dataset,
                "expiry": expiry_str,
                "nearest_strike": int(strike),
                "strike": int(strike),
                "close": float(spot),
                "ce": ce_ltp,
                "pe": pe_ltp,
            })
    else:
        # Backward-compatible fallback for dates without a consolidated file.
        chain_source = "contracts"
        for strike in strikes:
            option_data = load_required_option_data_for_date(
                folder=folder,
                date_str=date_str,
                expiry_str=expiry_str,
                strike=int(strike),
                instrument=dataset,
            )

            ce_ltp = _option_ltp_at_time_cached(
                raw_df=option_data.get("CE"),
                target_ts=target_ts,
                candle_interval_minutes=candle_interval_minutes,
                cache_key=(
                    "option_window", dataset, date_str, expiry_str, int(strike),
                    "CE", candle_interval_minutes,
                    target_ts.strftime("%Y-%m-%d %H:%M:%S%z"),
                ),
            )
            pe_ltp = _option_ltp_at_time_cached(
                raw_df=option_data.get("PE"),
                target_ts=target_ts,
                candle_interval_minutes=candle_interval_minutes,
                cache_key=(
                    "option_window", dataset, date_str, expiry_str, int(strike),
                    "PE", candle_interval_minutes,
                    target_ts.strftime("%Y-%m-%d %H:%M:%S%z"),
                ),
            )

            rows.append({
                "timestamp": target_ts.tz_convert(IST).tz_localize(None),
                "trade_date": datetime.strptime(date_str, "%Y%m%d").date(),
                "instrument": dataset,
                "expiry": expiry_str,
                "nearest_strike": int(strike),
                "strike": int(strike),
                "close": float(spot),
                "ce": ce_ltp,
                "pe": pe_ltp,
            })

    chain_df = pd.DataFrame.from_records(rows)

    stage = time.perf_counter()
    if ENABLE_IV_CALC:
        try:
            chain_df = append_black_scholes_iv(
                chain_df,
                compute_greeks=compute_greeks,
                inplace=True,
            )
        except TypeError:
            # Backward compatibility with an older function signature.
            chain_df = append_black_scholes_iv(
                chain_df,
                compute_greeks=compute_greeks,
            )
        except Exception as exc:
            logger.exception("IV calculation failed")
            chain_df["_iv_error"] = str(exc)

    for column in (
        "iv", "ce_iv", "pe_iv",
        "ce_delta", "pe_delta",
        "ce_gamma", "pe_gamma",
        "ce_vega", "pe_vega",
        "ce_theta", "pe_theta",
    ):
        if column not in chain_df.columns:
            chain_df[column] = np.nan
    timings["iv_greeks_ms"] = round((time.perf_counter() - stage) * 1000, 3)

    stage = time.perf_counter()
    chain_rows = [
        {
            "strike": _safe_int(row.strike),
            "atm": _safe_int(row.strike) == atm,
            "ce_ltp": _round_or_none(row.ce, 2),
            "pe_ltp": _round_or_none(row.pe, 2),
            "iv": _round_or_none(row.iv, 4),
            "ce_iv": _round_or_none(row.ce_iv, 4),
            "pe_iv": _round_or_none(row.pe_iv, 4),
            "ce_delta": _round_or_none(row.ce_delta, 4),
            "pe_delta": _round_or_none(row.pe_delta, 4),
            "ce_gamma": _round_or_none(row.ce_gamma, 6),
            "pe_gamma": _round_or_none(row.pe_gamma, 6),
            "ce_vega": _round_or_none(row.ce_vega, 4),
            "pe_vega": _round_or_none(row.pe_vega, 4),
            "ce_theta": _round_or_none(row.ce_theta, 4),
            "pe_theta": _round_or_none(row.pe_theta, 4),
        }
        for row in chain_df.itertuples(index=False)
    ]
    timings["response_build_ms"] = round((time.perf_counter() - stage) * 1000, 3)

    valid_iv = pd.to_numeric(chain_df["iv"], errors="coerce").dropna()
    atm_iv = valid_iv.mean() if not valid_iv.empty else None

    available_expiries = get_available_expiries_for_date(
        datetime.strptime(date_str, "%Y%m%d").date(),
        dataset=dataset,
        max_months=2,
    )

    timings["total_ms"] = round((time.perf_counter() - total_started) * 1000, 3)

    payload = {
        "ok": True,
        "dataset": dataset,
        "underlying": dataset,
        "query_date": query_date,
        "date_str": date_str,
        "query_time": target_ts.strftime("%H:%M"),
        "resolved_week_number": resolved_week,
        "expiry": expiry_str,
        "expiry_label": _fmt_expiry_label(expiry_str),
        "available_expiries": available_expiries,
        "spot": round(float(spot), 2),
        "india_vix": (
            round(float(india_vix_value), 2)
            if india_vix_value is not None
            else None
        ),
        "day_open": round(float(day_open), 2),
        "atm": int(atm),
        "strike_step": strike_step,
        "lot_size": int(LOT_SIZE_BY_INSTRUMENT.get(dataset, 65)),
        "interval": candle_interval_minutes,
        "max_allowed_query_date": MAX_ALLOWED_QUERY_DATE.strftime("%Y-%m-%d"),
        "window_candles_before": WINDOW_CANDLES_BEFORE,
        "window_candles_after": WINDOW_CANDLES_AFTER,
        "option_window_cache_size": len(OPTION_WINDOW_CACHE),
        "iv_enabled": ENABLE_IV_CALC,
        "chain_source": chain_source,
        "cache_hit": False,
        "atm_iv": (
            round(float(atm_iv), 4)
            if atm_iv is not None and np.isfinite(atm_iv)
            else None
        ),
        "performance": timings,
        "rows": chain_rows,
    }

    _put_chain_snapshot_cache(cache_key, payload)
    logger.info(
        "Option-chain snapshot dataset=%s date=%s time=%s expiry=%s "
        "source=%s rows=%d timings=%s",
        dataset,
        date_str,
        query_time,
        expiry_str,
        chain_source,
        len(chain_rows),
        timings,
    )
    return payload


# =========================================================
# BLACK-SCHOLES PAYOFF HELPERS
# =========================================================

def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _norm_pdf(x: float) -> float:
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)


def black_scholes_price_and_greeks(
    spot: float,
    strike: float,
    days_to_expiry: int,
    iv_percent: float,
    option_type: str,
    risk_free_rate: float = 0.0,
) -> Dict[str, float]:
    T = max(float(days_to_expiry), 1.0) / 365.0
    sigma = max(float(iv_percent), 0.01) / 100.0
    strike = max(float(strike), 1.0)
    spot = max(float(spot), 1.0)

    d1 = (
        math.log(spot / strike)
        + (risk_free_rate + 0.5 * sigma * sigma) * T
    ) / (sigma * math.sqrt(T))

    d2 = d1 - sigma * math.sqrt(T)

    if option_type.upper() == "CE":
        price = spot * _norm_cdf(d1) - strike * math.exp(-risk_free_rate * T) * _norm_cdf(d2)
        delta = _norm_cdf(d1)
    else:
        price = strike * math.exp(-risk_free_rate * T) * _norm_cdf(-d2) - spot * _norm_cdf(-d1)
        delta = _norm_cdf(d1) - 1.0

    gamma = _norm_pdf(d1) / (spot * sigma * math.sqrt(T))
    vega = spot * _norm_pdf(d1) * math.sqrt(T) / 100.0
    theta = -(spot * _norm_pdf(d1) * sigma) / (2.0 * math.sqrt(T)) / 365.0

    return {
        "price": round(float(price), 2),
        "delta": round(float(delta), 6),
        "gamma": round(float(gamma), 8),
        "vega": round(float(vega), 6),
        "theta": round(float(theta), 6),
    }


def _intrinsic_value(spot: float, strike: float, option_type: str) -> float:
    if option_type.upper() == "CE":
        return max(float(spot) - float(strike), 0.0)
    return max(float(strike) - float(spot), 0.0)


def _leg_payoff_at_expiry(spot: float, leg: Dict[str, Any], lot_size: int) -> float:
    strike = float(leg["strike"])
    premium = float(leg["premium"])
    qty = int(leg.get("qty", 1))
    lots = int(leg.get("lots", 1))
    side = str(leg["side"]).upper()
    option_type = str(leg["type"]).upper()

    intrinsic = _intrinsic_value(spot, strike, option_type)

    if side == "BUY":
        per_unit = intrinsic - premium
    else:
        per_unit = premium - intrinsic

    return per_unit * qty * lots


def _current_leg_pnl(
    spot: float,
    leg: Dict[str, Any],
    days_to_expiry: int,
    iv_percent: float,
    lot_size: int,
) -> float:
    """
    Used for the pink Current MTM curve.
    This should use Black-Scholes theoretical price.
    Do not use this for actual P&L card.
    """
    strike = float(leg["strike"])
    premium = float(leg["premium"])
    qty = int(leg.get("qty", 1))
    lots = int(leg.get("lots", 1))
    side = str(leg["side"]).upper()
    option_type = str(leg["type"]).upper()

    bs = black_scholes_price_and_greeks(
        spot=spot,
        strike=strike,
        days_to_expiry=days_to_expiry,
        iv_percent=iv_percent,
        option_type=option_type,
    )

    current_price = float(bs["price"])

    if side == "BUY":
        return float((current_price - premium) * qty * lots)

    return float((premium - current_price) * qty * lots)


def _actual_leg_pnl_now(leg: Dict[str, Any]) -> float:
    """
    Used only for actual P&L card.

    premium = entry price
    current_price = latest LTP sent from frontend
    """
    premium = float(leg["premium"])
    current_price = float(leg.get("current_price", premium))

    qty = int(leg.get("qty", 1))
    lots = int(leg.get("lots", 1))
    side = str(leg["side"]).upper()

    if side == "BUY":
        return float((current_price - premium) * qty * lots)

    return float((premium - current_price) * qty * lots)


def _signed_leg_units(leg: Dict[str, Any]) -> int:
    """
    BUY legs have positive payoff slope after the strike.
    SELL legs have negative payoff slope after the strike.
    """
    side = str(leg["side"]).upper()
    qty = int(leg.get("qty", 1))
    lots = int(leg.get("lots", 1))
    return qty * lots if side == "BUY" else -qty * lots


def _payoff_unlimited_flags(legs: List[Dict[str, Any]]) -> Dict[str, bool]:
    """
    For vanilla options on an index, spot cannot go below zero, so downside is bounded.
    Only uncovered net call exposure can create unlimited payoff/loss as spot -> infinity.

    net_call_units > 0  => unlimited profit potential
    net_call_units < 0  => unlimited loss risk
    net_call_units == 0 => upside is hedged/bounded
    """
    net_call_units = 0

    for leg in legs:
        if str(leg.get("type", "")).upper() == "CE":
            net_call_units += _signed_leg_units(leg)

    return {
        "max_profit_unlimited": net_call_units > 0,
        "max_loss_unlimited": net_call_units < 0,
    }


def _summary_payoff_points(spot: float, legs: List[Dict[str, Any]], strike_step: int) -> List[float]:
    """
    Payoff at expiry is piecewise linear. Finite maxima/minima happen at kink points
    (strikes) or at the zero boundary. Add the current spot for context.
    """
    points = {0.0, float(spot)}

    for leg in legs:
        try:
            points.add(float(leg["strike"]))
        except Exception:
            pass

    # Add one step around the traded strikes so fully hedged spreads are evaluated
    # just outside their wings even when the chart range is narrow.
    strikes = sorted(p for p in points if p > 0)
    if strikes:
        lo = max(0.0, min(strikes) - float(strike_step))
        hi = max(strikes) + float(strike_step)
        points.add(lo)
        points.add(hi)

    return sorted(points)


def calculate_strategy(
    spot: float,
    iv: float,
    days: int,
    legs: List[Dict[str, Any]],
    dataset: str = "NIFTY",
) -> Dict[str, Any]:
    dataset = _normalize_dataset(dataset)

    cfg = get_dataset_config(dataset)
    strike_step = int(cfg["strike_step"])
    lot_size = int(LOT_SIZE_BY_INSTRUMENT.get(dataset, 65))

    strikes_in_legs = [
        float(leg["strike"])
        for leg in legs
        if leg.get("strike") is not None
    ]

    # Keep the chart readable but make it wide enough to show hedges/wings.
    if strikes_in_legs:
        lower_seed = min(float(spot) * 0.94, min(strikes_in_legs) - 3 * strike_step)
        upper_seed = max(float(spot) * 1.06, max(strikes_in_legs) + 3 * strike_step)
    else:
        lower_seed = float(spot) * 0.94
        upper_seed = float(spot) * 1.06

    lower = max(0, int(lower_seed // strike_step * strike_step))
    upper = int(math.ceil(upper_seed / strike_step) * strike_step)

    spots = list(range(lower, upper + strike_step, strike_step))
    expiry_payoff = []
    current_payoff = []

    for s in spots:
        expiry_payoff.append(
            round(
                sum(_leg_payoff_at_expiry(s, leg, lot_size) for leg in legs),
                2,
            )
        )

        current_payoff.append(
            round(
                sum(_current_leg_pnl(s, leg, days, iv, lot_size) for leg in legs),
                2,
            )
        )

    total_credit = 0.0
    total_debit = 0.0

    greeks = {
        "delta": 0.0,
        "gamma": 0.0,
        "vega": 0.0,
        "theta": 0.0,
    }

    for leg in legs:
        side = str(leg["side"]).upper()
        option_type = str(leg["type"]).upper()
        strike = float(leg["strike"])
        premium = float(leg["premium"])
        qty_units = int(leg.get("qty", 1)) * int(leg.get("lots", 1))

        premium_value = premium * qty_units

        if side == "SELL":
            total_credit += premium_value
            greek_sign = -1
        else:
            total_debit += premium_value
            greek_sign = 1

        bs = black_scholes_price_and_greeks(
            spot=spot,
            strike=strike,
            days_to_expiry=days,
            iv_percent=iv,
            option_type=option_type,
        )

        greeks["delta"] += greek_sign * bs["delta"] * qty_units
        greeks["gamma"] += greek_sign * bs["gamma"] * qty_units
        greeks["vega"] += greek_sign * bs["vega"] * qty_units
        greeks["theta"] += greek_sign * bs["theta"] * qty_units

    unlimited = _payoff_unlimited_flags(legs)

    summary_points = _summary_payoff_points(
        spot=spot,
        legs=legs,
        strike_step=strike_step,
    )

    summary_payoffs = [
        round(
            sum(_leg_payoff_at_expiry(s, leg, lot_size) for leg in legs),
            2,
        )
        for s in summary_points
    ]

    finite_max_profit = max(summary_payoffs) if summary_payoffs else 0
    finite_max_loss = min(summary_payoffs) if summary_payoffs else 0

    max_profit = (
        "Unlimited"
        if unlimited["max_profit_unlimited"]
        else round(finite_max_profit, 2)
    )

    max_loss = (
        "Unlimited"
        if unlimited["max_loss_unlimited"]
        else round(finite_max_loss, 2)
    )

    breakevens = []

    for i in range(1, len(spots)):
        if expiry_payoff[i - 1] == 0:
            breakevens.append(spots[i - 1])
        elif expiry_payoff[i - 1] * expiry_payoff[i] < 0:
            x1, x2 = spots[i - 1], spots[i]
            y1, y2 = expiry_payoff[i - 1], expiry_payoff[i]
            breakeven = x1 + (0 - y1) * (x2 - x1) / (y2 - y1)
            breakevens.append(round(breakeven, 2))

    pnl_now = round(
        sum(_actual_leg_pnl_now(leg) for leg in legs),
        2,
    )

    return {
        "spots": spots,
        "payoff": expiry_payoff,
        "current": current_payoff,
        "summary": {
            "net_credit": round(total_credit - total_debit, 2),
            "max_profit": max_profit,
            "max_loss": max_loss,
            "breakevens": breakevens,
            "pnl_now": pnl_now,
            "delta": round(greeks["delta"], 2),
            "gamma": round(greeks["gamma"], 4),
            "vega": round(greeks["vega"], 2),
            "theta": round(greeks["theta"], 2),
            "lot_size": lot_size,
        },
    }

# =========================================================
# ROUTES
# =========================================================

@app.route("/")
def index():
    return render_template("simulator.html")


@app.route("/api/chain")
def api_chain():
    try:
        dataset = _normalize_dataset(
            request.args.get("dataset")
            or request.args.get("underlying")
        )

        query_date = (
            request.args.get("date")
            or request.args.get("query_date")
        )

        query_date = _normalize_date(query_date) if query_date else None

        query_time = _normalize_time(
            request.args.get("time")
            or request.args.get("query_time")
        )

        expiry_rule = request.args.get("expiry_rule", "current expiry")

        week_number = request.args.get("week_number")
        week_number = int(week_number) if week_number not in (None, "", "null") else None

        strike_count = int(request.args.get("strike_count", 14))
        interval = int(request.args.get("interval", CANDLE_INTERVAL_MINUTES))
        compute_greeks = _env_bool(
            "DEFAULT_CHAIN_COMPUTE_GREEKS",
            True,
        )
        if request.args.get("compute_greeks") is not None:
            compute_greeks = str(request.args.get("compute_greeks")).strip().lower() in {
                "1", "true", "yes", "on"
            }

        selected_expiry = request.args.get("expiry")

        result = build_option_chain_snapshot(
            query_date=query_date,
            query_time=query_time,
            dataset=dataset,
            week_number=week_number,
            expiry_rule=expiry_rule,
            strike_count_each_side=strike_count,
            candle_interval_minutes=interval,
            selected_expiry=selected_expiry,
            compute_greeks=compute_greeks,
        )

        response = jsonify(result)
        performance = result.get("performance") or {}
        if performance:
            response.headers["Server-Timing"] = ", ".join(
                f"{name.replace('_ms', '')};dur={value}"
                for name, value in performance.items()
                if name.endswith("_ms")
            )
        return response

    except Exception as exc:
        return _json_error(str(exc), 400)

# =========================================================
# ASYNC IV SURFACE ROUTES
# Redis + ThreadPoolExecutor
# =========================================================

if redis_client is not None:
    try:
        from iv_surface_async import register_iv_surface_routes

        register_iv_surface_routes(
            app=app,
            redis_client=redis_client,
            build_option_chain_snapshot=build_option_chain_snapshot,
            get_available_expiries_for_date=get_available_expiries_for_date,
            normalize_dataset=_normalize_dataset,
            normalize_date=_normalize_date,
            normalize_time=_normalize_time,
            resolve_default_week_date=_resolve_default_week_date,
            safe_float=_safe_float,
            json_error=_json_error,
            default_interval=CANDLE_INTERVAL_MINUTES,
        )

        logger.info(
            "IV-surface routes registered using Redis + ThreadPoolExecutor."
        )

    except Exception as exc:
        logger.exception(
            "Unable to register async IV-surface routes: %s",
            exc,
        )
        raise

else:
    @app.get("/api/iv-surface")
    def api_iv_surface_unavailable():
        return _json_error(
            "IV-surface service is unavailable because Redis is not connected.",
            503,
        )

    @app.get("/api/iv-surface/status/<job_id_value>")
    def api_iv_surface_status_unavailable(job_id_value: str):
        return _json_error(
            "IV-surface service is unavailable because Redis is not connected.",
            503,
        )


@app.route("/api/index-chart")
def api_index_chart():
    try:
        dataset = _normalize_dataset(
            request.args.get("dataset")
            or request.args.get("underlying")
        )

        start_date = _normalize_date(
            request.args.get("date")
            or request.args.get("query_date")
        )

        end_date = _normalize_date(
            request.args.get("end_date")
            or start_date
        )

        end_time = _normalize_time(
            request.args.get("time")
            or request.args.get("end_time")
            or "15:30"
        )

        interval = int(request.args.get("interval", CANDLE_INTERVAL_MINUTES))

        if interval <= 0:
            raise ValueError("interval must be greater than zero.")

        start_day = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_day = datetime.strptime(end_date, "%Y-%m-%d").date()

        if end_day < start_day:
            raise ValueError("end_date cannot be before start date.")

        all_candles = []
        resolved_week = None
        last_date_str = None

        current_day = start_day

        while current_day <= end_day:
            current_query_date = current_day.strftime("%Y-%m-%d")

            try:
                resolved_week, folder, date_str = _resolve_week_folder_for_date(
                    query_date=current_query_date,
                    dataset=dataset,
                    week_number=None,
                )

                day_candles = _get_spot_candles_for_day(
                    folder=folder,
                    date_str=date_str,
                    dataset=dataset,
                    candle_interval_minutes=interval,
                )

                if day_candles is not None and not day_candles.empty:
                    all_candles.append(day_candles)
                    last_date_str = date_str

            except Exception:
                pass

            current_day += timedelta(days=1)

        if not all_candles:
            return jsonify({
                "ok": True,
                "dataset": dataset,
                "query_date": start_date,
                "end_date": end_date,
                "date_str": None,
                "resolved_week_number": resolved_week,
                "interval": interval,
                "rows": [],
            })

        candles = pd.concat(all_candles).sort_index()

        start_dt = IST.localize(
            datetime.strptime(f"{start_date} 09:15", "%Y-%m-%d %H:%M")
        )

        end_dt = IST.localize(
            datetime.strptime(f"{end_date} {end_time}", "%Y-%m-%d %H:%M")
        )

        candles = candles[
            (candles.index >= start_dt)
            & (candles.index <= end_dt)
        ]

        rows = []

        for ts, row in candles.iterrows():
            ts = pd.Timestamp(ts)

            if ts.tzinfo is not None:
                ts = ts.tz_convert(IST).tz_localize(None)


            rows.append({
                "time": int(ts.timestamp()),
                "open": round(float(row["open"]), 2),
                "high": round(float(row["high"]), 2),
                "low": round(float(row["low"]), 2),
                "close": round(float(row["close"]), 2),
            })

        return jsonify({
            "ok": True,
            "dataset": dataset,
            "query_date": start_date,
            "end_date": end_date,
            "date_str": last_date_str,
            "resolved_week_number": resolved_week,
            "interval": interval,
            "rows": rows,
        })

    except Exception as exc:
        return _json_error(str(exc), 400)

@app.route("/api/option-metric-chart")
def api_option_metric_chart():
    request_started = time.perf_counter()
    timings: Dict[str, float] = {}

    try:
        dataset = _normalize_dataset(
            request.args.get("dataset") or request.args.get("underlying")
        )
        query_date_value = request.args.get("date") or request.args.get("query_date")
        query_date = _normalize_date(query_date_value) if query_date_value else None

        strike = int(request.args.get("strike"))
        metric = str(request.args.get("metric") or "").strip().lower()
        interval = int(request.args.get("interval", CANDLE_INTERVAL_MINUTES))
        selected_expiry = request.args.get("expiry")

        allowed = {
            "ce_ltp", "pe_ltp",
            "ce_iv", "pe_iv",
            "ce_delta", "pe_delta",
        }
        if metric not in allowed:
            raise ValueError(f"Unsupported metric: {metric}")
        if interval <= 0 or interval > 1440:
            raise ValueError("interval must be between 1 and 1440 minutes")

        stage = time.perf_counter()
        if query_date:
            resolved_week, folder, date_str = _resolve_week_folder_for_date(
                query_date=query_date,
                dataset=dataset,
                week_number=None,
            )
        else:
            resolved_week, folder, date_str, query_date = _resolve_default_week_date(dataset)
        timings["resolve_data_ms"] = round((time.perf_counter() - stage) * 1000, 3)

        end_date = _normalize_date(request.args.get("end_date") or query_date)
        end_time = _normalize_time(
            request.args.get("time") or request.args.get("end_time") or "15:30"
        )

        expiry_str = selected_expiry or get_upcoming_expiry_np(
            datetime.strptime(date_str, "%Y%m%d").date(),
            instrument=dataset,
        )
        if not expiry_str:
            raise ValueError("No upcoming expiry found")
        expiry_str = str(expiry_str).strip()

        metric_cache_key = (
            dataset, query_date, end_date, end_time, expiry_str,
            int(strike), metric, int(interval),
        )
        cached = _get_metric_chart_cache(metric_cache_key)
        if cached is not None:
            elapsed = round((time.perf_counter() - request_started) * 1000, 3)
            cached["cache_hit"] = True
            cached["performance"] = {"cache_lookup_ms": elapsed, "total_ms": elapsed}
            return jsonify(cached)

        start_ts = IST.localize(
            datetime.strptime(f"{query_date} 09:15", "%Y-%m-%d %H:%M")
        )
        end_ts = IST.localize(
            datetime.strptime(f"{end_date} {end_time}", "%Y-%m-%d %H:%M")
        )
        if end_ts < start_ts:
            raise ValueError("end date/time cannot be before start date/time")

        stage = time.perf_counter()
        consolidated = load_consolidated_option_chain(
            folder=folder,
            date_str=date_str,
            expiry_str=expiry_str,
            instrument=dataset,
        )
        opt = _load_option_data_fast(
            folder=folder,
            date_str=date_str,
            expiry_str=expiry_str,
            strike=strike,
            dataset=dataset,
            chain=consolidated,
        )
        timings["option_load_ms"] = round((time.perf_counter() - stage) * 1000, 3)

        stage = time.perf_counter()
        ce_close = _resample_option_close_for_session(
            opt.get("CE"), interval, start_ts, end_ts
        )
        pe_close = _resample_option_close_for_session(
            opt.get("PE"), interval, start_ts, end_ts
        )
        timings["option_resample_ms"] = round((time.perf_counter() - stage) * 1000, 3)

        stage = time.perf_counter()
        spot_candles = _get_spot_candles_for_day(
            folder=folder,
            date_str=date_str,
            dataset=dataset,
            candle_interval_minutes=interval,
        )
        timings["spot_load_ms"] = round((time.perf_counter() - stage) * 1000, 3)

        if spot_candles is None or spot_candles.empty:
            return _json_error(
                "No spot candles found for the selected date.",
                404,
                dataset=dataset, date_str=date_str, strike=strike, metric=metric, rows=[],
            )

        if getattr(spot_candles.index, "tz", None) is None:
            spot_index = spot_candles.index.tz_localize(IST)
        else:
            spot_index = spot_candles.index.tz_convert(IST)

        in_range = (spot_index >= start_ts) & (spot_index <= end_ts)
        spot_frame = spot_candles.loc[in_range].copy()
        spot_index = spot_index[in_range]
        if spot_frame.empty:
            return _json_error(
                "No spot candles found inside the requested chart range.",
                404, rows=[],
            )

        # Reindex option closes to the exact spot-candle grid. Forward-fill only
        # after the first real option observation; leading missing values stay NaN.
        ce_aligned = ce_close.reindex(spot_index).ffill() if not ce_close.empty else pd.Series(index=spot_index, dtype="float64")
        pe_aligned = pe_close.reindex(spot_index).ffill() if not pe_close.empty else pd.Series(index=spot_index, dtype="float64")

        rows_df = pd.DataFrame({
            "timestamp": spot_index.tz_localize(None),
            "trade_date": datetime.strptime(date_str, "%Y%m%d").date(),
            "instrument": dataset,
            "expiry": expiry_str,
            "nearest_strike": int(strike),
            "strike": int(strike),
            "close": pd.to_numeric(spot_frame["close"], errors="coerce").to_numpy(),
            "ce": ce_aligned.to_numpy(),
            "pe": pe_aligned.to_numpy(),
        })

        diagnostics = {
            "spot_rows": int(rows_df["close"].notna().sum()),
            "ce_price_rows": int(rows_df["ce"].notna().sum()),
            "pe_price_rows": int(rows_df["pe"].notna().sum()),
        }

        stage = time.perf_counter()
        if metric in {"ce_iv", "pe_iv", "ce_delta", "pe_delta"}:
            compute_greeks = metric in {"ce_delta", "pe_delta"}
            try:
                rows_df = append_black_scholes_iv(
                    rows_df,
                    compute_greeks=compute_greeks,
                    inplace=True,
                )
            except TypeError:
                rows_df = append_black_scholes_iv(
                    rows_df, compute_greeks=compute_greeks
                )
        timings["iv_greeks_ms"] = round((time.perf_counter() - stage) * 1000, 3)

        col_map = {
            "ce_ltp": "ce", "pe_ltp": "pe",
            "ce_iv": "ce_iv", "pe_iv": "pe_iv",
            "ce_delta": "ce_delta", "pe_delta": "pe_delta",
        }
        col = col_map[metric]
        if col not in rows_df.columns:
            rows_df[col] = np.nan

        for name in ("ce_iv", "pe_iv", "ce_delta", "pe_delta"):
            diagnostics[f"{name}_rows"] = int(
                rows_df[name].notna().sum() if name in rows_df.columns else 0
            )

        valid = rows_df.loc[rows_df[col].notna(), ["timestamp", col]]
        if valid.empty:
            timings["total_ms"] = round((time.perf_counter() - request_started) * 1000, 3)
            logger.warning(
                "No metric chart data dataset=%s date=%s expiry=%s strike=%s "
                "metric=%s diagnostics=%s timings=%s",
                dataset, date_str, expiry_str, strike, metric, diagnostics, timings,
            )
            return _json_error(
                f"No valid {metric} values were calculated for strike {strike}.",
                422,
                dataset=dataset, query_date=query_date, end_date=end_date,
                end_time=end_time, date_str=date_str, expiry=expiry_str,
                strike=strike, metric=metric, diagnostics=diagnostics,
                performance=timings, rows=[],
            )

        stage = time.perf_counter()
        rows = []
        for row in valid.itertuples(index=False, name=None):
            ts, value = row
            ts = pd.Timestamp(ts)
            if ts.tzinfo is not None:
                ts = ts.tz_convert(IST).tz_localize(None)
            rows.append({
                "time": int(ts.timestamp()),
                "value": round(float(value), 4),
            })
        timings["response_build_ms"] = round((time.perf_counter() - stage) * 1000, 3)
        timings["total_ms"] = round((time.perf_counter() - request_started) * 1000, 3)

        payload = {
            "ok": True,
            "dataset": dataset,
            "query_date": query_date,
            "end_date": end_date,
            "end_time": end_time,
            "date_str": date_str,
            "resolved_week_number": resolved_week,
            "expiry": expiry_str,
            "strike": strike,
            "metric": metric,
            "interval": interval,
            "cache_hit": False,
            "diagnostics": diagnostics,
            "performance": timings,
            "rows": rows,
        }
        _put_metric_chart_cache(metric_cache_key, payload)
        logger.info(
            "Metric chart dataset=%s date=%s expiry=%s strike=%s metric=%s "
            "rows=%d timings=%s",
            dataset, date_str, expiry_str, strike, metric, len(rows), timings,
        )
        return jsonify(payload)

    except ValueError as exc:
        return _json_error(str(exc), 400)
    except Exception as exc:
        logger.exception("Option metric chart failed")
        return _json_error("Unable to build option metric chart.", 500, detail=str(exc))


@app.route("/api/option-ltp-candle-chart")
def api_option_ltp_candle_chart():
    try:
        dataset = _normalize_dataset(request.args.get("dataset") or request.args.get("underlying"))
        query_date = request.args.get("date") or request.args.get("query_date")
        query_date = _normalize_date(query_date) if query_date else None

        strike = int(request.args.get("strike"))
        side = str(request.args.get("side") or "CE").upper()
        interval = int(request.args.get("interval", CANDLE_INTERVAL_MINUTES))
        selected_expiry = request.args.get("expiry")

        if side not in {"CE", "PE"}:
            raise ValueError("side must be CE or PE")

        if query_date:
            resolved_week, folder, date_str = _resolve_week_folder_for_date(
                query_date=query_date,
                dataset=dataset,
                week_number=None,
            )
        else:
            resolved_week, folder, date_str, query_date = _resolve_default_week_date(dataset)

        expiry_str = selected_expiry or get_upcoming_expiry_np(
            datetime.strptime(date_str, "%Y%m%d").date(),
            instrument=dataset,
        )

        opt = _load_option_data_fast(
            folder=folder,
            date_str=date_str,
            expiry_str=expiry_str,
            strike=strike,
            dataset=dataset,
        )

        raw_df = opt.get(side)

        if raw_df is None or raw_df.empty:
            return jsonify({
                "ok": True,
                "dataset": dataset,
                "query_date": query_date,
                "date_str": date_str,
                "strike": strike,
                "side": side,
                "rows": [],
            })

        raw_df = raw_df.copy()
        raw_df["value"] = raw_df["price"]

        end_date = _normalize_date(request.args.get("end_date") or query_date)
        end_time = _normalize_time(
            request.args.get("time") or request.args.get("end_time") or "15:30"
        )

        start_dt = IST.localize(
            datetime.strptime(f"{query_date} 09:15", "%Y-%m-%d %H:%M")
        )

        end_dt = IST.localize(
            datetime.strptime(f"{end_date} {end_time}", "%Y-%m-%d %H:%M")
        )

        payload = build_chart_payload(
            raw_df,
            interval=interval,
            start=start_dt,
            end=end_dt,
            previous_candles=100,
            sma=(20, 50),
            ema=(9, 20),
            price_col="value",
        )

        return jsonify({
            "ok": True,
            "dataset": dataset,
            "query_date": query_date,
            "date_str": date_str,
            "strike": strike,
            "side": side,
            "interval": interval,
            "rows": payload["candles"],
            "indicators": payload["indicators"],
        })

    except Exception as exc:
        return _json_error(str(exc), 400)
@app.route("/api/india-vix-chart")
def api_india_vix_chart():
    try:
        dataset = _normalize_dataset(
            request.args.get("dataset")
            or request.args.get("underlying")
        )

        query_date = (
            request.args.get("date")
            or request.args.get("query_date")
        )
        query_date = _normalize_date(query_date) if query_date else None

        end_date = _normalize_date(
            request.args.get("end_date") or query_date
        )

        end_time = _normalize_time(
            request.args.get("time")
            or request.args.get("end_time")
            or "15:30"
        )

        interval = int(request.args.get("interval", CANDLE_INTERVAL_MINUTES))

        if interval <= 0:
            raise ValueError("interval must be greater than zero.")

        if query_date:
            resolved_week, folder, date_str = _resolve_week_folder_for_date(
                query_date=query_date,
                dataset=dataset,
                week_number=None,
            )
        else:
            resolved_week, folder, date_str, query_date = _resolve_default_week_date(
                dataset=dataset,
            )

        vix_df = load_index_data_by_symbol(
            folder=folder,
            date_str=date_str,
            symbol_name="INDIAVIX",
        )

        if vix_df.empty:
            return jsonify({
                "ok": True,
                "dataset": dataset,
                "query_date": query_date,
                "end_date": end_date,
                "end_time": end_time,
                "date_str": date_str,
                "resolved_week_number": resolved_week,
                "interval": interval,
                "rows": [],
            })

        candles = create_candles(vix_df, interval)

        start_dt = IST.localize(
            datetime.strptime(f"{query_date} 09:15", "%Y-%m-%d %H:%M")
        )

        end_dt = IST.localize(
            datetime.strptime(f"{end_date} {end_time}", "%Y-%m-%d %H:%M")
        )

        candles = candles[
            (candles.index >= start_dt)
            & (candles.index <= end_dt)
        ]

        rows = []

        for ts, row in candles.iterrows():
            ts = pd.Timestamp(ts)

            # Important: convert IST candle time to naive IST
            # so Lightweight Charts displays Indian market wall-clock time.
            if ts.tzinfo is not None:
                ts = ts.tz_convert(IST).tz_localize(None)

            rows.append({
                "time": int(ts.timestamp()),
                "open": round(float(row["open"]), 2),
                "high": round(float(row["high"]), 2),
                "low": round(float(row["low"]), 2),
                "close": round(float(row["close"]), 2),
            })

        return jsonify({
            "ok": True,
            "dataset": dataset,
            "query_date": query_date,
            "end_date": end_date,
            "end_time": end_time,
            "date_str": date_str,
            "resolved_week_number": resolved_week,
            "interval": interval,
            "rows": rows,
        })

    except Exception as exc:
        return _json_error(str(exc), 400)

@app.route("/api/future-chart")
def api_future_chart():
    try:
        dataset = _normalize_dataset(
            request.args.get("dataset")
            or request.args.get("underlying")
        )

        start_date = _normalize_date(
            request.args.get("date")
            or request.args.get("query_date")
        )

        end_date = _normalize_date(
            request.args.get("end_date")
            or start_date
        )

        end_time = _normalize_time(
            request.args.get("time")
            or request.args.get("end_time")
            or "15:30"
        )

        interval = int(request.args.get("interval", CANDLE_INTERVAL_MINUTES))
        month = str(request.args.get("month") or "current").lower()

        if interval <= 0:
            raise ValueError("interval must be greater than zero.")

        start_day = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_day = datetime.strptime(end_date, "%Y-%m-%d").date()

        if end_day < start_day:
            raise ValueError("end_date cannot be before start date.")

        all_futures = []
        resolved_week = None
        last_date_str = None

        current_day = start_day

        while current_day <= end_day:
            current_query_date = current_day.strftime("%Y-%m-%d")

            try:
                resolved_week, folder, date_str = _resolve_week_folder_for_date(
                    query_date=current_query_date,
                    dataset=dataset,
                    week_number=None,
                )

                fut_df = load_future_data_for_date(
                    folder=folder,
                    date_str=date_str,
                    month=month,
                    instrument=dataset,
                )

                if fut_df is not None and not fut_df.empty:
                    all_futures.append(fut_df)
                    last_date_str = date_str

            except Exception:
                pass

            current_day += timedelta(days=1)

        if not all_futures:
            return jsonify({
                "ok": True,
                "dataset": dataset,
                "query_date": start_date,
                "end_date": end_date,
                "date_str": None,
                "resolved_week_number": resolved_week,
                "interval": interval,
                "month": month,
                "rows": [],
                "indicators": {},
            })

        fut_df = pd.concat(all_futures, ignore_index=True)

        if "value" not in fut_df.columns and "price" in fut_df.columns:
            fut_df["value"] = fut_df["price"]

        candles = create_candles(
            fut_df,
            interval,
        )

        start_dt = IST.localize(
            datetime.strptime(f"{start_date} 09:15", "%Y-%m-%d %H:%M")
        )

        end_dt = IST.localize(
            datetime.strptime(f"{end_date} {end_time}", "%Y-%m-%d %H:%M")
        )

        payload = build_chart_payload(
            candles,
            interval=interval,
            start=start_dt,
            end=end_dt,
            previous_candles=100,
            sma=(20, 50),
            ema=(9, 20),
        )

        return jsonify({
            "ok": True,
            "dataset": dataset,
            "query_date": start_date,
            "end_date": end_date,
            "date_str": last_date_str,
            "resolved_week_number": resolved_week,
            "interval": interval,
            "month": month,
            "rows": payload["candles"],
            "indicators": payload["indicators"],
        })

    except Exception as exc:
        return _json_error(str(exc), 400)

@app.route("/api/calculate", methods=["POST"])
def api_calculate():
    try:
        data = request.get_json(force=True) or {}

        dataset = _normalize_dataset(
            data.get("dataset")
            or data.get("underlying")
        )

        spot = float(data.get("spot"))
        iv = float(data.get("iv") or 20.0)
        days = int(data.get("days") or 1)
        legs = data.get("legs") or []

        if not legs:
            return jsonify(
                {
                    "spots": [],
                    "payoff": [],
                    "current": [],
                    "summary": {
                        "net_credit": 0,
                        "max_profit": 0,
                        "max_loss": 0,
                        "breakevens": [],
                        "pnl_now": 0,
                        "delta": 0,
                        "gamma": 0,
                        "vega": 0,
                        "theta": 0,
                        "lot_size": LOT_SIZE_BY_INSTRUMENT.get(dataset, 65),
                    },
                }
            )

        return jsonify(
            calculate_strategy(
                spot=spot,
                iv=iv,
                days=days,
                legs=legs,
                dataset=dataset,
            )
        )

    except Exception as exc:
        return _json_error(str(exc), 400)

@app.route("/api/previous-trading-session")
def api_previous_trading_session():
    try:
        dataset = _normalize_dataset(request.args.get("dataset"))
        query_date = _normalize_date(request.args.get("date"))

        current = datetime.strptime(query_date, "%Y-%m-%d").date()

        all_dates = []

        for num, folder in get_week_folders(instrument=dataset):
            for date_str in get_dates_for_week_folder(num, folder, instrument=dataset):
                d = datetime.strptime(date_str, "%Y%m%d").date()
                if d < current:
                    all_dates.append(d)

        if not all_dates:
            raise ValueError("No previous trading date found.")

        prev_date = max(all_dates)

        return jsonify({
            "ok": True,
            "date": prev_date.strftime("%Y-%m-%d"),
            "time": "15:30"
        })

    except Exception as exc:
        return _json_error(str(exc), 400)
@app.route("/api/defaults")
def api_defaults():
    try:
        dataset = _normalize_dataset(
            request.args.get("dataset")
            or request.args.get("underlying")
        )

        week_number, folder, date_str, query_date = _resolve_default_week_date(
            dataset=dataset,
        )

        return jsonify(
            {
                "ok": True,
                "dataset": dataset,
                "query_date": query_date,
                "date_str": date_str,
                "query_time": DEFAULT_QUERY_TIME,
                "max_allowed_query_date": MAX_ALLOWED_QUERY_DATE.strftime("%Y-%m-%d"),
                "resolved_week_number": week_number,
            }
        )

    except Exception as exc:
        return _json_error(str(exc), 400)


@app.route("/api/cache/status")
def api_cache_status():
    return jsonify(
        {
            "ok": True,
            "option_window_cache_size": len(OPTION_WINDOW_CACHE),
            "option_window_cache_limit": MAX_OPTION_WINDOW_CACHE_SIZE,
            "spot_candle_cache_size": len(SPOT_CANDLE_CACHE),
            "spot_candle_cache_limit": MAX_SPOT_CANDLE_CACHE_SIZE,
            "window_candles_before": WINDOW_CANDLES_BEFORE,
            "window_candles_after": WINDOW_CANDLES_AFTER,
            "iv_enabled": ENABLE_IV_CALC,
            "chain_snapshot_cache_size": len(CHAIN_SNAPSHOT_CACHE),
            "chain_snapshot_cache_limit": MAX_CHAIN_SNAPSHOT_CACHE_SIZE,
            "chain_snapshot_cache_ttl_seconds": CHAIN_SNAPSHOT_CACHE_TTL_SECONDS,
            "metric_chart_cache_size": len(METRIC_CHART_CACHE),
            "metric_chart_cache_limit": MAX_METRIC_CHART_CACHE_SIZE,
            "metric_chart_cache_ttl_seconds": METRIC_CHART_CACHE_TTL_SECONDS,
            "data_engine": runtime_cache_stats(),
            "iv_cache": iv_cache_stats(),
        }
    )


@app.route("/api/cache/clear", methods=["POST"])
def api_cache_clear():
    configured_token = os.getenv("CACHE_ADMIN_TOKEN")
    if configured_token and request.headers.get("X-Admin-Token") != configured_token:
        return _json_error("Unauthorized.", 401)
    with OPTION_WINDOW_CACHE_LOCK:
        OPTION_WINDOW_CACHE.clear()
    with SPOT_CANDLE_CACHE_LOCK:
        SPOT_CANDLE_CACHE.clear()
    _clear_chain_snapshot_cache()
    _clear_metric_chart_cache()
    clear_iv_cache()
    return jsonify({
        "ok": True,
        "message": "Application and IV RAM caches cleared.",
    })


@app.route("/api/health")
def api_health():
    redis_ok = False
    if redis_client is not None:
        try:
            redis_ok = bool(redis_client.ping())
        except Exception:
            redis_ok = False
    payload = {
        "ok": True,
        "service": "options-simulator",
        "storage_mode": STORAGE_MODE,
        "data_layout": DATA_LAYOUT,
        "iv_enabled": ENABLE_IV_CALC,
        "redis_connected": redis_ok,
        "iv_surface_backend": "redis_threadpool" if redis_ok else "unavailable",
        "option_window_cache_size": len(OPTION_WINDOW_CACHE),
        "spot_candle_cache_size": len(SPOT_CANDLE_CACHE),
        "chain_snapshot_cache_size": len(CHAIN_SNAPSHOT_CACHE),
        "metric_chart_cache_size": len(METRIC_CHART_CACHE),
    }
    return jsonify(payload), 200



if __name__ == "__main__":
    # Development runner only. In production use Gunicorn, for example:
    # gunicorn --workers 2 --threads 4 --timeout 120 --bind 0.0.0.0:8000 simulator:app
    port = int(os.getenv("PORT", "8000"))
    host = os.getenv("HOST", "0.0.0.0")
    debug = os.getenv("DEBUG_MODE", "false").lower() in {"1", "true", "yes", "on"}

    app.run(
        host=host,
        port=port,
        debug=debug,
    )
