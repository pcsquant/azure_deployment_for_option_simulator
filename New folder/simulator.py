"""
StockMock-style Options Simulator Flask app.

Place this file at:
    agent_for_production/app.py

This version adds:
- interval-aware RAM window caching
- only 10 candles before + current + 10 candles after are kept per option leg
- works for 1m, 2m, 3m, 5m, 10m, 15m, etc.
- keeps existing API contract unchanged

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

import math
import os
import json
# import redis
# from kafka import KafkaProducer

from collections import OrderedDict
from datetime import datetime, timedelta, date
from typing import Any, Dict, List, Optional, Tuple
import numpy as np
import pandas as pd
from flask import Flask, jsonify, render_template, request

from chart import build_chart_payload

from black_scholes_iv_for_simulation import append_black_scholes_iv
from config_for_simulation import CANDLE_INTERVAL_MINUTES, IST, get_dataset_config
from data_engine_for_simulation import (
    create_candles,
    get_dates_for_week_folder,
    get_nearest_strike,
    get_upcoming_expiry_np,
    get_week_folders,
    load_consolidated_option_chain,
    load_required_option_data_for_date,
    load_tick_data,
)

app = Flask(__name__)


@app.after_request
def add_no_cache_headers(response):
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


LOT_SIZE_BY_INSTRUMENT = {
    "NIFTY": 65,
    "SENSEX": 20,
}

DEFAULT_QUERY_TIME = "09:15"


MAX_ALLOWED_QUERY_DATE = date.max

# Number of candles to keep before and after selected candle.
WINDOW_CANDLES_BEFORE = 10
WINDOW_CANDLES_AFTER = 10

# Max option-window entries kept in RAM.
# One entry is for one instrument/date/expiry/strike/CE-or-PE/interval/target-time.
MAX_OPTION_WINDOW_CACHE_SIZE = int(os.getenv("OPTION_WINDOW_CACHE_SIZE", "500"))

# Max spot candle-day entries kept in RAM.
MAX_SPOT_CANDLE_CACHE_SIZE = int(os.getenv("SPOT_CANDLE_CACHE_SIZE", "50"))



OPTION_WINDOW_CACHE: "OrderedDict[Tuple[Any, ...], pd.DataFrame]" = OrderedDict()
SPOT_CANDLE_CACHE: "OrderedDict[Tuple[Any, ...], pd.DataFrame]" = OrderedDict()

# Consolidated per-day/per-expiry option chains (Tier 1). One entry is the whole
# [timestamp, strike, ce, pe] frame for an instrument/date/expiry.
CONSOLIDATED_CHAIN_CACHE: "OrderedDict[Tuple[Any, ...], Optional[pd.DataFrame]]" = OrderedDict()
MAX_CONSOLIDATED_CHAIN_CACHE_SIZE = int(os.getenv("CONSOLIDATED_CHAIN_CACHE_SIZE", "32"))

# Speed mode: IV/Greeks calculation is expensive.
# Default OFF for faster simulation. Set ENABLE_IV_CALC=true to enable it.
ENABLE_IV_CALC = True

# # Redis and kafka
#
# REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
# KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
#
# redis_client = redis.Redis.from_url(
#     REDIS_URL,
#     decode_responses=True,
# )
#
# kafka_producer = None
#
# def get_kafka_producer():
#     global kafka_producer
#
#     if kafka_producer is None:
#         kafka_producer = KafkaProducer(
#             bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
#             value_serializer=lambda v: json.dumps(v).encode("utf-8"),
#         )
#
#     return kafka_producer


# =========================================================
# BASIC HELPERS
# =========================================================

def _json_error(message: str, status: int = 400, **extra):
    payload = {"ok": False, "error": message}
    payload.update(extra)
    return jsonify(payload), status


def _normalize_dataset(value):
    value = str(value or "NIFTY").strip().upper()

    if value in {"BSE", "SENSEX"}:
        return "SENSEX"

    if value in {"BANKNIFTY", "BANK NIFTY", "BANK-NIFTY"}:
        return "BANKNIFTY"

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


def _touch_cache(cache: OrderedDict, key: Tuple[Any, ...], value: pd.DataFrame, max_size: int) -> None:
    cache[key] = value
    cache.move_to_end(key)

    while len(cache) > max_size:
        cache.popitem(last=False)


def _get_cache(cache: OrderedDict, key: Tuple[Any, ...]) -> Optional[pd.DataFrame]:
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


def _resolve_default_week_date(dataset: str):

    target_date = date.today() - timedelta(days=1)

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

            if dt > target_date:
                continue

            item = (dt, int(num), folder, date_str)

            if latest_item is None or dt > latest_item[0]:
                latest_item = item

    if latest_item is None:
        raise ValueError("No historical data found.")

    dt, week_number, folder, date_str = latest_item

    return (
        week_number,
        folder,
        date_str,
        dt.strftime("%Y-%m-%d"),
    )
def get_available_expiries_for_date(query_date, dataset="NIFTY", max_months=2):
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
# CONSOLIDATED OPTION CHAIN (Tier 1 fast path)
# =========================================================

def _get_consolidated_chain(
    folder: str,
    date_str: str,
    expiry_str: str,
    dataset: str,
) -> Optional[pd.DataFrame]:
    """
    Return the consolidated [timestamp, strike, ce, pe] frame for a day+expiry,
    or None when no consolidated file exists (caller falls back to per-contract
    reads). Cached in RAM; `None` results are cached too so a missing file is
    only probed once per process.
    """
    cache_key = ("consolidated", dataset, date_str, str(expiry_str), folder)

    if cache_key in CONSOLIDATED_CHAIN_CACHE:
        CONSOLIDATED_CHAIN_CACHE.move_to_end(cache_key)
        cached = CONSOLIDATED_CHAIN_CACHE[cache_key]
        return cached.copy() if cached is not None else None

    try:
        df = load_consolidated_option_chain(
            folder=folder,
            date_str=date_str,
            expiry_str=expiry_str,
            instrument=dataset,
        )
    except Exception as exc:
        print("Consolidated chain load error:", exc)
        df = None

    CONSOLIDATED_CHAIN_CACHE[cache_key] = df
    CONSOLIDATED_CHAIN_CACHE.move_to_end(cache_key)
    while len(CONSOLIDATED_CHAIN_CACHE) > MAX_CONSOLIDATED_CHAIN_CACHE_SIZE:
        CONSOLIDATED_CHAIN_CACHE.popitem(last=False)

    return df.copy() if df is not None else None


def _chain_ltps_from_consolidated(
    consolidated_df: pd.DataFrame,
    strikes: List[int],
    target_ts: pd.Timestamp,
    candle_interval_minutes: int,
) -> Dict[int, Tuple[Optional[float], Optional[float]]]:
    """
    Vectorized LTP-at-time for every strike at once.

    Mirrors `_build_option_candle_window` + `_option_ltp_from_window`: the LTP is
    the close of the last non-empty N-minute candle whose left label is at or
    before `target_ts`. Resampling the 1-minute closes to N minutes with `.last()`
    reproduces those candle closes (including ticks within the target candle), and
    `ffill().iloc[-1]` selects the most recent non-empty close up to the target.
    """
    target_ts = _to_ist_timestamp(target_ts)
    wanted = [int(s) for s in strikes]

    # Match the per-contract fallback's bounded lookback: only trades within
    # (before+1)*interval minutes before target count, so an illiquid strike
    # with no recent trade yields None rather than a stale carried-forward price.
    start_ts, _ = _window_time_bounds(
        target_ts=target_ts,
        candle_interval_minutes=candle_interval_minutes,
    )

    sub = consolidated_df[
        consolidated_df["strike"].isin(wanted)
        & (consolidated_df["timestamp"] >= start_ts)
        & (consolidated_df["timestamp"] <= target_ts + timedelta(minutes=int(candle_interval_minutes)))
    ]
    if sub.empty:
        return {}

    result: Dict[int, Tuple[Optional[float], Optional[float]]] = {}
    rule = f"{int(candle_interval_minutes)}min"

    side_values: Dict[str, "pd.Series"] = {}
    for col in ("ce", "pe"):
        wide = sub.pivot_table(
            index="timestamp",
            columns="strike",
            values=col,
            aggfunc="last",
        ).sort_index()

        if wide.empty:
            side_values[col] = pd.Series(dtype="float64")
            continue

        nmin = wide.resample(rule).last()
        nmin = nmin[nmin.index <= target_ts]

        if nmin.empty:
            side_values[col] = pd.Series(dtype="float64")
            continue

        # Last non-empty N-minute close per strike at or before target.
        side_values[col] = nmin.ffill().iloc[-1]

    ce_vals = side_values.get("ce", pd.Series(dtype="float64"))
    pe_vals = side_values.get("pe", pd.Series(dtype="float64"))

    for strike in wanted:
        ce_ltp = _safe_float(ce_vals.get(strike)) if len(ce_vals) else None
        pe_ltp = _safe_float(pe_vals.get(strike)) if len(pe_vals) else None
        result[strike] = (ce_ltp, pe_ltp)

    return result


# =========================================================
# SPOT CANDLES
# =========================================================

def _get_spot_candles_for_day(
    folder: str,
    date_str: str,
    dataset: str,
    candle_interval_minutes: int,
) -> pd.DataFrame:
    cache_key = (
        "spot",
        dataset,
        date_str,
        int(candle_interval_minutes),
        folder,
    )

    cached = _get_cache(SPOT_CANDLE_CACHE, cache_key)
    if cached is not None:
        return cached.copy()

    # Parquet-folder mode:
    # Your data is stored as:
    #   week_folder/
    #       NSE_IDX_TICK_YYYYMMDD/
    #       NSE_OPT_TICK_YYYYMMDD/
    #       NSE_FUT_TICK_YYYYMMDD/
    #
    # So pass the week folder directly. data_engine_for_simulation.py
    # will detect NSE_IDX_TICK_{date_str} internally.
    tick_df = load_tick_data(
        folder,
        instrument=dataset,
    )

    if tick_df.empty:
        return pd.DataFrame()

    candles = create_candles(
        tick_df,
        candle_interval_minutes,
    )

    if candles.empty:
        return pd.DataFrame()

    _touch_cache(
        SPOT_CANDLE_CACHE,
        cache_key,
        candles.copy(),
        MAX_SPOT_CANDLE_CACHE_SIZE,
    )

    return candles


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
    dataset = _normalize_dataset(dataset)
    query_time = _normalize_time(query_time)
    candle_interval_minutes = int(candle_interval_minutes)

    if candle_interval_minutes <= 0:
        raise ValueError("interval must be greater than zero.")

    cfg = get_dataset_config(dataset)
    strike_step = int(cfg["strike_step"])

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

    spot_candles = _get_spot_candles_for_day(
        folder=folder,
        date_str=date_str,
        dataset=dataset,
        candle_interval_minutes=candle_interval_minutes,
    )

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

    day_open = _safe_float(spot_candles.iloc[0].get("open"))
    if day_open is None:
        day_open = spot

    # =========================
    # INDIA VIX VALUE
    # =========================
    india_vix_value = None

    try:
        from data_engine_for_simulation import load_index_data_by_symbol

        vix_df = load_index_data_by_symbol(
            folder=folder,
            date_str=date_str,
            symbol_name="INDIAVIX",
        )

        if vix_df is not None and not vix_df.empty:
            vix_df = vix_df.copy()
            vix_df["datetime"] = pd.to_datetime(vix_df["datetime"], errors="coerce")
            vix_df["value"] = pd.to_numeric(vix_df["value"], errors="coerce")
            vix_df = vix_df.dropna(subset=["datetime", "value"])

            if not vix_df.empty:
                if getattr(vix_df["datetime"].dt, "tz", None) is None:
                    vix_df["datetime"] = vix_df["datetime"].dt.tz_localize(IST)
                else:
                    vix_df["datetime"] = vix_df["datetime"].dt.tz_convert(IST)

                target_vix = vix_df[vix_df["datetime"] <= target_ts]

                if not target_vix.empty:
                    india_vix_value = _safe_float(target_vix.iloc[-1]["value"])

    except Exception as exc:
        print("INDIA VIX load error:", exc)
        india_vix_value = None

    atm = int(
        get_nearest_strike(
            spot,
            instrument=dataset,
            expiry_rule=expiry_rule,
        )
    )

    expiry_str = selected_expiry or get_upcoming_expiry_np(
        datetime.strptime(date_str, "%Y%m%d").date(),
        instrument=dataset,
        expiry_rule=expiry_rule,
    )

    if expiry_str is None:
        raise ValueError("No upcoming expiry found.")

    trade_date = datetime.strptime(date_str, "%Y%m%d").date()
    expiry_date = datetime.strptime(str(expiry_str), "%y%m%d").date()
    dte = max((expiry_date - trade_date).days, 0)

    strikes = [
        atm + i * strike_step
        for i in range(-int(strike_count_each_side), int(strike_count_each_side) + 1)
    ]

    rows = []

    # Tier 1 fast path: a single consolidated read serves every strike. Falls
    # back to per-contract reads below when no consolidated file is present.
    consolidated_df = _get_consolidated_chain(
        folder=folder,
        date_str=date_str,
        expiry_str=expiry_str,
        dataset=dataset,
    )

    consolidated_ltps: Optional[Dict[int, Tuple[Optional[float], Optional[float]]]] = None
    if consolidated_df is not None and not consolidated_df.empty:
        consolidated_ltps = _chain_ltps_from_consolidated(
            consolidated_df=consolidated_df,
            strikes=strikes,
            target_ts=target_ts,
            candle_interval_minutes=candle_interval_minutes,
        )

    chain_source = "consolidated" if consolidated_ltps is not None else "per_contract"

    for strike in strikes:
        option_data = load_required_option_data_for_date(
            folder=folder,
            date_str=date_str,
            expiry_str=expiry_str,
            strike=int(strike),
            instrument=dataset,
        )

        if consolidated_ltps is not None:
            ce_ltp, pe_ltp = consolidated_ltps.get(int(strike), (None, None))
        else:

            ce_cache_key = (
                "option_window",
                dataset,
                date_str,
                expiry_str,
                int(strike),
                "CE",
                candle_interval_minutes,
                target_ts.strftime("%Y-%m-%d %H:%M:%S%z"),
            )

            pe_cache_key = (
                "option_window",
                dataset,
                date_str,
                expiry_str,
                int(strike),
                "PE",
                candle_interval_minutes,
                target_ts.strftime("%Y-%m-%d %H:%M:%S%z"),
            )

            ce_ltp = _option_ltp_at_time_cached(
                raw_df=option_data.get("CE"),
                target_ts=target_ts,
                candle_interval_minutes=candle_interval_minutes,
                cache_key=ce_cache_key,
            )

            pe_ltp = _option_ltp_at_time_cached(
                raw_df=option_data.get("PE"),
                target_ts=target_ts,
                candle_interval_minutes=candle_interval_minutes,
                cache_key=pe_cache_key,
            )

        ce_oi, ce_change_oi = _oi_at_time(option_data.get("CE"), target_ts, candle_interval_minutes)
        pe_oi, pe_change_oi = _oi_at_time(option_data.get("PE"), target_ts, candle_interval_minutes)

        rows.append(
            {
                "timestamp": pd.Timestamp(target_ts).tz_convert(IST).tz_localize(None),
                "trade_date": datetime.strptime(date_str, "%Y%m%d").date(),
                "instrument": dataset,
                "expiry": expiry_str,
                "nearest_strike": int(strike),
                "strike": int(strike),
                "close": float(spot),
                "ce": ce_ltp,
                "pe": pe_ltp,
                "ce_oi": ce_oi,
                "pe_oi": pe_oi,
                "ce_change_oi": ce_change_oi,
                "pe_change_oi": pe_change_oi,
            }
        )

    chain_df = pd.DataFrame(rows)

    if ENABLE_IV_CALC:
        try:
            chain_df = append_black_scholes_iv(chain_df, compute_greeks=compute_greeks)
        except Exception as exc:
            chain_df["iv"] = np.nan
            chain_df["ce_iv"] = np.nan
            chain_df["pe_iv"] = np.nan
            chain_df["ce_delta"] = np.nan
            chain_df["pe_delta"] = np.nan
            chain_df["ce_gamma"] = np.nan
            chain_df["pe_gamma"] = np.nan
            chain_df["ce_vega"] = np.nan
            chain_df["pe_vega"] = np.nan
            chain_df["ce_theta"] = np.nan
            chain_df["pe_theta"] = np.nan
            chain_df["_iv_error"] = str(exc)
    else:
        chain_df["iv"] = np.nan
        chain_df["ce_iv"] = np.nan
        chain_df["pe_iv"] = np.nan
        chain_df["ce_delta"] = np.nan
        chain_df["pe_delta"] = np.nan
        chain_df["ce_gamma"] = np.nan
        chain_df["pe_gamma"] = np.nan
        chain_df["ce_vega"] = np.nan
        chain_df["pe_vega"] = np.nan
        chain_df["ce_theta"] = np.nan
        chain_df["pe_theta"] = np.nan

    chain_rows = []

    for _, row in chain_df.iterrows():
        strike = _safe_int(row.get("strike"))

        chain_rows.append(
            {
                "strike": strike,
                "atm": strike == atm,
                "ce_ltp": _round_or_none(row.get("ce"), 2),
                "pe_ltp": _round_or_none(row.get("pe"), 2),
                "iv": _round_or_none(row.get("iv"), 4),
                "ce_iv": _round_or_none(row.get("ce_iv"), 4),
                "pe_iv": _round_or_none(row.get("pe_iv"), 4),
                "ce_delta": _round_or_none(row.get("ce_delta"), 4),
                "pe_delta": _round_or_none(row.get("pe_delta"), 4),
                "ce_gamma": _round_or_none(row.get("ce_gamma"), 6),
                "pe_gamma": _round_or_none(row.get("pe_gamma"), 6),
                "ce_vega": _round_or_none(row.get("ce_vega"), 4),
                "pe_vega": _round_or_none(row.get("pe_vega"), 4),
                "ce_theta": _round_or_none(row.get("ce_theta"), 4),
                "pe_theta": _round_or_none(row.get("pe_theta"), 4),
                "ce_oi": _round_or_none(row.get("ce_oi"), 0),
                "pe_oi": _round_or_none(row.get("pe_oi"), 0),
                "ce_change_oi": _round_or_none(row.get("ce_change_oi"), 0),
                "pe_change_oi": _round_or_none(row.get("pe_change_oi"), 0),
            }
        )

    valid_iv = pd.to_numeric(chain_df.get("iv"), errors="coerce").dropna()
    atm_iv = valid_iv.mean() if not valid_iv.empty else None

    available_expiries = get_available_expiries_for_date(
        datetime.strptime(date_str, "%Y%m%d").date(),
        dataset=dataset,
        max_months=2,
    )

    lot_size = int(LOT_SIZE_BY_INSTRUMENT.get(dataset, 65))
    analytics = _build_gex_analytics(chain_rows, float(spot), lot_size)

    return {
        "ok": True,
        "dataset": dataset,
        "underlying": dataset,
        "query_date": query_date,
        "date_str": date_str,
        "query_time": pd.Timestamp(target_ts).strftime("%H:%M"),
        "resolved_week_number": resolved_week,
        "expiry": expiry_str,
        "expiry_label": _fmt_expiry_label(expiry_str),
        "dte": int(dte),
        "available_expiries": available_expiries,
        "spot": round(float(spot), 2),
        "india_vix": (
            round(float(india_vix_value), 2)
            if india_vix_value is not None
            else None
        ),
        "day_open": round(float(day_open), 2),
        "atm": int(atm),
        "strike_step": int(strike_step),
        "lot_size": lot_size,
        "gex_rows": analytics["gex_rows"],
        "levels": analytics["levels"],
        "interval": candle_interval_minutes,
        "max_allowed_query_date": MAX_ALLOWED_QUERY_DATE.strftime("%Y-%m-%d"),
        "window_candles_before": WINDOW_CANDLES_BEFORE,
        "window_candles_after": WINDOW_CANDLES_AFTER,
        "option_window_cache_size": len(OPTION_WINDOW_CACHE),
        "iv_enabled": ENABLE_IV_CALC,
        "atm_iv": round(float(atm_iv), 4) if atm_iv is not None and np.isfinite(atm_iv) else None,
        "chain_source": chain_source,
        "rows": chain_rows,
    }



def _oi_at_time(raw_df: pd.DataFrame, target_ts: pd.Timestamp, interval_minutes: int) -> tuple[Optional[float], Optional[float]]:
    """Return current OI and interval-over-interval OI change."""
    if raw_df is None or raw_df.empty or "oi" not in raw_df.columns:
        return None, None
    df = raw_df[["datetime", "oi"]].copy()
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df["oi"] = pd.to_numeric(df["oi"], errors="coerce")
    df = df.dropna(subset=["datetime", "oi"]).sort_values("datetime")
    if df.empty:
        return None, None
    if getattr(df["datetime"].dt, "tz", None) is None:
        df["datetime"] = df["datetime"].dt.tz_localize(IST)
    else:
        df["datetime"] = df["datetime"].dt.tz_convert(IST)
    target_ts = _to_ist_timestamp(target_ts)
    current = df[df["datetime"] <= target_ts]
    if current.empty:
        return None, None
    current_oi = _safe_float(current.iloc[-1]["oi"])
    previous_cutoff = target_ts - timedelta(minutes=max(1, int(interval_minutes)))
    previous = df[df["datetime"] <= previous_cutoff]
    previous_oi = _safe_float(previous.iloc[-1]["oi"]) if not previous.empty else current_oi
    change = None if current_oi is None or previous_oi is None else current_oi - previous_oi
    return current_oi, change


def _build_gex_analytics(rows: List[Dict[str, Any]], spot: float, lot_size: int) -> Dict[str, Any]:
    """Build strike-wise gamma exposure and practical support/resistance levels."""
    gex_rows = []
    for row in rows:
        strike = _safe_int(row.get("strike"))
        ce_gamma = _safe_float(row.get("ce_gamma"), 0.0) or 0.0
        pe_gamma = _safe_float(row.get("pe_gamma"), 0.0) or 0.0
        ce_oi = _safe_float(row.get("ce_oi"), 0.0) or 0.0
        pe_oi = _safe_float(row.get("pe_oi"), 0.0) or 0.0
        ce_gex = ce_gamma * ce_oi * lot_size * (spot ** 2) * 0.01
        pe_gex = -pe_gamma * pe_oi * lot_size * (spot ** 2) * 0.01
        net = ce_gex + pe_gex
        row["ce_gex"] = round(ce_gex, 2)
        row["pe_gex"] = round(pe_gex, 2)
        row["net_gex"] = round(net, 2)
        gex_rows.append({"strike": strike, "ce_gex": round(ce_gex, 2), "pe_gex": round(pe_gex, 2), "net_gex": round(net, 2)})

    above = sorted([r for r in rows if (_safe_int(r.get("strike")) or 0) > spot], key=lambda r: (_safe_float(r.get("ce_oi"), 0.0) or 0.0), reverse=True)
    below = sorted([r for r in rows if (_safe_int(r.get("strike")) or 0) < spot], key=lambda r: (_safe_float(r.get("pe_oi"), 0.0) or 0.0), reverse=True)
    r1 = _safe_int(above[0].get("strike")) if above else None
    r2 = _safe_int(above[1].get("strike")) if len(above) > 1 else None
    s1 = _safe_int(below[0].get("strike")) if below else None
    s2 = _safe_int(below[1].get("strike")) if len(below) > 1 else None

    max_pos = max(gex_rows, key=lambda x: x["net_gex"], default=None)
    max_neg = min(gex_rows, key=lambda x: x["net_gex"], default=None)
    gamma_flip = None
    ordered = sorted(gex_rows, key=lambda x: x["strike"] or 0)
    for a, b in zip(ordered, ordered[1:]):
        if a["net_gex"] == 0:
            gamma_flip = a["strike"]
            break
        if a["net_gex"] * b["net_gex"] < 0:
            gamma_flip = a["strike"] if abs(a["net_gex"]) <= abs(b["net_gex"]) else b["strike"]
            break

    total_gex = sum(x["net_gex"] for x in gex_rows)
    return {
        "gex_rows": gex_rows,
        "levels": {
            "r1": r1, "r2": r2, "s1": s1, "s2": s2,
            "gamma_flip": gamma_flip,
            "zero_gamma": gamma_flip,
            "max_positive_gex": max_pos["strike"] if max_pos else None,
            "max_negative_gex": max_neg["strike"] if max_neg else None,
            "call_wall": r1, "put_wall": s1,
            "total_gex": round(total_gex, 2),
        },
    }

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
        )

        return jsonify(result)

    except Exception as exc:
        return _json_error(str(exc), 400)


# =========================================================
# ASYNC IV SURFACE ROUTES
# =========================================================

# from iv_surface_async import register_iv_surface_routes
#
# register_iv_surface_routes(
#     app=app,
#     redis_client=redis_client,
#     get_kafka_producer=get_kafka_producer,
#     build_option_chain_snapshot=build_option_chain_snapshot,
#     get_available_expiries_for_date=get_available_expiries_for_date,
#     normalize_dataset=_normalize_dataset,
#     normalize_date=_normalize_date,
#     normalize_time=_normalize_time,
#     resolve_default_week_date=_resolve_default_week_date,
#     safe_float=_safe_float,
#     json_error=_json_error,
#     default_interval=CANDLE_INTERVAL_MINUTES,
# )
@app.route("/api/iv-surface")
def api_iv_surface():
    try:
        from iv_surface_async import build_iv_surface_payload

        dataset = _normalize_dataset(request.args.get("dataset") or request.args.get("underlying"))
        query_date = request.args.get("date") or request.args.get("query_date")
        query_date = _normalize_date(query_date) if query_date else None
        query_time = _normalize_time(request.args.get("time") or request.args.get("query_time"))

        interval = int(request.args.get("interval", CANDLE_INTERVAL_MINUTES))
        strike_count = int(request.args.get("strike_count", 10))
        max_months = int(request.args.get("max_months", 2))

        result = build_iv_surface_payload(
            dataset=dataset,
            query_date=query_date,
            query_time=query_time,
            interval=interval,
            strike_count=strike_count,
            max_months=max_months,
            build_option_chain_snapshot=build_option_chain_snapshot,
            get_available_expiries_for_date=get_available_expiries_for_date,
            safe_float=_safe_float,
        )

        return jsonify(result)

    except Exception as exc:
        return _json_error(str(exc), 400)
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

        start_time = _normalize_time(
            request.args.get("start_time") or "09:15"
        )

        end_date = _normalize_date(
            request.args.get("end_date")
            or start_date
        )

        end_time = _normalize_time(
            request.args.get("end_time")
            or request.args.get("time")
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
                "start_time": start_time,
                "end_date": end_date,
                "end_time": end_time,
                "date_str": None,
                "resolved_week_number": resolved_week,
                "interval": interval,
                "rows": [],
            })

        candles = pd.concat(all_candles).sort_index()

        start_dt = IST.localize(
            datetime.strptime(f"{start_date} {start_time}", "%Y-%m-%d %H:%M")
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
            "start_time": start_time,
            "end_date": end_date,
            "end_time": end_time,
            "date_str": last_date_str,
            "resolved_week_number": resolved_week,
            "interval": interval,
            "rows": rows,
        })

    except Exception as exc:
        return _json_error(str(exc), 400)


@app.route("/api/option-metric-chart")
def api_option_metric_chart():
    try:
        dataset = _normalize_dataset(
            request.args.get("dataset")
            or request.args.get("underlying")
        )

        query_date = request.args.get("date") or request.args.get("query_date")
        query_date = _normalize_date(query_date) if query_date else None

        start_time = _normalize_time(
            request.args.get("start_time") or "09:15"
        )

        strike = int(request.args.get("strike"))
        metric = str(request.args.get("metric")).lower()
        interval = int(
            request.args.get(
                "interval",
                CANDLE_INTERVAL_MINUTES,
            )
        )
        selected_expiry = request.args.get("expiry")

        allowed = {
            "ce_ltp",
            "pe_ltp",
            "ce_iv",
            "pe_iv",
            "ce_delta",
            "pe_delta",
        }

        if metric not in allowed:
            raise ValueError(f"Unsupported metric: {metric}")

        if interval <= 0:
            raise ValueError("Interval must be greater than zero")

        if query_date:
            resolved_week, folder, date_str = _resolve_week_folder_for_date(
                query_date=query_date,
                dataset=dataset,
                week_number=None,
            )
        else:
            resolved_week, folder, date_str, query_date = (
                _resolve_default_week_date(dataset)
            )

        end_date = _normalize_date(
            request.args.get("end_date") or query_date
        )

        end_time = _normalize_time(
            request.args.get("end_time")
            or request.args.get("time")
            or "15:30"
        )

        start_dt = datetime.strptime(
            f"{query_date} {start_time}",
            "%Y-%m-%d %H:%M",
        )

        end_dt = datetime.strptime(
            f"{end_date} {end_time}",
            "%Y-%m-%d %H:%M",
        )

        if end_dt < start_dt:
            raise ValueError(
                "End date/time must be after start date/time"
            )

        expiry_str = selected_expiry or get_upcoming_expiry_np(
            datetime.strptime(date_str, "%Y%m%d").date(),
            instrument=dataset,
        )

        opt = load_required_option_data_for_date(
            folder=folder,
            date_str=date_str,
            expiry_str=expiry_str,
            strike=strike,
            instrument=dataset,
        )

        if not isinstance(opt, dict):
            raise ValueError(
                f"Option data not found for strike {strike}"
            )

        ce_raw = opt.get("CE", pd.DataFrame())
        pe_raw = opt.get("PE", pd.DataFrame())

        # Indian market session is approximately 375 minutes.
        # Build enough candles to include the complete trading session,
        # regardless of the selected interval.
        full_day_candles = math.ceil(375 / interval) + 5

        session_end_ts = IST.localize(
            datetime.strptime(
                date_str + " 15:30:00",
                "%Y%m%d %H:%M:%S",
            )
        )

        ce = _build_option_candle_window(
            raw_df=ce_raw,
            target_ts=session_end_ts,
            candle_interval_minutes=interval,
            before=full_day_candles,
            after=0,
        )

        pe = _build_option_candle_window(
            raw_df=pe_raw,
            target_ts=session_end_ts,
            candle_interval_minutes=interval,
            before=full_day_candles,
            after=0,
        )

        spot_candles = _get_spot_candles_for_day(
            folder=folder,
            date_str=date_str,
            dataset=dataset,
            candle_interval_minutes=interval,
        )

        if spot_candles is None or spot_candles.empty:
            return jsonify({
                "ok": True,
                "dataset": dataset,
                "query_date": query_date,
                "start_time": start_time,
                "end_date": end_date,
                "end_time": end_time,
                "date_str": date_str,
                "resolved_week_number": resolved_week,
                "strike": strike,
                "expiry": expiry_str,
                "metric": metric,
                "rows": [],
                "message": "No spot candles found",
            })

        spot_candles = spot_candles.copy()

        if getattr(spot_candles.index, "tz", None) is None:
            spot_index_ist = spot_candles.index.tz_localize(IST)
        else:
            spot_index_ist = spot_candles.index.tz_convert(IST)

        spot_candles.index = spot_index_ist

        ce_close = pd.Series(
            index=spot_index_ist,
            dtype="float64",
        )

        pe_close = pd.Series(
            index=spot_index_ist,
            dtype="float64",
        )

        if ce is not None and not ce.empty:
            ce_tmp = ce.copy()

            if "timestamp" not in ce_tmp.columns:
                ce_tmp["timestamp"] = ce_tmp.index

            ce_tmp["timestamp"] = pd.to_datetime(
                ce_tmp["timestamp"],
                errors="coerce",
            )

            ce_tmp["close"] = pd.to_numeric(
                ce_tmp["close"],
                errors="coerce",
            )

            ce_tmp = ce_tmp.dropna(
                subset=["timestamp", "close"]
            )

            if not ce_tmp.empty:
                ce_idx = ce_tmp.set_index("timestamp")
                ce_idx = ce_idx[~ce_idx.index.duplicated(
                    keep="last"
                )]
                ce_idx = ce_idx.sort_index()

                if getattr(ce_idx.index, "tz", None) is None:
                    ce_idx.index = ce_idx.index.tz_localize(IST)
                else:
                    ce_idx.index = ce_idx.index.tz_convert(IST)

                ce_close = ce_idx["close"].reindex(
                    spot_index_ist,
                    method="ffill",
                )

        if pe is not None and not pe.empty:
            pe_tmp = pe.copy()

            if "timestamp" not in pe_tmp.columns:
                pe_tmp["timestamp"] = pe_tmp.index

            pe_tmp["timestamp"] = pd.to_datetime(
                pe_tmp["timestamp"],
                errors="coerce",
            )

            pe_tmp["close"] = pd.to_numeric(
                pe_tmp["close"],
                errors="coerce",
            )

            pe_tmp = pe_tmp.dropna(
                subset=["timestamp", "close"]
            )

            if not pe_tmp.empty:
                pe_idx = pe_tmp.set_index("timestamp")
                pe_idx = pe_idx[~pe_idx.index.duplicated(
                    keep="last"
                )]
                pe_idx = pe_idx.sort_index()

                if getattr(pe_idx.index, "tz", None) is None:
                    pe_idx.index = pe_idx.index.tz_localize(IST)
                else:
                    pe_idx.index = pe_idx.index.tz_convert(IST)

                pe_close = pe_idx["close"].reindex(
                    spot_index_ist,
                    method="ffill",
                )

        rows_df = pd.DataFrame({
            "timestamp": spot_index_ist.tz_localize(None),
            "trade_date": datetime.strptime(
                date_str,
                "%Y%m%d",
            ).date(),
            "instrument": dataset,
            "expiry": expiry_str,
            "nearest_strike": strike,
            "strike": strike,
            "close": pd.to_numeric(
                spot_candles["close"],
                errors="coerce",
            ).to_numpy(),
            "ce": ce_close.to_numpy(),
            "pe": pe_close.to_numpy(),
        })

        rows_df = rows_df.dropna(
            subset=["timestamp", "close"]
        )

        rows_df = rows_df[
            (rows_df["timestamp"] >= start_dt)
            & (rows_df["timestamp"] <= end_dt)
        ].copy()

        if rows_df.empty:
            return jsonify({
                "ok": True,
                "dataset": dataset,
                "query_date": query_date,
                "start_time": start_time,
                "end_date": end_date,
                "end_time": end_time,
                "date_str": date_str,
                "resolved_week_number": resolved_week,
                "strike": strike,
                "expiry": expiry_str,
                "metric": metric,
                "rows": [],
                "message": "No rows found in the requested time range",
            })

        if metric in {
            "ce_iv",
            "pe_iv",
            "ce_delta",
            "pe_delta",
        }:
            rows_df = append_black_scholes_iv(
                rows_df,
                compute_greeks=True,
            )

        col_map = {
            "ce_ltp": "ce",
            "pe_ltp": "pe",
            "ce_iv": "ce_iv",
            "pe_iv": "pe_iv",
            "ce_delta": "ce_delta",
            "pe_delta": "pe_delta",
        }

        col = col_map[metric]

        if col not in rows_df.columns:
            raise ValueError(
                f"Metric column {col} was not generated"
            )

        rows_df[col] = pd.to_numeric(
            rows_df[col],
            errors="coerce",
        )

        valid_rows = rows_df.dropna(
            subset=[col]
        ).copy()

        rows = []

        for _, row in valid_rows.iterrows():
            ts = pd.Timestamp(row["timestamp"])

            if ts.tzinfo is not None:
                ts = ts.tz_convert(IST).tz_localize(None)

            rows.append({
                "time": int(ts.timestamp()),
                "value": round(float(row[col]), 4),
            })

        return jsonify({
            "ok": True,
            "dataset": dataset,
            "query_date": query_date,
            "start_time": start_time,
            "end_date": end_date,
            "end_time": end_time,
            "date_str": date_str,
            "resolved_week_number": resolved_week,
            "strike": strike,
            "expiry": expiry_str,
            "interval": interval,
            "metric": metric,
            "source_row_count": int(len(rows_df)),
            "valid_metric_row_count": int(len(valid_rows)),
            "ce_valid_count": int(rows_df["ce"].notna().sum()),
            "pe_valid_count": int(rows_df["pe"].notna().sum()),
            "rows": rows,
        })

    except Exception as exc:
        app.logger.exception(
            "Option metric chart failed"
        )
        return _json_error(str(exc), 400)

@app.route("/api/option-ltp-candle-chart")
def api_option_ltp_candle_chart():
    try:
        dataset = _normalize_dataset(request.args.get("dataset") or request.args.get("underlying"))
        query_date = request.args.get("date") or request.args.get("query_date")
        query_date = _normalize_date(query_date) if query_date else None

        start_time = _normalize_time(request.args.get("start_time") or "09:15")

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

        opt = load_required_option_data_for_date(
            folder=folder,
            date_str=date_str,
            expiry_str=expiry_str,
            strike=strike,
            instrument=dataset,
        )

        raw_df = opt.get(side)

        if raw_df is None or raw_df.empty:
            return jsonify({
                "ok": True,
                "dataset": dataset,
                "query_date": query_date,
                "start_time": start_time,
                "date_str": date_str,
                "strike": strike,
                "side": side,
                "rows": [],
            })

        raw_df = raw_df.copy()
        raw_df["value"] = raw_df["price"]

        end_date = _normalize_date(request.args.get("end_date") or query_date)
        end_time = _normalize_time(
            request.args.get("end_time")
            or request.args.get("time")
            or "15:30"
        )

        start_dt = IST.localize(
            datetime.strptime(f"{query_date} {start_time}", "%Y-%m-%d %H:%M")
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
            "start_time": start_time,
            "end_date": end_date,
            "end_time": end_time,
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

        start_time = _normalize_time(request.args.get("start_time") or "09:15")

        end_date = _normalize_date(
            request.args.get("end_date") or query_date
        )

        end_time = _normalize_time(
            request.args.get("end_time")
            or request.args.get("time")
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

        from data_engine_for_simulation import load_index_data_by_symbol

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
                "start_time": start_time,
                "end_date": end_date,
                "end_time": end_time,
                "date_str": date_str,
                "resolved_week_number": resolved_week,
                "interval": interval,
                "rows": [],
            })

        candles = create_candles(vix_df, interval)

        start_dt = IST.localize(
            datetime.strptime(f"{query_date} {start_time}", "%Y-%m-%d %H:%M")
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
                "open": round(float(row["open"],), 2),
                "high": round(float(row["high"]), 2),
                "low": round(float(row["low"]), 2),
                "close": round(float(row["close"]), 2),
            })

        return jsonify({
            "ok": True,
            "dataset": dataset,
            "query_date": query_date,
            "start_time": start_time,
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

        start_time = _normalize_time(
            request.args.get("start_time") or "09:15"
        )

        end_date = _normalize_date(
            request.args.get("end_date")
            or start_date
        )

        end_time = _normalize_time(
            request.args.get("end_time")
            or request.args.get("time")
            or "15:30"
        )

        interval = int(request.args.get("interval", CANDLE_INTERVAL_MINUTES))
        month = str(request.args.get("month") or "current").lower()

        if interval <= 0:
            raise ValueError("interval must be greater than zero.")

        from data_engine_for_simulation import load_future_data_for_date
        from chart import build_chart_payload

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

            except Exception as exc:
                print(
                    "Future chart load error:",
                    current_query_date,
                    exc,
                    flush=True,
                )

            current_day += timedelta(days=1)

        if not all_futures:
            return jsonify({
                "ok": True,
                "dataset": dataset,
                "query_date": start_date,
                "start_time": start_time,
                "end_date": end_date,
                "end_time": end_time,
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
            datetime.strptime(f"{start_date} {start_time}", "%Y-%m-%d %H:%M")
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
            "start_time": start_time,
            "end_date": end_date,
            "end_time": end_time,
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
            # "redis_url": REDIS_URL,
            # "kafka_bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
        }
    )


@app.route("/api/cache/clear", methods=["POST"])
def api_cache_clear():
    OPTION_WINDOW_CACHE.clear()
    SPOT_CANDLE_CACHE.clear()
    return jsonify({"ok": True, "message": "RAM caches cleared."})


@app.route("/api/health")
def api_health():
    return jsonify(
        {
            "ok": True,
            "service": "options-simulator",
            "iv_enabled": ENABLE_IV_CALC,
            "data_mode": "parquet-folder",
            # "redis_url": REDIS_URL,
            # "kafka_bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
        }
    )

def _get_fut_folder(week_folder, date_str):
    folder = os.path.join(week_folder, f"NSE_FUT_TICK_{date_str}")
    return folder if os.path.isdir(folder) else week_folder



if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    host = os.getenv("HOST", "0.0.0.0")
    debug = os.getenv("DEBUG_MODE", "true").lower() in {"1", "true", "yes"}

    app.run(
        host=host,
        port=port,
        debug=debug,
    )

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), debug=True)
