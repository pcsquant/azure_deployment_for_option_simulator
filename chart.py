"""Production chart utilities for the Option Simulator.

This module converts tick/OHLC market data into TradingView Lightweight Charts
payloads. It provides:

* strict interval normalization
* robust datetime and timezone handling
* market-session filtering
* session-aligned candle resampling
* bounded indicator lookback
* JSON-safe payload conversion
* reusable chart options

The public API remains compatible with the previous module:

    normalize_interval
    ensure_datetime_index
    resample_candles
    filter_window
    add_indicators
    candles_to_tradingview
    line_to_tradingview
    build_chart_payload
    tradingview_chart_options
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from datetime import time
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence, Union

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

Interval = Union[str, int, None]
TimestampLike = Optional[Union[str, pd.Timestamp]]

DEFAULT_TIMEZONE = "Asia/Kolkata"
DEFAULT_SESSION_START = "09:15"
DEFAULT_SESSION_END = "15:30"
MAX_PREVIOUS_CANDLES = 5_000
MAX_INDICATOR_PERIOD = 5_000

INTERVAL_MAP: Mapping[str, str] = {
    "1": "1min",
    "1m": "1min",
    "1min": "1min",
    "2": "2min",
    "2m": "2min",
    "2min": "2min",
    "3": "3min",
    "3m": "3min",
    "3min": "3min",
    "5": "5min",
    "5m": "5min",
    "5min": "5min",
    "10": "10min",
    "10m": "10min",
    "10min": "10min",
    "15": "15min",
    "15m": "15min",
    "15min": "15min",
    "30": "30min",
    "30m": "30min",
    "30min": "30min",
    "60": "60min",
    "60m": "60min",
    "60min": "60min",
    "1h": "60min",
    "1hr": "60min",
}

SUPPORTED_RULES = frozenset(INTERVAL_MAP.values())
OHLC_COLUMNS = ("open", "high", "low", "close")


@dataclass(frozen=True)
class ChartConfig:
    """Configuration used by :func:`build_chart_payload`."""

    timezone: str = DEFAULT_TIMEZONE
    session_start: str = DEFAULT_SESSION_START
    session_end: str = DEFAULT_SESSION_END
    align_to_session: bool = True
    drop_incomplete_last_candle: bool = False
    round_prices: int = 2
    round_indicators: int = 4


def normalize_interval(interval: Interval = "1m") -> str:
    """Return a validated pandas resampling rule.

    Unknown values raise ``ValueError`` instead of silently falling back to one
    minute. Silent fallback can make a production chart display the wrong
    timeframe without any visible error.
    """

    key = str(interval or "1m").strip().lower()
    normalized = INTERVAL_MAP.get(key)

    if normalized is None:
        raise ValueError(
            f"Unsupported interval {interval!r}. "
            f"Supported values: {', '.join(sorted(INTERVAL_MAP))}."
        )

    return normalized


def _validate_timezone(timezone: str) -> str:
    try:
        pd.Timestamp.now(tz=timezone)
    except Exception as exc:
        raise ValueError(f"Invalid timezone: {timezone!r}") from exc
    return timezone


def _parse_clock(value: str, field_name: str) -> time:
    try:
        return pd.Timestamp(value).time()
    except Exception as exc:
        raise ValueError(
            f"{field_name} must be a valid clock time such as '09:15'."
        ) from exc


def _normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Return a shallow frame with normalized lower-case column names."""

    renamed = {column: str(column).strip().lower() for column in df.columns}
    if all(original == new for original, new in renamed.items()):
        return df
    return df.rename(columns=renamed, copy=False)


def ensure_datetime_index(
    df: pd.DataFrame,
    datetime_col: str = "datetime",
    *,
    timezone: str = DEFAULT_TIMEZONE,
    assume_timezone: Optional[str] = None,
    copy: bool = False,
    remove_duplicate_timestamps: bool = False,
) -> pd.DataFrame:
    """Return a sorted DataFrame with a timezone-aware ``DatetimeIndex``.

    Parameters
    ----------
    df:
        Source DataFrame.
    datetime_col:
        Datetime column used when the source does not already have a
        ``DatetimeIndex``.
    timezone:
        Target timezone used internally by chart processing.
    assume_timezone:
        Timezone assigned to naive input timestamps. Defaults to ``timezone``.
    copy:
        Create a deep DataFrame copy. Production callers should normally leave
        this ``False`` and treat returned source data as read-only.
    remove_duplicate_timestamps:
        Keep the last row for duplicate timestamps. This is normally disabled
        for tick data because multiple ticks can share a timestamp.
    """

    if df is None or df.empty:
        return pd.DataFrame()

    if not isinstance(df, pd.DataFrame):
        raise TypeError("df must be a pandas DataFrame.")

    timezone = _validate_timezone(timezone)
    assume_timezone = _validate_timezone(assume_timezone or timezone)

    out = df.copy(deep=True) if copy else df.copy(deep=False)
    out = _normalize_column_names(out)
    datetime_col = datetime_col.strip().lower()

    if isinstance(out.index, pd.DatetimeIndex):
        index = pd.DatetimeIndex(out.index)
    else:
        if datetime_col not in out.columns:
            raise ValueError(
                f"DataFrame must have a DatetimeIndex or a "
                f"{datetime_col!r} column."
            )

        index = pd.DatetimeIndex(
            pd.to_datetime(out[datetime_col], errors="coerce")
        )
        valid_mask = ~index.isna()
        if not valid_mask.all():
            out = out.loc[valid_mask].copy(deep=False)
            index = index[valid_mask]

        out = out.drop(columns=[datetime_col], errors="ignore")

    if index.tz is None:
        try:
            index = index.tz_localize(
                assume_timezone,
                ambiguous="infer",
                nonexistent="shift_forward",
            )
        except Exception:
            index = index.tz_localize(
                assume_timezone,
                ambiguous="NaT",
                nonexistent="shift_forward",
            )
            valid_mask = ~index.isna()
            out = out.loc[valid_mask].copy(deep=False)
            index = index[valid_mask]
    else:
        index = index.tz_convert(timezone)

    if str(index.tz) != timezone:
        index = index.tz_convert(timezone)

    out.index = index
    out.index.name = "datetime"
    out = out.sort_index(kind="mergesort")

    if remove_duplicate_timestamps and out.index.has_duplicates:
        out = out.loc[~out.index.duplicated(keep="last")]

    return out


def _coerce_numeric_columns(
    df: pd.DataFrame,
    columns: Sequence[str],
) -> pd.DataFrame:
    out = df.copy(deep=False)
    for column in columns:
        if column in out.columns and not pd.api.types.is_numeric_dtype(out[column]):
            out[column] = pd.to_numeric(out[column], errors="coerce")
    return out


def _session_offset(session_start: str) -> pd.Timedelta:
    parsed = _parse_clock(session_start, "session_start")
    return pd.Timedelta(hours=parsed.hour, minutes=parsed.minute, seconds=parsed.second)


def _filter_market_session(
    df: pd.DataFrame,
    session_start: Optional[str],
    session_end: Optional[str],
) -> pd.DataFrame:
    if df.empty or not session_start or not session_end:
        return df

    start_clock = _parse_clock(session_start, "session_start")
    end_clock = _parse_clock(session_end, "session_end")
    if start_clock > end_clock:
        raise ValueError("session_start must not be after session_end.")

    clock = df.index.time
    mask = (clock >= start_clock) & (clock <= end_clock)
    return df.loc[mask]


def resample_candles(
    df: pd.DataFrame,
    interval: Union[str, int] = "1m",
    price_col: Optional[str] = None,
    *,
    datetime_col: str = "datetime",
    timezone: str = DEFAULT_TIMEZONE,
    session_start: Optional[str] = DEFAULT_SESSION_START,
    session_end: Optional[str] = DEFAULT_SESSION_END,
    align_to_session: bool = True,
    drop_incomplete_last_candle: bool = False,
    now: TimestampLike = None,
) -> pd.DataFrame:
    """Convert tick or OHLC input into validated OHLC candles.

    Resampling is aligned to the market session start. For example, 60-minute
    candles with a 09:15 session start become 09:15, 10:15, ... instead of the
    default pandas alignment at 09:00, 10:00, ...
    """

    out = ensure_datetime_index(
        df,
        datetime_col=datetime_col,
        timezone=timezone,
        assume_timezone=timezone,
        copy=False,
    )
    if out.empty:
        return pd.DataFrame(columns=list(OHLC_COLUMNS))

    out = _filter_market_session(out, session_start, session_end)
    if out.empty:
        return pd.DataFrame(columns=list(OHLC_COLUMNS))

    rule = normalize_interval(interval)
    has_ohlc = set(OHLC_COLUMNS).issubset(out.columns)

    resample_kwargs: Dict[str, Any] = {
        "rule": rule,
        "label": "left",
        "closed": "left",
    }
    if align_to_session and session_start:
        resample_kwargs.update(
            origin="start_day",
            offset=_session_offset(session_start),
        )

    if has_ohlc:
        source = _coerce_numeric_columns(out, OHLC_COLUMNS)
        candles = source.resample(**resample_kwargs).agg(
            open=("open", "first"),
            high=("high", "max"),
            low=("low", "min"),
            close=("close", "last"),
        )

        # Preserve optional volume/open-interest columns when available.
        if "volume" in source.columns:
            candles["volume"] = source["volume"].resample(**resample_kwargs).sum(min_count=1)
        if "oi" in source.columns:
            candles["oi"] = source["oi"].resample(**resample_kwargs).last()
    else:
        candidates = [price_col] if price_col else ["value", "price", "ltp", "close"]
        selected_price_col = next(
            (candidate for candidate in candidates if candidate and candidate in out.columns),
            None,
        )
        if selected_price_col is None:
            raise ValueError(
                "Tick data must contain one of: value, price, ltp, close, "
                "or provide price_col explicitly."
            )

        price_series = pd.to_numeric(out[selected_price_col], errors="coerce")
        candles = price_series.resample(**resample_kwargs).ohlc()
        candles.columns = list(OHLC_COLUMNS)

        if "volume" in out.columns:
            volume = pd.to_numeric(out["volume"], errors="coerce")
            candles["volume"] = volume.resample(**resample_kwargs).sum(min_count=1)
        elif "qty" in out.columns:
            qty = pd.to_numeric(out["qty"], errors="coerce")
            candles["volume"] = qty.resample(**resample_kwargs).sum(min_count=1)

        if "oi" in out.columns:
            oi = pd.to_numeric(out["oi"], errors="coerce")
            candles["oi"] = oi.resample(**resample_kwargs).last()

    candles = candles.replace([np.inf, -np.inf], np.nan)
    candles = candles.dropna(subset=list(OHLC_COLUMNS))

    # Reject malformed bars caused by corrupt source data.
    valid_ohlc = (
        (candles["high"] >= candles[["open", "close"]].max(axis=1))
        & (candles["low"] <= candles[["open", "close"]].min(axis=1))
        & (candles["high"] >= candles["low"])
    )
    invalid_count = int((~valid_ohlc).sum())
    if invalid_count:
        logger.warning("Dropping %s malformed OHLC candles.", invalid_count)
        candles = candles.loc[valid_ohlc]

    if drop_incomplete_last_candle and not candles.empty:
        current = pd.Timestamp(now) if now is not None else pd.Timestamp.now(tz=timezone)
        if current.tzinfo is None:
            current = current.tz_localize(timezone)
        else:
            current = current.tz_convert(timezone)

        interval_delta = pd.Timedelta(rule)
        last_open = candles.index[-1]
        if current < last_open + interval_delta:
            candles = candles.iloc[:-1]

    return candles


def _coerce_boundary(
    value: TimestampLike,
    *,
    timezone: str,
    fallback: pd.Timestamp,
) -> pd.Timestamp:
    if value is None:
        return fallback

    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize(timezone)
    else:
        timestamp = timestamp.tz_convert(timezone)
    return timestamp


def filter_window(
    candles: pd.DataFrame,
    start: TimestampLike = None,
    end: TimestampLike = None,
    previous_candles: int = 0,
    *,
    timezone: str = DEFAULT_TIMEZONE,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return ``(indicator_window, visible_window)`` without future rows."""

    if candles is None or candles.empty:
        return pd.DataFrame(), pd.DataFrame()

    previous_candles = int(previous_candles or 0)
    if previous_candles < 0:
        raise ValueError("previous_candles cannot be negative.")
    if previous_candles > MAX_PREVIOUS_CANDLES:
        raise ValueError(
            f"previous_candles cannot exceed {MAX_PREVIOUS_CANDLES}."
        )

    df = ensure_datetime_index(
        candles,
        timezone=timezone,
        assume_timezone=timezone,
        copy=False,
    )

    start_ts = _coerce_boundary(
        start,
        timezone=timezone,
        fallback=df.index.min(),
    )
    end_ts = _coerce_boundary(
        end,
        timezone=timezone,
        fallback=df.index.max(),
    )

    if start_ts > end_ts:
        raise ValueError("start must not be after end.")

    # Never expose future blank candles beyond the available source data.
    end_ts = min(end_ts, df.index.max())

    visible = df.loc[(df.index >= start_ts) & (df.index <= end_ts)]

    if previous_candles > 0:
        before = df.loc[df.index < start_ts].tail(previous_candles)
        full_window = pd.concat([before, visible], axis=0, copy=False)
        if full_window.index.has_duplicates:
            full_window = full_window.loc[
                ~full_window.index.duplicated(keep="last")
            ]
    else:
        full_window = visible

    return full_window, visible


def _normalize_periods(periods: Optional[Iterable[int]], name: str) -> tuple[int, ...]:
    if periods is None:
        return ()

    normalized: list[int] = []
    for value in periods:
        try:
            period = int(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{name} periods must be integers.") from exc

        if period <= 0:
            raise ValueError(f"{name} periods must be positive.")
        if period > MAX_INDICATOR_PERIOD:
            raise ValueError(
                f"{name} period {period} exceeds {MAX_INDICATOR_PERIOD}."
            )
        if period not in normalized:
            normalized.append(period)

    return tuple(normalized)


def add_indicators(
    full_window: pd.DataFrame,
    sma: Iterable[int] = (20, 50),
    ema: Iterable[int] = (9, 20),
    *,
    copy: bool = True,
) -> pd.DataFrame:
    """Add SMA and EMA columns to a candle window."""

    if full_window is None or full_window.empty:
        return pd.DataFrame()

    if "close" not in full_window.columns:
        raise ValueError("full_window must contain a close column.")

    sma_periods = _normalize_periods(sma, "SMA")
    ema_periods = _normalize_periods(ema, "EMA")

    out = full_window.copy(deep=True) if copy else full_window.copy(deep=False)
    close = pd.to_numeric(out["close"], errors="coerce")

    for period in sma_periods:
        out[f"sma_{period}"] = close.rolling(
            window=period,
            min_periods=period,
        ).mean()

    for period in ema_periods:
        out[f"ema_{period}"] = close.ewm(
            span=period,
            adjust=False,
            min_periods=period,
        ).mean()

    return out


def _to_epoch_seconds(
    ts: pd.Timestamp,
    *,
    timezone: str = DEFAULT_TIMEZONE,
) -> int:
    """Convert a timestamp to a real UTC Unix epoch second.

    Naive market timestamps are interpreted in ``timezone``. This avoids the
    server-local-time dependency of calling ``timestamp()`` on a naive value.
    """

    timestamp = pd.Timestamp(ts)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize(timezone)
    else:
        timestamp = timestamp.tz_convert(timezone)

    return int(timestamp.tz_convert("UTC").timestamp())


def _finite_float(value: Any, digits: int) -> Optional[float]:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None

    if not math.isfinite(numeric):
        return None
    return round(numeric, digits)


def candles_to_tradingview(
    candles: pd.DataFrame,
    *,
    timezone: str = DEFAULT_TIMEZONE,
    round_digits: int = 2,
) -> list[dict[str, Union[int, float]]]:
    """Convert OHLC candles into Lightweight Charts candlestick data."""

    if candles is None or candles.empty:
        return []

    df = ensure_datetime_index(
        candles,
        timezone=timezone,
        assume_timezone=timezone,
        copy=False,
    )

    missing = [column for column in OHLC_COLUMNS if column not in df.columns]
    if missing:
        raise ValueError(f"Candles are missing columns: {missing}")

    rows: list[dict[str, Union[int, float]]] = []
    for ts, row in df.iterrows():
        values = {
            column: _finite_float(row[column], round_digits)
            for column in OHLC_COLUMNS
        }
        if any(value is None for value in values.values()):
            continue

        payload: dict[str, Union[int, float]] = {
            "time": _to_epoch_seconds(ts, timezone=timezone),
            "open": values["open"],  # type: ignore[dict-item]
            "high": values["high"],  # type: ignore[dict-item]
            "low": values["low"],  # type: ignore[dict-item]
            "close": values["close"],  # type: ignore[dict-item]
        }

        if "volume" in df.columns:
            volume = _finite_float(row.get("volume"), 0)
            if volume is not None:
                payload["volume"] = volume

        rows.append(payload)

    return rows


def line_to_tradingview(
    df: pd.DataFrame,
    column: str,
    *,
    timezone: str = DEFAULT_TIMEZONE,
    round_digits: int = 4,
) -> list[dict[str, Union[int, float]]]:
    """Convert one numeric DataFrame column into line-series data."""

    if df is None or df.empty or column not in df.columns:
        return []

    data = ensure_datetime_index(
        df,
        timezone=timezone,
        assume_timezone=timezone,
        copy=False,
    )

    rows: list[dict[str, Union[int, float]]] = []
    numeric = pd.to_numeric(data[column], errors="coerce").replace(
        [np.inf, -np.inf], np.nan
    )

    for ts, value in numeric.dropna().items():
        clean_value = _finite_float(value, round_digits)
        if clean_value is None:
            continue
        rows.append(
            {
                "time": _to_epoch_seconds(ts, timezone=timezone),
                "value": clean_value,
            }
        )

    return rows


def build_chart_payload(
    data: pd.DataFrame,
    interval: Union[str, int] = "1m",
    start: TimestampLike = None,
    end: TimestampLike = None,
    previous_candles: int = 100,
    sma: Iterable[int] = (20, 50),
    ema: Iterable[int] = (9, 20),
    price_col: Optional[str] = None,
    *,
    datetime_col: str = "datetime",
    timezone: str = DEFAULT_TIMEZONE,
    session_start: Optional[str] = DEFAULT_SESSION_START,
    session_end: Optional[str] = DEFAULT_SESSION_END,
    align_to_session: bool = True,
    drop_incomplete_last_candle: bool = False,
    include_metadata: bool = True,
) -> Dict[str, Any]:
    """Build the full JSON-safe chart payload used by the simulator UI."""

    normalized_interval = normalize_interval(interval)
    candles = resample_candles(
        data,
        interval=normalized_interval,
        price_col=price_col,
        datetime_col=datetime_col,
        timezone=timezone,
        session_start=session_start,
        session_end=session_end,
        align_to_session=align_to_session,
        drop_incomplete_last_candle=drop_incomplete_last_candle,
    )

    full_window, visible = filter_window(
        candles,
        start=start,
        end=end,
        previous_candles=previous_candles,
        timezone=timezone,
    )

    indicator_df = add_indicators(full_window, sma=sma, ema=ema, copy=True)

    if not visible.empty and not indicator_df.empty:
        indicator_df = indicator_df.loc[
            indicator_df.index.intersection(visible.index, sort=False)
        ]

    indicator_columns = [
        column
        for column in indicator_df.columns
        if column.startswith("sma_") or column.startswith("ema_")
    ]

    payload: Dict[str, Any] = {
        "interval": normalized_interval,
        "timezone": timezone,
        "candles": candles_to_tradingview(
            visible,
            timezone=timezone,
            round_digits=2,
        ),
        "indicators": {
            column: line_to_tradingview(
                indicator_df,
                column,
                timezone=timezone,
                round_digits=4,
            )
            for column in indicator_columns
        },
    }

    if include_metadata:
        payload["meta"] = {
            "source_rows": int(len(data)) if data is not None else 0,
            "resampled_candles": int(len(candles)),
            "visible_candles": int(len(visible)),
            "previous_candles_requested": int(previous_candles),
            "start": visible.index.min().isoformat() if not visible.empty else None,
            "end": visible.index.max().isoformat() if not visible.empty else None,
            "session_start": session_start,
            "session_end": session_end,
        }

    return payload


def tradingview_chart_options(
    *,
    dark_mode: bool = False,
    price_precision: int = 2,
    auto_size: bool = True,
) -> Dict[str, Any]:
    """Return production defaults for TradingView Lightweight Charts."""

    if price_precision < 0 or price_precision > 10:
        raise ValueError("price_precision must be between 0 and 10.")

    minimum_move = 10 ** (-price_precision)

    if dark_mode:
        background = "#111827"
        text = "#e5e7eb"
        grid = "#243041"
        border = "#374151"
    else:
        background = "#ffffff"
        text = "#111827"
        grid = "#edf2f7"
        border = "#d1d5db"

    return {
        "autoSize": auto_size,
        "layout": {
            "background": {"type": "solid", "color": background},
            "textColor": text,
            "fontFamily": "Inter, system-ui, -apple-system, sans-serif",
            "fontSize": 12,
        },
        "localization": {
            "locale": "en-IN",
        },
        "grid": {
            "vertLines": {"color": grid},
            "horzLines": {"color": grid},
        },
        "rightPriceScale": {
            "visible": True,
            "borderVisible": True,
            "borderColor": border,
            "autoScale": True,
            "scaleMargins": {"top": 0.08, "bottom": 0.12},
        },
        "leftPriceScale": {
            "visible": False,
        },
        "timeScale": {
            "borderVisible": True,
            "borderColor": border,
            "timeVisible": True,
            "secondsVisible": False,
            "rightOffset": 5,
            "barSpacing": 8,
            "minBarSpacing": 2,
            "fixLeftEdge": False,
            "fixRightEdge": False,
            "lockVisibleTimeRangeOnResize": True,
        },
        "crosshair": {
            "mode": 1,
            "vertLine": {
                "visible": True,
                "labelVisible": True,
            },
            "horzLine": {
                "visible": True,
                "labelVisible": True,
            },
        },
        "handleScale": {
            "axisPressedMouseMove": True,
            "mouseWheel": True,
            "pinch": True,
        },
        "handleScroll": {
            "mouseWheel": True,
            "pressedMouseMove": True,
            "horzTouchDrag": True,
            "vertTouchDrag": True,
        },
        "kineticScroll": {
            "mouse": True,
            "touch": True,
        },
        "priceFormat": {
            "type": "price",
            "precision": price_precision,
            "minMove": minimum_move,
        },
    }


__all__ = [
    "ChartConfig",
    "INTERVAL_MAP",
    "normalize_interval",
    "ensure_datetime_index",
    "resample_candles",
    "filter_window",
    "add_indicators",
    "candles_to_tradingview",
    "line_to_tradingview",
    "build_chart_payload",
    "tradingview_chart_options",
]
