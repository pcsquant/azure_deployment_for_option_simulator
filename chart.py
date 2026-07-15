# chart.py replacement generated for the simulator.
# Copy this file content into your project chart.py.

from __future__ import annotations

from typing import Iterable, Optional, Union, Dict, Any
import pandas as pd

INTERVAL_MAP = {
    "1": "1min", "1m": "1min", "1min": "1min",
    "2": "2min", "2m": "2min", "2min": "2min",
    "3": "3min", "3m": "3min", "3min": "3min",
    "5": "5min", "5m": "5min", "5min": "5min",
    "10": "10min", "10m": "10min", "10min": "10min",
    "15": "15min", "15m": "15min", "15min": "15min",
    "30": "30min", "30m": "30min", "30min": "30min",
    "60": "60min", "1h": "60min", "1hr": "60min", "60min": "60min",
}


def normalize_interval(interval: Union[str, int, None] = "1m") -> str:
    key = str(interval or "1m").strip().lower()
    return INTERVAL_MAP.get(key, key if key.endswith("min") else "1min")


def ensure_datetime_index(df: pd.DataFrame, datetime_col: str = "datetime") -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    out = df.copy()

    if not isinstance(out.index, pd.DatetimeIndex):
        if datetime_col not in out.columns:
            raise ValueError(f"DataFrame must have DatetimeIndex or {datetime_col} column.")
        out[datetime_col] = pd.to_datetime(out[datetime_col], errors="coerce")
        out = out.dropna(subset=[datetime_col])
        out = out.sort_values(datetime_col).set_index(datetime_col)

    return out.sort_index()


def resample_candles(
    df: pd.DataFrame,
    interval: Union[str, int] = "1m",
    price_col: Optional[str] = None,
) -> pd.DataFrame:
    out = ensure_datetime_index(df)
    if out.empty:
        return out

    rule = normalize_interval(interval)

    if {"open", "high", "low", "close"}.issubset(out.columns):
        candles = out.resample(rule).agg({
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
        })
    else:
        if price_col is None:
            price_col = "value" if "value" in out.columns else "price"
        if price_col not in out.columns:
            raise ValueError("Tick data must contain value or price column.")
        candles = out[price_col].resample(rule).ohlc()
        candles.columns = ["open", "high", "low", "close"]

    return candles.dropna(subset=["open", "high", "low", "close"])


def filter_window(
    candles: pd.DataFrame,
    start: Optional[Union[str, pd.Timestamp]] = None,
    end: Optional[Union[str, pd.Timestamp]] = None,
    previous_candles: int = 0,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if candles is None or candles.empty:
        return pd.DataFrame(), pd.DataFrame()

    df = ensure_datetime_index(candles)
    start_ts = pd.to_datetime(start) if start else df.index.min()
    end_ts = pd.to_datetime(end) if end else df.index.max()

    # Never display future blank candles beyond available data.
    end_ts = min(end_ts, df.index.max())

    visible = df[(df.index >= start_ts) & (df.index <= end_ts)].copy()

    if previous_candles and previous_candles > 0:
        before = df[df.index < start_ts].tail(int(previous_candles))
        full_window = pd.concat([before, visible])
    else:
        full_window = visible.copy()

    return full_window, visible


def add_indicators(
    full_window: pd.DataFrame,
    sma: Iterable[int] = (20, 50),
    ema: Iterable[int] = (9, 20),
) -> pd.DataFrame:
    out = full_window.copy()

    for p in sma or []:
        out[f"sma_{int(p)}"] = out["close"].rolling(int(p)).mean()

    for p in ema or []:
        out[f"ema_{int(p)}"] = out["close"].ewm(span=int(p), adjust=False).mean()

    return out


def _to_epoch_seconds(ts: pd.Timestamp) -> int:
    ts = pd.Timestamp(ts)

    # Lightweight Charts displays epoch seconds in UTC.
    # So for Indian market charts, send IST wall-clock time as naive timestamp.
    if ts.tzinfo is not None:
        ts = ts.tz_convert("Asia/Kolkata").tz_localize(None)

    return int(ts.timestamp())


def candles_to_tradingview(candles: pd.DataFrame) -> list[dict]:
    rows = []
    if candles is None or candles.empty:
        return rows

    df = ensure_datetime_index(candles)

    for ts, row in df.iterrows():
        rows.append({
            "time": _to_epoch_seconds(ts),
            "open": round(float(row["open"]), 2),
            "high": round(float(row["high"]), 2),
            "low": round(float(row["low"]), 2),
            "close": round(float(row["close"]), 2),
        })

    return rows


def line_to_tradingview(df: pd.DataFrame, column: str) -> list[dict]:
    rows = []
    if df is None or df.empty or column not in df.columns:
        return rows

    data = ensure_datetime_index(df)

    for ts, value in data[column].dropna().items():
        rows.append({
            "time": _to_epoch_seconds(ts),
            "value": round(float(value), 4),
        })

    return rows


def build_chart_payload(
    data: pd.DataFrame,
    interval: Union[str, int] = "1m",
    start: Optional[Union[str, pd.Timestamp]] = None,
    end: Optional[Union[str, pd.Timestamp]] = None,
    previous_candles: int = 100,
    sma: Iterable[int] = (20, 50),
    ema: Iterable[int] = (9, 20),
    price_col: Optional[str] = None,
) -> Dict[str, Any]:
    candles = resample_candles(data, interval=interval, price_col=price_col)

    full_window, visible = filter_window(
        candles,
        start=start,
        end=end,
        previous_candles=previous_candles,
    )

    indicator_df = add_indicators(full_window, sma=sma, ema=ema)

    if not visible.empty:
        indicator_df = indicator_df.loc[indicator_df.index.intersection(visible.index)]

    indicator_cols = [
        c for c in indicator_df.columns
        if c.startswith("sma_") or c.startswith("ema_")
    ]

    return {
        "interval": normalize_interval(interval),
        "candles": candles_to_tradingview(visible),
        "indicators": {
            col: line_to_tradingview(indicator_df, col)
            for col in indicator_cols
        },
    }


def tradingview_chart_options() -> Dict[str, Any]:
    return {
        "layout": {
            "background": {"type": "solid", "color": "#ffffff"},
            "textColor": "#111827",
        },
        "grid": {
            "vertLines": {"color": "#edf2f7"},
            "horzLines": {"color": "#edf2f7"},
        },
        "rightPriceScale": {
            "borderVisible": True,
            "scaleMargins": {"top": 0.08, "bottom": 0.12},
        },
        "timeScale": {
            "borderVisible": True,
            "timeVisible": True,
            "secondsVisible": False,
            "rightOffset": 5,
            "barSpacing": 8,
        },
        "crosshair": {
            "mode": 1,
            "vertLine": {"visible": True, "labelVisible": True},
            "horzLine": {"visible": True, "labelVisible": True},
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
    }
