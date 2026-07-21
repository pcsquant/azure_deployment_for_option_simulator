"""Production-grade vectorized Black-Scholes IV and Greeks.

This module is designed for the historical Options Simulator.

Key properties
--------------
* Vectorized implied-volatility solving across the entire option-chain slice.
* Vectorized time-to-expiry calculation; no ``DataFrame.apply(axis=1)``.
* Optional bounded, thread-safe IV cache for repeated historical snapshots.
* Optional Greeks calculation so smile/surface requests can skip extra work.
* ``inplace=True`` support for small request-specific slices.
* Runtime profiling and cache diagnostics.

Important
---------
Treat shared market-data DataFrames as read-only. Use ``inplace=True`` only for
small request-specific DataFrames that are not stored in a shared cache.
"""

from __future__ import annotations

import logging
import math
import os
import time
from collections import OrderedDict
from datetime import datetime
from threading import RLock
from typing import Final, Iterable

import numpy as np
import pandas as pd
from scipy.special import ndtr

logger = logging.getLogger(__name__)


# ============================================================================
# CONFIGURATION
# ============================================================================


def _env_bool(name: str, default: bool = False) -> bool:
    return os.getenv(name, str(default)).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


DEBUG_IV: Final[bool] = _env_bool("DEBUG_IV", False)
PROFILE_IV: Final[bool] = _env_bool("PROFILE_IV", False)
ENABLE_IV_CACHE: Final[bool] = _env_bool("ENABLE_IV_CACHE", True)

RISK_FREE_RATE: Final[float] = float(os.getenv("RISK_FREE_RATE", "0.0"))
DIVIDEND_YIELD: Final[float] = float(os.getenv("DIVIDEND_YIELD", "0.0"))
TRADING_MINUTES_PER_DAY: Final[int] = max(
    1,
    int(os.getenv("TRADING_MINUTES_PER_DAY", "375")),
)
TRADING_DAYS_PER_YEAR: Final[int] = max(
    1,
    int(os.getenv("TRADING_DAYS_PER_YEAR", "252")),
)
MARKET_CLOSE: Final[str] = os.getenv("MARKET_CLOSE", "15:30").strip()

_IV_MIN: Final[float] = max(1e-8, float(os.getenv("IV_MIN", "0.0001")))
_IV_MAX: Final[float] = max(_IV_MIN * 10.0, float(os.getenv("IV_MAX", "5.0")))
_IV_MAX_ITER: Final[int] = max(8, int(os.getenv("IV_MAX_ITER", "48")))
_IV_ATOL: Final[float] = max(0.0, float(os.getenv("IV_ATOL", "1e-6")))
_IV_RTOL: Final[float] = max(0.0, float(os.getenv("IV_RTOL", "1e-7")))
_IV_MIN_TIME_VALUE: Final[float] = max(
    0.0,
    float(os.getenv("IV_MIN_TIME_VALUE", "0.001")),
)

IV_CACHE_MAX_ENTRIES: Final[int] = max(
    1,
    int(os.getenv("IV_CACHE_MAX_ENTRIES", "100000")),
)
IV_CACHE_PRICE_DECIMALS: Final[int] = max(
    0,
    int(os.getenv("IV_CACHE_PRICE_DECIMALS", "2")),
)
IV_CACHE_SPOT_DECIMALS: Final[int] = max(
    0,
    int(os.getenv("IV_CACHE_SPOT_DECIMALS", "2")),
)
IV_CACHE_TTE_DECIMALS: Final[int] = max(
    4,
    int(os.getenv("IV_CACHE_TTE_DECIMALS", "10")),
)

_IV_GREEK_COLUMNS: Final[tuple[str, ...]] = (
    "ce_iv",
    "pe_iv",
    "iv",
    "ce_delta",
    "pe_delta",
    "ce_gamma",
    "pe_gamma",
    "ce_vega",
    "pe_vega",
    "ce_theta",
    "pe_theta",
    "ce_rho",
    "pe_rho",
)

_REQUIRED_COLUMNS: Final[frozenset[str]] = frozenset(
    {"timestamp", "expiry", "nearest_strike", "close", "ce", "pe"}
)

_SQRT_2PI: Final[float] = math.sqrt(2.0 * math.pi)


# ============================================================================
# THREAD-SAFE BOUNDED IV CACHE
# ============================================================================


class _ThreadSafeIVCache:
    """Bounded LRU cache for scalar IV results.

    The vectorized solver still handles all cache misses together. This cache is
    most useful when users repeatedly request the same historical timestamp,
    expiry, strike set, spot, and option prices.
    """

    def __init__(self, max_entries: int) -> None:
        self.max_entries = max(1, int(max_entries))
        self._items: "OrderedDict[tuple, float]" = OrderedDict()
        self._lock = RLock()
        self._hits = 0
        self._misses = 0
        self._evictions = 0

    def get_many(self, keys: list[tuple]) -> tuple[np.ndarray, np.ndarray]:
        values = np.full(len(keys), np.nan, dtype=float)
        found = np.zeros(len(keys), dtype=bool)

        with self._lock:
            for index, key in enumerate(keys):
                if key not in self._items:
                    self._misses += 1
                    continue

                value = self._items.pop(key)
                self._items[key] = value
                values[index] = value
                found[index] = True
                self._hits += 1

        return values, found

    def put_many(self, items: Iterable[tuple[tuple, float]]) -> None:
        with self._lock:
            for key, value in items:
                if not np.isfinite(value):
                    continue

                if key in self._items:
                    self._items.pop(key)
                self._items[key] = float(value)

                while len(self._items) > self.max_entries:
                    self._items.popitem(last=False)
                    self._evictions += 1

    def clear(self) -> None:
        with self._lock:
            self._items.clear()
            self._hits = 0
            self._misses = 0
            self._evictions = 0

    def stats(self) -> dict[str, int | bool]:
        with self._lock:
            requests = self._hits + self._misses
            return {
                "enabled": ENABLE_IV_CACHE,
                "entries": len(self._items),
                "max_entries": self.max_entries,
                "hits": self._hits,
                "misses": self._misses,
                "evictions": self._evictions,
                "requests": requests,
                "hit_rate_percent": round(
                    (self._hits / requests) * 100.0,
                    2,
                )
                if requests
                else 0.0,
            }


_IV_CACHE = _ThreadSafeIVCache(IV_CACHE_MAX_ENTRIES)


def clear_iv_cache() -> None:
    """Clear all cached IV results and reset cache statistics."""
    _IV_CACHE.clear()


def iv_cache_stats() -> dict[str, int | bool]:
    """Return current IV-cache diagnostics."""
    return _IV_CACHE.stats()


# ============================================================================
# TIME TO EXPIRY
# ============================================================================


def _market_close_parts() -> tuple[int, int]:
    try:
        hour_text, minute_text = MARKET_CLOSE.split(":", 1)
        hour = int(hour_text)
        minute = int(minute_text)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"MARKET_CLOSE must use HH:MM format. Received {MARKET_CLOSE!r}."
        ) from exc

    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        raise ValueError(f"Invalid MARKET_CLOSE value: {MARKET_CLOSE!r}")

    return hour, minute


_MARKET_CLOSE_HOUR, _MARKET_CLOSE_MINUTE = _market_close_parts()


def calculate_tte_from_timestamp(expiry_str, timestamp) -> float:
    """Return time to expiry in trading-year units for one row."""
    try:
        expiry_date = datetime.strptime(str(expiry_str), "%y%m%d").date()
    except (TypeError, ValueError):
        return float("nan")

    ts = pd.to_datetime(timestamp, errors="coerce")
    if pd.isna(ts):
        return float("nan")

    if getattr(ts, "tzinfo", None) is not None:
        ts = ts.tz_localize(None)

    market_end = datetime.combine(
        ts.date(),
        datetime.min.time(),
    ).replace(
        hour=_MARKET_CLOSE_HOUR,
        minute=_MARKET_CLOSE_MINUTE,
    )

    minutes_left = max(
        0.0,
        (market_end - ts.to_pydatetime()).total_seconds() / 60.0,
    )
    days_after_today = max(0, (expiry_date - ts.date()).days)
    tte_days = days_after_today + minutes_left / TRADING_MINUTES_PER_DAY

    return max(
        tte_days / TRADING_DAYS_PER_YEAR,
        1.0 / TRADING_DAYS_PER_YEAR,
    )


def _calculate_tte_vectorized(
    expiry: pd.Series,
    timestamp: pd.Series,
) -> np.ndarray:
    """Calculate TTE for an entire option-chain slice."""
    ts = pd.to_datetime(timestamp, errors="coerce")
    if getattr(ts.dt, "tz", None) is not None:
        ts = ts.dt.tz_localize(None)

    expiry_text = expiry.astype("string").str.strip()
    expiry_dt = pd.to_datetime(expiry_text, format="%y%m%d", errors="coerce")

    market_end = ts.dt.normalize() + pd.Timedelta(
        hours=_MARKET_CLOSE_HOUR,
        minutes=_MARKET_CLOSE_MINUTE,
    )
    minutes_left = (
        (market_end - ts)
        .dt.total_seconds()
        .div(60.0)
        .clip(lower=0.0)
    )
    calendar_days = (
        expiry_dt.dt.normalize() - ts.dt.normalize()
    ).dt.days.clip(lower=0)

    tte_days = calendar_days.astype(float) + (
        minutes_left / TRADING_MINUTES_PER_DAY
    )
    tte = (tte_days / TRADING_DAYS_PER_YEAR).clip(
        lower=1.0 / TRADING_DAYS_PER_YEAR
    )

    invalid = ts.isna() | expiry_dt.isna()
    return tte.mask(invalid, np.nan).to_numpy(dtype=float)


# ============================================================================
# BLACK-SCHOLES CORE
# ============================================================================


def _normal_pdf(values: np.ndarray) -> np.ndarray:
    return np.exp(-0.5 * np.square(values)) / _SQRT_2PI


def _init_iv_greek_columns(df: pd.DataFrame) -> None:
    for column in _IV_GREEK_COLUMNS:
        if column not in df.columns:
            df[column] = np.nan
        else:
            df[column] = pd.to_numeric(df[column], errors="coerce")


def _bs_price(
    sigma: np.ndarray,
    spot: np.ndarray,
    strike: np.ndarray,
    tte: np.ndarray,
    flag: str,
) -> np.ndarray:
    sqrt_t = np.sqrt(tte)

    with np.errstate(divide="ignore", invalid="ignore", over="ignore"):
        d1 = (
            np.log(spot / strike)
            + (RISK_FREE_RATE - DIVIDEND_YIELD + 0.5 * sigma**2) * tte
        ) / (sigma * sqrt_t)

    d2 = d1 - sigma * sqrt_t
    disc_r = np.exp(-RISK_FREE_RATE * tte)
    disc_q = np.exp(-DIVIDEND_YIELD * tte)

    if flag == "c":
        return disc_q * spot * ndtr(d1) - disc_r * strike * ndtr(d2)

    return disc_r * strike * ndtr(-d2) - disc_q * spot * ndtr(-d1)


def _vectorized_iv_uncached(
    prices: np.ndarray,
    spots: np.ndarray,
    strikes: np.ndarray,
    ttes: np.ndarray,
    flag: str,
) -> np.ndarray:
    """Robust vectorized bisection solver without cache lookup."""
    prices = np.asarray(prices, dtype=float)
    spot = np.asarray(spots, dtype=float)
    strike = np.asarray(strikes, dtype=float)
    tte = np.asarray(ttes, dtype=float)

    result = np.full(prices.size, np.nan, dtype=float)
    if prices.size == 0:
        return result

    disc_r = np.exp(-RISK_FREE_RATE * tte)
    disc_q = np.exp(-DIVIDEND_YIELD * tte)

    if flag == "c":
        intrinsic = np.maximum(disc_q * spot - disc_r * strike, 0.0)
        upper_bound = disc_q * spot
    else:
        intrinsic = np.maximum(disc_r * strike - disc_q * spot, 0.0)
        upper_bound = disc_r * strike

    valid = (
        np.isfinite(prices)
        & np.isfinite(spot)
        & np.isfinite(strike)
        & np.isfinite(tte)
        & (prices > 0.0)
        & (spot > 0.0)
        & (strike > 0.0)
        & (tte > 0.0)
        & (prices >= intrinsic - _IV_ATOL)
        & (prices < upper_bound)
        & ((prices - intrinsic) > _IV_MIN_TIME_VALUE)
    )

    if not valid.any():
        return result

    lo = np.full(prices.size, _IV_MIN, dtype=float)
    hi = np.full(prices.size, _IV_MAX, dtype=float)

    # Stop early when every valid row is already within the configured tolerance.
    active = valid.copy()
    for _ in range(_IV_MAX_ITER):
        mid = 0.5 * (lo + hi)
        model = _bs_price(mid, spot, strike, tte, flag)
        error = model - prices

        too_low = error < 0.0
        lo = np.where(active & too_low, mid, lo)
        hi = np.where(active & ~too_low, mid, hi)

        tolerance = _IV_ATOL + _IV_RTOL * np.abs(prices)
        active = valid & (np.abs(error) > tolerance)
        if not active.any():
            break

    sigma = 0.5 * (lo + hi)
    model = _bs_price(sigma, spot, strike, tte, flag)
    tolerance = _IV_ATOL + _IV_RTOL * np.abs(prices)
    converged = np.abs(model - prices) <= tolerance

    ok = (
        valid
        & converged
        & np.isfinite(sigma)
        & (sigma >= _IV_MIN)
        & (sigma <= _IV_MAX)
    )
    result[ok] = sigma[ok]
    return result


def _iv_cache_keys(
    prices: np.ndarray,
    spots: np.ndarray,
    strikes: np.ndarray,
    ttes: np.ndarray,
    flag: str,
) -> list[tuple]:
    return [
        (
            flag,
            round(float(price), IV_CACHE_PRICE_DECIMALS),
            round(float(spot), IV_CACHE_SPOT_DECIMALS),
            int(round(float(strike))),
            round(float(tte), IV_CACHE_TTE_DECIMALS),
            round(RISK_FREE_RATE, 8),
            round(DIVIDEND_YIELD, 8),
        )
        for price, spot, strike, tte in zip(
            prices,
            spots,
            strikes,
            ttes,
            strict=True,
        )
    ]


def _vectorized_iv(
    prices: np.ndarray,
    spots: np.ndarray,
    strikes: np.ndarray,
    ttes: np.ndarray,
    flag: str,
) -> np.ndarray:
    """Vectorized IV solve with batched cache lookup for repeated values."""
    prices = np.asarray(prices, dtype=float)
    spots = np.asarray(spots, dtype=float)
    strikes = np.asarray(strikes, dtype=float)
    ttes = np.asarray(ttes, dtype=float)

    if prices.size == 0:
        return np.empty(0, dtype=float)

    if not ENABLE_IV_CACHE:
        return _vectorized_iv_uncached(prices, spots, strikes, ttes, flag)

    keys = _iv_cache_keys(prices, spots, strikes, ttes, flag)
    result, found = _IV_CACHE.get_many(keys)

    missing_indices = np.flatnonzero(~found)
    if missing_indices.size == 0:
        return result

    solved = _vectorized_iv_uncached(
        prices[missing_indices],
        spots[missing_indices],
        strikes[missing_indices],
        ttes[missing_indices],
        flag,
    )
    result[missing_indices] = solved

    _IV_CACHE.put_many(
        (keys[int(index)], float(value))
        for index, value in zip(missing_indices, solved, strict=True)
        if np.isfinite(value)
    )

    return result


# ============================================================================
# GREEKS
# ============================================================================


def _compute_greeks_for_side(
    df: pd.DataFrame,
    option_flag: str,
    iv_col: str,
    delta_col: str,
    gamma_col: str,
    vega_col: str,
    theta_col: str,
    rho_col: str,
) -> None:
    valid_mask = (
        df[iv_col].notna()
        & df["close"].notna()
        & df["nearest_strike"].notna()
        & df["tte"].notna()
        & (df[iv_col] > 0.0)
        & (df["close"] > 0.0)
        & (df["nearest_strike"] > 0.0)
        & (df["tte"] > 0.0)
    )

    idx = df.index[valid_mask]
    if len(idx) == 0:
        return

    spot = df.loc[idx, "close"].to_numpy(dtype=float, copy=False)
    strike = df.loc[idx, "nearest_strike"].to_numpy(dtype=float, copy=False)
    tte = df.loc[idx, "tte"].to_numpy(dtype=float, copy=False)
    sigma = df.loc[idx, iv_col].to_numpy(dtype=float, copy=False)

    sqrt_t = np.sqrt(tte)
    d1 = (
        np.log(spot / strike)
        + (RISK_FREE_RATE - DIVIDEND_YIELD + 0.5 * sigma**2) * tte
    ) / (sigma * sqrt_t)
    d2 = d1 - sigma * sqrt_t

    disc_q = np.exp(-DIVIDEND_YIELD * tte)
    disc_r = np.exp(-RISK_FREE_RATE * tte)
    pdf_d1 = _normal_pdf(d1)

    if option_flag == "c":
        delta = disc_q * ndtr(d1)
        theta = (
            -(spot * disc_q * pdf_d1 * sigma) / (2.0 * sqrt_t)
            - RISK_FREE_RATE * strike * disc_r * ndtr(d2)
            + DIVIDEND_YIELD * spot * disc_q * ndtr(d1)
        ) / 365.0
        rho = strike * tte * disc_r * ndtr(d2) / 100.0
    else:
        delta = disc_q * (ndtr(d1) - 1.0)
        theta = (
            -(spot * disc_q * pdf_d1 * sigma) / (2.0 * sqrt_t)
            + RISK_FREE_RATE * strike * disc_r * ndtr(-d2)
            - DIVIDEND_YIELD * spot * disc_q * ndtr(-d1)
        ) / 365.0
        rho = -strike * tte * disc_r * ndtr(-d2) / 100.0

    gamma = disc_q * pdf_d1 / (spot * sigma * sqrt_t)
    vega = spot * disc_q * pdf_d1 * sqrt_t / 100.0

    df.loc[idx, delta_col] = delta
    df.loc[idx, gamma_col] = gamma
    df.loc[idx, vega_col] = vega
    df.loc[idx, theta_col] = theta
    df.loc[idx, rho_col] = rho


# ============================================================================
# PUBLIC API
# ============================================================================


def append_black_scholes_iv(
    df: pd.DataFrame,
    compute_greeks: bool = True,
    *,
    inplace: bool = False,
) -> pd.DataFrame:
    """Compute CE/PE IV and optionally Greeks for an option-chain slice.

    Parameters
    ----------
    df:
        A small request-specific option-chain DataFrame.
    compute_greeks:
        Set to ``False`` for IV-smile and IV-surface requests that do not require
        Delta, Gamma, Vega, Theta, or Rho.
    inplace:
        Mutate the supplied DataFrame. Use only for a small private slice, never
        for a shared master cache frame.
    """
    started = time.perf_counter()

    if df is None:
        raise TypeError("df cannot be None")
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"df must be a pandas DataFrame, received {type(df)!r}")

    if not inplace:
        # A shallow copy avoids duplicating all existing arrays. Assignments below
        # create only the normalized/output columns needed by this working slice.
        df = df.copy(deep=False)

    _init_iv_greek_columns(df)
    if df.empty:
        return df

    missing = sorted(_REQUIRED_COLUMNS.difference(df.columns))
    if missing:
        logger.warning("IV/Greeks not computed. Missing columns: %s", missing)
        return df

    normalization_started = time.perf_counter()

    # Normalize only the working slice.
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["expiry"] = df["expiry"].astype("string").str.strip()

    for column in ("ce", "pe", "close", "nearest_strike"):
        df[column] = pd.to_numeric(df[column], errors="coerce")

    df["tte"] = _calculate_tte_vectorized(df["expiry"], df["timestamp"])
    normalization_ms = (time.perf_counter() - normalization_started) * 1000.0

    iv_started = time.perf_counter()

    for flag, price_col, iv_col in (
        ("c", "ce", "ce_iv"),
        ("p", "pe", "pe_iv"),
    ):
        valid = (
            df[price_col].notna()
            & df["nearest_strike"].notna()
            & df["close"].notna()
            & df["tte"].notna()
            & (df[price_col] > 0.0)
            & (df["nearest_strike"] > 0.0)
            & (df["close"] > 0.0)
            & (df["tte"] > 0.0)
        )

        idx = df.index[valid]
        if len(idx) == 0:
            continue

        df.loc[idx, iv_col] = _vectorized_iv(
            df.loc[idx, price_col].to_numpy(dtype=float, copy=False),
            df.loc[idx, "close"].to_numpy(dtype=float, copy=False),
            df.loc[idx, "nearest_strike"].to_numpy(dtype=float, copy=False),
            df.loc[idx, "tte"].to_numpy(dtype=float, copy=False),
            flag,
        )

    # Avoid DataFrame.mean overhead and preserve NaN semantics explicitly.
    ce_iv = df["ce_iv"].to_numpy(dtype=float, copy=False)
    pe_iv = df["pe_iv"].to_numpy(dtype=float, copy=False)
    count = np.isfinite(ce_iv).astype(np.int8) + np.isfinite(pe_iv).astype(np.int8)
    total = np.nan_to_num(ce_iv, nan=0.0) + np.nan_to_num(pe_iv, nan=0.0)
    df["iv"] = np.divide(
        total,
        count,
        out=np.full(len(df), np.nan, dtype=float),
        where=count > 0,
    )

    iv_ms = (time.perf_counter() - iv_started) * 1000.0

    greek_ms = 0.0
    if compute_greeks:
        greek_started = time.perf_counter()

        _compute_greeks_for_side(
            df,
            "c",
            "ce_iv",
            "ce_delta",
            "ce_gamma",
            "ce_vega",
            "ce_theta",
            "ce_rho",
        )
        _compute_greeks_for_side(
            df,
            "p",
            "pe_iv",
            "pe_delta",
            "pe_gamma",
            "pe_vega",
            "pe_theta",
            "pe_rho",
        )

        greek_ms = (time.perf_counter() - greek_started) * 1000.0

    result = df.drop(columns=["tte"], errors="ignore")
    total_ms = (time.perf_counter() - started) * 1000.0

    if DEBUG_IV or PROFILE_IV:
        logger.info(
            "IV calculation rows=%d ce=%d pe=%d greeks=%s "
            "normalize_ms=%.2f iv_ms=%.2f greek_ms=%.2f total_ms=%.2f cache=%s",
            len(result),
            int(result["ce_iv"].notna().sum()),
            int(result["pe_iv"].notna().sum()),
            bool(compute_greeks),
            normalization_ms,
            iv_ms,
            greek_ms,
            total_ms,
            iv_cache_stats(),
        )

    return result


__all__ = [
    "append_black_scholes_iv",
    "calculate_tte_from_timestamp",
    "clear_iv_cache",
    "iv_cache_stats",
]
