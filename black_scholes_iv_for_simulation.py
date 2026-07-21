"""Vectorized Black-Scholes IV and Greeks for the Option Simulator.

Production notes
----------------
* The expensive IV solve is vectorized across the whole option chain.
* TTE is computed vectorially; ``DataFrame.apply(axis=1)`` is avoided.
* ``inplace=True`` lets callers operate on a small timestamp slice without
  making another defensive copy. Never pass the shared master DataFrame with
  ``inplace=True``.
* The shared master market-data frame should be treated as read-only.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Final

import numpy as np
import pandas as pd
from scipy.stats import norm

logger = logging.getLogger(__name__)

DEBUG_IV: Final[bool] = os.getenv("DEBUG_IV", "false").lower() in {
    "1", "true", "yes", "on"
}
RISK_FREE_RATE: Final[float] = float(os.getenv("RISK_FREE_RATE", "0.0"))
DIVIDEND_YIELD: Final[float] = float(os.getenv("DIVIDEND_YIELD", "0.0"))
TRADING_MINUTES_PER_DAY: Final[int] = int(
    os.getenv("TRADING_MINUTES_PER_DAY", "375")
)
TRADING_DAYS_PER_YEAR: Final[int] = int(
    os.getenv("TRADING_DAYS_PER_YEAR", "252")
)
MARKET_CLOSE: Final[str] = os.getenv("MARKET_CLOSE", "15:30")

_IV_MIN: Final[float] = 1e-4
_IV_MAX: Final[float] = 5.0
_IV_MAX_ITER: Final[int] = int(os.getenv("IV_MAX_ITER", "48"))
_IV_ATOL: Final[float] = 1e-6
_IV_RTOL: Final[float] = 1e-7
_IV_MIN_TIME_VALUE: Final[float] = 1e-3

_IV_GREEK_COLUMNS: Final[tuple[str, ...]] = (
    "ce_iv", "pe_iv", "iv",
    "ce_delta", "pe_delta",
    "ce_gamma", "pe_gamma",
    "ce_vega", "pe_vega",
    "ce_theta", "pe_theta",
    "ce_rho", "pe_rho",
)


def calculate_tte_from_timestamp(expiry_str, timestamp) -> float:
    """Return time to expiry in trading-year units for one row."""
    expiry_date = datetime.strptime(str(expiry_str), "%y%m%d").date()
    ts = pd.to_datetime(timestamp, errors="coerce")
    if pd.isna(ts):
        return np.nan
    if getattr(ts, "tzinfo", None) is not None:
        ts = ts.tz_localize(None)

    close_time = datetime.strptime(MARKET_CLOSE, "%H:%M").time()
    market_end = datetime.combine(ts.date(), close_time)
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
    """Vectorized TTE calculation for an entire option-chain slice."""
    ts = pd.to_datetime(timestamp, errors="coerce")
    if getattr(ts.dt, "tz", None) is not None:
        ts = ts.dt.tz_localize(None)

    expiry_text = expiry.astype("string").str.strip()
    expiry_dt = pd.to_datetime(expiry_text, format="%y%m%d", errors="coerce")

    close_hour, close_minute = map(int, MARKET_CLOSE.split(":"))
    market_end = ts.dt.normalize() + pd.Timedelta(
        hours=close_hour,
        minutes=close_minute,
    )
    minutes_left = (
        (market_end - ts).dt.total_seconds().div(60.0).clip(lower=0.0)
    )
    calendar_days = (
        expiry_dt.dt.normalize() - ts.dt.normalize()
    ).dt.days.clip(lower=0)

    tte_days = calendar_days.astype(float) + (
        minutes_left / TRADING_MINUTES_PER_DAY
    )
    tte = tte_days / TRADING_DAYS_PER_YEAR
    tte = tte.clip(lower=1.0 / TRADING_DAYS_PER_YEAR)
    invalid = ts.isna() | expiry_dt.isna()
    return tte.mask(invalid, np.nan).to_numpy(dtype=float)


def _init_iv_greek_columns(df: pd.DataFrame) -> None:
    """Create/normalize output columns in-place on the working slice."""
    for col in _IV_GREEK_COLUMNS:
        if col not in df.columns:
            df[col] = np.nan
        else:
            df[col] = pd.to_numeric(df[col], errors="coerce")


def _bs_price(sigma, spot, strike, tte, flag):
    r = RISK_FREE_RATE
    q = DIVIDEND_YIELD
    sqrt_t = np.sqrt(tte)
    with np.errstate(divide="ignore", invalid="ignore", over="ignore"):
        d1 = (
            np.log(spot / strike)
            + (r - q + 0.5 * sigma**2) * tte
        ) / (sigma * sqrt_t)
    d2 = d1 - sigma * sqrt_t
    disc_r = np.exp(-r * tte)
    disc_q = np.exp(-q * tte)
    if flag == "c":
        return disc_q * spot * norm.cdf(d1) - disc_r * strike * norm.cdf(d2)
    return disc_r * strike * norm.cdf(-d2) - disc_q * spot * norm.cdf(-d1)


def _vectorized_iv(prices, spots, strikes, ttes, flag):
    """Robust vectorized bisection IV solver."""
    prices = np.asarray(prices, dtype=float)
    spot = np.asarray(spots, dtype=float)
    strike = np.asarray(strikes, dtype=float)
    tte = np.asarray(ttes, dtype=float)
    n = prices.size
    result = np.full(n, np.nan, dtype=float)
    if n == 0:
        return result

    disc_r = np.exp(-RISK_FREE_RATE * tte)
    if flag == "c":
        intrinsic = np.maximum(spot - strike * disc_r, 0.0)
        upper_bound = spot
    else:
        intrinsic = np.maximum(strike * disc_r - spot, 0.0)
        upper_bound = strike * disc_r

    valid = (
        np.isfinite(prices)
        & np.isfinite(spot)
        & np.isfinite(strike)
        & np.isfinite(tte)
        & (prices > 0)
        & (spot > 0)
        & (strike > 0)
        & (tte > 0)
        & (prices >= intrinsic)
        & (prices < upper_bound)
        & ((prices - intrinsic) > _IV_MIN_TIME_VALUE)
    )
    if not valid.any():
        return result

    lo = np.full(n, _IV_MIN, dtype=float)
    hi = np.full(n, _IV_MAX, dtype=float)
    for _ in range(_IV_MAX_ITER):
        mid = 0.5 * (lo + hi)
        model = _bs_price(mid, spot, strike, tte, flag)
        too_low = model < prices
        lo = np.where(valid & too_low, mid, lo)
        hi = np.where(valid & ~too_low, mid, hi)

    sigma = 0.5 * (lo + hi)
    model = _bs_price(sigma, spot, strike, tte, flag)
    tolerance = _IV_ATOL + _IV_RTOL * np.abs(prices)
    converged = np.abs(model - prices) <= tolerance
    ok = valid & converged & np.isfinite(sigma) & (sigma > _IV_MIN) & (sigma < _IV_MAX)
    result[ok] = sigma[ok]
    return result


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
        & (df[iv_col] > 0)
        & (df["close"] > 0)
        & (df["nearest_strike"] > 0)
        & (df["tte"] > 0)
    )
    idx = df.index[valid_mask]
    if len(idx) == 0:
        return

    spot = df.loc[idx, "close"].to_numpy(dtype=float, copy=False)
    strike = df.loc[idx, "nearest_strike"].to_numpy(dtype=float, copy=False)
    tte = df.loc[idx, "tte"].to_numpy(dtype=float, copy=False)
    sigma = df.loc[idx, iv_col].to_numpy(dtype=float, copy=False)

    r = RISK_FREE_RATE
    q = DIVIDEND_YIELD
    sqrt_t = np.sqrt(tte)
    d1 = (
        np.log(spot / strike)
        + (r - q + 0.5 * sigma**2) * tte
    ) / (sigma * sqrt_t)
    d2 = d1 - sigma * sqrt_t
    disc_q = np.exp(-q * tte)
    disc_r = np.exp(-r * tte)

    if option_flag == "c":
        delta = disc_q * norm.cdf(d1)
        theta = (
            -(spot * disc_q * norm.pdf(d1) * sigma) / (2 * sqrt_t)
            - r * strike * disc_r * norm.cdf(d2)
            + q * spot * disc_q * norm.cdf(d1)
        ) / 365.0
        rho = strike * tte * disc_r * norm.cdf(d2) / 100.0
    else:
        delta = disc_q * (norm.cdf(d1) - 1.0)
        theta = (
            -(spot * disc_q * norm.pdf(d1) * sigma) / (2 * sqrt_t)
            + r * strike * disc_r * norm.cdf(-d2)
            - q * spot * disc_q * norm.cdf(-d1)
        ) / 365.0
        rho = -strike * tte * disc_r * norm.cdf(-d2) / 100.0

    gamma = disc_q * norm.pdf(d1) / (spot * sigma * sqrt_t)
    vega = spot * disc_q * norm.pdf(d1) * sqrt_t / 100.0

    df.loc[idx, delta_col] = delta
    df.loc[idx, gamma_col] = gamma
    df.loc[idx, vega_col] = vega
    df.loc[idx, theta_col] = theta
    df.loc[idx, rho_col] = rho


def append_black_scholes_iv(
    df: pd.DataFrame,
    compute_greeks: bool = True,
    *,
    inplace: bool = False,
) -> pd.DataFrame:
    """Compute IV and optionally Greeks.

    Parameters
    ----------
    df:
        A small, request-specific option-chain slice. Avoid passing the shared
        full-week master frame.
    compute_greeks:
        Skip Greeks for IV-smile-only requests.
    inplace:
        When true, mutate the supplied *small slice*. This avoids another copy.
        The default remains safe and backward compatible.
    """
    if not inplace:
        df = df.copy(deep=False)

    _init_iv_greek_columns(df)
    if df.empty:
        return df

    required = {
        "timestamp", "expiry", "nearest_strike", "close", "ce", "pe"
    }
    missing = sorted(required.difference(df.columns))
    if missing:
        logger.warning("IV/Greeks not computed. Missing columns: %s", missing)
        return df

    # Normalize only the working slice, never the full master frame.
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    for column in ("ce", "pe", "close", "nearest_strike"):
        df[column] = pd.to_numeric(df[column], errors="coerce")

    df["tte"] = _calculate_tte_vectorized(df["expiry"], df["timestamp"])

    for flag, price_col, iv_col in (
        ("c", "ce", "ce_iv"),
        ("p", "pe", "pe_iv"),
    ):
        valid = (
            df[price_col].notna()
            & df["nearest_strike"].notna()
            & df["close"].notna()
            & df["tte"].notna()
            & (df[price_col] > 0)
            & (df["nearest_strike"] > 0)
            & (df["close"] > 0)
            & (df["tte"] > 0)
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

    df["iv"] = df[["ce_iv", "pe_iv"]].mean(axis=1, skipna=True)

    if compute_greeks:
        _compute_greeks_for_side(
            df, "c", "ce_iv", "ce_delta", "ce_gamma", "ce_vega",
            "ce_theta", "ce_rho",
        )
        _compute_greeks_for_side(
            df, "p", "pe_iv", "pe_delta", "pe_gamma", "pe_vega",
            "pe_theta", "pe_rho",
        )

    if DEBUG_IV:
        logger.info(
            "IV rows: ce=%d pe=%d",
            int(df["ce_iv"].notna().sum()),
            int(df["pe_iv"].notna().sum()),
        )

    return df.drop(columns=["tte"], errors="ignore")
