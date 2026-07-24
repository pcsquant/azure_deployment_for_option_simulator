import logging
from datetime import datetime

import numpy as np
import pandas as pd
from scipy.stats import norm
from py_vollib.black_scholes.implied_volatility import implied_volatility


logger = logging.getLogger(__name__)

DEBUG_IV = False
RISK_FREE_RATE = 0.0
DIVIDEND_YIELD = 0.0
TRADING_MINUTES_PER_DAY = 375
TRADING_DAYS_PER_YEAR = 252


def calculate_tte_from_timestamp(expiry_str, timestamp) -> float:
    expiry_date = datetime.strptime(str(expiry_str), "%y%m%d").date()
    ts = pd.to_datetime(timestamp, errors="coerce")

    if pd.isna(ts):
        return np.nan

    if getattr(ts, "tzinfo", None) is not None:
        ts = ts.tz_localize(None)

    trade_date = ts.date()

    market_end = datetime.combine(
        trade_date,
        datetime.strptime("15:30", "%H:%M").time(),
    )

    minutes_left = max(
        0,
        (market_end - ts.to_pydatetime()).total_seconds() / 60,
    )

    days_after_today = max(0, (expiry_date - trade_date).days)

    tte_days = days_after_today + (
        minutes_left / TRADING_MINUTES_PER_DAY
    )

    return max(tte_days / TRADING_DAYS_PER_YEAR, 1 / TRADING_DAYS_PER_YEAR)


def _init_iv_greek_columns(df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "ce_iv", "pe_iv", "iv",
        "ce_delta", "pe_delta",
        "ce_gamma", "pe_gamma",
        "ce_vega", "pe_vega",
        "ce_theta", "pe_theta",
        "ce_rho", "pe_rho",
    ]

    for col in columns:
        if col not in df.columns:
            df[col] = np.nan
        else:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def _safe_iv(price, spot, strike, tte, flag):
    try:
        price = float(price)
        spot = float(spot)
        strike = float(strike)
        tte = float(tte)

        if price <= 0 or spot <= 0 or strike <= 0 or tte <= 0:
            return np.nan

        if flag == "c":
            intrinsic = max(spot - strike, 0.0)
        else:
            intrinsic = max(strike - spot, 0.0)

        # py_vollib fails when option price is below intrinsic value.
        if price < intrinsic:
            return np.nan

        iv = implied_volatility(
            price,
            spot,
            strike,
            tte,
            RISK_FREE_RATE,
            flag,
        )

        if not np.isfinite(iv) or iv <= 0:
            return np.nan

        return float(iv)

    except Exception:
        return np.nan


# IV solver tuning. Newton-Raphson on Black-Scholes vega converges in a handful
# of iterations for liquid options; these bounds match what py_vollib accepts.
_IV_MIN = 1e-4
_IV_MAX = 5.0
_IV_MAX_ITER = 60
# Tight price tolerance. The rtol term is kept tiny so deep-ITM options (whose
# price is dominated by a large intrinsic component) are not accepted at a
# sloppy sigma; quadratic Newton convergence reaches this easily when the price
# actually depends on sigma.
_IV_ATOL = 1e-6
_IV_RTOL = 1e-7
# Below this much extrinsic (time) value the price is indistinguishable from
# intrinsic, so IV is not identifiable -> return NaN (py_vollib fails here too).
_IV_MIN_TIME_VALUE = 1e-3


def _bs_price_and_vega(sigma, S, K, T, flag):
    """Vectorized Black-Scholes price and vega (price units) for one flag."""
    r = RISK_FREE_RATE
    q = DIVIDEND_YIELD
    sqrt_T = np.sqrt(T)

    with np.errstate(divide="ignore", invalid="ignore"):
        d1 = (np.log(S / K) + (r - q + 0.5 * sigma ** 2) * T) / (sigma * sqrt_T)

    d2 = d1 - sigma * sqrt_T
    disc_r = np.exp(-r * T)
    disc_q = np.exp(-q * T)

    if flag == "c":
        price = disc_q * S * norm.cdf(d1) - disc_r * K * norm.cdf(d2)
    else:
        price = disc_r * K * norm.cdf(-d2) - disc_q * S * norm.cdf(-d1)

    vega = disc_q * S * norm.pdf(d1) * sqrt_T
    return price, vega


def _vectorized_iv(prices, spots, strikes, ttes, flag):
    """
    Vectorized implied volatility for a whole array of options of one flag.

    Replaces the per-row py_vollib solve. Returns an array with NaN where the
    input is invalid (price below intrinsic, non-positive inputs) or the solver
    did not converge to a sensible value, matching `_safe_iv` semantics.
    """
    prices = np.asarray(prices, dtype=float)
    S = np.asarray(spots, dtype=float)
    K = np.asarray(strikes, dtype=float)
    T = np.asarray(ttes, dtype=float)

    n = prices.shape[0]
    iv = np.full(n, np.nan)

    if n == 0:
        return iv

    r = RISK_FREE_RATE
    disc_r = np.exp(-r * T)

    if flag == "c":
        intrinsic = np.maximum(S - K * disc_r, 0.0)
        upper_bound = S  # call price is bounded above by spot
    else:
        intrinsic = np.maximum(K * disc_r - S, 0.0)
        upper_bound = K * disc_r  # put price is bounded above by discounted strike

    valid = (
        np.isfinite(prices)
        & np.isfinite(S)
        & np.isfinite(K)
        & np.isfinite(T)
        & (prices > 0)
        & (S > 0)
        & (K > 0)
        & (T > 0)
        & (prices >= intrinsic)
        & (prices < upper_bound)
        & (prices - intrinsic > _IV_MIN_TIME_VALUE)
    )

    if not valid.any():
        return iv

    # Vectorized bisection. BS price is monotonically increasing in sigma, and
    # the `valid` mask guarantees intrinsic < price < upper_bound, so the true
    # sigma is bracketed by [_IV_MIN, _IV_MAX]. Bisection is immune to the
    # zero-vega flat regions that trap Newton for deep ITM/OTM options.
    lo = np.full(n, _IV_MIN)
    hi = np.full(n, _IV_MAX)

    for _ in range(_IV_MAX_ITER):
        mid = 0.5 * (lo + hi)
        model, _ = _bs_price_and_vega(mid, S, K, T, flag)
        too_low = model < prices  # modelled price too small -> need larger sigma
        lo = np.where(too_low, mid, lo)
        hi = np.where(too_low, hi, mid)

    sigma = 0.5 * (lo + hi)

    model, _ = _bs_price_and_vega(sigma, S, K, T, flag)
    tol = _IV_ATOL + _IV_RTOL * np.abs(prices)
    converged = np.abs(model - prices) <= tol

    ok = (
        valid
        & converged
        & np.isfinite(sigma)
        & (sigma > _IV_MIN)
        & (sigma < _IV_MAX)
    )

    iv = np.where(ok, sigma, np.nan)
    return iv


def _compute_greeks_for_side(
    df: pd.DataFrame,
    option_flag: str,
    iv_col: str,
    delta_col: str,
    gamma_col: str,
    vega_col: str,
    theta_col: str,
    rho_col: str,
) -> pd.DataFrame:
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

    valid = df.loc[valid_mask]

    if valid.empty:
        return df

    S = valid["close"].astype(float)
    K = valid["nearest_strike"].astype(float)
    T = valid["tte"].astype(float)
    sigma = valid[iv_col].astype(float)

    r = RISK_FREE_RATE
    q = DIVIDEND_YIELD

    sqrt_T = np.sqrt(T)

    d1 = (
        np.log(S / K)
        + (r - q + 0.5 * sigma ** 2) * T
    ) / (sigma * sqrt_T)

    d2 = d1 - sigma * sqrt_T

    discount_q = np.exp(-q * T)
    discount_r = np.exp(-r * T)

    if option_flag == "c":
        delta = discount_q * norm.cdf(d1)

        theta = (
            -(
                S
                * discount_q
                * norm.pdf(d1)
                * sigma
            )
            / (2 * sqrt_T)
            - r * K * discount_r * norm.cdf(d2)
            + q * S * discount_q * norm.cdf(d1)
        ) / 365.0

        rho = (
            K
            * T
            * discount_r
            * norm.cdf(d2)
        ) / 100.0

    else:
        delta = discount_q * (norm.cdf(d1) - 1)

        theta = (
            -(
                S
                * discount_q
                * norm.pdf(d1)
                * sigma
            )
            / (2 * sqrt_T)
            + r * K * discount_r * norm.cdf(-d2)
            - q * S * discount_q * norm.cdf(-d1)
        ) / 365.0

        rho = (
            -K
            * T
            * discount_r
            * norm.cdf(-d2)
        ) / 100.0

    gamma = (
        discount_q
        * norm.pdf(d1)
        / (S * sigma * sqrt_T)
    )

    vega = (
        S
        * discount_q
        * norm.pdf(d1)
        * sqrt_T
    ) / 100.0

    df.loc[valid.index, delta_col] = np.asarray(delta)
    df.loc[valid.index, gamma_col] = np.asarray(gamma)
    df.loc[valid.index, vega_col] = np.asarray(vega)
    df.loc[valid.index, theta_col] = np.asarray(theta)
    df.loc[valid.index, rho_col] = np.asarray(rho)

    return df


def append_black_scholes_iv(df: pd.DataFrame, compute_greeks: bool = True) -> pd.DataFrame:
    """
    Compute IV (and, when `compute_greeks` is True, full Greeks) for an option
    chain frame.

    Callers that only need IV — e.g. the IV-surface smile — should pass
    `compute_greeks=False` to skip the two Greek passes entirely.
    """
    df = df.copy()
    df = _init_iv_greek_columns(df)

    if df.empty:
        return df

    required_cols = [
        "timestamp",
        "expiry",
        "nearest_strike",
        "close",
        "ce",
        "pe",
    ]

    missing_cols = [c for c in required_cols if c not in df.columns]

    if missing_cols:
        logger.warning(
            "IV/Greeks not computed. Missing columns: %s",
            missing_cols,
        )
        return df

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["ce"] = pd.to_numeric(df["ce"], errors="coerce")
    df["pe"] = pd.to_numeric(df["pe"], errors="coerce")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["nearest_strike"] = pd.to_numeric(
        df["nearest_strike"],
        errors="coerce",
    )

    try:
        df["tte"] = df.apply(
            lambda r: calculate_tte_from_timestamp(
                r["expiry"],
                r["timestamp"],
            ),
            axis=1,
        )
    except Exception as exc:
        logger.warning(
            "IV/Greeks not computed. TTE calculation failed: %s: %s",
            type(exc).__name__,
            exc,
        )
        return df

    for flag, price_col, iv_col in [
        ("c", "ce", "ce_iv"),
        ("p", "pe", "pe_iv"),
    ]:
        valid_mask = (
            df[price_col].notna()
            & df["nearest_strike"].notna()
            & df["close"].notna()
            & df["tte"].notna()
            & (df[price_col] > 0)
            & (df["nearest_strike"] > 0)
            & (df["close"] > 0)
            & (df["tte"] > 0)
        )

        idx = df.index[valid_mask]

        if len(idx) == 0:
            logger.warning("No valid rows for %s", iv_col)
            continue

        df.loc[idx, iv_col] = _vectorized_iv(
            prices=df.loc[idx, price_col].to_numpy(dtype=float),
            spots=df.loc[idx, "close"].to_numpy(dtype=float),
            strikes=df.loc[idx, "nearest_strike"].to_numpy(dtype=float),
            ttes=df.loc[idx, "tte"].to_numpy(dtype=float),
            flag=flag,
        )

    df["iv"] = df[["ce_iv", "pe_iv"]].mean(axis=1, skipna=True)

    if compute_greeks:
        df = _compute_greeks_for_side(
            df=df,
            option_flag="c",
            iv_col="ce_iv",
            delta_col="ce_delta",
            gamma_col="ce_gamma",
            vega_col="ce_vega",
            theta_col="ce_theta",
            rho_col="ce_rho",
        )

        df = _compute_greeks_for_side(
            df=df,
            option_flag="p",
            iv_col="pe_iv",
            delta_col="pe_delta",
            gamma_col="pe_gamma",
            vega_col="pe_vega",
            theta_col="pe_theta",
            rho_col="pe_rho",
        )

    if DEBUG_IV:
        logger.info("ce_iv non-null: %s", int(df["ce_iv"].notna().sum()))
        logger.info("pe_iv non-null: %s", int(df["pe_iv"].notna().sum()))
        logger.info("ce_delta non-null: %s", int(df["ce_delta"].notna().sum()))
        logger.info("pe_delta non-null: %s", int(df["pe_delta"].notna().sum()))

    df = df.drop(columns=["tte"], errors="ignore")

    return df
