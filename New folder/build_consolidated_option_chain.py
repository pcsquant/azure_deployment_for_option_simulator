"""
Offline builder for the consolidated option-chain store (Tier 1).

For each (trading date, expiry) it collapses the many per-contract option
parquet files:

    week_folder/NSE_OPT_TICK_YYYYMMDD/NIFTY<expiry><strike>CE.parquet
                                      NIFTY<expiry><strike>PE.parquet
    ...

into a single long-format file:

    week_folder/NSE_OPT_CONSOLIDATED_YYYYMMDD/NIFTY_YYYYMMDD_<expiry>.parquet

with columns [timestamp, strike, ce, pe] where ce/pe are 1-minute closes
(LTP). The simulator's read path then serves an option-chain snapshot from one
columnar read instead of ~2*(strikes) per-contract reads.

Performance:
    Each contract file is read + 1-minute-resampled independently, so the work
    is fanned out across processes (CPU-bound resample). The per-contract reader
    here is intentionally lean -- it projects only the datetime + price columns
    and does NOT populate the app's RAW_PARQUET_CACHE (which a bulk build would
    only thrash). Tune with --workers.

Usage:
    python build_consolidated_option_chain.py --instrument NIFTY
    python build_consolidated_option_chain.py --date 20260415
    python build_consolidated_option_chain.py --date 20260415 --expiry 260430
    python build_consolidated_option_chain.py --date 20260415 --overwrite
    python build_consolidated_option_chain.py --workers 8
    python build_consolidated_option_chain.py --workers 1      # serial (debug)
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed

import pandas as pd

from config_for_simulation import IST, SESSION_END, SESSION_START, get_dataset_config
from data_engine_for_simulation import (
    _get_opt_folder,
    consolidated_chain_folder,
    consolidated_chain_path,
    find_option_contract_files,
    get_dates_for_week_folder,
    get_week_folders,
)


# =========================================================
# LEAN PER-CONTRACT READER (picklable; runs in worker procs)
# =========================================================

def _pick_projection_columns(path: str):
    """Return the minimal [datetime(+time), price] column list, or None to read all."""
    try:
        import pyarrow.parquet as pq

        names = pq.ParquetFile(path).schema_arrow.names
    except Exception:
        return None

    lower = {str(c).lower(): c for c in names}
    wanted = []

    if "datetime" in lower:
        wanted.append(lower["datetime"])
    elif "date" in lower and "time" in lower:
        wanted.extend([lower["date"], lower["time"]])
    else:
        return None  # positional fallback needs the full frame

    for cand in ("price", "ltp", "value", "close"):
        if cand in lower:
            wanted.append(lower[cand])
            break
    else:
        return None  # no named price column -> read all and use positional

    return wanted


def _read_contract_1min_close(path: str):
    """
    Read one option contract file and return a 1-minute close (LTP) Series
    indexed by IST timestamp. Mirrors data_engine._read_parquet_normalized
    (option mode) -- IST localize + session filter -- so the consolidated store
    reproduces the per-contract fallback exactly. Returns None when empty.
    """
    try:
        cols = _pick_projection_columns(path)
        df = pd.read_parquet(path, columns=cols) if cols else pd.read_parquet(path)
    except Exception:
        return None

    if df is None or df.empty:
        return None

    lower_cols = {str(c).lower(): c for c in df.columns}

    if "datetime" in lower_cols:
        dt = pd.to_datetime(df[lower_cols["datetime"]], errors="coerce")
    elif {"date", "time"}.issubset(lower_cols):
        dt = pd.to_datetime(
            df[lower_cols["date"]].astype(str) + " " + df[lower_cols["time"]].astype(str),
            errors="coerce",
        )
    else:
        dt = pd.to_datetime(
            df.iloc[:, 0].astype(str) + " " + df.iloc[:, 1].astype(str),
            errors="coerce",
        )

    price_col = (
        lower_cols.get("price")
        or lower_cols.get("ltp")
        or lower_cols.get("value")
        or lower_cols.get("close")
        or df.columns[2]
    )

    out = pd.DataFrame(
        {"datetime": dt, "price": pd.to_numeric(df[price_col], errors="coerce")}
    ).dropna(subset=["datetime", "price"])

    if out.empty:
        return None

    if out["datetime"].dt.tz is None:
        out["datetime"] = out["datetime"].dt.tz_localize(
            IST, ambiguous="NaT", nonexistent="NaT"
        )
    else:
        out["datetime"] = out["datetime"].dt.tz_convert(IST)

    out = out.dropna(subset=["datetime"])
    out = out[
        (out["datetime"].dt.time >= SESSION_START)
        & (out["datetime"].dt.time <= SESSION_END)
    ]

    if out.empty:
        return None

    s = out.set_index("datetime")["price"].resample("1min").last().dropna()
    return s if not s.empty else None


# =========================================================
# DISCOVERY
# =========================================================

def discover_expiries(folder: str, date_str: str, instrument: str = "NIFTY") -> list[str]:
    """Distinct expiry tokens (yymmdd) present in the OPT tick folder for a date."""
    cfg = get_dataset_config(instrument)
    symbol = str(cfg["symbol"]).upper()
    opt_folder = _get_opt_folder(folder, date_str)

    if not os.path.isdir(opt_folder):
        return []

    pattern = re.compile(rf"^{re.escape(symbol)}(\d{{6}})(\d+)(CE|PE)$", re.IGNORECASE)

    expiries: set[str] = set()
    for root, _, files in os.walk(opt_folder):
        for f in files:
            if not f.lower().endswith(".parquet"):
                continue
            base = os.path.splitext(f)[0].upper()
            match = pattern.match(base)
            if match:
                expiries.add(match.group(1))

    return sorted(expiries)


# =========================================================
# BUILD
# =========================================================

def _read_all_contracts(contracts, executor):
    """Read+resample every contract, in parallel when an executor is given.

    Returns {(strike:int, side:str): pd.Series}.
    """
    series_map: dict[tuple[int, str], pd.Series] = {}

    if executor is None:
        for strike, side, path in contracts:
            s = _read_contract_1min_close(path)
            if s is not None and not s.empty:
                series_map[(strike, side)] = s
        return series_map

    futures = {
        executor.submit(_read_contract_1min_close, path): (strike, side)
        for strike, side, path in contracts
    }
    for fut in as_completed(futures):
        strike, side = futures[fut]
        try:
            s = fut.result()
        except Exception as exc:
            print(f"    read error {strike}{side}: {exc}")
            continue
        if s is not None and not s.empty:
            series_map[(strike, side)] = s

    return series_map


def build_for_expiry(
    week_folder: str,
    date_str: str,
    expiry_str: str,
    instrument: str = "NIFTY",
    overwrite: bool = False,
    executor: "ProcessPoolExecutor | None" = None,
) -> str | None:
    """Build the consolidated file for one (date, expiry). Returns the path written, or None."""
    out_path = consolidated_chain_path(week_folder, date_str, expiry_str, instrument)

    if os.path.isfile(out_path) and not overwrite:
        print(f"  SKIP (exists): {os.path.basename(out_path)}")
        return None

    contracts = find_option_contract_files(week_folder, date_str, expiry_str, instrument)

    if not contracts:
        print(f"  no contracts for expiry {expiry_str}")
        return None

    series_map = _read_all_contracts(contracts, executor)

    if not series_map:
        print(f"  no usable rows for expiry {expiry_str}")
        return None

    frames: list[pd.DataFrame] = []
    empty = pd.Series(dtype="float64")

    for strike in sorted({k[0] for k in series_map}):
        ce_series = series_map.get((strike, "CE"), empty)
        pe_series = series_map.get((strike, "PE"), empty)

        merged = pd.concat({"ce": ce_series, "pe": pe_series}, axis=1)
        merged.index.name = "timestamp"
        merged = merged.reset_index()
        merged["strike"] = int(strike)
        frames.append(merged[["timestamp", "strike", "ce", "pe"]])

    out = pd.concat(frames, ignore_index=True)

    # Store tz-naive IST wall-clock; the loader re-localizes to IST.
    if out["timestamp"].dt.tz is not None:
        out["timestamp"] = out["timestamp"].dt.tz_convert(IST).dt.tz_localize(None)

    out = out.sort_values(["timestamp", "strike"]).reset_index(drop=True)

    os.makedirs(consolidated_chain_folder(week_folder, date_str), exist_ok=True)
    out.to_parquet(out_path, engine="pyarrow", compression="snappy", index=False)

    print(
        f"  OK {os.path.basename(out_path)}  "
        f"strikes={out['strike'].nunique()} rows={len(out)}"
    )
    return out_path


def build_for_date(
    week_folder: str,
    date_str: str,
    instrument: str = "NIFTY",
    expiry: str | None = None,
    overwrite: bool = False,
    executor: "ProcessPoolExecutor | None" = None,
) -> None:
    expiries = [expiry] if expiry else discover_expiries(week_folder, date_str, instrument)

    if not expiries:
        print(f"{date_str}: no expiries found")
        return

    print(f"{date_str}: {len(expiries)} expiry(ies) -> {expiries}")
    for exp in expiries:
        build_for_expiry(week_folder, date_str, exp, instrument, overwrite, executor)


def build_all(
    instrument: str = "NIFTY",
    date: str | None = None,
    expiry: str | None = None,
    overwrite: bool = False,
    workers: int | None = None,
) -> None:
    started = time.time()
    folders = get_week_folders(instrument=instrument)

    if not folders:
        print(f"No week folders found for {instrument}.")
        return

    if workers is None:
        workers = min(8, os.cpu_count() or 1)

    executor = None
    try:
        if workers and workers > 1:
            executor = ProcessPoolExecutor(max_workers=workers)
            print(f"Using {workers} worker processes.")
        else:
            print("Running serially (workers=1).")

        for week_no, folder in folders:
            dates = get_dates_for_week_folder(week_no, folder, instrument=instrument)
            if date:
                dates = [d for d in dates if d == date]
            if not dates:
                continue

            print("\n" + "=" * 70)
            print(f"Week {week_no}: {folder}")
            print("=" * 70)

            for date_str in dates:
                build_for_date(folder, date_str, instrument, expiry, overwrite, executor)
    finally:
        if executor is not None:
            executor.shutdown(wait=True)

    print(f"\nDone in {time.time() - started:.1f}s")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Build consolidated option-chain parquet store.")
    parser.add_argument("--instrument", default="NIFTY")
    parser.add_argument("--date", default=None, help="Single trading date YYYYMMDD")
    parser.add_argument("--expiry", default=None, help="Single expiry yymmdd")
    parser.add_argument("--overwrite", action="store_true", help="Rebuild existing files")
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Parallel worker processes (default min(8, cpu_count); 1 = serial).",
    )
    args = parser.parse_args(argv)

    build_all(
        instrument=args.instrument,
        date=args.date,
        expiry=args.expiry,
        overwrite=args.overwrite,
        workers=args.workers,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
