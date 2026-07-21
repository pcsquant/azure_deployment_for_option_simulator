"""
Production builder for consolidated option-chain Parquet files.

For each (trade date, expiry), many per-contract Parquet files are collapsed
into one long-format file with the stable schema:

    timestamp, strike, ce, pe

The output is written atomically so the simulator never observes a partially
written file. Contract reads are parallelized with ProcessPoolExecutor.

Examples
--------
python build_consolidated_option_chain.py --instrument NIFTY
python build_consolidated_option_chain.py --date 20260415
python build_consolidated_option_chain.py --date 20260415 --expiry 260430
python build_consolidated_option_chain.py --week 165 --workers 6
python build_consolidated_option_chain.py --overwrite --log-level DEBUG
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import tempfile
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable, Sequence

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

LOGGER = logging.getLogger("option_chain_builder")

DEFAULT_MAX_WORKERS = min(8, os.cpu_count() or 1)
DEFAULT_COMPRESSION = "snappy"
OUTPUT_COLUMNS = ["timestamp", "strike", "ce", "pe"]
PRICE_CANDIDATES = ("price", "ltp", "value", "close")
READ_RETRIES = 2


@dataclass(slots=True)
class ContractReadResult:
    strike: int
    side: str
    path: str
    series: pd.Series | None
    error: str | None = None


@dataclass(slots=True)
class BuildResult:
    week_folder: str
    date: str
    expiry: str
    instrument: str
    output_path: str | None
    status: str
    contracts_discovered: int = 0
    contracts_loaded: int = 0
    rows_written: int = 0
    unique_strikes: int = 0
    elapsed_seconds: float = 0.0
    error: str | None = None


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(processName)s | %(message)s",
    )


def _pick_projection_columns(path: str) -> list[str] | None:
    """Return the minimum columns required to build one-minute closes."""
    try:
        import pyarrow.parquet as pq

        names = pq.ParquetFile(path).schema_arrow.names
    except Exception as exc:
        LOGGER.debug("Schema projection unavailable for %s: %s", path, exc)
        return None

    lower = {str(column).lower(): str(column) for column in names}
    wanted: list[str] = []

    if "datetime" in lower:
        wanted.append(lower["datetime"])
    elif "date" in lower and "time" in lower:
        wanted.extend([lower["date"], lower["time"]])
    else:
        return None

    for candidate in PRICE_CANDIDATES:
        if candidate in lower:
            wanted.append(lower[candidate])
            return wanted

    return None


def _first_existing_column(columns: dict[str, object], candidates: Sequence[str]) -> object | None:
    for candidate in candidates:
        if candidate in columns:
            return columns[candidate]
    return None


def _normalize_contract_frame(df: pd.DataFrame) -> pd.Series | None:
    if df is None or df.empty:
        return None

    lower_cols = {str(column).lower(): column for column in df.columns}

    if "datetime" in lower_cols:
        timestamps = pd.to_datetime(df[lower_cols["datetime"]], errors="coerce")
    elif "date" in lower_cols and "time" in lower_cols:
        timestamps = pd.to_datetime(
            df[lower_cols["date"]].astype(str).str.strip()
            + " "
            + df[lower_cols["time"]].astype(str).str.strip(),
            errors="coerce",
        )
    elif len(df.columns) >= 2:
        timestamps = pd.to_datetime(
            df.iloc[:, 0].astype(str).str.strip()
            + " "
            + df.iloc[:, 1].astype(str).str.strip(),
            errors="coerce",
        )
    else:
        return None

    price_column = _first_existing_column(lower_cols, PRICE_CANDIDATES)
    if price_column is None:
        if len(df.columns) < 3:
            return None
        price_column = df.columns[2]

    normalized = pd.DataFrame(
        {
            "timestamp": timestamps,
            "price": pd.to_numeric(df[price_column], errors="coerce"),
        }
    ).dropna(subset=["timestamp", "price"])

    if normalized.empty:
        return None

    if normalized["timestamp"].dt.tz is None:
        normalized["timestamp"] = normalized["timestamp"].dt.tz_localize(
            IST,
            ambiguous="NaT",
            nonexistent="NaT",
        )
    else:
        normalized["timestamp"] = normalized["timestamp"].dt.tz_convert(IST)

    normalized = normalized.dropna(subset=["timestamp"])
    normalized = normalized.loc[
        normalized["timestamp"].dt.time.between(SESSION_START, SESSION_END)
    ]

    if normalized.empty:
        return None

    normalized = normalized.sort_values("timestamp")
    one_minute_close = (
        normalized.set_index("timestamp")["price"]
        .resample("1min")
        .last()
        .dropna()
        .astype("float64")
    )
    one_minute_close.index.name = "timestamp"
    return one_minute_close if not one_minute_close.empty else None


def _read_contract_1min_close(path: str, retries: int = READ_RETRIES) -> pd.Series | None:
    """Read one contract with short retry handling for transient I/O errors."""
    last_error: Exception | None = None

    for attempt in range(retries + 1):
        try:
            columns = _pick_projection_columns(path)
            frame = pd.read_parquet(path, columns=columns) if columns else pd.read_parquet(path)
            return _normalize_contract_frame(frame)
        except Exception as exc:
            last_error = exc
            if attempt < retries:
                time.sleep(0.25 * (2**attempt))

    if last_error is not None:
        raise RuntimeError(f"Failed to read contract after {retries + 1} attempts: {path}") from last_error
    return None


def _read_contract_job(contract: tuple[int, str, str]) -> ContractReadResult:
    strike, side, path = contract
    try:
        return ContractReadResult(
            strike=int(strike),
            side=str(side).upper(),
            path=str(path),
            series=_read_contract_1min_close(str(path)),
        )
    except Exception as exc:
        return ContractReadResult(
            strike=int(strike),
            side=str(side).upper(),
            path=str(path),
            series=None,
            error=f"{type(exc).__name__}: {exc}",
        )


def discover_expiries(folder: str, date_str: str, instrument: str = "NIFTY") -> list[str]:
    """Discover yymmdd expiry tokens under a date's option folder."""
    config = get_dataset_config(instrument)
    symbol = str(config["symbol"]).upper()
    option_folder = _get_opt_folder(folder, date_str)

    if not os.path.isdir(option_folder):
        return []

    pattern = re.compile(
        rf"^{re.escape(symbol)}(\d{{6}})(\d+)(CE|PE)$",
        re.IGNORECASE,
    )

    expiries: set[str] = set()
    for _, _, filenames in os.walk(option_folder):
        for filename in filenames:
            if not filename.lower().endswith(".parquet"):
                continue
            match = pattern.match(Path(filename).stem.upper())
            if match:
                expiries.add(match.group(1))

    return sorted(expiries)


def _read_all_contracts(
    contracts: Sequence[tuple[int, str, str]],
    executor: ProcessPoolExecutor | None,
    fail_fast: bool,
) -> tuple[dict[tuple[int, str], pd.Series], list[str]]:
    series_map: dict[tuple[int, str], pd.Series] = {}
    errors: list[str] = []

    if executor is None:
        results: Iterable[ContractReadResult] = map(_read_contract_job, contracts)
    else:
        futures = {executor.submit(_read_contract_job, contract): contract for contract in contracts}
        results = (future.result() for future in as_completed(futures))

    for result in results:
        if result.error:
            message = f"{result.strike}{result.side} {result.path}: {result.error}"
            errors.append(message)
            LOGGER.error("Contract read failed: %s", message)
            if fail_fast:
                raise RuntimeError(message)
            continue

        if result.series is not None and not result.series.empty:
            series_map[(result.strike, result.side)] = result.series

    return series_map, errors


def _assemble_output(series_map: dict[tuple[int, str], pd.Series]) -> pd.DataFrame:
    empty = pd.Series(dtype="float64")
    frames: list[pd.DataFrame] = []

    for strike in sorted({key[0] for key in series_map}):
        ce_series = series_map.get((strike, "CE"), empty)
        pe_series = series_map.get((strike, "PE"), empty)

        merged = pd.concat({"ce": ce_series, "pe": pe_series}, axis=1)
        if merged.empty:
            continue

        merged.index.name = "timestamp"
        merged = merged.reset_index()
        merged["strike"] = int(strike)
        frames.append(merged[OUTPUT_COLUMNS])

    if not frames:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    output = pd.concat(frames, ignore_index=True, copy=False)
    output["timestamp"] = pd.to_datetime(output["timestamp"], errors="coerce")
    output = output.dropna(subset=["timestamp", "strike"])

    if output["timestamp"].dt.tz is not None:
        output["timestamp"] = output["timestamp"].dt.tz_convert(IST).dt.tz_localize(None)

    output["strike"] = pd.to_numeric(output["strike"], errors="coerce").astype("int32")
    output["ce"] = pd.to_numeric(output["ce"], errors="coerce").astype("float64")
    output["pe"] = pd.to_numeric(output["pe"], errors="coerce").astype("float64")

    output = (
        output.sort_values(["timestamp", "strike"], kind="mergesort")
        .drop_duplicates(["timestamp", "strike"], keep="last")
        .reset_index(drop=True)
    )
    return output[OUTPUT_COLUMNS]


def _validate_output(output: pd.DataFrame) -> None:
    missing = [column for column in OUTPUT_COLUMNS if column not in output.columns]
    if missing:
        raise ValueError(f"Output is missing required columns: {missing}")
    if output.empty:
        raise ValueError("Consolidated output is empty")
    if output["timestamp"].isna().any():
        raise ValueError("Output contains invalid timestamps")
    if output["strike"].isna().any():
        raise ValueError("Output contains invalid strikes")
    if output.duplicated(["timestamp", "strike"]).any():
        raise ValueError("Output contains duplicate timestamp/strike rows")


def _atomic_write_parquet(
    output: pd.DataFrame,
    output_path: str,
    compression: str,
) -> None:
    destination = Path(output_path)
    destination.parent.mkdir(parents=True, exist_ok=True)

    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.",
        suffix=".tmp",
        dir=str(destination.parent),
    )
    os.close(descriptor)

    try:
        output.to_parquet(
            temporary_name,
            engine="pyarrow",
            compression=compression,
            index=False,
        )

        # Verify the file is readable before replacing the production file.
        verification = pd.read_parquet(temporary_name, columns=["timestamp", "strike"])
        if len(verification) != len(output):
            raise RuntimeError(
                f"Parquet verification failed: expected {len(output)} rows, "
                f"read {len(verification)}"
            )

        os.replace(temporary_name, destination)
    finally:
        if os.path.exists(temporary_name):
            os.remove(temporary_name)


def build_for_expiry(
    week_folder: str,
    date_str: str,
    expiry_str: str,
    instrument: str = "NIFTY",
    overwrite: bool = False,
    executor: ProcessPoolExecutor | None = None,
    compression: str = DEFAULT_COMPRESSION,
    fail_fast: bool = False,
    dry_run: bool = False,
) -> BuildResult:
    started = time.perf_counter()
    output_path = consolidated_chain_path(week_folder, date_str, expiry_str, instrument)
    result = BuildResult(
        week_folder=week_folder,
        date=date_str,
        expiry=expiry_str,
        instrument=instrument,
        output_path=output_path,
        status="pending",
    )

    try:
        if os.path.isfile(output_path) and not overwrite:
            result.status = "skipped_existing"
            return result

        contracts = find_option_contract_files(
            week_folder,
            date_str,
            expiry_str,
            instrument,
        )
        result.contracts_discovered = len(contracts)

        if not contracts:
            result.status = "no_contracts"
            return result

        if dry_run:
            result.status = "dry_run"
            return result

        series_map, errors = _read_all_contracts(contracts, executor, fail_fast)
        result.contracts_loaded = len(series_map)

        if not series_map:
            result.status = "no_usable_rows"
            result.error = "; ".join(errors[:5]) if errors else None
            return result

        output = _assemble_output(series_map)
        _validate_output(output)

        _atomic_write_parquet(output, output_path, compression)

        result.status = "written"
        result.rows_written = len(output)
        result.unique_strikes = int(output["strike"].nunique())
        if errors:
            result.error = f"{len(errors)} contract(s) failed; output built from remaining files"
        return result

    except Exception as exc:
        result.status = "failed"
        result.error = f"{type(exc).__name__}: {exc}"
        LOGGER.exception(
            "Build failed for instrument=%s date=%s expiry=%s",
            instrument,
            date_str,
            expiry_str,
        )
        if fail_fast:
            raise
        return result
    finally:
        result.elapsed_seconds = round(time.perf_counter() - started, 3)


def build_for_date(
    week_folder: str,
    date_str: str,
    instrument: str,
    expiry: str | None,
    overwrite: bool,
    executor: ProcessPoolExecutor | None,
    compression: str,
    fail_fast: bool,
    dry_run: bool,
) -> list[BuildResult]:
    expiries = [expiry] if expiry else discover_expiries(week_folder, date_str, instrument)
    if not expiries:
        return [
            BuildResult(
                week_folder=week_folder,
                date=date_str,
                expiry=expiry or "",
                instrument=instrument,
                output_path=None,
                status="no_expiries",
            )
        ]

    results: list[BuildResult] = []
    for expiry_str in expiries:
        result = build_for_expiry(
            week_folder=week_folder,
            date_str=date_str,
            expiry_str=expiry_str,
            instrument=instrument,
            overwrite=overwrite,
            executor=executor,
            compression=compression,
            fail_fast=fail_fast,
            dry_run=dry_run,
        )
        results.append(result)
        LOGGER.info(
            "%s | date=%s expiry=%s contracts=%s/%s rows=%s strikes=%s %.2fs",
            result.status,
            result.date,
            result.expiry,
            result.contracts_loaded,
            result.contracts_discovered,
            result.rows_written,
            result.unique_strikes,
            result.elapsed_seconds,
        )
    return results


def build_all(
    instrument: str = "NIFTY",
    date: str | None = None,
    expiry: str | None = None,
    week: int | None = None,
    overwrite: bool = False,
    workers: int = DEFAULT_MAX_WORKERS,
    compression: str = DEFAULT_COMPRESSION,
    fail_fast: bool = False,
    dry_run: bool = False,
) -> list[BuildResult]:
    started = time.perf_counter()
    folders = get_week_folders(instrument=instrument)

    if week is not None:
        folders = [(week_no, folder) for week_no, folder in folders if int(week_no) == int(week)]

    if not folders:
        LOGGER.warning("No matching week folders found for %s", instrument)
        return []

    workers = max(1, int(workers))
    results: list[BuildResult] = []
    executor: ProcessPoolExecutor | None = None

    try:
        if workers > 1:
            executor = ProcessPoolExecutor(max_workers=workers)
            LOGGER.info("Using %s worker processes", workers)
        else:
            LOGGER.info("Running serially")

        for week_no, folder in folders:
            dates = get_dates_for_week_folder(week_no, folder, instrument=instrument)
            if date:
                dates = [current_date for current_date in dates if current_date == date]

            LOGGER.info("Week %s | %s | dates=%s", week_no, folder, len(dates))
            for date_str in dates:
                results.extend(
                    build_for_date(
                        week_folder=folder,
                        date_str=date_str,
                        instrument=instrument,
                        expiry=expiry,
                        overwrite=overwrite,
                        executor=executor,
                        compression=compression,
                        fail_fast=fail_fast,
                        dry_run=dry_run,
                    )
                )
    finally:
        if executor is not None:
            executor.shutdown(wait=True, cancel_futures=False)

    LOGGER.info("Finished %s build target(s) in %.2fs", len(results), time.perf_counter() - started)
    return results


def _write_report(results: Sequence[BuildResult], report_path: str | None) -> None:
    if not report_path:
        return

    destination = Path(report_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": pd.Timestamp.utcnow().isoformat(),
        "results": [asdict(result) for result in results],
    }
    destination.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build production consolidated option-chain Parquet files."
    )
    parser.add_argument("--instrument", default="NIFTY")
    parser.add_argument("--date", default=None, help="Single trade date YYYYMMDD")
    parser.add_argument("--expiry", default=None, help="Single expiry yymmdd")
    parser.add_argument("--week", type=int, default=None, help="Single week number")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--workers", type=int, default=DEFAULT_MAX_WORKERS)
    parser.add_argument(
        "--compression",
        choices=("snappy", "gzip", "brotli", "zstd", "none"),
        default=DEFAULT_COMPRESSION,
    )
    parser.add_argument("--fail-fast", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--report", default=None, help="Optional JSON report path")
    parser.add_argument(
        "--log-level",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        default="INFO",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    configure_logging(args.log_level)

    compression = None if args.compression == "none" else args.compression
    results = build_all(
        instrument=args.instrument.upper(),
        date=args.date,
        expiry=args.expiry,
        week=args.week,
        overwrite=args.overwrite,
        workers=args.workers,
        compression=compression,
        fail_fast=args.fail_fast,
        dry_run=args.dry_run,
    )
    _write_report(results, args.report)

    failed = [result for result in results if result.status == "failed"]
    written = sum(result.status == "written" for result in results)
    skipped = sum(result.status == "skipped_existing" for result in results)

    LOGGER.info(
        "Summary | written=%s skipped=%s failed=%s total=%s",
        written,
        skipped,
        len(failed),
        len(results),
    )
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
