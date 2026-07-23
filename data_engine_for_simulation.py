

import hashlib
import io
import logging
import os
import re
import time
from collections import OrderedDict
from pathlib import Path
from threading import Lock, RLock
from typing import Any, Optional

import numpy as np
import pandas as pd

from config_for_simulation import (
    IST,
    SESSION_START,
    SESSION_END,
    PARQUET_BASE_PATH,
    OPTION_PARQUET_BASE_PATH,
    get_dataset_config,
)

try:
    from config_for_simulation import (
        DATA_LAYOUT,
        OPT_SEGMENT_NAME,
        FUT_SEGMENT_NAME,
        IDX_SEGMENT_NAME,
    )
except ImportError:
    DATA_LAYOUT = "date_segment"
    OPT_SEGMENT_NAME = "OPT_TICK"
    FUT_SEGMENT_NAME = "FUT_TICK"
    IDX_SEGMENT_NAME = "IDX_TICK"


from azure_blob_storage import (
    blob_exists,
    find_blob_by_filename,
    list_blob_names,
    read_parquet_blob,
)

logger = logging.getLogger(__name__)

STORAGE_MODE = os.getenv("STORAGE_MODE", "local").strip().lower()
if STORAGE_MODE not in {"local", "blob"}:
    raise ValueError(
        "STORAGE_MODE must be either 'local' or 'blob'. "
        f"Received: {STORAGE_MODE!r}"
    )

DEBUG_MODE = os.getenv("DEBUG_MODE", "false").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}

MAX_RAW_PARQUET_CACHE_SIZE = max(1, int(os.getenv("MAX_RAW_PARQUET_CACHE_SIZE", "128")))
MAX_OPTION_CHAIN_CACHE_SIZE = max(1, int(os.getenv("MAX_OPTION_CHAIN_CACHE_SIZE", "16")))


class _ThreadSafeLRU:
    """Small process-local LRU used for metadata and DataFrame caches.

    The cache is intentionally bounded. Values are returned by reference; callers that
    mutate a cached DataFrame must first make a copy. The data engine treats cached
    frames as read-only master objects and only mutates timestamp-specific slices.
    """

    def __init__(self, max_entries: int):
        self.max_entries = max(1, int(max_entries))
        self._items: "OrderedDict[Any, Any]" = OrderedDict()
        self._lock = RLock()

    def get(self, key, default=None):
        with self._lock:
            if key not in self._items:
                return default
            value = self._items.pop(key)
            self._items[key] = value
            return value

    def put(self, key, value):
        with self._lock:
            if key in self._items:
                self._items.pop(key)
            self._items[key] = value
            while len(self._items) > self.max_entries:
                self._items.popitem(last=False)

    def pop(self, key, default=None):
        with self._lock:
            return self._items.pop(key, default)

    def clear(self):
        with self._lock:
            self._items.clear()

    def __len__(self):
        with self._lock:
            return len(self._items)

    def stats(self) -> dict:
        with self._lock:
            approx_bytes = 0
            for value in self._items.values():
                if isinstance(value, pd.DataFrame):
                    approx_bytes += int(value.memory_usage(index=True, deep=True).sum())
            return {
                "entries": len(self._items),
                "max_entries": self.max_entries,
                "approx_bytes": approx_bytes,
            }


PARQUET_FILE_PATH_CACHE: dict[tuple[str, str], Optional[str]] = {}
RAW_PARQUET_CACHE = _ThreadSafeLRU(MAX_RAW_PARQUET_CACHE_SIZE)
OPTION_PARQUET_CACHE = _ThreadSafeLRU(MAX_OPTION_CHAIN_CACHE_SIZE)
OPTION_CONTRACT_CACHE = _ThreadSafeLRU(MAX_OPTION_CHAIN_CACHE_SIZE)

# Option-contract manifest cache. Building the manifest requires either a
# recursive local filesystem walk or an Azure Blob listing, so it must never
# happen once per contract lookup. A finite TTL allows newly uploaded files to
# become visible without restarting the service. Set the TTL to 0 to keep a
# manifest until it is explicitly invalidated.
_OPTION_CONTRACT_INDEX_CACHE = {}
_OPTION_CONTRACT_INDEX_CACHE_LOCK = Lock()
OPTION_CONTRACT_INDEX_TTL_SECONDS = max(
    0.0,
    float(os.getenv("OPTION_CONTRACT_INDEX_TTL_SECONDS", "300")),
)
_OPTION_CONTRACT_FILENAME_PATTERN = re.compile(
    r"^(.+?)(\d{6})(\d+(?:\.\d+)?)(CE|PE)$",
    re.IGNORECASE,
)


# =========================================================
# PATH HELPERS
# =========================================================

def is_path_allowed(path, instrument="NIFTY"):
    """Validate a local path or Blob prefix before reading market data."""
    if STORAGE_MODE == "blob":
        value = str(path).replace("\\", "/").strip("/")
        parts = [part for part in value.split("/") if part]
        return bool(value) and ".." not in parts

    cfg = get_dataset_config(instrument)
    roots = [
        os.path.abspath(str(cfg.get("base_path") or PARQUET_BASE_PATH)),
        os.path.abspath(str(OPTION_PARQUET_BASE_PATH)),
    ]
    target = os.path.abspath(os.path.expanduser(str(path)))

    if not os.path.exists(target) and target.lower().endswith(".zip"):
        target = os.path.dirname(target)

    for root in roots:
        try:
            if os.path.commonpath([root, target]) == root:
                return True
        except ValueError:
            # Different Windows drives, or otherwise incomparable roots.
            continue
    return False


def _extract_date_from_text(text):
    match = re.search(r"(\d{8})", str(text))
    return match.group(1) if match else None


def _join_storage_path(*parts):
    cleaned = [
        str(part).replace("\\", "/").strip("/")
        for part in parts
        if part is not None and str(part).strip()
    ]

    return "/".join(cleaned)


def _segment_folder_candidates(week_folder, segment_name, date_str=None):
    """Return the strict week-166 date-first segment folder."""
    if not date_str:
        raise ValueError("date_str is required for date_segment layout")
    date_value = str(date_str).strip()
    if not re.fullmatch(r"\d{8}", date_value):
        raise ValueError(f"date_str must be YYYYMMDD, got {date_str!r}")
    if STORAGE_MODE == "blob":
        week_value = str(week_folder).replace("\\", "/").strip("/")
        return [_join_storage_path(week_value, date_value, segment_name)]
    week_value = os.path.abspath(os.path.expanduser(str(week_folder)))
    return [os.path.join(week_value, date_value, segment_name)]


def _resolve_segment_folder(week_folder, segment_name, date_str=None):
    candidates = _segment_folder_candidates(
        week_folder,
        segment_name,
        date_str,
    )
    if STORAGE_MODE == "blob":
        return candidates[0]
    for candidate in candidates:
        if os.path.isdir(candidate):
            return candidate
    return candidates[0]


def _get_idx_folder(week_folder, date_str=None):
    return _resolve_segment_folder(
        week_folder,
        IDX_SEGMENT_NAME,
        date_str,
    )


def _get_opt_folder(week_folder, date_str=None):
    return _resolve_segment_folder(
        week_folder,
        OPT_SEGMENT_NAME,
        date_str,
    )


def _get_fut_folder(week_folder, date_str=None):
    return _resolve_segment_folder(
        week_folder,
        FUT_SEGMENT_NAME,
        date_str,
    )


def _normalize_storage_folder(folder):
    """Return a stable cache key for a local folder or Blob prefix."""
    value = str(folder).replace("\\", "/").strip("/")

    if STORAGE_MODE == "blob":
        return value

    return os.path.normcase(os.path.abspath(value))


def invalidate_option_contract_index(opt_folder=None):
    """
    Invalidate cached option-contract manifests.

    Pass a specific OPT_TICK folder/prefix to invalidate only that manifest,
    or omit it to clear all manifests. This is useful immediately after a data
    upload or deployment that changes the option files.
    """
    with _OPTION_CONTRACT_INDEX_CACHE_LOCK:
        if opt_folder is None:
            _OPTION_CONTRACT_INDEX_CACHE.clear()
            return

        cache_key = (STORAGE_MODE, _normalize_storage_folder(opt_folder))
        _OPTION_CONTRACT_INDEX_CACHE.pop(cache_key, None)


def _get_option_contract_index(opt_folder):
    """
    Return a cached option-contract manifest for ``opt_folder``.

    The manifest maps ``(symbol, expiry, strike, side)`` to the full local path
    or Azure Blob name. The expensive filesystem walk / Blob listing is done
    only on a cache miss or after the configured TTL expires.
    """
    normalized_folder = _normalize_storage_folder(opt_folder)
    cache_key = (STORAGE_MODE, normalized_folder)
    now = time.monotonic()

    with _OPTION_CONTRACT_INDEX_CACHE_LOCK:
        cached = _OPTION_CONTRACT_INDEX_CACHE.get(cache_key)
        if cached is not None:
            age_seconds = now - cached["created_at"]
            if (
                OPTION_CONTRACT_INDEX_TTL_SECONDS == 0
                or age_seconds < OPTION_CONTRACT_INDEX_TTL_SECONDS
            ):
                return cached["index"]

        index = {}

        if STORAGE_MODE == "blob":
            try:
                file_paths = list_blob_names(normalized_folder)
            except Exception as exc:
                logger.exception(
                    "Unable to list option-contract blobs under %s: %s",
                    normalized_folder,
                    exc,
                )
                file_paths = []
        else:
            if not os.path.isdir(normalized_folder):
                _OPTION_CONTRACT_INDEX_CACHE[cache_key] = {
                    "created_at": now,
                    "index": index,
                }
                return index

            file_paths = (
                os.path.join(root, filename)
                for root, _, files in os.walk(normalized_folder)
                for filename in files
            )

        for file_path in file_paths:
            file_path = str(file_path)
            filename = file_path.replace("\\", "/").rsplit("/", 1)[-1]

            if not filename.lower().endswith(".parquet"):
                continue

            base_name = os.path.splitext(filename)[0].upper()
            match = _OPTION_CONTRACT_FILENAME_PATTERN.fullmatch(base_name)

            if match is None:
                continue

            symbol = match.group(1).upper()
            expiry = match.group(2)

            try:
                strike = int(float(match.group(3)))
            except (TypeError, ValueError, OverflowError):
                continue

            side = match.group(4).upper()
            contract_key = (symbol, expiry, strike, side)

            # Keep the first deterministic match. Duplicate contract files are
            # logged because silently replacing one can make results unstable.
            if contract_key in index:
                logger.warning(
                    "Duplicate option contract %s under %s; keeping %s and "
                    "ignoring %s",
                    contract_key,
                    normalized_folder,
                    index[contract_key],
                    file_path,
                )
                continue

            index[contract_key] = file_path

        _OPTION_CONTRACT_INDEX_CACHE[cache_key] = {
            "created_at": now,
            "index": index,
        }
        logger.info(
            "Built option-contract manifest for %s with %d contracts",
            normalized_folder,
            len(index),
        )
        return index


def _find_parquet_file(folder, filename):
    folder = str(folder).replace("\\", "/").strip("/")
    filename = str(filename)

    key = (folder, filename.lower())

    if key in PARQUET_FILE_PATH_CACHE:
        return PARQUET_FILE_PATH_CACHE[key]

    if STORAGE_MODE == "blob":
        blob_name = find_blob_by_filename(
            prefix=folder,
            filename=filename,
        )

        PARQUET_FILE_PATH_CACHE[key] = blob_name
        return blob_name

    # Local filesystem fallback
    folder = os.path.abspath(folder)

    if not os.path.isdir(folder):
        PARQUET_FILE_PATH_CACHE[key] = None
        return None

    for root, _, files in os.walk(folder):
        for current_file in files:
            if current_file.lower() == filename.lower():
                path = os.path.join(root, current_file)
                PARQUET_FILE_PATH_CACHE[key] = path
                return path

    PARQUET_FILE_PATH_CACHE[key] = None
    return None


OPTION_WEEK_FOLDER_CACHE = {}


def _resolve_option_week_folder(week_folder):
    if STORAGE_MODE == "blob":
        return str(week_folder).replace("\\", "/").strip("/")
    return os.path.abspath(os.path.expanduser(str(week_folder)))


# =========================================================
# MARKET FILE READER (PARQUET + CSV)
# =========================================================

def _read_market_file(path, columns=None):
    """Read Parquet market data from Azure Blob or local storage."""
    path = str(path)
    if Path(path.split("?", 1)[0]).suffix.lower() != ".parquet":
        raise ValueError(f"Only Parquet files are supported: {path}")
    if STORAGE_MODE == "blob":
        return read_parquet_blob(path, columns=columns)
    return pd.read_parquet(os.path.abspath(path), columns=columns)


def _find_market_file(folder, filenames):
    """Find a Parquet file under a local folder or Blob prefix."""
    if isinstance(filenames, str):
        filenames = [filenames]

    wanted = {
        str(name).replace("\\", "/").rsplit("/", 1)[-1].lower()
        for name in filenames
        if name
    }
    if not wanted:
        return None

    normalized_folder = str(folder).replace("\\", "/").strip("/")

    if STORAGE_MODE == "blob":
        try:
            for blob_name in list_blob_names(normalized_folder):
                filename = str(blob_name).replace("\\", "/").rsplit("/", 1)[-1]
                if filename.lower() in wanted:
                    return str(blob_name)
        except Exception as exc:
            logger.warning(
                "Unable to search market files under %s: %s",
                normalized_folder,
                exc,
            )
        return None

    local_folder = os.path.abspath(os.path.expanduser(normalized_folder))
    if not os.path.isdir(local_folder):
        return None

    for root, _, files in os.walk(local_folder):
        for filename in files:
            if filename.lower() in wanted:
                return os.path.join(root, filename)
    return None


def _candidate_segment_folders(week_folder, segment_name, date_str=None):
    """Return every supported week-166/legacy folder candidate."""
    return _segment_folder_candidates(
        week_folder=week_folder,
        segment_name=segment_name,
        date_str=date_str,
    )

# =========================================================
# PARQUET READER
# =========================================================

def _read_parquet_normalized(path, mode="spot"):
    path = str(path)
    mode = str(mode).lower()
    cache_key = (path, mode)

    cached = RAW_PARQUET_CACHE.get(cache_key)

    if cached is not None:
        # Shallow wrapper: avoids duplicating the full cached frame. Treat cached
        # columns as read-only; make a deep copy only before destructive mutation.
        return cached.copy(deep=False)

    df = _read_market_file(path)

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

    if mode == "spot":
        value_col = (
            lower_cols.get("value")
            or lower_cols.get("ltp")
            or lower_cols.get("price")
            or lower_cols.get("close")
            or df.columns[2]
        )

        out = pd.DataFrame({
            "datetime": dt,
            "value": pd.to_numeric(df[value_col], errors="coerce"),
        }).dropna(subset=["datetime", "value"])

    else:
        price_col = (
            lower_cols.get("price")
            or lower_cols.get("ltp")
            or lower_cols.get("value")
            or lower_cols.get("close")
            or df.columns[2]
        )

        volume_col = (
            lower_cols.get("volume")
            or lower_cols.get("qty")
            or lower_cols.get("quantity")
        )

        out = pd.DataFrame({
            "datetime": dt,
            "price": pd.to_numeric(df[price_col], errors="coerce"),
            "volume": pd.to_numeric(df[volume_col], errors="coerce") if volume_col else 0,
        }).dropna(subset=["datetime", "price"])

    if out.empty:
        return out

    if out["datetime"].dt.tz is None:
        out["datetime"] = out["datetime"].dt.tz_localize(
            IST,
            ambiguous="NaT",
            nonexistent="NaT",
        )
    else:
        out["datetime"] = out["datetime"].dt.tz_convert(IST)

    out = out.dropna(subset=["datetime"])
    out = out[
        (out["datetime"].dt.time >= SESSION_START)
        & (out["datetime"].dt.time <= SESSION_END)
    ]

    out = out.sort_values("datetime").reset_index(drop=True)

    RAW_PARQUET_CACHE.put(cache_key, out)
    return out.copy(deep=False)


# =========================================================
# INDEX / SPOT LOADERS - OLD PATH
# =========================================================

def load_tick_data(folder_or_path, instrument="NIFTY"):
    """Load spot data from a direct Parquet file path."""
    if not is_path_allowed(folder_or_path, instrument):
        raise PermissionError(f"Access denied: {folder_or_path}")
    input_path = str(folder_or_path)
    if not input_path.lower().endswith(".parquet"):
        return pd.DataFrame(columns=["datetime", "value"])
    if STORAGE_MODE == "local" and not os.path.isfile(input_path):
        return pd.DataFrame(columns=["datetime", "value"])
    return _read_parquet_normalized(input_path, mode="spot")


def load_index_data_by_symbol(folder, date_str, symbol_name="INDIAVIX"):
    """Load <week>/<YYYYMMDD>/IDX_TICK/<SYMBOL>.parquet."""
    symbol_name = str(symbol_name).upper().strip()
    date_value = str(date_str).strip()
    idx_folder = _get_idx_folder(folder, date_value)
    filename = f"{symbol_name}.parquet"
    path = _join_storage_path(idx_folder, filename) if STORAGE_MODE == "blob" else os.path.join(idx_folder, filename)
    try:
        exists = blob_exists(path) if STORAGE_MODE == "blob" else os.path.isfile(path)
    except Exception as exc:
        logger.exception("Unable to check index file %s: %s", path, exc)
        exists = False
    if not exists:
        logger.warning("Index Parquet not found: %s", path)
        return pd.DataFrame(columns=["datetime", "value"])
    try:
        frame = _read_parquet_normalized(path, mode="spot")
    except Exception as exc:
        logger.exception("Unable to read index Parquet %s: %s", path, exc)
        return pd.DataFrame(columns=["datetime", "value"])
    if frame.empty:
        return frame
    return frame[frame["datetime"].dt.strftime("%Y%m%d") == date_value].reset_index(drop=True)


# =========================================================
# OPTIONS - NEW OPTION PATH ONLY
# =========================================================

CONSOLIDATED_SCHEMA_VERSION = 2


def consolidated_chain_folder(week_folder, date_str):
    option_week_folder = _resolve_option_week_folder(week_folder)
    return _get_opt_folder(option_week_folder, date_str)


def consolidated_chain_path(
    week_folder,
    date_str,
    expiry_str,
    instrument="NIFTY",
):
    """Return the consolidated option-chain path or blob name."""
    cfg = get_dataset_config(instrument)
    symbol = str(cfg["symbol"]).upper()
    option_folder = consolidated_chain_folder(
        week_folder,
        date_str,
    )
    filename = f"{symbol}_{expiry_str}.parquet"

    if STORAGE_MODE == "blob":
        return _join_storage_path(option_folder, filename)

    return os.path.join(option_folder, filename)


_OPTION_CHAIN_CACHE = _ThreadSafeLRU(MAX_OPTION_CHAIN_CACHE_SIZE)
_OPTION_CHAIN_CACHE_LOCK = RLock()

_DEFAULT_CACHE_DIR = (
    Path.home()
    / ".cache"
    / "option-simulator"
    / "option_chain"
)

SHARED_OPTION_CACHE_DIR = os.path.abspath(
    os.path.expanduser(
        os.getenv(
            "SHARED_OPTION_CACHE_DIR",
            str(_DEFAULT_CACHE_DIR),
        )
    )
)

os.makedirs(
    SHARED_OPTION_CACHE_DIR,
    exist_ok=True,
)


def _option_chain_disk_cache_path(cache_key):
    key_hash = hashlib.md5(
        cache_key.encode("utf-8"),
        usedforsecurity=False,
    ).hexdigest()
    return os.path.join(SHARED_OPTION_CACHE_DIR, f"{key_hash}.parquet")


def _normalize_timestamp_series(values: pd.Series) -> pd.Series:
    ts = pd.to_datetime(values, errors="coerce")
    if getattr(ts.dt, "tz", None) is None:
        return ts.dt.tz_localize(IST, ambiguous="NaT", nonexistent="NaT")
    return ts.dt.tz_convert(IST)


def _normalize_wide_consolidated(df: pd.DataFrame, date_str: str) -> Optional[pd.DataFrame]:
    """Normalize schema [timestamp, strike, ce, pe]."""
    required = {"timestamp", "strike", "ce", "pe"}
    if not required.issubset(df.columns):
        return None

    out = df.loc[:, ["timestamp", "strike", "ce", "pe"]].copy()
    out["timestamp"] = _normalize_timestamp_series(out["timestamp"])
    out["strike"] = pd.to_numeric(out["strike"], errors="coerce")
    out["ce"] = pd.to_numeric(out["ce"], errors="coerce")
    out["pe"] = pd.to_numeric(out["pe"], errors="coerce")
    out = out.dropna(subset=["timestamp", "strike"])

    requested = pd.to_datetime(str(date_str), format="%Y%m%d", errors="coerce")
    if pd.isna(requested):
        return None
    out = out[out["timestamp"].dt.strftime("%Y%m%d") == requested.strftime("%Y%m%d")]
    out = out[
        (out["timestamp"].dt.time >= SESSION_START)
        & (out["timestamp"].dt.time <= SESSION_END)
    ]
    if out.empty:
        return None

    out["strike"] = out["strike"].astype("int32")
    return (
        out.drop_duplicates(subset=["timestamp", "strike"], keep="last")
        .sort_values(["timestamp", "strike"], kind="mergesort")
        .reset_index(drop=True)
    )


def _normalize_long_consolidated(df: pd.DataFrame, date_str: str) -> Optional[pd.DataFrame]:
    """Normalize legacy schema [date, time, strike, option_type, price]."""
    required = {"date", "time", "strike", "option_type", "price"}
    if not required.issubset(df.columns):
        return None

    work = df.loc[:, ["date", "time", "strike", "option_type", "price"]].copy()
    work["date"] = pd.to_numeric(work["date"], errors="coerce")
    requested_date = pd.to_numeric(date_str, errors="coerce")
    if pd.isna(requested_date):
        return None
    work = work[work["date"] == int(requested_date)]
    if work.empty:
        return None

    work["timestamp"] = _normalize_timestamp_series(
        work["date"].astype("Int64").astype(str) + " " + work["time"].astype(str)
    )
    work["strike"] = pd.to_numeric(work["strike"], errors="coerce")
    work["price"] = pd.to_numeric(work["price"], errors="coerce")
    work["option_type"] = work["option_type"].astype(str).str.upper().str.strip()
    work = work.dropna(subset=["timestamp", "strike", "price"])
    work = work[work["option_type"].isin(["CE", "PE"])]
    work = work[
        (work["timestamp"].dt.time >= SESSION_START)
        & (work["timestamp"].dt.time <= SESSION_END)
    ]
    if work.empty:
        return None

    work["strike"] = work["strike"].astype("int32")
    # Last tick in each minute for each strike/side.
    work["timestamp"] = work["timestamp"].dt.floor("min")
    work = (
        work.sort_values("timestamp", kind="mergesort")
        .drop_duplicates(["timestamp", "strike", "option_type"], keep="last")
    )
    out = work.pivot_table(
        index=["timestamp", "strike"],
        columns="option_type",
        values="price",
        aggfunc="last",
    ).reset_index()
    out.columns.name = None
    out = out.rename(columns={"CE": "ce", "PE": "pe"})
    if "ce" not in out.columns:
        out["ce"] = np.nan
    if "pe" not in out.columns:
        out["pe"] = np.nan
    return out[["timestamp", "strike", "ce", "pe"]].sort_values(
        ["timestamp", "strike"], kind="mergesort"
    ).reset_index(drop=True)


def _atomic_write_parquet(df: pd.DataFrame, destination: str) -> None:
    os.makedirs(os.path.dirname(destination), exist_ok=True)
    tmp_path = f"{destination}.{os.getpid()}.{time.time_ns()}.tmp"
    try:
        df.to_parquet(tmp_path, engine="pyarrow", compression="snappy", index=False)
        os.replace(tmp_path, destination)
    finally:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except OSError:
            pass


def load_consolidated_option_chain(
    folder,
    date_str,
    expiry_str,
    instrument="NIFTY",
):
    """Load one consolidated option chain as [timestamp, strike, ce, pe].

    Supports both the production wide schema generated by the current builder and
    the older long schema. The normalized frame is cached in a bounded process-local
    LRU and in an atomic local Parquet cache.
    """
    path = consolidated_chain_path(folder, date_str, expiry_str, instrument)
    cache_key = f"v{CONSOLIDATED_SCHEMA_VERSION}|{STORAGE_MODE}|{path}|{date_str}|{instrument.upper()}"
    disk_cache_path = _option_chain_disk_cache_path(cache_key)

    cached = _OPTION_CHAIN_CACHE.get(cache_key)
    if cached is not None:
        return cached.copy(deep=False)

    if os.path.isfile(disk_cache_path):
        try:
            cached_disk = pd.read_parquet(
                disk_cache_path,
                columns=["timestamp", "strike", "ce", "pe"],
            )
            normalized = _normalize_wide_consolidated(cached_disk, date_str)
            if normalized is not None and not normalized.empty:
                _OPTION_CHAIN_CACHE.put(cache_key, normalized)
                return normalized.copy(deep=False)
        except Exception as exc:
            logger.warning("Discarding invalid option-chain disk cache %s: %s", disk_cache_path, exc)
            try:
                os.remove(disk_cache_path)
            except OSError:
                pass

    if STORAGE_MODE == "blob":
        try:
            if not blob_exists(path):
                return None
        except Exception as exc:
            logger.exception("Unable to check consolidated option-chain blob %s: %s", path, exc)
            return None
    elif not os.path.isfile(path):
        return None

    df = None
    # Prefer the wide production schema and projected reads.
    projections = [
        ["timestamp", "strike", "ce", "pe"],
        ["date", "time", "strike", "option_type", "price"],
        None,
    ]
    last_error = None
    for columns in projections:
        try:
            if STORAGE_MODE == "blob":
                df = read_parquet_blob(path, columns=columns)
            else:
                df = pd.read_parquet(path, columns=columns)
            break
        except Exception as exc:
            last_error = exc
            df = None

    if df is None:
        logger.error("Unable to read consolidated option chain %s: %s", path, last_error)
        return None
    if df.empty:
        return None

    normalized = _normalize_wide_consolidated(df, date_str)
    if normalized is None:
        normalized = _normalize_long_consolidated(df, date_str)
    if normalized is None or normalized.empty:
        logger.error(
            "Unsupported or empty consolidated schema in %s; columns=%s",
            path,
            list(df.columns),
        )
        return None

    _OPTION_CHAIN_CACHE.put(cache_key, normalized)
    try:
        _atomic_write_parquet(normalized, disk_cache_path)
    except Exception as exc:
        logger.warning("Unable to save option-chain disk cache %s: %s", disk_cache_path, exc)

    return normalized.copy(deep=False)


def load_required_option_data_for_date(
    folder,
    date_str,
    expiry_str,
    strike,
    instrument="NIFTY",
):
    empty = {
        "CE": pd.DataFrame(
            columns=["datetime", "price", "volume"]
        ),
        "PE": pd.DataFrame(
            columns=["datetime", "price", "volume"]
        ),
    }

    cfg = get_dataset_config(instrument)
    symbol = str(cfg["symbol"]).upper()
    opt_folder = _get_opt_folder(folder, date_str)

    result = {}
    contract_index = _get_option_contract_index(opt_folder)

    try:
        normalized_strike = int(float(strike))
    except (TypeError, ValueError, OverflowError):
        logger.warning("Invalid option strike requested: %r", strike)
        return empty

    normalized_expiry = str(expiry_str).strip()

    for side in ["CE", "PE"]:
        # -----------------------------------------------------
        # 1. Resolve the option contract from the cached manifest
        # -----------------------------------------------------
        matched_path = contract_index.get(
            (symbol, normalized_expiry, normalized_strike, side)
        )

        if not matched_path:
            result[side] = empty[side]
            continue

        # -----------------------------------------------------
        # 2. Read the Parquet file
        # -----------------------------------------------------

        try:
            df = _read_market_file(matched_path)

        except Exception as exc:
            print(
                f"Unable to read option file: {matched_path}. "
                f"Error: {exc}",
                flush=True,
            )

            result[side] = empty[side]
            continue

        if df is None or df.empty:
            result[side] = empty[side]
            continue

        # -----------------------------------------------------
        # 3. Detect columns
        # -----------------------------------------------------

        lower_cols = {
            str(column).lower(): column
            for column in df.columns
        }

        # Create timestamp
        if "datetime" in lower_cols:
            dt = pd.to_datetime(
                df[lower_cols["datetime"]],
                errors="coerce",
            )

        elif {
            "date",
            "time",
        }.issubset(lower_cols):
            dt = pd.to_datetime(
                df[lower_cols["date"]].astype(str)
                + " "
                + df[lower_cols["time"]].astype(str),
                errors="coerce",
            )

        elif len(df.columns) >= 2:
            dt = pd.to_datetime(
                df.iloc[:, 0].astype(str)
                + " "
                + df.iloc[:, 1].astype(str),
                errors="coerce",
            )

        else:
            result[side] = empty[side]
            continue

        price_col = (
            lower_cols.get("price")
            or lower_cols.get("ltp")
            or lower_cols.get("value")
            or lower_cols.get("close")
        )

        if price_col is None:
            if len(df.columns) >= 3:
                price_col = df.columns[2]
            else:
                result[side] = empty[side]
                continue

        volume_col = (
            lower_cols.get("volume")
            or lower_cols.get("qty")
            or lower_cols.get("quantity")
        )

        # -----------------------------------------------------
        # 4. Normalize option data
        # -----------------------------------------------------

        out = pd.DataFrame(
            {
                "datetime": dt,
                "price": pd.to_numeric(
                    df[price_col],
                    errors="coerce",
                ),
                "volume": (
                    pd.to_numeric(
                        df[volume_col],
                        errors="coerce",
                    ).fillna(0)
                    if volume_col is not None
                    else 0
                ),
            }
        )

        out = out.dropna(
            subset=[
                "datetime",
                "price",
            ]
        )

        if out.empty:
            result[side] = empty[side]
            continue

        # -----------------------------------------------------
        # 5. Convert timestamps to IST
        # -----------------------------------------------------

        if out["datetime"].dt.tz is None:
            out["datetime"] = (
                out["datetime"].dt.tz_localize(
                    IST,
                    ambiguous="NaT",
                    nonexistent="NaT",
                )
            )
        else:
            out["datetime"] = (
                out["datetime"].dt.tz_convert(IST)
            )

        out = out.dropna(subset=["datetime"])

        # -----------------------------------------------------
        # 6. Filter trading-session records
        # -----------------------------------------------------

        out = out[
            (
                out["datetime"].dt.time
                >= SESSION_START
            )
            & (
                out["datetime"].dt.time
                <= SESSION_END
            )
        ]

        if out.empty:
            result[side] = empty[side]
            continue

        result[side] = (
            out.sort_values("datetime")
            .reset_index(drop=True)
        )

    return {
        "CE": result.get("CE", empty["CE"]),
        "PE": result.get("PE", empty["PE"]),
    }


def load_single_option_file_from_zip(folder, member_name):
    member_name = str(member_name).replace(".csv", ".parquet").replace(".zip", ".parquet")

    path = _find_parquet_file(folder, member_name)

    if not path:
        return pd.DataFrame(columns=["datetime", "price", "volume"])

    return _read_parquet_normalized(path, mode="option")



def find_option_contract_files(
    folder,
    date_str,
    expiry_str,
    instrument="NIFTY",
):
    """Return contracts for an expiry using the cached option manifest."""
    cfg = get_dataset_config(instrument)
    symbol = str(cfg["symbol"]).upper()
    expiry = str(expiry_str).strip()
    opt_folder = _get_opt_folder(folder, date_str)
    contract_index = _get_option_contract_index(opt_folder)

    found = [
        (strike, side, path)
        for (current_symbol, current_expiry, strike, side), path
        in contract_index.items()
        if current_symbol == symbol and current_expiry == expiry
    ]

    # Stable ordering helps callers, tests, and reproducible API responses.
    return sorted(found, key=lambda item: (item[0], item[1], item[2]))


def create_candles(tick_df, interval_minutes):
    """Create session-aligned OHLC candles without copying unrelated columns."""
    if tick_df is None or tick_df.empty:
        return pd.DataFrame(columns=["open", "high", "low", "close"])
    if "datetime" not in tick_df.columns:
        raise ValueError("tick_df must contain a 'datetime' column")
    value_column = "value" if "value" in tick_df.columns else "price" if "price" in tick_df.columns else None
    if value_column is None:
        raise ValueError("tick_df must contain either 'value' or 'price'")

    interval = int(interval_minutes)
    if interval <= 0 or interval > 1440:
        raise ValueError("interval_minutes must be between 1 and 1440")

    df = pd.DataFrame({
        "datetime": pd.to_datetime(tick_df["datetime"], errors="coerce"),
        "value": pd.to_numeric(tick_df[value_column], errors="coerce"),
    }).dropna(subset=["datetime", "value"])
    if df.empty:
        return pd.DataFrame(columns=["open", "high", "low", "close"])

    if getattr(df["datetime"].dt, "tz", None) is None:
        df["datetime"] = df["datetime"].dt.tz_localize(
            IST, ambiguous="NaT", nonexistent="NaT"
        )
    else:
        df["datetime"] = df["datetime"].dt.tz_convert(IST)
    df = df.dropna(subset=["datetime"])

    session_offset = pd.Timedelta(
        hours=SESSION_START.hour,
        minutes=SESSION_START.minute,
        seconds=SESSION_START.second,
    )
    candles = (
        df.set_index("datetime")["value"]
        .sort_index()
        .resample(
            f"{interval}min",
            origin="start_day",
            offset=session_offset,
            label="left",
            closed="left",
        )
        .ohlc()
        .dropna(how="any")
    )
    candles.columns = ["open", "high", "low", "close"]
    return candles


def get_week_folders(instrument="NIFTY"):
    cfg = get_dataset_config(instrument)

    if STORAGE_MODE == "blob":
        week_names = set()

        for blob_name in list_blob_names():
            top_level = blob_name.split("/", 1)[0]

            match = re.match(r"^\s*(\d+)", top_level)

            if not match:
                continue

            week_no = int(match.group(1))

            if cfg["week_start"] <= week_no <= cfg["week_end"]:
                week_names.add((week_no, top_level))

        return sorted(week_names, key=lambda item: item[0])

    base_path = cfg["base_path"]

    if not os.path.isdir(base_path):
        return []

    folders = []

    for name in os.listdir(base_path):
        path = os.path.join(base_path, name)

        if not os.path.isdir(path):
            continue

        match = re.match(r"^\s*(\d+)", name)

        if not match:
            continue

        week_no = int(match.group(1))

        if cfg["week_start"] <= week_no <= cfg["week_end"]:
            folders.append((week_no, path))

    return sorted(folders, key=lambda item: item[0])


# Cache of per-(week folder, instrument) trading-date lists so repeated
# /api/defaults calls don't re-read blobs on every request.
_WEEK_DATES_CACHE = {}
_WEEK_DATES_CACHE_LOCK = Lock()


def _dates_from_frame(d: "pd.DataFrame | None") -> set:
    """
    Extract YYYYMMDD date strings from a frame that has either a `date`
    column (int/str like 20260702) or a `datetime` column.
    """
    dates: set = set()

    if d is None or d.empty:
        return dates

    lower = {str(c).lower(): c for c in d.columns}

    if "date" in lower:
        col = pd.to_numeric(d[lower["date"]], errors="coerce").dropna()
        if not col.empty:
            dates.update(
                col.astype("int64").astype(str).unique().tolist()
            )
            return dates

    if "datetime" in lower:
        dt = pd.to_datetime(d[lower["datetime"]], errors="coerce").dropna()
        if not dt.empty:
            dates.update(dt.dt.strftime("%Y%m%d").unique().tolist())

    return dates


def get_dates_for_week_folder(week_number, folder, instrument="NIFTY"):
    """Discover YYYYMMDD folders directly under the week folder."""
    del week_number, instrument
    prefix = str(folder).replace("\\", "/").strip("/")
    cache_key = (prefix, "date_segment")
    with _WEEK_DATES_CACHE_LOCK:
        cached = _WEEK_DATES_CACHE.get(cache_key)
    if cached is not None:
        return list(cached)
    dates = set()
    if STORAGE_MODE == "blob":
        try:
            for blob_name in list_blob_names(prefix):
                relative = str(blob_name).replace("\\", "/")
                if relative.startswith(prefix + "/"):
                    relative = relative[len(prefix)+1:]
                first = relative.split("/",1)[0]
                if re.fullmatch(r"\d{8}", first) and pd.notna(pd.to_datetime(first, format="%Y%m%d", errors="coerce")):
                    dates.add(first)
        except Exception as exc:
            logger.exception("Unable to discover date folders under %s: %s", prefix, exc)
    else:
        local = os.path.abspath(os.path.expanduser(str(folder)))
        if os.path.isdir(local):
            for name in os.listdir(local):
                if re.fullmatch(r"\d{8}", name) and os.path.isdir(os.path.join(local,name)):
                    if pd.notna(pd.to_datetime(name, format="%Y%m%d", errors="coerce")):
                        dates.add(name)
    result = sorted(dates)
    with _WEEK_DATES_CACHE_LOCK:
        _WEEK_DATES_CACHE[cache_key] = list(result)
    return result


def get_option_chain_snapshot(
    folder,
    date_str,
    expiry_str,
    target_timestamp,
    instrument="NIFTY",
):
    """Return the latest CE/PE values per strike at or before target_timestamp."""
    empty = pd.DataFrame(
        columns=["timestamp", "strike", "ce", "pe"]
    )

    chain = load_consolidated_option_chain(
        folder=folder,
        date_str=date_str,
        expiry_str=expiry_str,
        instrument=instrument,
    )

    if chain is None or chain.empty:
        return empty

    required = {"timestamp", "strike", "ce", "pe"}
    if not required.issubset(chain.columns):
        logger.warning(
            "Option chain missing columns. required=%s available=%s",
            sorted(required),
            list(chain.columns),
        )
        return empty

    frame = chain.loc[
        :,
        ["timestamp", "strike", "ce", "pe"],
    ].copy()

    frame["timestamp"] = pd.to_datetime(
        frame["timestamp"],
        errors="coerce",
    )
    frame["strike"] = pd.to_numeric(
        frame["strike"],
        errors="coerce",
    )
    frame["ce"] = pd.to_numeric(frame["ce"], errors="coerce")
    frame["pe"] = pd.to_numeric(frame["pe"], errors="coerce")
    frame = frame.dropna(subset=["timestamp", "strike"])

    if frame.empty:
        return empty

    if getattr(frame["timestamp"].dt, "tz", None) is None:
        frame["timestamp"] = frame["timestamp"].dt.tz_localize(
            IST,
            ambiguous="NaT",
            nonexistent="NaT",
        )
    else:
        frame["timestamp"] = frame["timestamp"].dt.tz_convert(IST)

    frame = frame.dropna(subset=["timestamp"])
    if frame.empty:
        return empty

    target = pd.Timestamp(target_timestamp)
    if target.tzinfo is None:
        target = target.tz_localize(IST)
    else:
        target = target.tz_convert(IST)

    frame = frame.loc[frame["timestamp"] <= target]
    if frame.empty:
        return empty

    frame["strike"] = frame["strike"].astype("int32")

    frame = (
        frame.sort_values(
            ["strike", "timestamp"],
            kind="mergesort",
        )
        .drop_duplicates(subset=["strike"], keep="last")
        .sort_values("strike", kind="mergesort")
        .reset_index(drop=True)
    )

    return frame.loc[
        :,
        ["timestamp", "strike", "ce", "pe"],
    ]



def clear_runtime_caches(clear_disk_option_cache: bool = False) -> None:
    """Clear process-local caches; optionally remove normalized disk cache files."""
    PARQUET_FILE_PATH_CACHE.clear()
    RAW_PARQUET_CACHE.clear()
    OPTION_PARQUET_CACHE.clear()
    OPTION_CONTRACT_CACHE.clear()
    _OPTION_CHAIN_CACHE.clear()
    OPTION_WEEK_FOLDER_CACHE.clear()
    invalidate_option_contract_index()
    with _WEEK_DATES_CACHE_LOCK:
        _WEEK_DATES_CACHE.clear()

    if clear_disk_option_cache and os.path.isdir(SHARED_OPTION_CACHE_DIR):
        for entry in os.scandir(SHARED_OPTION_CACHE_DIR):
            if entry.is_file() and entry.name.lower().endswith(".parquet"):
                try:
                    os.remove(entry.path)
                except OSError as exc:
                    logger.warning("Unable to remove cache file %s: %s", entry.path, exc)


def runtime_cache_stats() -> dict:
    """Return lightweight process-local cache diagnostics."""
    with _OPTION_CONTRACT_INDEX_CACHE_LOCK:
        manifest_count = len(_OPTION_CONTRACT_INDEX_CACHE)
    with _WEEK_DATES_CACHE_LOCK:
        week_date_entries = len(_WEEK_DATES_CACHE)
    return {
        "storage_mode": STORAGE_MODE,
        "data_layout": DATA_LAYOUT,
        "raw_parquet": RAW_PARQUET_CACHE.stats(),
        "option_chain": _OPTION_CHAIN_CACHE.stats(),
        "path_cache_entries": len(PARQUET_FILE_PATH_CACHE),
        "option_manifest_entries": manifest_count,
        "week_date_cache_entries": week_date_entries,
        "shared_option_cache_dir": SHARED_OPTION_CACHE_DIR,
    }


# =========================================================
# OPTION HELPERS
# =========================================================

def get_upcoming_expiry_np(query_date, instrument="NIFTY", expiry_rule="current expiry"):
    cfg = get_dataset_config(instrument)
    expiries = pd.to_datetime(cfg["combined_expiry"])
    q = pd.Timestamp(query_date).normalize()

    upcoming = expiries[expiries >= q]

    if len(upcoming) == 0:
        return None

    return pd.Timestamp(upcoming[0]).strftime("%y%m%d")


def get_nearest_strike(spot, instrument="NIFTY", expiry_rule="current expiry"):
    step = int(get_dataset_config(instrument)["strike_step"])
    return int(round(float(spot) / step) * step)


def _export_dataframe(df, output_path, output_format="parquet"):
    output_format = str(output_format).strip().lower()

    if output_format == "parquet":
        if not output_path.lower().endswith(".parquet"):
            output_path = f"{output_path}.parquet"
        df.to_parquet(output_path, index=False)
        return output_path

    if output_format == "csv":
        if not output_path.lower().endswith(".csv"):
            output_path = f"{output_path}.csv"
        df.to_csv(output_path, index=False)
        return output_path

    if output_format in {"xlsx", "excel"}:
        if not output_path.lower().endswith(".xlsx"):
            output_path = f"{output_path}.xlsx"
        df.to_excel(output_path, index=False)
        return output_path

    raise ValueError(f"Unsupported output format: {output_format}")


# =========================================================
# FUTURE LOADER - OLD PATH
# =========================================================


def load_future_data_for_date(folder, date_str, month="current", instrument="NIFTY"):
    """Load <week>/<YYYYMMDD>/FUT_TICK/<CONTRACT>.parquet."""
    cfg = get_dataset_config(instrument)
    if not is_path_allowed(folder, instrument):
        raise PermissionError(f"Access denied: {folder}")
    symbol = str(cfg["symbol"]).upper()
    date_value = str(date_str).strip()
    month_value = str(month or "current").strip().lower().replace("-", "_").replace(" ", "_")
    future_folder = _get_fut_folder(folder, date_value)
    if STORAGE_MODE == "blob":
        try:
            paths = list_blob_names(str(future_folder).replace("\\", "/").strip("/"))
        except Exception as exc:
            logger.exception("Unable to list futures under %s: %s", future_folder, exc)
            paths = []
    else:
        paths = [] if not os.path.isdir(future_folder) else (os.path.join(root,f) for root,_,files in os.walk(future_folder) for f in files)
    pattern = re.compile(rf"^{re.escape(symbol)}\d{{2}}(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)FUT$", re.I)
    files=[]
    for p in paths:
        p=str(p); name=p.replace("\\", "/").rsplit("/",1)[-1]
        if not name.lower().endswith(".parquet"):
            continue
        base=os.path.splitext(name)[0].upper()
        if pattern.fullmatch(base):
            files.append((base,p))
    if not files:
        return pd.DataFrame(columns=["datetime","price","volume"])
    order={m:i+1 for i,m in enumerate(["JAN","FEB","MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC"])}
    def rank(name):
        return next((v for m,v in order.items() if f"{m}FUT" in name),999)
    files.sort(key=lambda x: rank(x[0]))
    idx=0 if month_value in {"current","this_month","current_month","near","nearby"} else 1 if month_value in {"next","next_month"} else 2 if month_value in {"far","far_month","next_to_next","next_to_next_month"} else 0
    selected=files[min(idx,len(files)-1)][1]
    try:
        frame=_read_parquet_normalized(selected, mode="option")
    except Exception as exc:
        logger.exception("Unable to read futures Parquet %s: %s", selected, exc)
        return pd.DataFrame(columns=["datetime","price","volume"])
    if frame.empty:
        return frame
    return frame[frame["datetime"].dt.strftime("%Y%m%d") == date_value].reset_index(drop=True)
