

import hashlib
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
    DATA_LAYOUT = "segment_date"
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
    """Return production-first and backward-compatible folder candidates."""
    week_value = str(week_folder)
    date_value = str(date_str).strip() if date_str else None

    if STORAGE_MODE == "blob":
        week_value = week_value.replace("\\", "/").strip("/")
        candidates = []
        if date_value:
            # Production layout is date-first:
            # <week>/<YYYYMMDD>/<SEGMENT>/
            preferred = _join_storage_path(
                week_value, date_value, segment_name
            )
            candidates.extend([
                preferred,
                _join_storage_path(week_value, segment_name, date_value),
                _join_storage_path(week_value, date_value, segment_name),
            ])
        candidates.extend([
            _join_storage_path(week_value, segment_name),
            week_value,
        ])
    else:
        week_value = os.path.abspath(os.path.expanduser(week_value))
        candidates = []
        if date_value:
            # Production layout is date-first:
            # <week>/<YYYYMMDD>/<SEGMENT>/
            preferred = os.path.join(
                week_value, date_value, segment_name
            )
            candidates.extend([
                preferred,
                os.path.join(week_value, segment_name, date_value),
                os.path.join(week_value, date_value, segment_name),
            ])
        candidates.extend([
            os.path.join(week_value, segment_name),
            week_value,
        ])

    return list(dict.fromkeys(str(candidate) for candidate in candidates))


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
    week_folder = str(week_folder).replace("\\", "/").strip("/")

    if STORAGE_MODE == "blob":
        return week_folder

    week_folder_key = os.path.abspath(week_folder)

    cached = OPTION_WEEK_FOLDER_CACHE.get(week_folder_key)
    if cached is not None:
        return cached

    week_name = os.path.basename(
        os.path.normpath(week_folder)
    )

    candidates = [
        week_name,
        week_name.replace(" - PARQUET", " - TICK"),
        week_name.replace("- PARQUET", "- TICK"),
        week_name.replace("PARQUET", "TICK"),
    ]

    for name in candidates:
        option_folder = os.path.join(
            OPTION_PARQUET_BASE_PATH,
            name,
        )

        if os.path.isdir(option_folder):
            OPTION_WEEK_FOLDER_CACHE[
                week_folder_key
            ] = option_folder

            return option_folder

    fallback = week_folder

    OPTION_WEEK_FOLDER_CACHE[
        week_folder_key
    ] = fallback

    return fallback

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

    if STORAGE_MODE == "blob":
        df = read_parquet_blob(path)
    else:
        path = os.path.abspath(path)
        df = pd.read_parquet(path)

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
    cfg = get_dataset_config(instrument)

    if not is_path_allowed(folder_or_path, instrument):
        raise PermissionError(f"Access denied: {folder_or_path}")

    if os.path.isfile(folder_or_path) and str(folder_or_path).lower().endswith(".parquet"):
        return _read_parquet_normalized(folder_or_path, mode="spot")

    input_path = str(folder_or_path)

    if input_path.lower().endswith(".zip"):
        week_folder = os.path.dirname(input_path)
    elif os.path.isfile(input_path):
        week_folder = os.path.dirname(input_path)
    else:
        week_folder = input_path

    date_str = _extract_date_from_text(input_path)

    idx_folder = _get_idx_folder(week_folder, date_str)

    if not date_str:
        possible_names = [
            cfg.get("zip_member", f"{cfg['symbol']}.parquet").replace(".csv", ".parquet"),
            cfg["symbol"],
            f"{cfg['symbol']}.parquet",
        ]

        for name in possible_names:
            path = _find_parquet_file(idx_folder, name)
            if path:
                return _read_parquet_normalized(path, mode="spot")

        if os.path.isdir(idx_folder):
            for root, _, files in os.walk(idx_folder):
                for f in files:
                    if os.path.splitext(f)[0].upper() == cfg["symbol"]:
                        return _read_parquet_normalized(os.path.join(root, f), mode="spot")

        return pd.DataFrame(columns=["datetime", "value"])

    possible_names = [
        f"{cfg['idx_zip_prefix']}{date_str}.parquet",
        f"{cfg['symbol']}{date_str}.parquet",
        f"{date_str}.parquet",
        cfg.get("zip_member", f"{cfg['symbol']}.parquet").replace(".csv", ".parquet"),
        cfg["symbol"],
        f"{cfg['symbol']}.parquet",
    ]

    for name in possible_names:
        path = _find_parquet_file(idx_folder, name)
        if path:
            return _read_parquet_normalized(path, mode="spot")

    if os.path.isdir(idx_folder):
        for root, _, files in os.walk(idx_folder):
            for f in files:
                if os.path.splitext(f)[0].upper() == cfg["symbol"]:
                    return _read_parquet_normalized(os.path.join(root, f), mode="spot")

    return pd.DataFrame(columns=["datetime", "value"])


def load_index_data_by_symbol(
    folder,
    date_str,
    symbol_name="INDIAVIX",
):
    idx_folder = _get_idx_folder(folder, date_str)
    symbol_name = str(symbol_name).upper().strip()

    possible_names = [
        symbol_name,
        f"{symbol_name}.parquet",
    ]

    for filename in possible_names:
        path = _find_parquet_file(
            idx_folder,
            filename,
        )

        if path:
            return _read_parquet_normalized(
                path,
                mode="spot",
            )

    return pd.DataFrame(
        columns=["datetime", "value"]
    )


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
            if STORAGE_MODE == "blob":
                df = read_parquet_blob(matched_path)
            else:
                df = pd.read_parquet(matched_path)

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
    if "datetime" not in tick_df.columns or "value" not in tick_df.columns:
        raise ValueError("tick_df must contain 'datetime' and 'value' columns")

    interval = int(interval_minutes)
    if interval <= 0 or interval > 1440:
        raise ValueError("interval_minutes must be between 1 and 1440")

    df = pd.DataFrame({
        "datetime": pd.to_datetime(tick_df["datetime"], errors="coerce"),
        "value": pd.to_numeric(tick_df["value"], errors="coerce"),
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


def get_dates_for_week_folder(
    week_number,
    folder,
    instrument="NIFTY",
):
    dates = set()

    if STORAGE_MODE == "blob":
        prefix = str(folder).replace("\\", "/").strip("/")
        cache_key = (prefix, str(instrument).upper())

        with _WEEK_DATES_CACHE_LOCK:
            cached = _WEEK_DATES_CACHE.get(cache_key)

        if cached is not None:
            return list(cached)

        cfg = get_dataset_config(instrument)

        # -----------------------------------------------------
        # 1. Read the trading dates from the index parquet
        #    (IDX_TICK/<SYMBOL>.parquet). Blob names in this
        #    layout carry no YYYYMMDD tokens, so the old
        #    filename-regex approach matched garbage digits from
        #    contract names (expiry+strike) instead of dates.
        # -----------------------------------------------------

        idx_prefix = _get_idx_folder(
            prefix,
            None,
        )

        idx_blob = None
        for candidate in (
            f"{cfg['symbol']}.parquet",
            str(cfg["symbol"]),
            cfg.get("zip_member", f"{cfg['symbol']}.parquet").replace(
                ".csv", ".parquet"
            ),
        ):
            try:
                idx_blob = find_blob_by_filename(
                    prefix=idx_prefix,
                    filename=candidate,
                )
            except Exception:
                idx_blob = None

            if idx_blob:
                break

        if idx_blob:
            # Try a cheap projected read of just the date column first.
            try:
                d = read_parquet_blob(idx_blob, columns=["date"])
                dates.update(_dates_from_frame(d))
            except Exception:
                pass

            # Fall back to a full read if the file has no plain
            # `date` column (e.g. datetime-only schema).
            if not dates:
                try:
                    d = read_parquet_blob(idx_blob)
                    dates.update(_dates_from_frame(d))
                except Exception as exc:
                    logger.warning(
                        "Unable to read index blob %s for dates: %s",
                        idx_blob,
                        exc,
                    )

        # -----------------------------------------------------
        # 2. Fallback: date-stamped folder names in blob paths
        #    (e.g. NSE_OPT_TICK_20260702/...). Only accept
        #    8-digit runs that parse as real calendar dates so
        #    contract digits like 26063043000 are rejected.
        # -----------------------------------------------------

        if not dates:
            try:
                for blob_name in list_blob_names(prefix):
                    for token in re.findall(r"(?<!\d)(\d{8})(?!\d)", str(blob_name)):
                        parsed = pd.to_datetime(
                            token, format="%Y%m%d", errors="coerce"
                        )
                        if pd.notna(parsed) and 2000 <= parsed.year <= 2100:
                            dates.add(token)
            except Exception as exc:
                logger.warning(
                    "Blob-name date fallback failed for %s: %s",
                    prefix,
                    exc,
                )

        result = sorted(dates)

        with _WEEK_DATES_CACHE_LOCK:
            _WEEK_DATES_CACHE[cache_key] = list(result)

        return result


    # 1. Old structure: folders with date in name
    for name in os.listdir(folder):
        path = os.path.join(folder, name)

        if os.path.isdir(path):
            date_str = _extract_date_from_text(name)
            if date_str:
                dates.add(date_str)

    if dates:
        return sorted(dates)

    # 2. New structure:
    # week_folder/
    #   OPT_TICK/
    #   IDX_TICK/
    opt_folder = _get_opt_folder(folder, None)

    if os.path.isdir(opt_folder):
        for root, _, files in os.walk(opt_folder):
            for file in files:
                path = os.path.join(root, file)

                try:
                    d = pd.read_parquet(path, columns=["date"])

                    if not d.empty:
                        dates.update(
                            d["date"]
                            .dropna()
                            .astype("int64")
                            .astype(str)
                            .unique()
                            .tolist()
                        )

                        # Once dates found, enough for this week
                        if dates:
                            return sorted(dates)

                except Exception:
                    continue

    # 3. Fallback: read IDX_TICK/NIFTY
    idx_folder = _get_idx_folder(folder, None)

    if os.path.isdir(idx_folder):
        cfg = get_dataset_config(instrument)

        possible_names = [
            cfg["symbol"],
            f"{cfg['symbol']}.parquet",
            cfg.get("zip_member", f"{cfg['symbol']}.parquet").replace(".csv", ".parquet"),
        ]

        for name in possible_names:
            path = _find_parquet_file(idx_folder, name)

            if path:
                try:
                    d = pd.read_parquet(path, columns=["date"])

                    if not d.empty:
                        dates.update(
                            d["date"]
                            .dropna()
                            .astype("int64")
                            .astype(str)
                            .unique()
                            .tolist()
                        )
                        return sorted(dates)

                except Exception:
                    continue

    return sorted(dates)



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


def load_future_data_for_date(
    folder,
    date_str,
    month="current",
    instrument="NIFTY",
):
    """Load the selected futures contract from local or Azure Blob storage."""
    cfg = get_dataset_config(instrument)

    if not is_path_allowed(folder, instrument):
        raise PermissionError(f"Access denied: {folder}")

    symbol = str(cfg["symbol"]).upper()
    month = str(month or "current").strip().lower()
    month = month.replace("-", "_").replace(" ", "_")

    primary_future_folder = _get_fut_folder(folder, date_str)

    if STORAGE_MODE == "blob":
        candidate_folders = [
            primary_future_folder,
            _join_storage_path(folder, FUT_SEGMENT_NAME, date_str),
            _join_storage_path(folder, date_str, FUT_SEGMENT_NAME),
            _join_storage_path(
                folder,
                f"NSE_FUT_TICK_{date_str}",
                "Contract Futures",
            ),
            _join_storage_path(folder, f"NSE_FUT_TICK_{date_str}"),
            str(folder).replace("\\", "/").strip("/"),
        ]
    else:
        candidate_folders = [
            primary_future_folder,
            os.path.join(folder, FUT_SEGMENT_NAME, date_str),
            os.path.join(folder, date_str, FUT_SEGMENT_NAME),
            os.path.join(
                folder,
                f"NSE_FUT_TICK_{date_str}",
                "Contract Futures",
            ),
            os.path.join(folder, f"NSE_FUT_TICK_{date_str}"),
            folder,
        ]

    candidate_folders = list(dict.fromkeys(str(x) for x in candidate_folders))

    pattern = re.compile(
        rf"^{re.escape(symbol)}\d{{2}}"
        r"(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)"
        r"FUT$",
        re.IGNORECASE,
    )

    future_files = []
    seen_paths = set()

    for search_folder in candidate_folders:
        if STORAGE_MODE == "blob":
            paths = list_blob_names(search_folder)
        else:
            if not os.path.isdir(search_folder):
                continue

            paths = (
                os.path.join(root, filename)
                for root, _, files in os.walk(search_folder)
                for filename in files
            )

        for file_path in paths:
            file_path = str(file_path)

            if file_path in seen_paths:
                continue
            seen_paths.add(file_path)

            filename = (
                file_path
                .replace("\\", "/")
                .rsplit("/", 1)[-1]
            )

            if not filename.lower().endswith(".parquet"):
                continue

            base_name = os.path.splitext(filename)[0].upper()

            if pattern.match(base_name):
                future_files.append((base_name, file_path))

    if not future_files:
        print(
            f"No future files found for {symbol} in {folder}",
            flush=True,
        )
        return pd.DataFrame(
            columns=["datetime", "price", "volume"]
        )

    month_order = {
        "JAN": 1,
        "FEB": 2,
        "MAR": 3,
        "APR": 4,
        "MAY": 5,
        "JUN": 6,
        "JUL": 7,
        "AUG": 8,
        "SEP": 9,
        "OCT": 10,
        "NOV": 11,
        "DEC": 12,
    }

    def get_month_rank(file_name):
        file_name = str(file_name).upper()

        for mon, rank in month_order.items():
            if f"{mon}FUT" in file_name:
                return rank

        return 999

    future_files.sort(key=lambda item: get_month_rank(item[0]))

    if month in {
        "current",
        "this_month",
        "current_month",
        "near",
        "nearby",
    }:
        selected_index = 0
    elif month in {"next", "next_month"}:
        selected_index = 1
    elif month in {
        "far",
        "far_month",
        "next_to_next",
        "next_to_next_month",
    }:
        selected_index = 2
    else:
        selected_index = 0

    selected_index = min(
        selected_index,
        len(future_files) - 1,
    )
    selected = future_files[selected_index][1]

    logger.info("Using future file: %s", selected)

    try:
        return _read_parquet_normalized(
            selected,
            mode="option",
        )
    except Exception as exc:
        print(
            f"Unable to read future file {selected}: {exc}",
            flush=True,
        )
        return pd.DataFrame(
            columns=["datetime", "price", "volume"]
        )
