"""
Production data engine for the historical Options Simulator.

Azure/local layout:
    <week-folder>/IDX_TICK/<YYYYMMDD>/<SYMBOL>.parquet
    <week-folder>/OPT_TICK/<YYYYMMDD>/<SYMBOL>.parquet
    <week-folder>/FUT_TICK/<YYYYMMDD>/<SYMBOL>.parquet

All caches are process-local RAM caches. This module does not create a
shared disk-cache directory.
"""

import logging
import os
import re
import time
from threading import Lock

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

PARQUET_FILE_PATH_CACHE = {}
RAW_PARQUET_CACHE = {}
MAX_RAW_PARQUET_CACHE_SIZE = int(os.getenv("MAX_RAW_PARQUET_CACHE_SIZE", "1000"))

OPTION_PARQUET_CACHE = {}
OPTION_CONTRACT_CACHE = {}

# Full consolidated source-file cache. Each NIFTY.parquet/BANKNIFTY.parquet
# is downloaded once per Python worker and reused for all timestamp requests.
_OPTION_SOURCE_CACHE = {}
_OPTION_SOURCE_CACHE_LOCK = Lock()
MAX_OPTION_SOURCE_CACHE_SIZE = max(
    1, int(os.getenv("MAX_OPTION_SOURCE_CACHE_SIZE", "8"))
)

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
    if STORAGE_MODE == "blob":
        value = str(path).replace("\\", "/").strip("/")
        return ".." not in value.split("/")

    cfg = get_dataset_config(instrument)

    base = os.path.abspath(
        str(cfg.get("base_path") or PARQUET_BASE_PATH)
    )
    option_base = os.path.abspath(
        str(OPTION_PARQUET_BASE_PATH)
    )
    target = os.path.abspath(str(path))

    if (
        not os.path.exists(target)
        and target.lower().endswith(".zip")
    ):
        target = os.path.dirname(target)

    return (
        os.path.commonpath([base, target]) == base
        or os.path.commonpath([option_base, target])
        == option_base
    )

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


def _resolve_data_folder(week_folder, data_type, date_str=None):
    """
    Resolve the production storage layout:

        <week-folder>/<DATA_TYPE>/<YYYYMMDD>/

    A legacy date-first local layout is kept as a fallback.
    """
    week_folder = str(week_folder)
    data_type = str(data_type).upper().strip()
    normalized_date = (
        str(date_str).replace("-", "").strip()
        if date_str is not None
        else ""
    )

    if STORAGE_MODE == "blob":
        if normalized_date:
            return _join_storage_path(
                week_folder,
                data_type,
                normalized_date,
            )

        return _join_storage_path(week_folder, data_type)

    candidates = []

    if normalized_date:
        candidates.extend(
            [
                os.path.join(
                    week_folder,
                    data_type,
                    normalized_date,
                ),
                os.path.join(
                    week_folder,
                    normalized_date,
                    data_type,
                ),
            ]
        )

    candidates.extend(
        [
            os.path.join(week_folder, data_type),
            week_folder,
        ]
    )

    for candidate in candidates:
        if os.path.isdir(candidate):
            return candidate

    return candidates[0] if candidates else week_folder


def _get_idx_folder(week_folder, date_str=None):
    return _resolve_data_folder(
        week_folder,
        "IDX_TICK",
        date_str,
    )


def _get_opt_folder(week_folder, date_str=None):
    return _resolve_data_folder(
        week_folder,
        "OPT_TICK",
        date_str,
    )


def _get_fut_folder(week_folder, date_str=None):
    return _resolve_data_folder(
        week_folder,
        "FUT_TICK",
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
        return cached.copy()

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

    if len(RAW_PARQUET_CACHE) >= MAX_RAW_PARQUET_CACHE_SIZE:
        RAW_PARQUET_CACHE.pop(next(iter(RAW_PARQUET_CACHE)))

    RAW_PARQUET_CACHE[cache_key] = out.copy()
    return out


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
    """
    Load index ticks from:

        <week-folder>/IDX_TICK/<YYYYMMDD>/<SYMBOL>.parquet
    """
    normalized_date = str(date_str).replace("-", "").strip()
    symbol_name = str(symbol_name).upper().strip()

    if not re.fullmatch(r"\d{8}", normalized_date):
        logger.warning("Invalid index date: %r", date_str)
        return pd.DataFrame(columns=["datetime", "value"])

    idx_folder = _get_idx_folder(
        folder,
        normalized_date,
    )

    candidate_names = [
        f"{symbol_name}.parquet",
        symbol_name,
        f"{symbol_name}_{normalized_date}.parquet",
        f"{symbol_name}{normalized_date}.parquet",
        f"{normalized_date}.parquet",
    ]

    logger.info(
        "Searching index data symbol=%s date=%s prefix=%s",
        symbol_name,
        normalized_date,
        idx_folder,
    )

    for filename in candidate_names:
        try:
            path = _find_parquet_file(
                idx_folder,
                filename,
            )
        except Exception as exc:
            logger.exception(
                "Index search failed prefix=%s filename=%s error=%s",
                idx_folder,
                filename,
                exc,
            )
            continue

        if not path:
            continue

        logger.info("Using index source path=%s", path)

        try:
            result = _read_parquet_normalized(
                path,
                mode="spot",
            )
        except Exception as exc:
            logger.exception(
                "Index read failed path=%s error=%s",
                path,
                exc,
            )
            continue

        if result is None or result.empty:
            continue

        requested = result[
            result["datetime"].dt.strftime("%Y%m%d")
            == normalized_date
        ]

        if not requested.empty:
            return requested.reset_index(drop=True)

        logger.warning(
            "Index source has no rows for date=%s path=%s",
            normalized_date,
            path,
        )

    logger.warning(
        "Index source not found symbol=%s date=%s prefix=%s",
        symbol_name,
        normalized_date,
        idx_folder,
    )

    return pd.DataFrame(columns=["datetime", "value"])


# =========================================================
# OPTIONS - NEW OPTION PATH ONLY
# =========================================================

CONSOLIDATED_SCHEMA_VERSION = 3


def consolidated_chain_folder(week_folder, date_str):
    option_week_folder = _resolve_option_week_folder(week_folder)
    return _get_opt_folder(option_week_folder, date_str)


def consolidated_chain_path(
    week_folder,
    date_str,
    expiry_str,
    instrument="NIFTY",
):
    """Return <week>/<YYYYMMDD>/OPT_TICK/<SYMBOL>.parquet."""
    cfg = get_dataset_config(instrument)
    symbol = str(cfg["symbol"]).upper()
    return _join_storage_path(
        consolidated_chain_folder(week_folder, date_str),
        f"{symbol}.parquet",
    ) if STORAGE_MODE == "blob" else os.path.join(
        consolidated_chain_folder(week_folder, date_str),
        f"{symbol}.parquet",
    )


_OPTION_CHAIN_CACHE = {}
_OPTION_CHAIN_CACHE_LOCK = Lock()


def _load_consolidated_option_source(
    path=None,
    *,
    folder=None,
    date_str=None,
    instrument="NIFTY",
):
    """
    Read and cache the complete consolidated option Parquet file.

    Supported calls
    ---------------

    1. Direct path:

        _load_consolidated_option_source(
            "166 13 Jul to 17 Jul (NSE FO) - TICK - CSV/"
            "20260717/OPT_TICK/NIFTY.parquet"
        )

    2. Build the path from its components:

        _load_consolidated_option_source(
            folder=folder,
            date_str="20260717",
            instrument="NIFTY",
        )

    The complete daily Parquet file is loaded only once per
    Python process and is then reused from RAM.
    """

    # -----------------------------------------------------
    # Validate and normalize the instrument
    # -----------------------------------------------------

    instrument = str(instrument or "NIFTY").strip().upper()

    if not instrument:
        raise ValueError("instrument cannot be empty")

    # -----------------------------------------------------
    # Resolve the complete Parquet path
    # -----------------------------------------------------

    if path is None:
        if folder is None:
            raise ValueError(
                "Either path or folder must be provided."
            )

        if date_str is None:
            raise ValueError(
                "date_str is required when folder is provided."
            )

        normalized_folder = (
            str(folder)
            .replace("\\", "/")
            .strip()
            .strip("/")
        )

        normalized_date = (
            str(date_str)
            .strip()
            .replace("-", "")
            .replace("/", "")
        )

        if not normalized_folder:
            raise ValueError("folder cannot be empty")

        if (
            len(normalized_date) != 8
            or not normalized_date.isdigit()
        ):
            raise ValueError(
                "date_str must be in YYYYMMDD or YYYY-MM-DD format."
            )

        # Production Azure layout:
        #
        # <week-folder>/
        #     <YYYYMMDD>/
        #         OPT_TICK/
        #             NIFTY.parquet
        #
        path = _join_storage_path(
            normalized_folder,
            "OPT_TICK",
            normalized_date,
            f"{instrument}.parquet",
        )

    normalized_path = (
        str(path)
        .replace("\\", "/")
        .strip()
        .strip("/")
    )

    if not normalized_path:
        raise ValueError(
            "Resolved consolidated option path is empty."
        )

    # -----------------------------------------------------
    # Return the complete day from RAM when available
    # -----------------------------------------------------

    with _OPTION_SOURCE_CACHE_LOCK:
        cached = _OPTION_SOURCE_CACHE.get(
            normalized_path
        )

        if cached is not None:
            logger.debug(
                "Option source cache hit path=%s rows=%d",
                normalized_path,
                len(cached),
            )

            return cached

    # -----------------------------------------------------
    # Read the complete daily Parquet source
    # -----------------------------------------------------

    started = time.perf_counter()

    try:
        if STORAGE_MODE == "blob":
            source = read_parquet_blob(
                normalized_path
            )
        else:
            source = pd.read_parquet(
                normalized_path
            )

    except Exception as exc:
        logger.exception(
            "Unable to read consolidated option source "
            "path=%s storage_mode=%s error=%s",
            normalized_path,
            STORAGE_MODE,
            exc,
        )

        return None

    # -----------------------------------------------------
    # Validate the loaded DataFrame
    # -----------------------------------------------------

    if source is None:
        logger.warning(
            "Option source loader returned None: %s",
            normalized_path,
        )

        return None

    if not isinstance(source, pd.DataFrame):
        logger.error(
            "Option source is not a DataFrame: "
            "path=%s type=%s",
            normalized_path,
            type(source).__name__,
        )

        return None

    if source.empty:
        logger.warning(
            "Option source is empty: %s",
            normalized_path,
        )

        return None

    required_columns = {
        "date",
        "time",
        "price",
        "contract_name",
    }

    missing_columns = (
        required_columns - set(source.columns)
    )

    if missing_columns:
        logger.error(
            "Option source is missing columns: "
            "path=%s missing=%s available=%s",
            normalized_path,
            sorted(missing_columns),
            source.columns.tolist(),
        )

        return None

    # -----------------------------------------------------
    # Normalize important source columns once
    # -----------------------------------------------------

    source = source.copy()

    source["contract_name"] = (
        source["contract_name"]
        .astype(str)
        .str.upper()
        .str.replace(r"\s+", "", regex=True)
        .str.strip()
    )

    source["price"] = pd.to_numeric(
        source["price"],
        errors="coerce",
    )

    if "qty" in source.columns:
        source["qty"] = pd.to_numeric(
            source["qty"],
            errors="coerce",
        ).fillna(0)

    if "oi" in source.columns:
        source["oi"] = pd.to_numeric(
            source["oi"],
            errors="coerce",
        ).fillna(0)

    source = source.dropna(
        subset=[
            "price",
            "contract_name",
        ]
    ).reset_index(drop=True)

    if source.empty:
        logger.warning(
            "Option source contains no valid rows after "
            "normalization: %s",
            normalized_path,
        )

        return None

    # -----------------------------------------------------
    # Save the complete daily DataFrame in RAM
    # -----------------------------------------------------

    with _OPTION_SOURCE_CACHE_LOCK:
        # Another thread may have loaded the same source while
        # this thread was reading it.
        existing = _OPTION_SOURCE_CACHE.get(
            normalized_path
        )

        if existing is not None:
            return existing

        while (
            len(_OPTION_SOURCE_CACHE)
            >= MAX_OPTION_SOURCE_CACHE_SIZE
        ):
            oldest_key = next(
                iter(_OPTION_SOURCE_CACHE),
                None,
            )

            if oldest_key is None:
                break

            _OPTION_SOURCE_CACHE.pop(
                oldest_key,
                None,
            )

            logger.info(
                "Evicted option source cache entry path=%s",
                oldest_key,
            )

        _OPTION_SOURCE_CACHE[
            normalized_path
        ] = source

    elapsed_ms = (
        time.perf_counter() - started
    ) * 1000

    logger.info(
        "Loaded and cached option source "
        "path=%s rows=%d columns=%d "
        "memory_mb=%.2f time_ms=%.2f",
        normalized_path,
        len(source),
        len(source.columns),
        source.memory_usage(
            index=True,
            deep=True,
        ).sum()
        / (1024 * 1024),
        elapsed_ms,
    )

    return source

def load_consolidated_option_chain(
    folder,
    date_str,
    expiry_str,
    instrument="NIFTY",
):
    """Normalize a consolidated underlying Parquet into timestamp/strike/ce/pe."""
    path = consolidated_chain_path(folder, date_str, expiry_str, instrument)
    cache_key = (
        f"schema-{CONSOLIDATED_SCHEMA_VERSION}|{STORAGE_MODE}|{path}|"
        f"{date_str}|{expiry_str}|{str(instrument).upper()}"
    )

    with _OPTION_CHAIN_CACHE_LOCK:
        cached = _OPTION_CHAIN_CACHE.get(cache_key)
    if cached is not None:
        return cached.copy()

    if STORAGE_MODE == "blob":
        try:
            if not blob_exists(path):
                logger.warning("Option Parquet not found: %s", path)
                return None
        except Exception as exc:
            logger.exception("Unable to check option blob %s: %s", path, exc)
            return None
    elif not os.path.isfile(path):
        logger.warning("Option Parquet not found: %s", path)
        return None

    source = _load_consolidated_option_source(path)
    if source is None or source.empty:
        return None

    lower = {str(c).strip().lower(): c for c in source.columns}
    required = {"date", "time", "price", "contract_name"}
    if not required.issubset(lower):
        logger.error(
            "Unsupported option schema in %s. Required=%s Available=%s",
            path, sorted(required), list(source.columns),
        )
        return None

    work = source[[
        lower["date"], lower["time"], lower["price"], lower["contract_name"]
    ]].copy()
    work.columns = ["date", "time", "price", "contract_name"]

    requested_date = pd.to_numeric(str(date_str), errors="coerce")
    if pd.isna(requested_date):
        return None
    work["date"] = pd.to_numeric(work["date"], errors="coerce")
    work = work[work["date"] == int(requested_date)]
    if work.empty:
        logger.warning("No option rows for date=%s in %s", date_str, path)
        return None

    contract = (
        work["contract_name"].astype(str).str.strip().str.upper()
        .str.replace(".PARQUET", "", regex=False)
        .str.replace(".CSV", "", regex=False)
    )
    extracted = contract.str.extract(
        r"^(?P<symbol>[A-Z]+)(?P<expiry>\d{6})"
        r"(?P<strike>\d+(?:\.\d+)?)(?P<option_type>CE|PE)$"
    )
    work = work.join(extracted)
    cfg = get_dataset_config(instrument)
    symbol = str(cfg["symbol"]).upper()
    work["price"] = pd.to_numeric(work["price"], errors="coerce")
    work["strike"] = pd.to_numeric(work["strike"], errors="coerce")
    work = work.dropna(subset=["price", "strike"])
    work = work[
        work["symbol"].eq(symbol)
        & work["expiry"].eq(str(expiry_str).strip())
        & work["option_type"].isin(["CE", "PE"])
    ]
    if work.empty:
        logger.warning(
            "No option contracts matched instrument=%s date=%s expiry=%s in %s",
            symbol, date_str, expiry_str, path,
        )
        return None

    work["timestamp"] = pd.to_datetime(
        work["date"].astype("Int64").astype(str) + " " + work["time"].astype(str),
        errors="coerce",
    )
    work = work.dropna(subset=["timestamp"])
    if getattr(work["timestamp"].dt, "tz", None) is None:
        work["timestamp"] = work["timestamp"].dt.tz_localize(
            IST, ambiguous="NaT", nonexistent="NaT"
        )
    else:
        work["timestamp"] = work["timestamp"].dt.tz_convert(IST)
    work = work.dropna(subset=["timestamp"])
    work = work[
        (work["timestamp"].dt.time >= SESSION_START)
        & (work["timestamp"].dt.time <= SESSION_END)
    ]
    if work.empty:
        return None

    work["strike"] = work["strike"].round().astype("int32")
    work["timestamp"] = work["timestamp"].dt.floor("min")
    work = (
        work.sort_values("timestamp", kind="mergesort")
        .drop_duplicates(["timestamp", "strike", "option_type"], keep="last")
    )

    out = (
        work.pivot_table(
            index=["timestamp", "strike"],
            columns="option_type",
            values="price",
            aggfunc="last",
        )
        .reset_index()
        .rename(columns={"CE": "ce", "PE": "pe"})
    )
    out.columns.name = None
    if "ce" not in out.columns:
        out["ce"] = np.nan
    if "pe" not in out.columns:
        out["pe"] = np.nan
    out = out[["timestamp", "strike", "ce", "pe"]].sort_values(
        ["timestamp", "strike"], kind="mergesort"
    ).reset_index(drop=True)
    if out.empty:
        return None

    with _OPTION_CHAIN_CACHE_LOCK:
        _OPTION_CHAIN_CACHE[cache_key] = out.copy()

    logger.info(
        "Loaded option chain instrument=%s date=%s expiry=%s rows=%d strikes=%d",
        symbol, date_str, expiry_str, len(out), out["strike"].nunique(),
    )
    return out.copy()


def load_required_option_data_for_date(
    folder,
    date_str,
    expiry_str,
    strike,
    instrument="NIFTY",
):
    """Return CE and PE time-series for one strike from the consolidated chain."""
    empty_frame = pd.DataFrame(columns=["datetime", "price", "volume"])
    empty = {"CE": empty_frame.copy(), "PE": empty_frame.copy()}
    try:
        strike_value = int(float(strike))
    except (TypeError, ValueError, OverflowError):
        return empty

    chain = load_consolidated_option_chain(
        folder, date_str, expiry_str, instrument
    )
    if chain is None or chain.empty:
        return empty
    selected = chain[pd.to_numeric(chain["strike"], errors="coerce") == strike_value]
    if selected.empty:
        return empty

    result = {}
    for side, column in (("CE", "ce"), ("PE", "pe")):
        frame = pd.DataFrame({
            "datetime": pd.to_datetime(selected["timestamp"], errors="coerce"),
            "price": pd.to_numeric(selected[column], errors="coerce"),
            "volume": 0,
        }).dropna(subset=["datetime", "price"])
        result[side] = frame.sort_values("datetime").reset_index(drop=True)
    return result


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
    if tick_df is None or tick_df.empty:
        return pd.DataFrame()

    df = tick_df.copy()
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["datetime", "value"])

    if df.empty:
        return pd.DataFrame()

    if getattr(df["datetime"].dt, "tz", None) is None:
        df["datetime"] = df["datetime"].dt.tz_localize(IST)
    else:
        df["datetime"] = df["datetime"].dt.tz_convert(IST)

    candles = (
        df.set_index("datetime")["value"]
        .resample(f"{int(interval_minutes)}min")
        .ohlc()
        .dropna()
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

        idx_prefix = _join_storage_path(prefix, "IDX_TICK")

        try:
            prefix_with_slash = idx_prefix.rstrip("/") + "/"

            for blob_name in list_blob_names(idx_prefix):
                normalized_blob = str(blob_name).replace("\\", "/")

                if not normalized_blob.startswith(prefix_with_slash):
                    continue

                remainder = normalized_blob[len(prefix_with_slash):]
                date_segment = remainder.split("/", 1)[0]

                if re.fullmatch(r"\d{8}", date_segment):
                    parsed = pd.to_datetime(
                        date_segment,
                        format="%Y%m%d",
                        errors="coerce",
                    )

                    if pd.notna(parsed):
                        dates.add(date_segment)
        except Exception as exc:
            logger.warning(
                "Date-folder discovery failed prefix=%s error=%s",
                idx_prefix,
                exc,
            )

        if dates:
            result = sorted(dates)

            with _WEEK_DATES_CACHE_LOCK:
                _WEEK_DATES_CACHE[cache_key] = list(result)

            return result

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


# =========================================================
# OPTION HELPERS
# =========================================================

def get_upcoming_expiry_np(
    query_date,
    instrument="NIFTY",
    expiry_rule="current expiry",
):
    """
    Return the appropriate option expiry in YYMMDD format.

    Supported expiry rules:
        current expiry
        next expiry
        next to next expiry
        monthly expiry
        next monthly expiry
        next to next monthly expiry
    """

    cfg = get_dataset_config(instrument)

    instrument = (
        str(instrument or "NIFTY")
        .strip()
        .upper()
        .replace(" ", "")
        .replace("-", "")
        .replace("_", "")
    )

    rule = (
        str(expiry_rule or "current expiry")
        .strip()
        .lower()
        .replace("-", " ")
        .replace("_", " ")
    )

    rule = " ".join(rule.split())

    query_ts = pd.Timestamp(query_date).normalize()

    monthly_rules = {
        "monthly",
        "monthly expiry",
        "current monthly",
        "current monthly expiry",
        "next monthly",
        "next monthly expiry",
        "next to next monthly",
        "next to next monthly expiry",
    }

    # BANKNIFTY always uses monthly expiries.
    if instrument == "BANKNIFTY":
        expiry_values = cfg.get("monthly_expiry")

        if expiry_values is None:
            expiry_values = _derive_monthly_expiries(
                cfg["combined_expiry"]
            )

    elif rule in monthly_rules:
        expiry_values = cfg.get("monthly_expiry")

        if expiry_values is None:
            expiry_values = _derive_monthly_expiries(
                cfg["combined_expiry"]
            )

    else:
        expiry_values = cfg["combined_expiry"]

    expiries = (
        pd.DatetimeIndex(
            pd.to_datetime(expiry_values)
        )
        .normalize()
        .sort_values()
        .unique()
    )

    upcoming = expiries[expiries >= query_ts]

    if len(upcoming) == 0:
        return None

    if rule in {
        "current expiry",
        "current",
        "weekly",
        "weekly expiry",
        "monthly",
        "monthly expiry",
        "current monthly",
        "current monthly expiry",
    }:
        position = 0

    elif rule in {
        "next expiry",
        "next",
        "next weekly",
        "next weekly expiry",
        "next monthly",
        "next monthly expiry",
    }:
        position = 1

    elif rule in {
        "next to next expiry",
        "next to next",
        "next to next weekly",
        "next to next weekly expiry",
        "next to next monthly",
        "next to next monthly expiry",
    }:
        position = 2

    else:
        logger.warning(
            "Unsupported expiry rule '%s'; using current expiry.",
            expiry_rule,
        )
        position = 0

    if position >= len(upcoming):
        return None

    selected_expiry = pd.Timestamp(
        upcoming[position]
    )

    return selected_expiry.strftime("%y%m%d")


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
    """
    Load futures ticks from:

        <week-folder>/FUT_TICK/<YYYYMMDD>/<INSTRUMENT>.parquet
    """

    instrument = str(instrument).upper().strip()
    date_str = str(date_str).replace("-", "").strip()

    normalized_folder = (
        str(folder)
        .replace("\\", "/")
        .strip("/")
    )

    future_path = _join_storage_path(
        normalized_folder,
        "FUT_TICK",
        date_str,
        f"{instrument}.parquet",
    )

    logger.info(
        "Loading future data path=%s month=%s",
        future_path,
        month,
    )

    try:
        if STORAGE_MODE == "blob":
            frame = read_parquet_blob(future_path)
        else:
            frame = pd.read_parquet(future_path)

    except Exception as exc:
        logger.exception(
            "Unable to load future data path=%s error=%s",
            future_path,
            exc,
        )
        return pd.DataFrame()

    if frame is None or frame.empty:
        logger.warning(
            "Future data is empty path=%s",
            future_path,
        )
        return pd.DataFrame()

    frame = frame.copy()

    logger.info(
        "Future source loaded rows=%d columns=%s",
        len(frame),
        frame.columns.tolist(),
    )

    # Build datetime
    if "datetime" not in frame.columns:
        if "date" not in frame.columns or "time" not in frame.columns:
            logger.error(
                "Future file requires datetime or date/time columns. "
                "Available=%s",
                frame.columns.tolist(),
            )
            return pd.DataFrame()

        date_part = (
            frame["date"]
            .astype(str)
            .str.replace(r"\.0$", "", regex=True)
            .str.replace(r"\D", "", regex=True)
            .str.zfill(8)
        )

        time_part = (
            frame["time"]
            .astype(str)
            .str.strip()
        )

        frame["datetime"] = pd.to_datetime(
            date_part + " " + time_part,
            errors="coerce",
        )
    else:
        frame["datetime"] = pd.to_datetime(
            frame["datetime"],
            errors="coerce",
        )

    # Normalize price
    if "value" not in frame.columns:
        if "price" in frame.columns:
            frame["value"] = pd.to_numeric(
                frame["price"],
                errors="coerce",
            )
        else:
            logger.error(
                "Future file has no price/value column. Available=%s",
                frame.columns.tolist(),
            )
            return pd.DataFrame()
    else:
        frame["value"] = pd.to_numeric(
            frame["value"],
            errors="coerce",
        )

    frame = frame.dropna(
        subset=["datetime", "value"]
    )

    # Optional contract/month filtering
    month_value = str(month or "current").lower().strip()

    if "contract_name" in frame.columns:
        frame["contract_name"] = (
            frame["contract_name"]
            .astype(str)
            .str.upper()
            .str.strip()
        )

        # Do not filter unless your file actually has multiple expiries.
        # For now, keep all rows so the chart can display.
        logger.info(
            "Future contracts sample=%s",
            frame["contract_name"]
            .drop_duplicates()
            .head(10)
            .tolist(),
        )

    frame = (
        frame
        .sort_values("datetime")
        .reset_index(drop=True)
    )

    logger.info(
        "Future data ready rows=%d min_time=%s max_time=%s",
        len(frame),
        frame["datetime"].min(),
        frame["datetime"].max(),
    )

    return frame

def get_option_chain_snapshot(
    folder,
    date_str,
    expiry_str,
    target_timestamp,
    instrument="NIFTY",
):
    """Return latest CE/PE values by strike at or before target_timestamp."""
    empty = pd.DataFrame(columns=["timestamp", "strike", "ce", "pe"])
    chain = load_consolidated_option_chain(
        folder, date_str, expiry_str, instrument
    )
    if chain is None or chain.empty:
        return empty

    frame = chain[["timestamp", "strike", "ce", "pe"]].copy()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], errors="coerce")
    frame["strike"] = pd.to_numeric(frame["strike"], errors="coerce")
    frame["ce"] = pd.to_numeric(frame["ce"], errors="coerce")
    frame["pe"] = pd.to_numeric(frame["pe"], errors="coerce")
    frame = frame.dropna(subset=["timestamp", "strike"])
    if frame.empty:
        return empty

    if getattr(frame["timestamp"].dt, "tz", None) is None:
        frame["timestamp"] = frame["timestamp"].dt.tz_localize(
            IST, ambiguous="NaT", nonexistent="NaT"
        )
    else:
        frame["timestamp"] = frame["timestamp"].dt.tz_convert(IST)

    target = pd.Timestamp(target_timestamp)
    target = target.tz_localize(IST) if target.tzinfo is None else target.tz_convert(IST)
    frame = frame[frame["timestamp"] <= target]
    if frame.empty:
        return empty

    frame["strike"] = frame["strike"].astype("int32")
    return (
        frame.sort_values(["strike", "timestamp"], kind="mergesort")
        .drop_duplicates("strike", keep="last")
        .sort_values("strike", kind="mergesort")
        .reset_index(drop=True)
    )[["timestamp", "strike", "ce", "pe"]]


def runtime_cache_stats():
    with _OPTION_SOURCE_CACHE_LOCK:
        source_entries = len(_OPTION_SOURCE_CACHE)
        source_rows = sum(
            len(value) for value in _OPTION_SOURCE_CACHE.values()
            if isinstance(value, pd.DataFrame)
        )
    with _OPTION_CHAIN_CACHE_LOCK:
        chain_entries = len(_OPTION_CHAIN_CACHE)
    return {
        "storage_mode": STORAGE_MODE,
        "option_source_cache_entries": source_entries,
        "option_source_cache_rows": source_rows,
        "option_chain_cache_entries": chain_entries,
        "raw_parquet_cache_entries": len(RAW_PARQUET_CACHE),
        "path_cache_entries": len(PARQUET_FILE_PATH_CACHE),
        "week_folder_cache_entries": len(OPTION_WEEK_FOLDER_CACHE),
    }


def clear_runtime_caches():
    """Clear all process-local caches. No disk cache is used."""
    PARQUET_FILE_PATH_CACHE.clear()
    RAW_PARQUET_CACHE.clear()
    OPTION_PARQUET_CACHE.clear()
    OPTION_CONTRACT_CACHE.clear()
    OPTION_WEEK_FOLDER_CACHE.clear()

    with _OPTION_SOURCE_CACHE_LOCK:
        _OPTION_SOURCE_CACHE.clear()

    with _OPTION_CHAIN_CACHE_LOCK:
        _OPTION_CHAIN_CACHE.clear()

    with _OPTION_CONTRACT_INDEX_CACHE_LOCK:
        _OPTION_CONTRACT_INDEX_CACHE.clear()

    with _WEEK_DATES_CACHE_LOCK:
        _WEEK_DATES_CACHE.clear()


def load_raw_option_contract_ticks(
    folder: str,
    date_str: str,
    expiry_str: str,
    strike: int,
    option_type: str,
    instrument: str = "NIFTY",
) -> pd.DataFrame:
    instrument = str(instrument).upper().strip()
    option_type = str(option_type).upper().strip()
    date_str = str(date_str).replace("-", "").strip()
    expiry_str = str(expiry_str).strip()
    strike = int(strike)

    if option_type not in {"CE", "PE"}:
        raise ValueError("option_type must be CE or PE")

    # This must return the complete raw NIFTY.parquet DataFrame,
    # not the normalized or pivoted option chain.
    source = _load_consolidated_option_source(
        folder=folder,
        date_str=date_str,
        instrument=instrument,
    )

    if source is None or source.empty:
        return pd.DataFrame(
            columns=[
                "datetime",
                "price",
                "volume",
                "oi",
                "contract_name",
            ]
        )

    contract_name = (
        f"{instrument}"
        f"{expiry_str}"
        f"{strike}"
        f"{option_type}"
    )

    contract_series = (
        source["contract_name"]
        .astype(str)
        .str.upper()
        .str.strip()
    )

    frame = source.loc[
        contract_series == contract_name
    ].copy()

    if frame.empty:
        return pd.DataFrame(
            columns=[
                "datetime",
                "price",
                "volume",
                "oi",
                "contract_name",
            ]
        )

    date_part = (
        frame["date"]
        .astype(str)
        .str.replace(r"\D", "", regex=True)
    )

    time_part = (
        frame["time"]
        .astype(str)
        .str.strip()
    )

    frame["datetime"] = pd.to_datetime(
        date_part + " " + time_part,
        errors="coerce",
    )

    frame["price"] = pd.to_numeric(
        frame["price"],
        errors="coerce",
    )

    frame["volume"] = pd.to_numeric(
        frame.get("qty", 0),
        errors="coerce",
    ).fillna(0)

    frame["oi"] = pd.to_numeric(
        frame.get("oi", 0),
        errors="coerce",
    ).fillna(0)

    frame = (
        frame
        .dropna(subset=["datetime", "price"])
        .sort_values("datetime")
        .reset_index(drop=True)
    )

    return frame[
        [
            "datetime",
            "price",
            "volume",
            "oi",
            "contract_name",
        ]
    ]
