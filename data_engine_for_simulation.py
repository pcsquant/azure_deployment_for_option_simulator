import os
import re

import numpy as np
import pandas as pd
import hashlib

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

STORAGE_MODE = os.getenv("STORAGE_MODE", "local").strip().lower()

DEBUG_MODE = False

PARQUET_FILE_PATH_CACHE = {}
RAW_PARQUET_CACHE = {}
MAX_RAW_PARQUET_CACHE_SIZE = int(os.getenv("MAX_RAW_PARQUET_CACHE_SIZE", "1000"))

OPTION_PARQUET_CACHE = {}
OPTION_CONTRACT_CACHE = {}


# =========================================================
# PATH HELPERS
# =========================================================

def is_path_allowed(path, instrument="NIFTY"):
    cfg = get_dataset_config(instrument)
    base = os.path.abspath(cfg.get("base_path") or PARQUET_BASE_PATH)
    option_base = os.path.abspath(OPTION_PARQUET_BASE_PATH)

    target = os.path.abspath(str(path))

    if not os.path.exists(target) and target.lower().endswith(".zip"):
        target = os.path.dirname(target)

    return (
        os.path.commonpath([base, target]) == base
        or os.path.commonpath([option_base, target]) == option_base
    )


def _extract_date_from_text(text):
    match = re.search(r"(\d{8})", str(text))
    return match.group(1) if match else None


def _get_idx_folder(week_folder, date_str=None):
    folder = os.path.join(week_folder, "IDX_TICK")
    return folder if os.path.isdir(folder) else week_folder


def _get_opt_folder(week_folder, date_str=None):
    folder = os.path.join(week_folder, "OPT_TICK")
    return folder if os.path.isdir(folder) else week_folder


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
    week_folder_key = os.path.abspath(str(week_folder))

    cached = OPTION_WEEK_FOLDER_CACHE.get(week_folder_key)
    if cached is not None:
        return cached

    week_name = os.path.basename(os.path.normpath(str(week_folder)))

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

        print("Checking option folder:", option_folder, flush=True)

        if os.path.isdir(option_folder):
            print("Using option folder:", option_folder, flush=True)
            OPTION_WEEK_FOLDER_CACHE[week_folder_key] = option_folder
            return option_folder

    print("WARNING: option folder not found. Falling back to:", week_folder, flush=True)

    fallback = str(week_folder)
    OPTION_WEEK_FOLDER_CACHE[week_folder_key] = fallback
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


def load_index_data_by_symbol(folder, date_str, symbol_name="INDIAVIX"):
    idx_folder = os.path.join(folder, "IDX_TICK")

    if not os.path.isdir(idx_folder):
        return pd.DataFrame(columns=["datetime", "value"])

    symbol_name = str(symbol_name).upper().strip()

    possible_paths = [
        os.path.join(idx_folder, symbol_name),
        os.path.join(idx_folder, f"{symbol_name}.parquet"),
    ]

    for path in possible_paths:
        if os.path.exists(path):
            return _read_parquet_normalized(path, mode="spot")

    return pd.DataFrame(columns=["datetime", "value"])


# =========================================================
# OPTIONS - NEW OPTION PATH ONLY
# =========================================================

CONSOLIDATED_SCHEMA_VERSION = 2


def consolidated_chain_folder(week_folder, date_str):
    return _resolve_option_week_folder(week_folder)


def consolidated_chain_path(week_folder, date_str, expiry_str, instrument="NIFTY"):
    cfg = get_dataset_config(instrument)
    symbol = str(cfg["symbol"]).upper()

    option_week_folder = _resolve_option_week_folder(week_folder)

    return os.path.join(
        option_week_folder,
        "OPT_TICK",
        f"{symbol}_{expiry_str}.parquet"
    )


from threading import Lock

_OPTION_CHAIN_CACHE = {}
_OPTION_CHAIN_CACHE_LOCK = Lock()

SHARED_OPTION_CACHE_DIR = os.getenv(
    "SHARED_OPTION_CACHE_DIR",
    "/opt/option-simulator/shared_cache/option_chain"
)

os.makedirs(SHARED_OPTION_CACHE_DIR, exist_ok=True)

def _option_chain_disk_cache_path(cache_key):
    key_hash = hashlib.md5(cache_key.encode("utf-8")).hexdigest()
    return os.path.join(SHARED_OPTION_CACHE_DIR, f"{key_hash}.parquet")


def load_consolidated_option_chain(folder, date_str, expiry_str, instrument="NIFTY"):
    path = consolidated_chain_path(
        week_folder=folder,
        date_str=date_str,
        expiry_str=expiry_str,
        instrument=instrument,
    )

    cache_key = f"{os.path.abspath(path)}|{date_str}|{instrument}"
    disk_cache_path = _option_chain_disk_cache_path(cache_key)

    with _OPTION_CHAIN_CACHE_LOCK:
        cached = _OPTION_CHAIN_CACHE.get(cache_key)

    if cached is not None:
        return cached.copy()

    if os.path.isfile(disk_cache_path):
        try:
            out = pd.read_parquet(disk_cache_path)

            with _OPTION_CHAIN_CACHE_LOCK:
                _OPTION_CHAIN_CACHE[cache_key] = out.copy()

            return out.copy()

        except Exception:
            try:
                os.remove(disk_cache_path)
            except OSError:
                pass

    # No consolidated file available.
    # This is okay. Simulator will fall back to per-contract files.
    if not os.path.isfile(path):
        return None

    try:
        df = pd.read_parquet(
            path,
            columns=["date", "time", "strike", "option_type", "price"],
        )
    except Exception:
        df = pd.read_parquet(path)

    required = {"date", "time", "strike", "option_type", "price"}

    if df.empty:
        return None

    if not required.issubset(df.columns):
        return None

    df = df.copy()

    df["date"] = pd.to_numeric(df["date"], errors="coerce")
    df = df[df["date"] == int(date_str)]

    if df.empty:
        return None

    df["timestamp"] = pd.to_datetime(
        df["date"].astype("int64").astype(str)
        + " "
        + df["time"].astype(str),
        errors="coerce",
    )

    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["strike"] = pd.to_numeric(df["strike"], errors="coerce")
    df["option_type"] = df["option_type"].astype(str).str.upper().str.strip()

    df = df.dropna(subset=["timestamp", "price", "strike"])

    if df.empty:
        return None

    df["strike"] = df["strike"].astype(int)

    if df["timestamp"].dt.tz is None:
        df["timestamp"] = df["timestamp"].dt.tz_localize(
            IST,
            ambiguous="NaT",
            nonexistent="NaT",
        )
    else:
        df["timestamp"] = df["timestamp"].dt.tz_convert(IST)

    df = df.dropna(subset=["timestamp"])

    df = df[
        (df["timestamp"].dt.time >= SESSION_START)
        & (df["timestamp"].dt.time <= SESSION_END)
    ]

    if df.empty:
        return None

    ce_raw = df[df["option_type"] == "CE"]
    pe_raw = df[df["option_type"] == "PE"]

    ce = (
        ce_raw
        .set_index("timestamp")
        .groupby("strike")["price"]
        .resample("1min")
        .last()
        .dropna()
        .reset_index()
        .rename(columns={"price": "ce"})
    )

    pe = (
        pe_raw
        .set_index("timestamp")
        .groupby("strike")["price"]
        .resample("1min")
        .last()
        .dropna()
        .reset_index()
        .rename(columns={"price": "pe"})
    )

    if ce.empty and pe.empty:
        return None

    out = pd.merge(
        ce,
        pe,
        on=["strike", "timestamp"],
        how="outer",
    )

    if out.empty:
        return None

    out = (
        out[["timestamp", "strike", "ce", "pe"]]
        .sort_values(["timestamp", "strike"])
        .reset_index(drop=True)
    )

    with _OPTION_CHAIN_CACHE_LOCK:
        _OPTION_CHAIN_CACHE[cache_key] = out.copy()

    try:
        tmp_path = f"{disk_cache_path}.tmp"
        out.to_parquet(tmp_path, index=False)
        os.replace(tmp_path, disk_cache_path)
    except Exception:
        pass

    return out.copy()

def load_required_option_data_for_date(folder, date_str, expiry_str, strike, instrument="NIFTY"):
    empty = {
        "CE": pd.DataFrame(columns=["datetime", "price", "volume"]),
        "PE": pd.DataFrame(columns=["datetime", "price", "volume"]),
    }

    cfg = get_dataset_config(instrument)
    symbol = str(cfg["symbol"]).upper()

    opt_folder = _get_opt_folder(folder, date_str)

    if not os.path.isdir(opt_folder):
        return empty

    result = {}

    for side in ["CE", "PE"]:
        pattern = re.compile(
            rf"^{re.escape(symbol)}{re.escape(str(expiry_str))}{int(strike)}{side}$",
            re.IGNORECASE
        )

        matched_path = None

        for root, _, files in os.walk(opt_folder):
            for f in files:
                base = os.path.splitext(f)[0].upper()

                if pattern.match(base):
                    matched_path = os.path.join(root, f)
                    break

            if matched_path:
                break

        if not matched_path:
            result[side] = empty[side]
            continue

        try:
            df = pd.read_parquet(matched_path)
        except Exception:
            result[side] = empty[side]
            continue

        if df.empty:
            result[side] = empty[side]
            continue

        lower_cols = {str(c).lower(): c for c in df.columns}

        if "datetime" in lower_cols:
            dt = pd.to_datetime(df[lower_cols["datetime"]], errors="coerce")
        elif {"date", "time"}.issubset(lower_cols):
            dt = pd.to_datetime(
                df[lower_cols["date"]].astype(str) + " " + df[lower_cols["time"]].astype(str),
                errors="coerce"
            )
        else:
            dt = pd.to_datetime(
                df.iloc[:, 0].astype(str) + " " + df.iloc[:, 1].astype(str),
                errors="coerce"
            )

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
            "volume": (
                pd.to_numeric(df[volume_col], errors="coerce").fillna(0)
                if volume_col
                else 0
            ),
        }).dropna(subset=["datetime", "price"])

        if out.empty:
            result[side] = empty[side]
            continue

        if getattr(out["datetime"].dt, "tz", None) is None:
            out["datetime"] = out["datetime"].dt.tz_localize(
                IST,
                ambiguous="NaT",
                nonexistent="NaT"
            )
        else:
            out["datetime"] = out["datetime"].dt.tz_convert(IST)

        out = out.dropna(subset=["datetime"])

        out = out[
            (out["datetime"].dt.time >= SESSION_START)
            & (out["datetime"].dt.time <= SESSION_END)
        ]

        result[side] = out.sort_values("datetime").reset_index(drop=True)

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


def find_option_contract_files(folder, date_str, expiry_str, instrument="NIFTY"):
    opt_folder = _get_opt_folder(folder, date_str)

    if not os.path.isdir(opt_folder):
        return []

    found = []

    pattern = re.compile(
        rf"^(.+?){re.escape(str(expiry_str))}(\d+(?:\.\d+)?)(CE|PE)$",
        re.IGNORECASE
    )

    for root, _, files in os.walk(opt_folder):
        for f in files:
            base = os.path.splitext(f)[0].upper()
            match = pattern.match(base)

            if not match:
                continue

            strike = float(match.group(2))
            side = match.group(3).upper()

            found.append((int(strike), side, os.path.join(root, f)))

    return found


# =========================================================
# CANDLES / FOLDER DISCOVERY
# =========================================================

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

    return sorted(folders, key=lambda x: x[0])


def get_dates_for_week_folder(week_number, folder, instrument="NIFTY"):
    dates = set()

    if not os.path.isdir(folder):
        return []

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
    cfg = get_dataset_config(instrument)

    if not is_path_allowed(folder, instrument):
        raise PermissionError(f"Access denied: {folder}")

    symbol = str(cfg["symbol"]).upper()

    month = str(month or "current").strip().lower()
    month = month.replace("-", "_").replace(" ", "_")

    candidate_folders = [
        os.path.join(folder, "FUT_TICK"),
        os.path.join(folder, f"NSE_FUT_TICK_{date_str}", "Contract Futures"),
        os.path.join(folder, f"NSE_FUT_TICK_{date_str}"),
        folder,
    ]

    future_files = []

    pattern = re.compile(
        rf"^{re.escape(symbol)}\d{{2}}(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)FUT$",
        re.IGNORECASE
    )

    for search_folder in candidate_folders:
        if not os.path.isdir(search_folder):
            continue

        for root, _, files in os.walk(search_folder):
            for f in files:
                if not f.lower().endswith(".parquet"):
                    continue

                base_name = os.path.splitext(f.upper())[0]

                if pattern.match(base_name):
                    future_files.append((base_name, os.path.join(root, f)))

    if not future_files:
        print(f"No future files found for {symbol} in {folder}")
        return pd.DataFrame(columns=["datetime", "price", "volume"])

    month_order = {
        "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4,
        "MAY": 5, "JUN": 6, "JUL": 7, "AUG": 8,
        "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
    }

    def get_month_rank(file_name):
        file_name = str(file_name).upper()
        for mon, rank in month_order.items():
            if mon + "FUT" in file_name:
                return rank
        return 999

    future_files = sorted(future_files, key=lambda x: get_month_rank(x[0]))

    if month in {"current", "this_month", "current_month", "near", "nearby"}:
        selected = future_files[0][1]
    elif month in {"next", "next_month"}:
        selected = future_files[min(1, len(future_files) - 1)][1]
    elif month in {"far", "far_month", "next_to_next", "next_to_next_month"}:
        selected = future_files[min(2, len(future_files) - 1)][1]
    else:
        selected = future_files[0][1]

    print("Using future file:", selected)

    return _read_parquet_normalized(selected, mode="option")
