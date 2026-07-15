"""
Configuration for the Option Simulator.

Supported environments
----------------------
Local Windows development:
    PARQUET_BASE_PATH=C:\\Users\\admin\\Documents\\shamil\\agent\\weekely_data
    OPTION_PARQUET_BASE_PATH=C:\\Users\\admin\\Documents\\shamil\\agent\\weekely_data

Azure Ubuntu VM:
    PARQUET_BASE_PATH=/opt/option-simulator/data
    OPTION_PARQUET_BASE_PATH=/opt/option-simulator/data

The environment variables should normally be configured outside the source code,
for example through systemd or a local .env file.
"""

from __future__ import annotations

import logging
import os
from datetime import time
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytz


logger = logging.getLogger(__name__)


# =========================================================
# GENERAL SETTINGS
# =========================================================

CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "100000"))

IST = pytz.timezone("Asia/Kolkata")

SESSION_START = time(9, 15)
SESSION_END = time(15, 30)

CANDLE_INTERVAL_MINUTES = int(
    os.getenv("CANDLE_INTERVAL_MINUTES", "3")
)

WEEK_START = int(os.getenv("WEEK_START", "1"))
WEEK_END = int(os.getenv("WEEK_END", "999"))


# =========================================================
# PATH SETTINGS
# =========================================================

DEFAULT_AZURE_DATA_PATH = Path(
    "/opt/option-simulator/data"
)

PARQUET_BASE_PATH = Path(
    os.getenv(
        "PARQUET_BASE_PATH",
        str(DEFAULT_AZURE_DATA_PATH),
    )
).expanduser().resolve()

OPTION_PARQUET_BASE_PATH = Path(
    os.getenv(
        "OPTION_PARQUET_BASE_PATH",
        str(PARQUET_BASE_PATH),
    )
).expanduser().resolve()


def validate_configuration(
    require_data_paths: bool = False,
) -> None:
    """
    Validate the simulator configuration.

    Parameters
    ----------
    require_data_paths:
        When True, raise an error if the configured historical-data
        directories do not exist.

        Keep this False during deployment or repository import when the
        data may be downloaded or mounted later.
    """

    if CHUNK_SIZE <= 0:
        raise ValueError("CHUNK_SIZE must be greater than zero.")

    if CANDLE_INTERVAL_MINUTES <= 0:
        raise ValueError(
            "CANDLE_INTERVAL_MINUTES must be greater than zero."
        )

    if WEEK_START < 0:
        raise ValueError("WEEK_START cannot be negative.")

    if WEEK_END < WEEK_START:
        raise ValueError(
            "WEEK_END must be greater than or equal to WEEK_START."
        )

    if SESSION_START >= SESSION_END:
        raise ValueError(
            "SESSION_START must be earlier than SESSION_END."
        )

    if require_data_paths:
        missing_paths = [
            path
            for path in {
                PARQUET_BASE_PATH,
                OPTION_PARQUET_BASE_PATH,
            }
            if not path.exists()
        ]

        if missing_paths:
            missing_text = ", ".join(
                str(path) for path in missing_paths
            )

            raise FileNotFoundError(
                f"Historical-data path does not exist: {missing_text}"
            )


def create_runtime_directories() -> None:
    """
    Create the configured data directories when appropriate.

    This should normally be called by a deployment/setup script rather
    than automatically during import.
    """

    PARQUET_BASE_PATH.mkdir(
        parents=True,
        exist_ok=True,
    )

    OPTION_PARQUET_BASE_PATH.mkdir(
        parents=True,
        exist_ok=True,
    )


# =========================================================
# NIFTY WEEKLY EXPIRY DATES
# =========================================================

nifty_expiry_2025 = pd.to_datetime(
    [
        "2025-01-02",
        "2025-01-09",
        "2025-01-16",
        "2025-01-23",
        "2025-01-30",
        "2025-02-06",
        "2025-02-13",
        "2025-02-20",
        "2025-02-27",
        "2025-03-06",
        "2025-03-13",
        "2025-03-20",
        "2025-03-27",
        "2025-04-03",
        "2025-04-09",
        "2025-04-17",
        "2025-04-24",
        "2025-04-30",
        "2025-05-08",
        "2025-05-15",
        "2025-05-22",
        "2025-05-29",
        "2025-06-05",
        "2025-06-12",
        "2025-06-19",
        "2025-06-26",
        "2025-07-03",
        "2025-07-10",
        "2025-07-17",
        "2025-07-24",
        "2025-07-31",
        "2025-08-07",
        "2025-08-14",
        "2025-08-21",
        "2025-08-28",
        "2025-09-02",
        "2025-09-09",
        "2025-09-16",
        "2025-09-23",
        "2025-09-30",
        "2025-10-07",
        "2025-10-14",
        "2025-10-20",
        "2025-10-28",
        "2025-11-04",
        "2025-11-11",
        "2025-11-18",
        "2025-11-25",
        "2025-12-02",
        "2025-12-09",
        "2025-12-16",
        "2025-12-23",
        "2025-12-30",
    ]
)

nifty_expiry_2026 = pd.to_datetime(
    [
        "2026-01-06",
        "2026-01-13",
        "2026-01-20",
        "2026-01-27",
        "2026-02-03",
        "2026-02-10",
        "2026-02-17",
        "2026-02-24",
        "2026-03-02",
        "2026-03-10",
        "2026-03-17",
        "2026-03-24",
        "2026-03-30",
        "2026-04-07",
        "2026-04-14",
        "2026-04-21",
        "2026-04-28",
        "2026-05-05",
        "2026-05-12",
        "2026-05-19",
        "2026-05-26",
        "2026-06-02",
        "2026-06-09",
        "2026-06-16",
        "2026-06-23",
        "2026-06-30",
        "2026-07-07",
        "2026-07-14",
        "2026-07-21",
        "2026-07-28",
        "2026-08-04",
        "2026-08-11",
        "2026-08-18",
        "2026-08-25",
        "2026-09-01",
        "2026-09-08",
        "2026-09-15",
        "2026-09-22",
        "2026-09-29",
        "2026-10-06",
        "2026-10-13",
        "2026-10-20",
        "2026-10-27",
        "2026-11-03",
        "2026-11-10",
        "2026-11-17",
        "2026-11-24",
        "2026-12-01",
        "2026-12-08",
        "2026-12-15",
        "2026-12-22",
        "2026-12-29",
    ]
)


# =========================================================
# NIFTY MONTHLY EXPIRY DATES
# =========================================================

nifty_monthly_expiry_2025 = pd.to_datetime(
    [
        "2025-01-30",
        "2025-02-27",
        "2025-03-27",
        "2025-04-30",
        "2025-05-29",
        "2025-06-26",
        "2025-07-31",
        "2025-08-28",
        "2025-09-30",
        "2025-10-28",
        "2025-11-25",
        "2025-12-30",
    ]
)

nifty_monthly_expiry_2026 = pd.to_datetime(
    [
        "2026-01-27",
        "2026-02-24",
        "2026-03-30",
        "2026-04-28",
        "2026-05-26",
        "2026-06-30",
        "2026-07-28",
        "2026-08-25",
        "2026-09-29",
        "2026-10-27",
        "2026-11-24",
        "2026-12-29",
    ]
)


NIFTY_MONTHLY_COMBINED_EXPIRY = np.sort(
    np.concatenate(
        [
            nifty_monthly_expiry_2025.values,
            nifty_monthly_expiry_2026.values,
        ]
    )
)

NIFTY_COMBINED_EXPIRY = np.sort(
    np.concatenate(
        [
            nifty_expiry_2025.values,
            nifty_expiry_2026.values,
        ]
    )
)


# =========================================================
# BSE / SENSEX EXPIRY DATES
# =========================================================

bse_expiry_2025 = pd.to_datetime(
    [
        "2025-01-07",
        "2025-01-14",
        "2025-01-21",
        "2025-01-28",
        "2025-02-04",
        "2025-02-11",
        "2025-02-18",
        "2025-02-25",
        "2025-03-04",
        "2025-03-11",
        "2025-03-18",
        "2025-03-25",
        "2025-04-01",
        "2025-04-08",
        "2025-04-15",
        "2025-04-22",
        "2025-04-29",
        "2025-05-06",
        "2025-05-13",
        "2025-05-20",
        "2025-05-27",
        "2025-06-03",
        "2025-06-10",
        "2025-06-17",
        "2025-06-24",
        "2025-07-01",
        "2025-07-08",
        "2025-07-15",
        "2025-07-22",
        "2025-07-29",
        "2025-08-05",
        "2025-08-12",
        "2025-08-19",
        "2025-08-26",
        "2025-09-04",
        "2025-09-11",
        "2025-09-18",
        "2025-09-25",
        "2025-10-09",
        "2025-10-16",
        "2025-10-23",
        "2025-10-30",
        "2025-11-06",
        "2025-11-13",
        "2025-11-20",
        "2025-11-27",
        "2025-12-04",
        "2025-12-11",
        "2025-12-18",
        "2025-12-24",
    ]
)

bse_expiry_2026 = pd.to_datetime(
    [
        "2026-01-01",
        "2026-01-08",
        "2026-01-15",
        "2026-01-22",
        "2026-01-29",
        "2026-02-05",
        "2026-02-12",
        "2026-02-19",
        "2026-02-26",
        "2026-03-05",
        "2026-03-12",
        "2026-03-19",
        "2026-03-25",
        "2026-04-02",
        "2026-04-09",
        "2026-04-16",
        "2026-04-23",
        "2026-04-30",
        "2026-05-07",
        "2026-05-14",
        "2026-05-21",
        "2026-05-27",
        "2026-06-04",
        "2026-06-11",
        "2026-06-18",
        "2026-06-25",
        "2026-07-02",
        "2026-07-09",
        "2026-07-16",
        "2026-07-23",
        "2026-07-30",
        "2026-08-06",
        "2026-08-13",
        "2026-08-20",
        "2026-08-27",
        "2026-09-03",
        "2026-09-10",
        "2026-09-17",
        "2026-09-24",
        "2026-10-01",
        "2026-10-08",
        "2026-10-15",
        "2026-10-22",
        "2026-10-29",
        "2026-11-05",
        "2026-11-12",
        "2026-11-19",
        "2026-11-26",
        "2026-12-03",
        "2026-12-10",
        "2026-12-17",
        "2026-12-24",
        "2026-12-31",
    ]
)

BSE_COMBINED_EXPIRY = np.sort(
    np.concatenate(
        [
            bse_expiry_2025.values,
            bse_expiry_2026.values,
        ]
    )
)


# =========================================================
# DATASET CONFIGURATION
# =========================================================

def get_dataset_config(
    instrument: str = "NIFTY",
) -> dict[str, Any]:
    """
    Return dataset configuration for the requested instrument.

    Supported values:
        NIFTY / NSE
        SENSEX / BSE
        BANKNIFTY / BANK NIFTY / BANK-NIFTY
    """

    normalized_instrument = str(
        instrument or "NIFTY"
    ).strip().upper()

    if normalized_instrument in {"NIFTY", "NSE"}:
        return {
            "instrument": "NIFTY",
            "symbol": "NIFTY",
            "base_path": str(PARQUET_BASE_PATH),
            "option_base_path": str(OPTION_PARQUET_BASE_PATH),
            "idx_zip_prefix": "NIFTY",
            "opt_zip_prefix": "NIFTY",
            "zip_member": "NIFTY.parquet",
            "strike_step": 50,
            "week_start": 0,
            "week_end": WEEK_END,
            "combined_expiry": NIFTY_COMBINED_EXPIRY.copy(),
            "monthly_expiry": NIFTY_MONTHLY_COMBINED_EXPIRY.copy(),
        }

    if normalized_instrument in {"SENSEX", "BSE"}:
        return {
            "instrument": "SENSEX",
            "symbol": "SENSEX",
            "base_path": str(PARQUET_BASE_PATH),
            "option_base_path": str(OPTION_PARQUET_BASE_PATH),
            "idx_zip_prefix": "SENSEX",
            "opt_zip_prefix": "SENSEX",
            "zip_member": "SENSEX.parquet",
            "strike_step": 100,
            "week_start": WEEK_START,
            "week_end": WEEK_END,
            "combined_expiry": BSE_COMBINED_EXPIRY.copy(),
            "monthly_expiry": BSE_COMBINED_EXPIRY.copy(),
        }

    if normalized_instrument in {
        "BANKNIFTY",
        "BANK NIFTY",
        "BANK-NIFTY",
    }:
        return {
            "instrument": "BANKNIFTY",
            "symbol": "BANKNIFTY",
            "base_path": str(PARQUET_BASE_PATH),
            "option_base_path": str(OPTION_PARQUET_BASE_PATH),
            "idx_zip_prefix": "BANKNIFTY",
            "opt_zip_prefix": "BANKNIFTY",
            "zip_member": "BANKNIFTY.parquet",
            "strike_step": 100,
            "week_start": WEEK_START,
            "week_end": WEEK_END,
            "combined_expiry": NIFTY_MONTHLY_COMBINED_EXPIRY.copy(),
            "monthly_expiry": NIFTY_MONTHLY_COMBINED_EXPIRY.copy(),
        }

    raise ValueError(
        "Unsupported instrument: "
        f"{instrument!r}. Supported instruments are "
        "NIFTY, SENSEX and BANKNIFTY."
    )


def log_configuration() -> None:
    """
    Log non-sensitive runtime configuration.

    This is helpful when diagnosing data-path problems on Azure.
    """

    logger.info(
        "Option Simulator configuration: "
        "PARQUET_BASE_PATH=%s, "
        "OPTION_PARQUET_BASE_PATH=%s, "
        "CANDLE_INTERVAL_MINUTES=%s, "
        "SESSION=%s-%s",
        PARQUET_BASE_PATH,
        OPTION_PARQUET_BASE_PATH,
        CANDLE_INTERVAL_MINUTES,
        SESSION_START.strftime("%H:%M"),
        SESSION_END.strftime("%H:%M"),
    )


# Validate values that do not depend on whether data has already been mounted.
validate_configuration(require_data_paths=False)
