"""
Azure Blob Storage helpers for the Option Simulator.

Features:
1. Connect to Azure Blob Storage.
2. Cache blob listings per prefix.
3. Find blobs by filename or stem.
4. Read Parquet directly from Azure when local caching is disabled.
5. Optionally maintain a bounded local disk cache for the current and next
   contract weeks.
6. Download files atomically and reuse an existing cached file.
7. Remove expired/old week folders explicitly and enforce disk limits.

Recommended Azure authentication:
    Managed Identity using DefaultAzureCredential.

Development alternative:
    AZURE_STORAGE_CONNECTION_STRING.
"""

from __future__ import annotations

import hashlib
import io
import logging
import os
import shutil
import tempfile
import threading
import time
from functools import lru_cache
from pathlib import Path, PurePosixPath
from typing import Iterable, Iterator, Optional

import pandas as pd
from azure.core.exceptions import (
    ClientAuthenticationError,
    HttpResponseError,
    ResourceNotFoundError,
)
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, ContainerClient

logger = logging.getLogger(__name__)


# =========================================================
# ENVIRONMENT CONFIGURATION
# =========================================================

AZURE_STORAGE_ACCOUNT_NAME = os.getenv(
    "AZURE_STORAGE_ACCOUNT_NAME",
    "",
).strip()

AZURE_STORAGE_CONTAINER_NAME = os.getenv(
    "AZURE_STORAGE_CONTAINER_NAME",
    "historical-data",
).strip()

AZURE_STORAGE_CONNECTION_STRING = os.getenv(
    "AZURE_STORAGE_CONNECTION_STRING",
    "",
).strip()

# Local cache is optional. Keep it disabled until you intentionally enable it.
LOCAL_BLOB_CACHE_ENABLED = os.getenv(
    "LOCAL_BLOB_CACHE_ENABLED",
    "false",
).strip().lower() in {"1", "true", "yes", "on"}

LOCAL_BLOB_CACHE_DIR = Path(
    os.getenv(
        "LOCAL_BLOB_CACHE_DIR",
        "/tmp/option-simulator-cache",
    )
).expanduser()

# Hard safety boundaries for local cache growth.
LOCAL_BLOB_CACHE_MAX_GB = float(
    os.getenv("LOCAL_BLOB_CACHE_MAX_GB", "8")
)
LOCAL_BLOB_CACHE_MAX_WEEKS = int(
    os.getenv("LOCAL_BLOB_CACHE_MAX_WEEKS", "2")
)

# Read Azure downloads in chunks instead of loading the complete blob into RAM.
AZURE_DOWNLOAD_CHUNK_BYTES = int(
    os.getenv("AZURE_DOWNLOAD_CHUNK_BYTES", str(8 * 1024 * 1024))
)

_CACHE_LOCK = threading.RLock()
_DOWNLOAD_LOCKS: dict[str, threading.Lock] = {}
_DOWNLOAD_LOCKS_GUARD = threading.Lock()


# =========================================================
# PATH HELPERS
# =========================================================

def normalize_blob_name(blob_name: str) -> str:
    """Convert a local-style path into an Azure Blob name."""

    value = str(blob_name or "").strip().replace("\\", "/")
    return value.lstrip("/")


def blob_filename(blob_name: str) -> str:
    """Return only the final filename from a blob name."""

    return PurePosixPath(normalize_blob_name(blob_name)).name


def blob_week_prefix(blob_name: str) -> str:
    """
    Return the first path component, treated as the week-folder prefix.

    Example:
        165 ... - TICK/OPT_TICK/file.parquet
    returns:
        165 ... - TICK
    """

    normalized = normalize_blob_name(blob_name)
    return normalized.split("/", 1)[0] if normalized else ""


def _safe_local_component(value: str) -> str:
    """Create a filesystem-safe, collision-resistant directory component."""

    cleaned = "".join(
        character if character.isalnum() or character in {"-", "_", "."}
        else "_"
        for character in value.strip()
    ).strip("._")

    digest = hashlib.sha1(value.encode("utf-8")).hexdigest()[:10]
    return f"{cleaned[:100] or 'item'}_{digest}"


def local_cache_path(blob_name: str) -> Path:
    """Map an Azure blob name to its deterministic local cache path."""

    normalized = normalize_blob_name(blob_name)
    if not normalized:
        raise ValueError("blob_name cannot be empty.")

    parts = PurePosixPath(normalized).parts
    if not parts:
        raise ValueError("blob_name cannot be empty.")

    week_dir = _safe_local_component(parts[0])
    remainder = [_safe_local_component(part) for part in parts[1:]]
    return LOCAL_BLOB_CACHE_DIR.joinpath(week_dir, *remainder)


def local_week_cache_path(week_prefix: str) -> Path:
    """Return the local directory used for one week prefix."""

    normalized = normalize_blob_name(week_prefix).split("/", 1)[0]
    if not normalized:
        raise ValueError("week_prefix cannot be empty.")
    return LOCAL_BLOB_CACHE_DIR / _safe_local_component(normalized)


# =========================================================
# AZURE CLIENT CREATION
# =========================================================

@lru_cache(maxsize=1)
def get_blob_service_client() -> BlobServiceClient:
    """Create and cache the Azure BlobServiceClient."""

    if AZURE_STORAGE_CONNECTION_STRING:
        logger.info("Connecting to Azure Blob Storage using a connection string.")
        return BlobServiceClient.from_connection_string(
            AZURE_STORAGE_CONNECTION_STRING
        )

    if not AZURE_STORAGE_ACCOUNT_NAME:
        raise RuntimeError(
            "Azure Storage configuration is missing. Set either "
            "AZURE_STORAGE_CONNECTION_STRING or AZURE_STORAGE_ACCOUNT_NAME."
        )

    account_url = (
        f"https://{AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net"
    )

    logger.info(
        "Connecting to Azure Blob Storage using DefaultAzureCredential."
    )

    return BlobServiceClient(
        account_url=account_url,
        credential=DefaultAzureCredential(),
    )


@lru_cache(maxsize=1)
def get_container_client() -> ContainerClient:
    """Return and cache the configured Azure container client."""

    if not AZURE_STORAGE_CONTAINER_NAME:
        raise RuntimeError(
            "AZURE_STORAGE_CONTAINER_NAME is not configured."
        )

    return get_blob_service_client().get_container_client(
        AZURE_STORAGE_CONTAINER_NAME
    )


# =========================================================
# BLOB OPERATIONS
# =========================================================

def blob_exists(blob_name: str) -> bool:
    """Return True when the requested blob exists."""

    normalized_name = normalize_blob_name(blob_name)
    if not normalized_name:
        return False

    try:
        return get_container_client().get_blob_client(
            normalized_name
        ).exists()
    except (ClientAuthenticationError, HttpResponseError) as exc:
        logger.exception(
            "Unable to check whether blob exists: %s",
            normalized_name,
        )
        raise RuntimeError(
            f"Unable to check Azure blob: {normalized_name}"
        ) from exc


def list_blob_names(prefix: str = "") -> Iterator[str]:
    """Yield all blob names under an Azure prefix."""

    normalized_prefix = normalize_blob_name(prefix)

    try:
        for blob in get_container_client().list_blobs(
            name_starts_with=normalized_prefix or None
        ):
            yield blob.name
    except ClientAuthenticationError as exc:
        raise RuntimeError(
            "Authentication failed while listing Azure blobs. "
            "Check the VM Managed Identity or storage credentials."
        ) from exc
    except HttpResponseError as exc:
        raise RuntimeError(
            f"Unable to list Azure blobs for prefix: {normalized_prefix}"
        ) from exc


# =========================================================
# CACHED LISTINGS
# =========================================================

@lru_cache(maxsize=64)
def _cached_blob_names(prefix: str) -> tuple[str, ...]:
    """List every blob under a prefix once per process."""

    return tuple(list_blob_names(prefix))


def clear_listing_cache() -> None:
    """Clear cached Azure blob listings after new uploads."""

    _cached_blob_names.cache_clear()


def find_blob_by_filename(
    prefix: str,
    filename: str,
    case_sensitive: bool = False,
) -> Optional[str]:
    """Find a blob under a prefix using its final filename."""

    normalized_filename = blob_filename(filename)
    if not normalized_filename:
        return None

    expected = (
        normalized_filename
        if case_sensitive
        else normalized_filename.lower()
    )

    for current_blob_name in _cached_blob_names(
        normalize_blob_name(prefix)
    ):
        current_filename = blob_filename(current_blob_name)
        current_value = (
            current_filename
            if case_sensitive
            else current_filename.lower()
        )
        if current_value == expected:
            return current_blob_name

    return None


def find_blob_by_stem(prefix: str, stem: str) -> Optional[str]:
    """Find a blob by filename without its extension."""

    expected_stem = str(stem).strip().lower()
    if not expected_stem:
        return None

    for current_blob_name in _cached_blob_names(
        normalize_blob_name(prefix)
    ):
        current_stem = PurePosixPath(
            normalize_blob_name(current_blob_name)
        ).stem.lower()
        if current_stem == expected_stem:
            return current_blob_name

    return None


def download_blob_bytes(blob_name: str) -> bytes:
    """Download a blob completely into RAM."""

    normalized_name = normalize_blob_name(blob_name)
    if not normalized_name:
        raise ValueError("blob_name cannot be empty.")

    try:
        blob_client = get_container_client().get_blob_client(
            normalized_name
        )
        return blob_client.download_blob().readall()
    except ResourceNotFoundError as exc:
        raise FileNotFoundError(
            f"Azure blob not found: {normalized_name}"
        ) from exc
    except ClientAuthenticationError as exc:
        raise RuntimeError(
            "Azure Blob Storage authentication failed. "
            "Check Managed Identity, RBAC or the connection string."
        ) from exc
    except HttpResponseError as exc:
        raise RuntimeError(
            f"Unable to download Azure blob: {normalized_name}"
        ) from exc


# =========================================================
# BOUNDED LOCAL DISK CACHE
# =========================================================

def _download_lock(blob_name: str) -> threading.Lock:
    """Return one process-local lock for a blob download."""

    normalized = normalize_blob_name(blob_name)
    with _DOWNLOAD_LOCKS_GUARD:
        return _DOWNLOAD_LOCKS.setdefault(normalized, threading.Lock())


def _touch(path: Path) -> None:
    """Update access/modification times for LRU-style eviction."""

    try:
        now = time.time()
        os.utime(path, (now, now))
        os.utime(path.parent, (now, now))
    except OSError:
        logger.debug("Unable to touch cache path: %s", path)


def cache_size_bytes() -> int:
    """Return total bytes currently stored in the local cache."""

    if not LOCAL_BLOB_CACHE_DIR.exists():
        return 0

    total = 0
    for path in LOCAL_BLOB_CACHE_DIR.rglob("*"):
        try:
            if path.is_file():
                total += path.stat().st_size
        except FileNotFoundError:
            continue
    return total


def cached_week_directories() -> list[Path]:
    """Return cached week directories ordered oldest first."""

    if not LOCAL_BLOB_CACHE_DIR.exists():
        return []

    week_dirs = [
        path for path in LOCAL_BLOB_CACHE_DIR.iterdir()
        if path.is_dir()
    ]
    return sorted(
        week_dirs,
        key=lambda path: path.stat().st_mtime,
    )


def remove_cached_week(week_prefix: str) -> bool:
    """Delete one local week cache. Azure Blob data is not affected."""

    target = local_week_cache_path(week_prefix)
    with _CACHE_LOCK:
        if not target.exists():
            return False
        shutil.rmtree(target)
        logger.info("Removed cached week: %s", target)
        return True


def enforce_local_cache_limits(
    protected_week_prefixes: Iterable[str] = (),
) -> list[str]:
    """
    Enforce maximum week count and disk size.

    Protected prefixes, normally current and next week, are never evicted.
    Returns the local directory names that were removed.
    """

    if not LOCAL_BLOB_CACHE_ENABLED:
        return []

    LOCAL_BLOB_CACHE_DIR.mkdir(parents=True, exist_ok=True)

    protected_names = {
        local_week_cache_path(prefix).name
        for prefix in protected_week_prefixes
        if normalize_blob_name(prefix)
    }

    max_bytes = max(0, int(LOCAL_BLOB_CACHE_MAX_GB * 1024**3))
    max_weeks = max(1, LOCAL_BLOB_CACHE_MAX_WEEKS)
    removed: list[str] = []

    with _CACHE_LOCK:
        while True:
            weeks = cached_week_directories()
            total_bytes = cache_size_bytes()
            over_week_limit = len(weeks) > max_weeks
            over_size_limit = max_bytes > 0 and total_bytes > max_bytes

            if not over_week_limit and not over_size_limit:
                break

            candidate = next(
                (
                    week for week in weeks
                    if week.name not in protected_names
                ),
                None,
            )

            if candidate is None:
                logger.warning(
                    "Cache limits are exceeded, but every cached week is "
                    "protected. Current size: %.2f GB.",
                    total_bytes / 1024**3,
                )
                break

            removed.append(candidate.name)
            shutil.rmtree(candidate)
            logger.info("Evicted old cached week: %s", candidate)

    return removed


def download_blob_to_cache(
    blob_name: str,
    *,
    force: bool = False,
) -> Path:
    """
    Download one blob into the bounded local cache and return its path.

    The file is first written to a temporary path and then atomically renamed,
    so another request never sees a partially downloaded Parquet file.
    """

    if not LOCAL_BLOB_CACHE_ENABLED:
        raise RuntimeError(
            "Local blob cache is disabled. Set LOCAL_BLOB_CACHE_ENABLED=true."
        )

    normalized_name = normalize_blob_name(blob_name)
    if not normalized_name:
        raise ValueError("blob_name cannot be empty.")

    destination = local_cache_path(normalized_name)
    lock = _download_lock(normalized_name)

    with lock:
        if destination.exists() and destination.stat().st_size > 0 and not force:
            _touch(destination)
            return destination

        destination.parent.mkdir(parents=True, exist_ok=True)

        try:
            blob_client = get_container_client().get_blob_client(
                normalized_name
            )
            downloader = blob_client.download_blob(
                max_concurrency=4,
            )

            with tempfile.NamedTemporaryFile(
                mode="wb",
                dir=destination.parent,
                prefix=f".{destination.name}.",
                suffix=".part",
                delete=False,
            ) as temporary_file:
                temporary_path = Path(temporary_file.name)

                for chunk in downloader.chunks():
                    temporary_file.write(chunk)

            if temporary_path.stat().st_size == 0:
                temporary_path.unlink(missing_ok=True)
                raise RuntimeError(
                    f"Downloaded Azure blob is empty: {normalized_name}"
                )

            temporary_path.replace(destination)
            _touch(destination)
            logger.info(
                "Cached Azure blob locally: %s -> %s",
                normalized_name,
                destination,
            )
            return destination

        except ResourceNotFoundError as exc:
            raise FileNotFoundError(
                f"Azure blob not found: {normalized_name}"
            ) from exc
        except ClientAuthenticationError as exc:
            raise RuntimeError(
                "Azure Blob Storage authentication failed."
            ) from exc
        except HttpResponseError as exc:
            raise RuntimeError(
                f"Unable to download Azure blob: {normalized_name}"
            ) from exc
        finally:
            # Remove abandoned partial files left by failed downloads.
            for partial in destination.parent.glob(
                f".{destination.name}.*.part"
            ):
                try:
                    partial.unlink()
                except OSError:
                    logger.warning(
                        "Unable to remove partial cache file: %s",
                        partial,
                    )


def ensure_week_cached(
    week_prefix: str,
    *,
    suffixes: tuple[str, ...] = (".parquet",),
    force: bool = False,
) -> list[Path]:
    """
    Download matching files for one week prefix.

    For lower disk usage, pass a narrower prefix such as:
        <week>/OPT_TICK
    instead of the complete week folder.
    """

    normalized_prefix = normalize_blob_name(week_prefix).rstrip("/")
    if not normalized_prefix:
        raise ValueError("week_prefix cannot be empty.")

    wanted_suffixes = tuple(suffix.lower() for suffix in suffixes)
    blob_names = [
        blob_name
        for blob_name in _cached_blob_names(normalized_prefix)
        if blob_name.lower().endswith(wanted_suffixes)
    ]

    downloaded = [
        download_blob_to_cache(blob_name, force=force)
        for blob_name in blob_names
    ]

    enforce_local_cache_limits(
        protected_week_prefixes=(blob_week_prefix(normalized_prefix),)
    )
    return downloaded


def prepare_contract_week_cache(
    current_week_prefix: str,
    next_week_prefix: str,
    *,
    data_subfolders: tuple[str, ...] = ("OPT_TICK", "FUT_TICK", "IDX_TICK"),
) -> dict[str, object]:
    """
    Cache the current and next contract weeks and evict older local weeks.

    This function does not infer expiry dates. The caller must provide the two
    Azure week prefixes selected by the simulator's own expiry/week logic.
    """

    if not LOCAL_BLOB_CACHE_ENABLED:
        return {
            "enabled": False,
            "current_week_files": [],
            "next_week_files": [],
            "evicted_weeks": [],
        }

    current_root = normalize_blob_name(current_week_prefix).rstrip("/")
    next_root = normalize_blob_name(next_week_prefix).rstrip("/")

    if not current_root or not next_root:
        raise ValueError(
            "Both current_week_prefix and next_week_prefix are required."
        )

    current_files: list[Path] = []
    next_files: list[Path] = []

    for subfolder in data_subfolders:
        current_files.extend(
            ensure_week_cached(f"{current_root}/{subfolder}")
        )
        next_files.extend(
            ensure_week_cached(f"{next_root}/{subfolder}")
        )

    evicted = enforce_local_cache_limits(
        protected_week_prefixes=(current_root, next_root)
    )

    return {
        "enabled": True,
        "current_week_files": current_files,
        "next_week_files": next_files,
        "evicted_weeks": evicted,
        "cache_size_bytes": cache_size_bytes(),
    }


# =========================================================
# PARQUET READERS
# =========================================================

def read_parquet_blob(
    blob_name: str,
    columns: Optional[list[str]] = None,
    *,
    prefer_local_cache: Optional[bool] = None,
) -> pd.DataFrame:
    """
    Read a Parquet blob into a DataFrame.

    When local caching is enabled, reads the cached file and reuses it on later
    requests. Otherwise, downloads the blob into memory as before.
    """

    use_local_cache = (
        LOCAL_BLOB_CACHE_ENABLED
        if prefer_local_cache is None
        else prefer_local_cache
    )

    try:
        if use_local_cache:
            local_path = download_blob_to_cache(blob_name)
            frame = pd.read_parquet(local_path, columns=columns)
            _touch(local_path)
            return frame

        content = download_blob_bytes(blob_name)
        if not content:
            return pd.DataFrame()
        return pd.read_parquet(io.BytesIO(content), columns=columns)

    except Exception as exc:
        raise RuntimeError(
            f"Unable to read Parquet data from blob: {blob_name}"
        ) from exc


def test_blob_connection() -> dict:
    """Test the Blob Storage connection and return basic information."""

    try:
        container_client = get_container_client()
        first_blob = next(
            iter(container_client.list_blobs(results_per_page=1)),
            None,
        )
        return {
            "success": True,
            "account_name": AZURE_STORAGE_ACCOUNT_NAME,
            "container_name": AZURE_STORAGE_CONTAINER_NAME,
            "first_blob": first_blob.name if first_blob else None,
            "local_cache_enabled": LOCAL_BLOB_CACHE_ENABLED,
            "local_cache_dir": str(LOCAL_BLOB_CACHE_DIR),
            "local_cache_size_bytes": cache_size_bytes(),
        }
    except Exception as exc:
        return {
            "success": False,
            "account_name": AZURE_STORAGE_ACCOUNT_NAME,
            "container_name": AZURE_STORAGE_CONTAINER_NAME,
            "error": str(exc),
        }


if __name__ == "__main__":
    print(test_blob_connection())
