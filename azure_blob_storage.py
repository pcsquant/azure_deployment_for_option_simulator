"""
Azure Blob Storage helper functions for the Option Simulator.

This module:

1. Connects to an Azure Blob Storage container.
2. Lists blobs by prefix.
3. Finds Parquet files by filename.
4. Downloads Parquet files into memory.
5. Reads Parquet files directly into pandas DataFrames.

Recommended authentication on an Azure VM:
    Managed Identity using DefaultAzureCredential.

Alternative for development:
    Azure Storage connection string.
"""

from __future__ import annotations

import io
import logging
import os
from functools import lru_cache
from pathlib import PurePosixPath
from typing import Iterator, Optional

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


# =========================================================
# PATH HELPERS
# =========================================================

def normalize_blob_name(blob_name: str) -> str:
    """
    Convert a local-style path into an Azure Blob name.

    Azure Blob Storage always uses forward slashes.

    Example:
        165 06 Jul to 10 Jul (NSE FO) - TICK\\OPT_TICK\\file.parquet

    becomes:
        165 06 Jul to 10 Jul (NSE FO) - TICK/OPT_TICK/file.parquet
    """

    value = str(blob_name or "").strip()
    value = value.replace("\\", "/")

    # Avoid leading slash because blob names normally do not begin with "/".
    return value.lstrip("/")


def blob_filename(blob_name: str) -> str:
    """Return only the final filename from a blob name."""

    normalized = normalize_blob_name(blob_name)
    return PurePosixPath(normalized).name


# =========================================================
# AZURE CLIENT CREATION
# =========================================================

@lru_cache(maxsize=1)
def get_blob_service_client() -> BlobServiceClient:
    """
    Create and cache the Azure BlobServiceClient.

    Authentication priority:

    1. AZURE_STORAGE_CONNECTION_STRING
    2. Managed Identity / Azure CLI / environment credentials through
       DefaultAzureCredential
    """

    if AZURE_STORAGE_CONNECTION_STRING:
        logger.info(
            "Connecting to Azure Blob Storage using a connection string."
        )

        return BlobServiceClient.from_connection_string(
            AZURE_STORAGE_CONNECTION_STRING
        )

    if not AZURE_STORAGE_ACCOUNT_NAME:
        raise RuntimeError(
            "Azure Storage configuration is missing. Set either "
            "AZURE_STORAGE_CONNECTION_STRING or "
            "AZURE_STORAGE_ACCOUNT_NAME."
        )

    account_url = (
        f"https://{AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net"
    )

    logger.info(
        "Connecting to Azure Blob Storage using DefaultAzureCredential."
    )

    credential = DefaultAzureCredential()

    return BlobServiceClient(
        account_url=account_url,
        credential=credential,
    )


@lru_cache(maxsize=1)
def get_container_client() -> ContainerClient:
    """Return and cache the configured Azure container client."""

    if not AZURE_STORAGE_CONTAINER_NAME:
        raise RuntimeError(
            "AZURE_STORAGE_CONTAINER_NAME is not configured."
        )

    service_client = get_blob_service_client()

    return service_client.get_container_client(
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
        blob_client = get_container_client().get_blob_client(
            normalized_name
        )

        return blob_client.exists()

    except (ClientAuthenticationError, HttpResponseError) as exc:
        logger.exception(
            "Unable to check whether blob exists: %s",
            normalized_name,
        )
        raise RuntimeError(
            f"Unable to check Azure blob: {normalized_name}"
        ) from exc


def list_blob_names(prefix: str = "") -> Iterator[str]:
    """
    Yield all blob names under a prefix.

    Azure folders are not real directories. They are prefixes inside blob
    names.

    Example prefix:
        165 06 Jul to 10 Jul (NSE FO) - TICK/OPT_TICK
    """

    normalized_prefix = normalize_blob_name(prefix)

    try:
        container_client = get_container_client()

        for blob in container_client.list_blobs(
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
            f"Unable to list Azure blobs for prefix: "
            f"{normalized_prefix}"
        ) from exc


def find_blob_by_filename(
    prefix: str,
    filename: str,
    case_sensitive: bool = False,
) -> Optional[str]:
    """
    Find a blob under a prefix using its final filename.

    Parameters
    ----------
    prefix:
        Blob prefix representing the folder to search.

    filename:
        Filename to locate, for example:
        NIFTY.parquet
        NIFTY26070923400CE.parquet

    case_sensitive:
        Whether the filename match must preserve case.

    Returns
    -------
    str | None
        Full blob name when found, otherwise None.
    """

    normalized_filename = blob_filename(filename)

    if not normalized_filename:
        return None

    expected = (
        normalized_filename
        if case_sensitive
        else normalized_filename.lower()
    )

    for current_blob_name in list_blob_names(prefix):
        current_filename = blob_filename(current_blob_name)

        current_value = (
            current_filename
            if case_sensitive
            else current_filename.lower()
        )

        if current_value == expected:
            return current_blob_name

    return None


def find_blob_by_stem(
    prefix: str,
    stem: str,
) -> Optional[str]:
    """
    Find a blob by filename without its extension.

    Example:
        stem="NIFTY26070923400CE"

    can match:
        NIFTY26070923400CE.parquet
    """

    expected_stem = str(stem).strip().lower()

    if not expected_stem:
        return None

    for current_blob_name in list_blob_names(prefix):
        current_stem = PurePosixPath(
            normalize_blob_name(current_blob_name)
        ).stem.lower()

        if current_stem == expected_stem:
            return current_blob_name

    return None


def download_blob_bytes(blob_name: str) -> bytes:
    """Download a blob completely into memory."""

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


def read_parquet_blob(
    blob_name: str,
    columns: Optional[list[str]] = None,
) -> pd.DataFrame:
    """
    Read a Parquet blob directly into a pandas DataFrame.

    The file is downloaded into memory. It is not permanently written to
    the VM disk.

    Parameters
    ----------
    blob_name:
        Full blob name inside the configured container.

    columns:
        Optional list of Parquet columns to read.
    """

    content = download_blob_bytes(blob_name)

    if not content:
        return pd.DataFrame()

    buffer = io.BytesIO(content)

    try:
        return pd.read_parquet(
            buffer,
            columns=columns,
        )

    except Exception as exc:
        raise RuntimeError(
            f"Unable to read Parquet data from blob: {blob_name}"
        ) from exc


def test_blob_connection() -> dict:
    """
    Test the Blob Storage connection and return basic information.

    This is useful during VM setup.
    """

    try:
        container_client = get_container_client()

        # Fetch one item only to confirm listing permission.
        first_blob = next(
            iter(container_client.list_blobs(results_per_page=1)),
            None,
        )

        return {
            "success": True,
            "account_name": AZURE_STORAGE_ACCOUNT_NAME,
            "container_name": AZURE_STORAGE_CONTAINER_NAME,
            "first_blob": first_blob.name if first_blob else None,
        }

    except Exception as exc:
        return {
            "success": False,
            "account_name": AZURE_STORAGE_ACCOUNT_NAME,
            "container_name": AZURE_STORAGE_CONTAINER_NAME,
            "error": str(exc),
        }


if __name__ == "__main__":
    result = test_blob_connection()
    print(result)
