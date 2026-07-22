from __future__ import annotations

import logging
import threading
from collections import OrderedDict
from dataclasses import dataclass
from typing import Callable, Hashable

import pandas as pd


logger = logging.getLogger(__name__)


@dataclass
class CacheStats:
    hits: int = 0
    misses: int = 0
    evictions: int = 0


class ContractDataFrameCache:
    """
    Thread-safe bounded LRU cache for option-contract DataFrames.
    """

    def __init__(self, max_items: int = 300):
        if max_items <= 0:
            raise ValueError("max_items must be greater than zero")

        self.max_items = max_items
        self._cache: OrderedDict[Hashable, pd.DataFrame] = OrderedDict()
        self._lock = threading.RLock()
        self._stats = CacheStats()

    def get(self, key: Hashable) -> pd.DataFrame | None:
        with self._lock:
            dataframe = self._cache.get(key)

            if dataframe is None:
                self._stats.misses += 1
                return None

            # Move the recently accessed item to the end.
            self._cache.move_to_end(key)
            self._stats.hits += 1

            return dataframe

    def put(self, key: Hashable, dataframe: pd.DataFrame) -> None:
        with self._lock:
            if key in self._cache:
                self._cache[key] = dataframe
                self._cache.move_to_end(key)
                return

            self._cache[key] = dataframe
            self._cache.move_to_end(key)

            while len(self._cache) > self.max_items:
                removed_key, removed_df = self._cache.popitem(last=False)
                self._stats.evictions += 1

                logger.info(
                    "Evicted contract from RAM cache key=%s rows=%s",
                    removed_key,
                    len(removed_df),
                )

    def get_or_load(
        self,
        key: Hashable,
        loader: Callable[[], pd.DataFrame],
    ) -> tuple[pd.DataFrame, bool]:
        """
        Returns:
            dataframe
            cache_hit
        """

        cached = self.get(key)

        if cached is not None:
            return cached, True

        loaded = loader()

        if loaded is None:
            loaded = pd.DataFrame()

        self.put(key, loaded)

        return loaded, False

    def clear(self) -> None:
        with self._lock:
            self._cache.clear()

    def info(self) -> dict:
        with self._lock:
            return {
                "current_items": len(self._cache),
                "max_items": self.max_items,
                "hits": self._stats.hits,
                "misses": self._stats.misses,
                "evictions": self._stats.evictions,
            }
