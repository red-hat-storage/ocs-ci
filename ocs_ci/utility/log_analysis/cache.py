"""
File-based cache for failure analysis results.

Avoids re-analyzing identical failure signatures across runs.
Uses JSON files in a cache directory with configurable TTL.
"""

import json
import logging
import os
import time
from typing import Optional

from ocs_ci.utility.log_analysis.models import FailureSignature

logger = logging.getLogger(__name__)


class AnalysisCache:
    """File-based cache for failure analysis results."""

    def __init__(
        self, cache_dir: str = "~/.ocs-ci/analysis_cache", ttl_hours: int = 168
    ):
        """
        Args:
            cache_dir: Directory for cache files (supports ~ expansion)
            ttl_hours: Cache time-to-live in hours (default: 7 days)
        """
        self.cache_dir = os.path.expanduser(cache_dir)
        self.ttl_seconds = ttl_hours * 3600
        os.makedirs(self.cache_dir, exist_ok=True)
        logger.debug(f"Analysis cache: {self.cache_dir} (TTL: {ttl_hours}h)")

    def get(self, signature: FailureSignature) -> Optional[dict]:
        """
        Retrieve cached analysis for a failure signature.

        Args:
            signature: FailureSignature to look up

        Returns:
            Cached analysis dict, or None if not found/expired
        """
        path = self._cache_path(signature)
        if not os.path.exists(path):
            return None

        try:
            with open(path) as f:
                data = json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            logger.debug(f"Cache read error for {signature.cache_key}: {e}")
            self._safe_remove(path)
            return None

        # Check TTL
        cached_time = data.get("timestamp", 0)
        if time.time() - cached_time > self.ttl_seconds:
            logger.debug(f"Cache expired for {signature.cache_key}")
            self._safe_remove(path)
            return None

        logger.debug(f"Cache hit for {signature.cache_key}")
        return data.get("analysis")

    def put(self, signature: FailureSignature, analysis: dict):
        """
        Store analysis result in cache.

        Args:
            signature: FailureSignature key
            analysis: Analysis dict to cache
        """
        path = self._cache_path(signature)
        try:
            with open(path, "w") as f:
                json.dump(
                    {
                        "timestamp": time.time(),
                        "signature": signature.to_dict(),
                        "analysis": analysis,
                    },
                    f,
                    indent=2,
                )
            logger.debug(f"Cached analysis for {signature.cache_key}")
        except IOError as e:
            logger.warning(f"Cache write failed for {signature.cache_key}: {e}")

    def clear(self):
        """Remove all cached entries."""
        count = 0
        for filename in os.listdir(self.cache_dir):
            if filename.endswith(".json"):
                self._safe_remove(os.path.join(self.cache_dir, filename))
                count += 1
        logger.info(f"Cleared {count} cache entries")

    def _cache_path(self, signature: FailureSignature) -> str:
        return os.path.join(self.cache_dir, f"{signature.cache_key}.json")

    @staticmethod
    def _safe_remove(path: str):
        try:
            os.remove(path)
        except OSError:
            pass
