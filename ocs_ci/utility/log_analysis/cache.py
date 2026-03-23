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
        self, cache_dir: str = "~/.ocs-ci/analysis_cache", ttl_hours: int = 720
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

    def get(self, signature: FailureSignature) -> Optional[tuple]:
        """
        Retrieve cached analysis for a failure signature.

        Args:
            signature: FailureSignature to look up

        Returns:
            Tuple of (analysis_dict, cache_file_path), or None if not found/expired
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
        analysis = data.get("analysis")
        # Include the cached test name so callers can detect cross-test hits
        cached_test_name = data.get("test_name", "")
        if cached_test_name and analysis is not None:
            analysis["_cached_test_name"] = cached_test_name
        return analysis, path

    def put(
        self,
        signature: FailureSignature,
        analysis: dict,
        run_metadata: dict = None,
        test_name: str = "",
        test_class: str = "",
        squad: str = "",
        traceback: str = "",
        status: str = "",
        polarion_id: str = "",
    ):
        """
        Store analysis result in cache.

        Args:
            signature: FailureSignature key
            analysis: Analysis dict to cache
            run_metadata: Run metadata (platform, versions, etc.) for traceability
            test_name: Human-readable test name
            test_class: Test classname
            squad: Test squad
            traceback: Full traceback text
            status: Test status (failed/error)
            polarion_id: Polarion test case ID
        """
        path = self._cache_path(signature)
        try:
            data = {
                "timestamp": time.time(),
                "signature": signature.to_dict(),
                "analysis": analysis,
            }
            if test_name:
                data["test_name"] = test_name
            if test_class:
                data["test_class"] = test_class
            if squad:
                data["squad"] = squad
            if traceback:
                data["traceback"] = traceback
            if status:
                data["status"] = status
            if polarion_id:
                data["polarion_id"] = polarion_id
            if run_metadata:
                data["run_metadata"] = {
                    k: run_metadata.get(k, "")
                    for k in [
                        "platform",
                        "deployment_type",
                        "ocp_version",
                        "ocs_version",
                        "ocs_build",
                        "logs_url",
                        "run_id",
                        "launch_name",
                        "jenkins_url",
                        "run_timestamp",
                    ]
                }
            with open(path, "w") as f:
                json.dump(data, f, indent=2)
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
