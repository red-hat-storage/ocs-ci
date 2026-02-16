"""
Parse must-gather tar.gz archives to extract infrastructure context.

For failed tests, OCS-CI collects must-gather bundles (12MB+ each)
containing Ceph logs, OSD info, pod events, etc. This parser extracts
the most relevant information for AI-powered root cause analysis.
"""

import io
import logging
import os
import re
import tarfile

logger = logging.getLogger(__name__)

# Files within must-gather that are most useful for failure analysis
RELEVANT_PATHS = [
    # Ceph status and health
    r"ceph/health",
    r"ceph/status",
    r"ceph/osd/tree",
    r"ceph/osd/dump",
    r"ceph/pg/stat",
    # Crash reports
    r"ceph/crash",
    # Cluster operator status
    r"cluster-scoped-resources/storage.k8s.io",
    # Pod logs and events
    r"namespaces/openshift-storage/pods/.*/logs/current\.log",
    # Events
    r"namespaces/openshift-storage/events\.yaml",
    # Storage cluster status
    r"storageclusters",
    r"cephclusters",
]

RELEVANT_RE = re.compile("|".join(RELEVANT_PATHS), re.IGNORECASE)

# Max chars to extract per file within the archive
MAX_CHARS_PER_FILE = 4000
# Max total chars for the combined output
MAX_TOTAL_CHARS = 8000


class MustGatherParser:
    """Extract relevant infrastructure context from must-gather archives."""

    def parse_from_bytes(self, archive_bytes: bytes) -> dict:
        """
        Parse a must-gather tar.gz from bytes content.

        Args:
            archive_bytes: Raw bytes of the tar.gz archive

        Returns:
            dict with keys:
                ceph_health: str - Ceph health/status output
                osd_info: str - OSD tree/dump info
                crash_reports: str - Any crash reports found
                pod_events: str - Pod events from openshift-storage namespace
                relevant_files: list - Paths of files that were examined
                stats: dict - Counts of files found
        """
        try:
            fileobj = io.BytesIO(archive_bytes)
            return self._parse_tarfile(fileobj)
        except (tarfile.TarError, IOError) as e:
            logger.warning(f"Failed to parse must-gather archive: {e}")
            return self._empty_result()

    def parse_from_file(self, file_path: str) -> dict:
        """
        Parse a must-gather tar.gz from a file path.

        Args:
            file_path: Path to the tar.gz file

        Returns:
            dict (same structure as parse_from_bytes)
        """
        try:
            return self._parse_tarfile(file_path)
        except (tarfile.TarError, IOError) as e:
            logger.warning(f"Failed to parse must-gather from {file_path}: {e}")
            return self._empty_result()

    def build_context(self, parsed: dict) -> str:
        """
        Build a single text context string from parsed must-gather data.

        Args:
            parsed: Output from parse_from_bytes() or parse_from_file()

        Returns:
            Combined text ready for AI prompt
        """
        sections = []

        if parsed["ceph_health"]:
            sections.append(f"=== CEPH HEALTH/STATUS ===\n{parsed['ceph_health']}")

        if parsed["osd_info"]:
            sections.append(f"=== OSD INFO ===\n{parsed['osd_info']}")

        if parsed["crash_reports"]:
            sections.append(f"=== CRASH REPORTS ===\n{parsed['crash_reports']}")

        if parsed["pod_events"]:
            sections.append(f"=== POD EVENTS ===\n{parsed['pod_events']}")

        result = "\n\n".join(sections)

        if len(result) > MAX_TOTAL_CHARS:
            result = result[:MAX_TOTAL_CHARS] + "\n... [truncated]"

        return result

    def _parse_tarfile(self, source) -> dict:
        """Parse a tarfile from either a file path or file object."""
        result = self._empty_result()
        relevant_files = []

        mode = "r:gz" if isinstance(source, str) else "r:*"

        with tarfile.open(name=source if isinstance(source, str) else None,
                          fileobj=source if not isinstance(source, str) else None,
                          mode=mode) as tar:
            for member in tar.getmembers():
                if not member.isfile():
                    continue

                if not RELEVANT_RE.search(member.name):
                    continue

                relevant_files.append(member.name)

                try:
                    f = tar.extractfile(member)
                    if f is None:
                        continue
                    content = f.read(MAX_CHARS_PER_FILE).decode("utf-8", errors="replace")
                    f.close()
                except (KeyError, IOError, UnicodeDecodeError) as e:
                    logger.debug(f"Skipping {member.name}: {e}")
                    continue

                self._categorize_content(member.name, content, result)

        result["relevant_files"] = relevant_files
        result["stats"]["files_examined"] = len(relevant_files)

        logger.debug(f"Must-gather: examined {len(relevant_files)} relevant files")

        return result

    def _categorize_content(self, path: str, content: str, result: dict):
        """Categorize extracted file content into the appropriate result bucket."""
        path_lower = path.lower()

        if "health" in path_lower or "status" in path_lower:
            if "ceph" in path_lower:
                result["ceph_health"] += f"\n--- {os.path.basename(path)} ---\n{content}\n"
        elif "osd" in path_lower:
            result["osd_info"] += f"\n--- {os.path.basename(path)} ---\n{content}\n"
        elif "crash" in path_lower:
            result["crash_reports"] += f"\n--- {os.path.basename(path)} ---\n{content}\n"
        elif "event" in path_lower:
            result["pod_events"] += f"\n--- {os.path.basename(path)} ---\n{content}\n"

    @staticmethod
    def _empty_result() -> dict:
        return {
            "ceph_health": "",
            "osd_info": "",
            "crash_reports": "",
            "pod_events": "",
            "relevant_files": [],
            "stats": {"files_examined": 0},
        }
