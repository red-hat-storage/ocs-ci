"""
Fetches log artifacts from local paths or remote HTTP URLs.

Handles Apache/nginx directory listing HTML parsing for remote sources.
"""

import logging
import os
import tempfile
from dataclasses import dataclass, field
from typing import Optional

import requests
from bs4 import BeautifulSoup

from ocs_ci.utility.log_analysis.exceptions import ArtifactFetchError

logger = logging.getLogger(__name__)


@dataclass
class ArtifactManifest:
    """Describes the available artifacts in a log directory."""

    junit_xml: Optional[str] = None
    config_yaml: Optional[str] = None
    test_report_html: Optional[str] = None
    timing_csv: Optional[str] = None
    test_logs_dir: Optional[str] = None
    failed_logs_dir: Optional[str] = None
    ui_logs_dir: Optional[str] = None
    deploy_log: Optional[str] = None
    all_files: list = field(default_factory=list)


class ArtifactFetcher:
    """Fetch log artifacts from local paths or remote HTTP URLs."""

    # Request timeout in seconds
    TIMEOUT = 30

    def __init__(self, source: str):
        """
        Args:
            source: URL or local path to the log directory
        """
        self.source = source.rstrip("/")
        self.is_remote = source.startswith("http://") or source.startswith("https://")
        self._session = None

    @property
    def session(self):
        if self._session is None:
            self._session = requests.Session()
            self._session.verify = False
        return self._session

    def discover(self) -> ArtifactManifest:
        """
        Discover available artifacts in the log directory.

        Returns:
            ArtifactManifest with paths/URLs to found artifacts
        """
        logger.info(f"Discovering artifacts at {self.source}")

        if self.is_remote:
            files = self._list_remote(self.source)
        else:
            files = self._list_local(self.source)

        manifest = ArtifactManifest(all_files=files)
        config_candidates = []

        junit_candidates = []

        for f in files:
            name = os.path.basename(f.rstrip("/"))

            if name.startswith("test_results_") and name.endswith(".xml"):
                junit_candidates.append(f)
            elif name.startswith("run-") and name.endswith("-config-end.yaml"):
                config_candidates.append(f)
            elif name.startswith("test_report_") and name.endswith(".html"):
                manifest.test_report_html = f
            elif name.startswith("deploy-ocs-cluster-build-") and name.endswith(".log"):
                manifest.deploy_log = f
            elif name.startswith("ocs-ci-logs-"):
                # Pick the test run logs dir (the one with tests/ subdir)
                manifest.test_logs_dir = f
            elif name.startswith("failed_testcase_ocs_logs_"):
                manifest.failed_logs_dir = f
            elif name.startswith("ui_logs_dir_"):
                manifest.ui_logs_dir = f

        # Pick the latest JUnit XML (highest number = most recent run)
        if junit_candidates:
            manifest.junit_xml = sorted(junit_candidates)[-1]
            if len(junit_candidates) > 1:
                logger.info(
                    f"Multiple JUnit XMLs found, using latest: "
                    f"{os.path.basename(manifest.junit_xml)}"
                )

        # Pick the first config that parses as a valid dict
        # (last run in multi-run dirs may be empty/truncated)
        if config_candidates:
            manifest.config_yaml = config_candidates[0]
            if len(config_candidates) > 1:
                for candidate in config_candidates:
                    try:
                        content = self.fetch_text(candidate)
                        import yaml

                        parsed = yaml.safe_load(content)
                        if isinstance(parsed, dict) and parsed.get("ENV_DATA"):
                            manifest.config_yaml = candidate
                            break
                    except Exception:
                        continue

        # Find timing CSV inside test logs dir
        if manifest.test_logs_dir:
            csv_path = self._join(
                manifest.test_logs_dir, "session_test_time_report_file.csv"
            )
            if self._exists(csv_path):
                manifest.timing_csv = csv_path

        logger.info(
            f"Found: junit_xml={'yes' if manifest.junit_xml else 'no'}, "
            f"config={'yes' if manifest.config_yaml else 'no'}, "
            f"test_logs={'yes' if manifest.test_logs_dir else 'no'}, "
            f"failed_logs={'yes' if manifest.failed_logs_dir else 'no'}, "
            f"ui_logs={'yes' if manifest.ui_logs_dir else 'no'}"
        )

        return manifest

    def fetch_text(self, path: str) -> str:
        """
        Fetch file content as text.

        Args:
            path: URL or local file path

        Returns:
            File content as string
        """
        if self.is_remote or path.startswith("http"):
            return self._download_text(path)
        return self._read_local(path)

    def fetch_binary(self, path: str) -> bytes:
        """
        Fetch file content as bytes.

        Args:
            path: URL or local file path

        Returns:
            File content as bytes
        """
        if self.is_remote or path.startswith("http"):
            return self._download_binary(path)
        with open(path, "rb") as f:
            return f.read()

    def fetch_to_tempfile(self, path: str, suffix: str = "") -> str:
        """
        Download a file to a temp file and return the temp path.

        Args:
            path: URL or local file path
            suffix: File suffix for the temp file

        Returns:
            Path to the temporary file
        """
        content = self.fetch_binary(path)
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
        tmp.write(content)
        tmp.close()
        return tmp.name

    def list_dir(self, path: str) -> list:
        """
        List files/directories at the given path.

        Args:
            path: URL or local directory path

        Returns:
            List of file/directory paths
        """
        if self.is_remote or path.startswith("http"):
            return self._list_remote(path)
        return self._list_local(path)

    def _join(self, base: str, *parts: str) -> str:
        if self.is_remote or base.startswith("http"):
            base = base.rstrip("/")
            return "/".join([base] + list(parts))
        return os.path.join(base, *parts)

    def _exists(self, path: str) -> bool:
        if self.is_remote or path.startswith("http"):
            try:
                resp = self.session.head(path, timeout=self.TIMEOUT)
                return resp.status_code == 200
            except requests.RequestException:
                return False
        return os.path.exists(path)

    def _list_remote(self, url: str) -> list:
        """Parse Apache/nginx directory listing HTML."""
        try:
            resp = self.session.get(url.rstrip("/") + "/", timeout=self.TIMEOUT)
            resp.raise_for_status()
        except requests.RequestException as e:
            raise ArtifactFetchError(f"Failed to list remote directory {url}: {e}")

        soup = BeautifulSoup(resp.text, "html.parser")
        files = []
        base_url = url.rstrip("/")

        for link in soup.find_all("a"):
            href = link.get("href", "")
            # Skip parent directory and sorting links
            if not href or href.startswith("?") or href.startswith("/"):
                continue
            files.append(f"{base_url}/{href.lstrip('/')}")

        return files

    def _list_local(self, path: str) -> list:
        """List local directory contents."""
        if not os.path.isdir(path):
            raise ArtifactFetchError(f"Not a directory: {path}")
        return [os.path.join(path, f) for f in os.listdir(path)]

    def _download_text(self, url: str) -> str:
        """Download a URL and return text content."""
        try:
            resp = self.session.get(url, timeout=self.TIMEOUT)
            resp.raise_for_status()
            return resp.text
        except requests.RequestException as e:
            raise ArtifactFetchError(f"Failed to fetch {url}: {e}")

    def _download_binary(self, url: str) -> bytes:
        """Download a URL and return binary content."""
        try:
            resp = self.session.get(url, timeout=self.TIMEOUT)
            resp.raise_for_status()
            return resp.content
        except requests.RequestException as e:
            raise ArtifactFetchError(f"Failed to fetch {url}: {e}")

    def _read_local(self, path: str) -> str:
        """Read a local file and return text content."""
        try:
            with open(path, "r") as f:
                return f.read()
        except (IOError, OSError) as e:
            raise ArtifactFetchError(f"Failed to read {path}: {e}")
