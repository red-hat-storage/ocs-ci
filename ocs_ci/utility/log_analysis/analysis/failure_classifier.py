"""
Failure classifier orchestrator.

Coordinates the full analysis pipeline for each test failure:
1. Known issue regex matching (instant, no cost)
2. Failure signature computation and cache lookup
3. Log preprocessing (extract relevant context)
4. AI classification (via pluggable backend)
5. Cache storage of results
"""

import logging
import os
import re
import shutil
import tarfile
import time
from typing import Optional

import requests

from ocs_ci.utility.log_analysis.ai.base import AIBackend
from ocs_ci.utility.log_analysis.analysis.known_issues import KnownIssuesMatcher
from ocs_ci.utility.log_analysis.cache import AnalysisCache
from ocs_ci.utility.log_analysis.models import (
    FailureAnalysis,
    FailureCategory,
    FailureSignature,
    TestResult,
)
from ocs_ci.utility.log_analysis.parsers.test_log_parser import TestLogParser
from ocs_ci.utility.log_analysis.parsers.must_gather_parser import MustGatherParser

logger = logging.getLogger(__name__)

# Directory for extracted must-gather archives
MG_CACHE_DIR = os.path.expanduser("~/.ocs-ci/must_gather_cache")

# Directory for recorded agentic session outputs
SESSIONS_DIR = os.path.expanduser("~/.ocs-ci/recorded_sessions")


class FailureClassifier:
    """
    Orchestrates the full failure analysis pipeline.

    Combines regex matching, caching, log parsing, and AI classification
    to produce a FailureAnalysis for each failed test.
    """

    def __init__(
        self,
        ai_backend: AIBackend,
        known_issues_matcher: Optional[KnownIssuesMatcher] = None,
        cache: Optional[AnalysisCache] = None,
        skip_ai_for_known: bool = True,
        max_failures: int = 30,
        failed_logs_dir: Optional[str] = None,
        test_logs_dir: Optional[str] = None,
        ui_logs_dir: Optional[str] = None,
        run_id: Optional[str] = None,
    ):
        """
        Args:
            ai_backend: AI backend for classification
            known_issues_matcher: Regex matcher (uses default if None)
            cache: Analysis cache (disabled if None)
            skip_ai_for_known: Skip AI for regex-matched known issues
            max_failures: Max unique failures to analyze with AI
            failed_logs_dir: URL to failed_testcase_ocs_logs dir for must-gather access
            test_logs_dir: URL to ocs-ci-logs-{runid} dir for per-test logs
            ui_logs_dir: URL to ui_logs_dir_{runid} for UI test artifacts
            run_id: Run ID extracted from directory names
        """
        self.ai_backend = ai_backend
        self.known_issues = known_issues_matcher or KnownIssuesMatcher()
        self.cache = cache
        self.skip_ai_for_known = skip_ai_for_known
        self.max_failures = max_failures
        self.failed_logs_dir = failed_logs_dir
        self.test_logs_dir = test_logs_dir
        self.ui_logs_dir = ui_logs_dir
        self.run_id = run_id or "unknown"
        self.log_parser = TestLogParser()
        self.mg_parser = MustGatherParser()
        self._http_session = None
        self._mg_cleanup_paths = []

    def classify_failures(
        self,
        failures: list,
        fetcher=None,
    ) -> list:
        """
        Classify a list of test failures.

        Args:
            failures: List of TestResult objects (status=FAILED or ERROR)
            fetcher: ArtifactFetcher for downloading logs (optional)

        Returns:
            List of FailureAnalysis objects
        """
        results = []
        ai_call_count = 0
        cache_hit_count = 0
        known_issue_count = 0
        start_time = time.monotonic()

        # Group failures by signature to avoid duplicate AI calls
        signature_groups = {}
        for failure in failures:
            sig = FailureSignature.from_test_result(failure)
            if sig.cache_key not in signature_groups:
                signature_groups[sig.cache_key] = {
                    "signature": sig,
                    "failures": [],
                }
            signature_groups[sig.cache_key]["failures"].append(failure)

        logger.info(
            f"Classifying {len(failures)} failures "
            f"({len(signature_groups)} unique signatures)"
        )

        for cache_key, group in signature_groups.items():
            sig = group["signature"]
            group_failures = group["failures"]
            representative = group_failures[0]

            # Step 1: Known issue matching
            known_matches = self.known_issues.match_test_result(representative)
            if known_matches and self.skip_ai_for_known:
                analysis_dict = {
                    "category": "known_issue",
                    "confidence": 1.0,
                    "root_cause_summary": (
                        f"Matched known issue(s): "
                        f"{', '.join(m['issue'] for m in known_matches)}"
                    ),
                    "evidence": [
                        f"Pattern match: {m.get('description', m['pattern'])}"
                        for m in known_matches
                    ],
                    "matched_known_issues": [m["issue"] for m in known_matches],
                    "recommended_action": "See linked Jira issue(s)",
                }
                known_issue_count += len(group_failures)
                for f in group_failures:
                    results.append(self._build_analysis(f, analysis_dict))
                continue

            # Step 2: Cache lookup
            if self.cache:
                cached = self.cache.get(sig)
                if cached:
                    cache_hit_count += len(group_failures)
                    for f in group_failures:
                        results.append(self._build_analysis(f, cached))
                    continue

            # Step 3: AI classification (respecting budget)
            if (
                self.ai_backend.requires_budget_limit
                and ai_call_count >= self.max_failures
            ):
                logger.warning(
                    f"AI call limit ({self.max_failures}) reached. "
                    f"Remaining failures will be unclassified."
                )
                for f in group_failures:
                    results.append(self._build_unclassified(f))
                continue

            # Fetch and parse logs if available
            log_excerpt = ""
            infra_context = ""

            if fetcher and representative.log_path:
                log_excerpt = self._fetch_and_parse_log(
                    fetcher, representative.log_path
                )

            # Pre-resolve must-gather paths
            must_gather_info = self._resolve_must_gather(representative.name)

            # Compute test log URL
            test_log_url = self._build_test_log_url(
                representative.name, representative.classname or ""
            )

            # Check for UI logs (only for UI tests)
            ui_logs = self._build_ui_logs_url(representative.name)

            # Step 4: Call AI backend
            try:
                analysis_dict = self.ai_backend.classify_failure(
                    test_name=representative.name,
                    test_class=representative.classname,
                    duration=representative.duration,
                    squad=representative.squad or "Unknown",
                    traceback=representative.traceback or "",
                    log_excerpt=log_excerpt,
                    infra_context=infra_context,
                    must_gather_info=must_gather_info,
                    test_log_url=test_log_url,
                    ui_logs=ui_logs,
                )
                ai_call_count += 1

                # Save agentic session output
                session_text = analysis_dict.pop("session_text", "")
                if session_text:
                    session_file = self._save_session(
                        representative.name,
                        session_text,
                        analysis_dict.get("session_id", ""),
                    )
                    analysis_dict["session_file"] = session_file

                # Merge known issue matches if any (partial matches)
                if known_matches:
                    analysis_dict.setdefault("matched_known_issues", [])
                    analysis_dict["matched_known_issues"].extend(
                        m["issue"] for m in known_matches
                    )

                # Cache the result (without session_text which was already popped)
                if self.cache:
                    self.cache.put(sig, analysis_dict)

            except Exception as e:
                ai_call_count += 1  # Count failed calls toward the limit
                logger.warning(
                    f"AI classification failed for {representative.name}: {e}"
                )
                analysis_dict = {
                    "category": "unknown",
                    "confidence": 0.0,
                    "root_cause_summary": self._extract_error_summary(representative),
                    "evidence": [],
                    "recommended_action": f"AI classification failed: {e}",
                }

            for f in group_failures:
                results.append(self._build_analysis(f, analysis_dict))

        elapsed = time.monotonic() - start_time
        elapsed_str = f"{elapsed / 60:.1f}min" if elapsed >= 60 else f"{elapsed:.0f}s"
        cost = self.ai_backend.total_cost_usd
        cost_str = f", ${cost:.2f}" if cost > 0 else ""

        logger.info(
            f"Classification complete in {elapsed_str}{cost_str}: "
            f"{ai_call_count} AI calls, "
            f"{cache_hit_count} cache hits, {known_issue_count} known issues"
        )

        # Clean up any locally extracted must-gather archives
        self.cleanup_must_gather()

        return results

    def _fetch_and_parse_log(self, fetcher, log_path: str) -> str:
        """Fetch and parse a per-test log file."""
        try:
            log_content = fetcher.fetch_text(log_path)
            parsed = self.log_parser.parse(log_content)
            return self.log_parser.build_excerpt(parsed)
        except Exception as e:
            logger.debug(f"Could not fetch/parse log at {log_path}: {e}")
            return ""

    @property
    def http_session(self):
        """Lazy HTTP session for pre-resolution requests."""
        if self._http_session is None:
            self._http_session = requests.Session()
            self._http_session.verify = False
        return self._http_session

    def _url_encode_test_name(self, test_name: str) -> str:
        """URL-encode brackets in parameterized test names."""
        return test_name.replace("[", "%5b").replace("]", "%5d")

    def _dash_encode_test_name(self, test_name: str) -> str:
        """Replace brackets with dashes for test log directory names."""
        return test_name.replace("[", "-").replace("]", "")

    def _list_http_dir(self, url: str) -> list:
        """List entries in an HTTP directory listing. Returns list of names."""
        try:
            resp = self.http_session.get(url.rstrip("/") + "/", timeout=15)
            if resp.status_code != 200:
                return []
            return re.findall(r'<a href="([^"?/][^"]*)"', resp.text)
        except requests.RequestException:
            return []

    def _resolve_must_gather(self, test_name: str) -> dict:
        """Pre-resolve must-gather paths for a test.

        Returns a dict with:
            mg_type: "local" | "http" | "none"
            mg_base: local path or HTTP URL to the quay-io data dir
            ocs_mg: path/URL to ocs_must_gather data dir
            ocp_mg: path/URL to ocp_must_gather data dir (may be empty)
            cluster_id: cluster ID string
        """
        if not self.failed_logs_dir:
            return {"mg_type": "none"}

        safe_name = self._url_encode_test_name(test_name)
        base = self.failed_logs_dir.rstrip("/")
        test_mg_url = f"{base}/{safe_name}_ocs_logs"

        # Step 1: Find cluster ID directory
        entries = self._list_http_dir(test_mg_url)
        if not entries:
            logger.debug(f"No must-gather directory found at {test_mg_url}")
            return {"mg_type": "none"}

        # Cluster ID is typically the only directory entry
        cluster_id = entries[0].rstrip("/")
        cluster_url = f"{test_mg_url}/{cluster_id}"

        # Step 2: Check what's inside (extracted dirs or tar.gz)
        cluster_entries = self._list_http_dir(cluster_url)
        if not cluster_entries:
            return {"mg_type": "none"}

        has_ocs_dir = any(e.rstrip("/") == "ocs_must_gather" for e in cluster_entries)
        has_tar = any(e.endswith(".tar.gz") for e in cluster_entries)

        if has_ocs_dir:
            # Already extracted — resolve the quay-io hash dir via HTTP
            return self._resolve_extracted_mg(cluster_url, cluster_id)

        if has_tar:
            # Download and extract tar.gz archives locally
            ocs_tar = next(
                (e for e in cluster_entries if e == "ocs_must_gather.tar.gz"),
                None,
            )
            ocp_tar = next(
                (e for e in cluster_entries if e == "ocp_must_gather.tar.gz"),
                None,
            )
            if not ocs_tar:
                # Fallback: pick any tar.gz
                ocs_tar = next(
                    (e for e in cluster_entries if e.endswith(".tar.gz")),
                    None,
                )
            if ocs_tar:
                tar_url = f"{cluster_url}/{ocs_tar}"
                ocp_tar_url = f"{cluster_url}/{ocp_tar}" if ocp_tar else ""
                return self._extract_mg_tarball(
                    tar_url,
                    test_name,
                    cluster_id,
                    ocp_tar_url=ocp_tar_url,
                )

        logger.debug(f"Must-gather at {cluster_url} has no ocs_must_gather/ or tar.gz")
        return {"mg_type": "none"}

    def _resolve_extracted_mg(self, cluster_url: str, cluster_id: str) -> dict:
        """Resolve paths for an already-extracted must-gather on HTTP."""
        ocs_url = f"{cluster_url}/ocs_must_gather"
        ocs_entries = self._list_http_dir(ocs_url)

        # Find the quay-io image hash directory
        quay_dir = ""
        for entry in ocs_entries:
            name = entry.rstrip("/")
            if name.startswith("quay-io") or name.startswith("quay.io"):
                quay_dir = name
                break

        ocs_data_url = f"{ocs_url}/{quay_dir}" if quay_dir else ocs_url

        # Check for OCP must-gather
        ocp_url = f"{cluster_url}/ocp_must_gather"
        ocp_entries = self._list_http_dir(ocp_url)
        ocp_data_url = ""
        if ocp_entries:
            for entry in ocp_entries:
                name = entry.rstrip("/")
                if name.startswith("quay-io") or name.startswith("quay.io"):
                    ocp_data_url = f"{ocp_url}/{name}"
                    break
            if not ocp_data_url:
                ocp_data_url = ocp_url

        logger.info(
            f"Resolved must-gather (HTTP): ocs={ocs_data_url}, "
            f"ocp={'yes' if ocp_data_url else 'no'}"
        )

        return {
            "mg_type": "http",
            "mg_base": ocs_data_url,
            "ocs_mg": ocs_data_url,
            "ocp_mg": ocp_data_url,
            "cluster_id": cluster_id,
        }

    def _extract_mg_tarball(
        self,
        tar_url: str,
        test_name: str,
        cluster_id: str,
        ocp_tar_url: str = "",
    ) -> dict:
        """Download and extract must-gather tar.gz archives to local disk."""
        safe_test = re.sub(r"[^\w\-]", "_", test_name)[:80]
        extract_dir = os.path.join(MG_CACHE_DIR, self.run_id, safe_test)
        os.makedirs(extract_dir, exist_ok=True)
        self._mg_cleanup_paths.append(extract_dir)

        # Download and extract each archive
        for url, label in [(tar_url, "ocs"), (ocp_tar_url, "ocp")]:
            if not url:
                continue
            tar_path = os.path.join(extract_dir, f"{label}_must_gather.tar.gz")
            try:
                logger.info(f"Downloading {label} must-gather tar.gz for {test_name}")
                resp = self.http_session.get(url, timeout=120, stream=True)
                resp.raise_for_status()
                with open(tar_path, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=8192):
                        f.write(chunk)

                logger.info(
                    f"Extracting {label} must-gather to {extract_dir} "
                    f"({os.path.getsize(tar_path) / 1024 / 1024:.1f} MB)"
                )
                with tarfile.open(tar_path, "r:gz") as tar:
                    tar.extractall(path=extract_dir)

                os.remove(tar_path)

            except Exception as e:
                logger.warning(f"Failed to download/extract {label} must-gather: {e}")

        # Find the extracted data directories
        ocs_mg_path = self._find_local_mg_dir(extract_dir, "ocs_must_gather")
        ocp_mg_path = self._find_local_mg_dir(extract_dir, "ocp_must_gather")

        if not ocs_mg_path:
            logger.warning(
                f"No ocs_must_gather found in extracted archive at {extract_dir}"
            )
            return {"mg_type": "none"}

        # Find quay-io hash dir inside ocs_must_gather
        ocs_data_path = ocs_mg_path
        for entry in os.listdir(ocs_mg_path):
            if entry.startswith("quay-io") or entry.startswith("quay.io"):
                ocs_data_path = os.path.join(ocs_mg_path, entry)
                break

        ocp_data_path = ""
        if ocp_mg_path:
            for entry in os.listdir(ocp_mg_path):
                if entry.startswith("quay-io") or entry.startswith("quay.io"):
                    ocp_data_path = os.path.join(ocp_mg_path, entry)
                    break
            if not ocp_data_path:
                ocp_data_path = ocp_mg_path

        logger.info(
            f"Resolved must-gather (local): ocs={ocs_data_path}, "
            f"ocp={'yes' if ocp_data_path else 'no'}"
        )

        return {
            "mg_type": "local",
            "mg_base": ocs_data_path,
            "ocs_mg": ocs_data_path,
            "ocp_mg": ocp_data_path,
            "cluster_id": cluster_id,
        }

    @staticmethod
    def _find_local_mg_dir(base_dir: str, target: str) -> str:
        """Recursively find a directory by name under base_dir."""
        for root, dirs, files in os.walk(base_dir):
            if target in dirs:
                return os.path.join(root, target)
        return ""

    def _build_test_log_url(self, test_name: str, test_class: str) -> str:
        """Build the direct URL to the per-test log file.

        Test logs live at:
            {test_logs_dir}/tests/{classname_path}/{test_name_dashed}/logs

        Where classname_path is the dotted classname converted to path:
            tests.functional.object.mcg.test_bucket_replication.TestReplication
            → tests/functional/object/mcg/test_bucket_replication.py/TestReplication
        """
        if not self.test_logs_dir:
            return ""

        # Convert classname to path
        # e.g., "tests.functional.object.mcg.test_bucket_replication.TestReplication"
        parts = test_class.split(".")
        if not parts:
            return ""

        # The last part that starts with "test_" is the .py file
        path_parts = []
        for i, part in enumerate(parts):
            if part.startswith("test_") and i < len(parts) - 1:
                path_parts.append(part + ".py")
            else:
                path_parts.append(part)

        class_path = "/".join(path_parts)
        dashed_name = self._dash_encode_test_name(test_name)
        base = self.test_logs_dir.rstrip("/")
        url = f"{base}/{class_path}/{dashed_name}/logs"
        return url

    def _build_ui_logs_url(self, test_name: str) -> dict:
        """Check if UI logs exist for this test and return URLs.

        Returns dict with dom_url and screenshots_url, or empty dict.
        """
        if not self.ui_logs_dir:
            return {}

        safe_name = self._url_encode_test_name(test_name)
        base = self.ui_logs_dir.rstrip("/")
        dom_url = f"{base}/dom/{safe_name}"
        screenshots_url = f"{base}/screenshots_ui/{safe_name}"

        # Check if this test has UI logs
        dom_entries = self._list_http_dir(dom_url)
        if not dom_entries:
            return {}

        screenshots_entries = self._list_http_dir(screenshots_url)

        logger.debug(
            f"UI logs found for {test_name}: "
            f"{len(dom_entries)} DOM, {len(screenshots_entries)} screenshots"
        )
        return {
            "dom_url": dom_url,
            "screenshots_url": screenshots_url,
            "dom_files": dom_entries,
            "screenshot_files": screenshots_entries,
        }

    def _save_session(self, test_name: str, session_text: str, session_id: str) -> str:
        """Save the full agentic session transcript to a readable text file.

        Finds the Claude Code JSONL session file by session_id, converts it
        to a human-readable transcript showing all tool calls and responses.

        Returns the file path, or empty string on failure.
        """
        os.makedirs(SESSIONS_DIR, exist_ok=True)
        safe_name = re.sub(r"[^\w\-]", "_", test_name)[:80]
        filename = f"{self.run_id}_session_record_{safe_name}.txt"
        filepath = os.path.join(SESSIONS_DIR, filename)

        try:
            # Find the JSONL session transcript
            jsonl_path = self._find_session_jsonl(session_id) if session_id else ""

            with open(filepath, "w") as f:
                f.write(f"Session ID: {session_id}\n")
                f.write(f"Test: {test_name}\n")
                f.write(f"Run ID: {self.run_id}\n")
                f.write("=" * 80 + "\n\n")

                if jsonl_path:
                    self._write_readable_transcript(jsonl_path, f)
                else:
                    # Fall back to just the result text
                    f.write(session_text)

            logger.debug(f"Saved session record to {filepath}")
            return filepath
        except Exception as e:
            logger.debug(f"Failed to save session record: {e}")
            return ""

    @staticmethod
    def _find_session_jsonl(session_id: str) -> str:
        """Find the Claude Code JSONL session file by session ID."""
        claude_dir = os.path.expanduser("~/.claude/projects")
        if not os.path.isdir(claude_dir):
            return ""
        for project_dir in os.listdir(claude_dir):
            candidate = os.path.join(claude_dir, project_dir, f"{session_id}.jsonl")
            if os.path.isfile(candidate):
                return candidate
        return ""

    @staticmethod
    def _write_readable_transcript(jsonl_path: str, out_file):
        """Convert a JSONL session file to a human-readable transcript."""
        import json as _json

        with open(jsonl_path, "r") as jf:
            for line in jf:
                line = line.strip()
                if not line:
                    continue
                try:
                    msg = _json.loads(line)
                except _json.JSONDecodeError:
                    continue

                msg_type = msg.get("type", "")

                if msg_type == "user":
                    content = msg.get("message", {}).get("content", "")
                    if isinstance(content, str) and content:
                        out_file.write(f"{'=' * 60}\n")
                        out_file.write("USER / PROMPT:\n")
                        out_file.write(f"{'=' * 60}\n")
                        # Truncate very long prompts (initial prompt can be huge)
                        if len(content) > 5000:
                            out_file.write(content[:2000])
                            out_file.write(
                                f"\n\n... [{len(content) - 4000} chars omitted] ...\n\n"
                            )
                            out_file.write(content[-2000:])
                        else:
                            out_file.write(content)
                        out_file.write("\n\n")
                    elif isinstance(content, list):
                        # Tool results come as list of content blocks
                        out_file.write(f"{'- ' * 30}\n")
                        out_file.write("TOOL RESULT:\n")
                        for block in content:
                            if isinstance(block, dict):
                                if block.get("type") == "tool_result":
                                    result_content = block.get("content", "")
                                    if isinstance(result_content, str):
                                        if len(result_content) > 3000:
                                            out_file.write(result_content[:1500])
                                            out_file.write(
                                                f"\n... [{len(result_content) - 3000} chars omitted] ...\n"
                                            )
                                            out_file.write(result_content[-1500:])
                                        else:
                                            out_file.write(result_content)
                                        out_file.write("\n")
                        out_file.write("\n")

                elif msg_type == "assistant":
                    content = msg.get("message", {}).get("content", [])
                    if isinstance(content, list):
                        for block in content:
                            if not isinstance(block, dict):
                                continue
                            block_type = block.get("type", "")

                            if block_type == "text":
                                text = block.get("text", "")
                                if text.strip():
                                    out_file.write(f"{'- ' * 30}\n")
                                    out_file.write("CLAUDE:\n")
                                    out_file.write(text)
                                    out_file.write("\n\n")

                            elif block_type == "tool_use":
                                tool_name = block.get("name", "")
                                tool_input = block.get("input", {})
                                out_file.write(f"{'- ' * 30}\n")
                                out_file.write(f"TOOL CALL: {tool_name}\n")
                                if tool_name == "Bash":
                                    cmd = tool_input.get("command", "")
                                    out_file.write(f"$ {cmd}\n")
                                elif tool_name == "Read":
                                    out_file.write(
                                        f"file: {tool_input.get('file_path', '')}\n"
                                    )
                                else:
                                    out_file.write(
                                        _json.dumps(tool_input, indent=2)[:500]
                                    )
                                    out_file.write("\n")
                                out_file.write("\n")

    def cleanup_must_gather(self):
        """Remove any locally extracted must-gather directories."""
        for path in self._mg_cleanup_paths:
            try:
                if os.path.exists(path):
                    shutil.rmtree(path)
                    logger.debug(f"Cleaned up must-gather: {path}")
            except OSError as e:
                logger.warning(f"Failed to clean up {path}: {e}")
        self._mg_cleanup_paths.clear()

        # Also remove the run-level directory if empty
        run_dir = os.path.join(MG_CACHE_DIR, self.run_id)
        try:
            if os.path.exists(run_dir) and not os.listdir(run_dir):
                os.rmdir(run_dir)
        except OSError:
            pass

    @staticmethod
    def _build_analysis(
        test_result: TestResult, analysis_dict: dict
    ) -> FailureAnalysis:
        """Build a FailureAnalysis from a test result and analysis dict."""
        return FailureAnalysis(
            test_result=test_result,
            category=FailureCategory(analysis_dict.get("category", "unknown")),
            confidence=float(analysis_dict.get("confidence", 0.0)),
            root_cause_summary=analysis_dict.get("root_cause_summary", ""),
            evidence=analysis_dict.get("evidence", []),
            matched_known_issues=analysis_dict.get("matched_known_issues", []),
            suggested_jira_issues=analysis_dict.get("suggested_jira_issues", []),
            recommended_action=analysis_dict.get("recommended_action", ""),
            session_id=analysis_dict.get("session_id", ""),
            session_file=analysis_dict.get("session_file", ""),
        )

    @staticmethod
    def _build_unclassified(test_result: TestResult) -> FailureAnalysis:
        """Build an unclassified FailureAnalysis."""
        summary = ""
        if test_result.traceback:
            lines = test_result.traceback.strip().splitlines()
            if lines:
                summary = lines[-1].strip()[:200]

        return FailureAnalysis(
            test_result=test_result,
            category=FailureCategory.UNKNOWN,
            confidence=0.0,
            root_cause_summary=summary,
            recommended_action="AI call limit reached. Re-run with higher --max-failures.",
        )

    @staticmethod
    def _extract_error_summary(test_result: TestResult) -> str:
        """Extract a one-line error summary from a traceback."""
        if not test_result.traceback:
            return "No traceback available"
        lines = test_result.traceback.strip().splitlines()
        if lines:
            return lines[-1].strip()[:200]
        return "No traceback available"
