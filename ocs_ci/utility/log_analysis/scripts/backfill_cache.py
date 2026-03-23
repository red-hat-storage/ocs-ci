#!/usr/bin/env python3
"""
Backfill cache files with bug_details, suggested_fix, metadata, and evidence paths.

Works on existing cache files that were created before these fields were added.
Supports multiple modes:
  - metadata-only: Fill status, polarion_id, test_class, run_metadata, traceback (no AI)
  - reparse-sessions: Re-extract JSON from recorded sessions using brace-depth (no AI)
  - bug-details-only: Generate bug_details for product_bug entries (lightweight AI)
  - fix-only: Generate suggested_fix for test_bug entries (lightweight AI)
  - all: All of the above

Usage:
    python -m ocs_ci.utility.log_analysis.scripts.backfill_cache \\
        --cache-dir ~/.ocs-ci/analysis_cache \\
        --dry-run
"""

import argparse
import glob
import json
import logging
import os
import re
import subprocess
import sys
import time

logger = logging.getLogger(__name__)

# Upstream bare repo for reading test code from release branches
DEFAULT_UPSTREAM_REPO = os.path.expanduser("~/.ocs-ci/upstream-repo/ocs-ci")

# Where session records live on the agent
DEFAULT_SESSIONS_DIR = "/mnt/ocsci-jenkins/log_analysis/sessions_dir"

# NFS base for scanner logs
DEFAULT_LOGS_BASE = "/mnt/ocsci-jenkins"


class CacheBackfiller:
    """Backfill cache files with missing fields."""

    def __init__(
        self,
        cache_dir,
        upstream_repo=None,
        sessions_dir=None,
        model="sonnet",
        delay=1.0,
    ):
        self.cache_dir = os.path.expanduser(cache_dir)
        self.upstream_repo = os.path.expanduser(upstream_repo or DEFAULT_UPSTREAM_REPO)
        self.sessions_dir = os.path.expanduser(sessions_dir or DEFAULT_SESSIONS_DIR)
        self.model = model
        self.delay = delay
        self._stats = {
            "scanned": 0,
            "needs_bug_details": 0,
            "needs_suggested_fix": 0,
            "needs_metadata": 0,
            "reparse_candidates": 0,
            "updated_metadata": 0,
            "updated_bug_details": 0,
            "updated_suggested_fix": 0,
            "reparsed_sessions": 0,
            "errors": 0,
        }

    def scan_candidates(self):
        """Scan cache files and categorize what needs backfilling.

        Returns dict with lists of (path, data) tuples grouped by need.
        """
        candidates = {
            "needs_bug_details": [],
            "needs_suggested_fix": [],
            "needs_metadata": [],
            "reparse_candidates": [],
        }

        cache_files = glob.glob(os.path.join(self.cache_dir, "*.json"))
        logger.info(f"Scanning {len(cache_files)} cache files in {self.cache_dir}")

        for path in sorted(cache_files):
            try:
                with open(path) as f:
                    data = json.load(f)
            except (json.JSONDecodeError, IOError) as e:
                logger.debug(f"Skipping unreadable cache file {path}: {e}")
                continue

            self._stats["scanned"] += 1
            analysis = data.get("analysis", {})
            category = analysis.get("category", "unknown")

            # Normalize: ensure test_name is at top level
            if not data.get("test_name"):
                sig_name = data.get("signature", {}).get("test_name", "")
                if sig_name:
                    data["test_name"] = sig_name

            # Check for missing metadata fields
            if not data.get("run_metadata") or not data.get("traceback"):
                candidates["needs_metadata"].append((path, data))
                self._stats["needs_metadata"] += 1

            # product_bug without bug_details
            if category == "product_bug" and not analysis.get("bug_details"):
                candidates["needs_bug_details"].append((path, data))
                self._stats["needs_bug_details"] += 1

            # test_bug without suggested_fix
            if category == "test_bug" and not analysis.get("suggested_fix"):
                candidates["needs_suggested_fix"].append((path, data))
                self._stats["needs_suggested_fix"] += 1

            # unknown with empty root_cause — may be a broken JSON parse
            if (
                category == "unknown"
                and not analysis.get("root_cause_summary")
                and not analysis.get("evidence")
            ):
                candidates["reparse_candidates"].append((path, data))
                self._stats["reparse_candidates"] += 1

        return candidates

    def backfill_metadata_from_reports(self, path, data):
        """Try to fill run_metadata, traceback, status from scanner logs or reports.

        Looks for the scanner .log file that mentions this cache file's hash,
        then extracts run_metadata from the same log or associated report.
        """
        cache_hash = os.path.basename(path).replace(".json", "")
        test_name = data.get("test_name", "")

        # If we already have all metadata, skip
        if data.get("run_metadata") and data.get("traceback") and data.get("status"):
            return False

        # Search scanner logs for this cache hash
        log_data = self._find_scanner_log_for_hash(cache_hash)
        if not log_data:
            return False

        updated = False

        # Fill run_metadata from log
        if not data.get("run_metadata") and log_data.get("run_metadata"):
            data["run_metadata"] = log_data["run_metadata"]
            updated = True

        # Fill status
        if not data.get("status") and log_data.get("status"):
            data["status"] = log_data["status"]
            updated = True

        # Fill test_class
        if not data.get("test_class") and log_data.get("test_class"):
            data["test_class"] = log_data["test_class"]
            updated = True

        if updated:
            self._write_cache(path, data)
            self._stats["updated_metadata"] += 1

        return updated

    def reparse_session(self, path, data):
        """Re-parse a recorded session file to recover misclassified entries.

        For cache entries that are 'unknown' with empty root_cause, find the
        corresponding session record and re-extract JSON using brace-depth.
        """
        test_name = data.get("test_name", "")
        if not test_name:
            return False

        session_text = self._find_session_for_test(test_name)
        if not session_text:
            return False

        # Try brace-depth JSON extraction
        try:
            parsed = self._extract_json_brace_depth(session_text)
        except ValueError:
            return False

        if not parsed or not parsed.get("category"):
            return False

        # Only update if we got a better result
        old_cat = data.get("analysis", {}).get("category", "unknown")
        new_cat = parsed.get("category", "unknown")
        old_summary = data.get("analysis", {}).get("root_cause_summary", "")
        new_summary = parsed.get("root_cause_summary", "")

        if new_cat == "unknown" and not new_summary:
            return False

        # Update the analysis
        analysis = data.get("analysis", {})
        analysis["category"] = new_cat
        analysis["confidence"] = float(parsed.get("confidence", 0.5))
        analysis["root_cause_summary"] = new_summary
        analysis["evidence"] = parsed.get("evidence", [])
        analysis["recommended_action"] = parsed.get("recommended_action", "")
        if parsed.get("bug_details"):
            analysis["bug_details"] = parsed["bug_details"]
        if parsed.get("suggested_fix"):
            analysis["suggested_fix"] = parsed["suggested_fix"]
        data["analysis"] = analysis

        self._write_cache(path, data)
        self._stats["reparsed_sessions"] += 1
        logger.info(
            f"Re-parsed session: {test_name}: {old_cat} -> {new_cat} "
            f"(had summary: {bool(old_summary)}, now: {bool(new_summary)})"
        )
        return True

    def backfill_bug_details(self, path, data):
        """Generate bug_details for a product_bug entry using lightweight AI call."""
        analysis = data.get("analysis", {})
        if analysis.get("bug_details"):
            return False

        test_name = data.get("test_name", "")
        run_metadata = data.get("run_metadata", {})

        prompt = self._build_bug_details_prompt(
            test_name=test_name,
            root_cause=analysis.get("root_cause_summary", ""),
            evidence=analysis.get("evidence", []),
            traceback=data.get("traceback", ""),
            run_metadata=run_metadata,
        )

        result = self._call_ai(prompt, test_name)
        if not result:
            return False

        bug_details = result.get("bug_details")
        # AI sometimes returns the fields directly without the wrapper
        if not bug_details and result.get("bug_subject"):
            bug_details = result
        if not bug_details:
            logger.warning(f"AI returned no bug_details for {test_name}, keys: {list(result.keys())[:10]}")
            return False

        analysis["bug_details"] = bug_details
        data["analysis"] = analysis
        self._write_cache(path, data)
        self._stats["updated_bug_details"] += 1
        logger.info(f"Added bug_details: {test_name}")
        return True

    def backfill_suggested_fix(self, path, data):
        """Generate suggested_fix for a test_bug entry using lightweight AI call."""
        analysis = data.get("analysis", {})
        if analysis.get("suggested_fix"):
            return False

        test_name = data.get("test_name", "")
        test_class = data.get("test_class", "")
        run_metadata = data.get("run_metadata", {})
        traceback_text = data.get("traceback", "")

        # Derive file path from test_class or traceback
        source_file, source_code, branch = self._get_test_source(
            test_class, traceback_text, run_metadata
        )

        prompt = self._build_suggested_fix_prompt(
            test_name=test_name,
            test_class=test_class,
            root_cause=analysis.get("root_cause_summary", ""),
            evidence=analysis.get("evidence", []),
            traceback=traceback_text,
            source_file=source_file,
            source_code=source_code,
            branch=branch,
        )

        result = self._call_ai(prompt, test_name)
        if not result:
            return False

        suggested_fix = result.get("suggested_fix")
        # AI sometimes returns the fields directly without the wrapper
        if not suggested_fix and result.get("file") and result.get("description"):
            suggested_fix = result
        if not suggested_fix:
            logger.warning(f"AI returned no suggested_fix for {test_name}, keys: {list(result.keys())[:10]}")
            return False

        analysis["suggested_fix"] = suggested_fix
        data["analysis"] = analysis
        self._write_cache(path, data)
        self._stats["updated_suggested_fix"] += 1
        logger.info(f"Added suggested_fix: {test_name}")
        return True

    def run(self, dry_run=False, limit=None, mode="all"):
        """Run the backfill process.

        Args:
            dry_run: If True, only report candidates without modifying anything
            limit: Max entries to process per category
            mode: 'all', 'metadata-only', 'reparse-only',
                  'bug-details-only', 'fix-only'
        """
        candidates = self.scan_candidates()

        print(f"\n=== Cache Backfill Scan ===")
        print(f"Cache dir: {self.cache_dir}")
        print(f"Total files scanned: {self._stats['scanned']}")
        print(f"Needs metadata: {self._stats['needs_metadata']}")
        print(f"Needs bug_details (product_bug): {self._stats['needs_bug_details']}")
        print(f"Needs suggested_fix (test_bug): {self._stats['needs_suggested_fix']}")
        print(f"Reparse candidates (broken unknown): {self._stats['reparse_candidates']}")

        if dry_run:
            self._print_candidates(candidates)
            return

        # Process based on mode
        if mode in ("all", "metadata-only"):
            self._process_metadata(candidates["needs_metadata"], limit)

        if mode in ("all", "reparse-only"):
            self._process_reparse(candidates["reparse_candidates"], limit)

        if mode in ("all", "bug-details-only"):
            self._process_bug_details(candidates["needs_bug_details"], limit)

        if mode in ("all", "fix-only"):
            self._process_suggested_fix(candidates["needs_suggested_fix"], limit)

        self._print_summary()

    def _process_metadata(self, items, limit):
        """Process metadata backfill (no AI)."""
        count = 0
        for path, data in items:
            if limit and count >= limit:
                break
            try:
                if self.backfill_metadata_from_reports(path, data):
                    count += 1
            except Exception as e:
                logger.warning(f"Metadata backfill failed for {path}: {e}")
                self._stats["errors"] += 1

    def _process_reparse(self, items, limit):
        """Process session re-parsing (no AI)."""
        count = 0
        for path, data in items:
            if limit and count >= limit:
                break
            try:
                if self.reparse_session(path, data):
                    count += 1
            except Exception as e:
                logger.warning(f"Session reparse failed for {path}: {e}")
                self._stats["errors"] += 1

    def _process_bug_details(self, items, limit):
        """Process bug_details generation (AI calls)."""
        count = 0
        for path, data in items:
            if limit and count >= limit:
                break
            try:
                if self.backfill_bug_details(path, data):
                    count += 1
                    if self.delay > 0:
                        time.sleep(self.delay)
            except Exception as e:
                logger.warning(f"Bug details backfill failed for {path}: {e}")
                self._stats["errors"] += 1

    def _process_suggested_fix(self, items, limit):
        """Process suggested_fix generation (AI calls)."""
        count = 0
        for path, data in items:
            if limit and count >= limit:
                break
            try:
                if self.backfill_suggested_fix(path, data):
                    count += 1
                    if self.delay > 0:
                        time.sleep(self.delay)
            except Exception as e:
                logger.warning(f"Suggested fix backfill failed for {path}: {e}")
                self._stats["errors"] += 1

    # ---- AI call helpers ----

    def _call_ai(self, prompt, context=""):
        """Make a single-turn AI call via claude CLI with JSON schema."""
        schema = json.dumps({
            "type": "object",
            "properties": {
                "bug_details": {"type": "object"},
                "suggested_fix": {"type": "object"},
            },
        })

        cmd = [
            "claude", "-p", prompt,
            "--output-format", "json",
            "--json-schema", schema,
            "--model", self.model,
            "--max-budget-usd", "0.10",
        ]

        env = os.environ.copy()
        env.pop("CLAUDECODE", None)

        # Auto-detect GCP credentials
        if "GOOGLE_APPLICATION_CREDENTIALS" not in env:
            for cred_path in [
                "/opt/claude/auth/gcp-auth.json",
                os.path.expanduser("~/.gcp/gcp-auth.json"),
            ]:
                if os.path.isfile(cred_path):
                    env["GOOGLE_APPLICATION_CREDENTIALS"] = cred_path
                    break

        try:
            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=120, env=env,
            )
        except subprocess.TimeoutExpired:
            logger.warning(f"AI call timed out for {context}")
            self._stats["errors"] += 1
            return None
        except FileNotFoundError:
            logger.error("claude CLI not found")
            return None

        if result.returncode != 0:
            logger.warning(f"AI call failed for {context}: {result.stderr[:200]}")
            self._stats["errors"] += 1
            return None

        try:
            response = json.loads(result.stdout)
        except json.JSONDecodeError:
            logger.warning(f"Failed to parse AI response for {context}")
            self._stats["errors"] += 1
            return None

        # Extract structured output
        structured = response.get("structured_output")
        if structured is None:
            result_text = response.get("result", "")
            # Try direct JSON parse first
            try:
                structured = json.loads(result_text)
            except (json.JSONDecodeError, TypeError):
                pass
            # Fall back to brace-depth extraction from result text
            if structured is None and result_text:
                try:
                    structured = self._extract_json_brace_depth(result_text)
                except ValueError:
                    logger.warning(f"No structured output from AI for {context}")
                    self._stats["errors"] += 1
                    return None

        cost = response.get("total_cost_usd", 0)
        logger.debug(f"AI call: ${cost:.4f} ({context})")

        if structured and isinstance(structured, dict):
            logger.debug(f"AI result keys: {list(structured.keys())} ({context})")
        return structured

    def _build_bug_details_prompt(
        self, test_name, root_cause, evidence, traceback, run_metadata
    ):
        """Build a prompt to generate bug_details from existing analysis."""
        evidence_str = "\n".join(f"- {e}" for e in evidence) if evidence else "None"
        tb_str = traceback[:3000] if traceback else "Not available"

        platform = run_metadata.get("platform", "unknown")
        ocp_ver = run_metadata.get("ocp_version", "unknown")
        ocs_ver = run_metadata.get("ocs_version", "unknown")
        ocs_build = run_metadata.get("ocs_build", "unknown")
        deploy_type = run_metadata.get("deployment_type", "unknown")

        return f"""You are an expert in OpenShift Data Foundation (ODF/OCS) test automation.

A test failure has already been classified as a **product_bug**. Your job is to generate
a structured bug report (DFBUGS form) based on the existing analysis.

## Test: {test_name}

## Root Cause (already determined)
{root_cause}

## Evidence
{evidence_str}

## Traceback
```
{tb_str}
```

## Run Metadata
- Platform: {platform}
- Deployment Type: {deploy_type}
- OCP Version: {ocp_ver}
- ODF Version: {ocs_ver}
- ODF Build: {ocs_build}

Respond with ONLY a JSON object (no markdown, no explanation outside the JSON). The JSON must have a single top-level key "bug_details" containing:
- bug_subject: "ODF: <concise bug title>"
- description: detailed description with log snippets
- platform: "{platform}"
- deployment_type_ocp: "{'IPI' if 'ipi' in deploy_type.lower() else 'UPI' if 'upi' in deploy_type.lower() else deploy_type}"
- deployment_type_odf: "Internal"
- component_versions: {{"ocp": "{ocp_ver}", "odf": "{ocs_ver}", "rhcs": "", "acm": ""}}
- impacts_ability_to_work, workaround, reproducible, reproducible_from_ui, regression_info
- steps_to_reproduce (array), actual_results, expected_results, additional_info

Be thorough and informative. Quote actual error messages from the evidence and traceback."""

    def _build_suggested_fix_prompt(
        self,
        test_name,
        test_class,
        root_cause,
        evidence,
        traceback,
        source_file,
        source_code,
        branch,
    ):
        """Build a prompt to generate suggested_fix from existing analysis + source code."""
        evidence_str = "\n".join(f"- {e}" for e in evidence) if evidence else "None"
        tb_str = traceback[:3000] if traceback else "Not available"

        source_section = ""
        if source_code:
            source_section = f"""
## Source Code ({source_file} from branch {branch})
```python
{source_code}
```
"""

        return f"""You are an expert in OpenShift Data Foundation (ODF/OCS) test automation.

A test failure has already been classified as a **test_bug**. Your job is to generate
a suggested code fix based on the existing analysis and the actual source code.

## Test: {test_name}
## Class: {test_class}

## Root Cause (already determined)
{root_cause}

## Evidence
{evidence_str}

## Traceback
```
{tb_str}
```
{source_section}
Respond with ONLY a JSON object (no markdown, no explanation outside the JSON). The JSON must have a single top-level key "suggested_fix" containing:
- file: path to the file (e.g., ocs_ci/ocs/bucket_utils.py)
- function: function name where the fix goes
- line: exact line number where the change starts (verify from the source code above)
- description: what's wrong and how to fix it
- code_snippet: ONLY the added/changed lines (match the + lines in the diff, not surrounding code)
- diff: unified diff showing the fix with context lines
- fixed_on_master: false
- needs_cherry_pick: false

Use the actual source code provided to produce accurate line numbers and a correct diff.
If no source code is available, use the traceback to identify the file and provide your best fix."""

    # ---- Source code helpers ----

    def _derive_release_branch(self, ocs_version):
        """e.g., '4.21.1-2.konflux' -> 'release-4.21'"""
        match = re.match(r"(\d+\.\d+)", ocs_version)
        if match:
            return f"release-{match.group(1)}"
        return ""

    def _get_test_source(self, test_class, traceback_text, run_metadata):
        """Get the test source code from the upstream repo.

        Returns (file_path, source_code, branch).
        """
        if not os.path.isdir(self.upstream_repo):
            return "", "", ""

        # Derive branch
        ocs_version = run_metadata.get("ocs_version", "") if run_metadata else ""
        branch = self._derive_release_branch(ocs_version) or "master"

        # Derive file path from test_class or traceback
        file_path = self._derive_file_path(test_class, traceback_text)
        if not file_path:
            return "", "", branch

        # Read source with git show
        source_code = self._git_show(branch, file_path)
        if not source_code:
            source_code = self._git_show("master", file_path)
            if source_code:
                branch = "master"

        return file_path, source_code, branch

    def _derive_file_path(self, test_class, traceback_text):
        """Derive file path from test_class or traceback.

        test_class: 'tests.functional.pv.test_clone.TestClone'
            -> 'tests/functional/pv/test_clone.py'
        """
        # Try from test_class
        if test_class:
            parts = test_class.split(".")
            # Find the part that starts with test_ (the module)
            for i, part in enumerate(parts):
                if part.startswith("test_") and i < len(parts) - 1:
                    # Everything up to and including this part is the file path
                    file_parts = parts[: i + 1]
                    return "/".join(file_parts) + ".py"

        # Try from traceback — look for ocs_ci/ or tests/ file paths
        if traceback_text:
            matches = re.findall(
                r'File ".*?/((?:ocs_ci|tests)/\S+\.py)"', traceback_text
            )
            if matches:
                return matches[-1]

        return ""

    def _git_show(self, branch, file_path):
        """Read a file from the upstream repo using git show."""
        try:
            result = subprocess.run(
                ["git", "show", f"{branch}:{file_path}"],
                cwd=self.upstream_repo,
                capture_output=True,
                text=True,
                timeout=10,
            )
            if result.returncode == 0:
                return result.stdout
        except Exception:
            pass
        return ""

    # ---- Session helpers ----

    def _find_session_for_test(self, test_name):
        """Find a recorded session file for a test name."""
        if not os.path.isdir(self.sessions_dir):
            return ""

        safe_name = re.sub(r"[^\w\-]", "_", test_name)[:80]

        for filename in os.listdir(self.sessions_dir):
            if safe_name in filename and filename.endswith(".txt"):
                filepath = os.path.join(self.sessions_dir, filename)
                try:
                    with open(filepath) as f:
                        return f.read()
                except IOError:
                    continue
        return ""

    def _find_scanner_log_for_hash(self, cache_hash):
        """Search scanner logs for a cache hash and extract metadata.

        Scanner logs contain lines like:
          'Cached analysis for <hash>'
        and session records with run metadata.
        """
        # This is a best-effort search — scanner logs may not exist for all runs
        # For now, return empty dict; can be enhanced later with actual log parsing
        return {}

    # ---- JSON extraction ----

    @staticmethod
    def _extract_json_brace_depth(text, preferred_keys=None):
        """Extract JSON dict from text using brace-depth counting.

        Args:
            text: Text containing embedded JSON
            preferred_keys: If set, prefer dicts containing one of these keys.
                            Defaults to ["bug_details", "suggested_fix", "category"].
        """
        if preferred_keys is None:
            preferred_keys = ["bug_details", "suggested_fix", "category"]
        candidates = []
        for i, ch in enumerate(text):
            if ch == "{":
                depth = 0
                for j in range(i, len(text)):
                    if text[j] == "{":
                        depth += 1
                    elif text[j] == "}":
                        depth -= 1
                        if depth == 0:
                            try:
                                parsed = json.loads(text[i : j + 1])
                                if isinstance(parsed, dict):
                                    if any(k in parsed for k in preferred_keys):
                                        return parsed
                                    candidates.append(parsed)
                            except json.JSONDecodeError:
                                pass
                            break

        for c in candidates:
            if isinstance(c, dict):
                return c

        raise ValueError("No valid JSON found in text")

    # ---- I/O helpers ----

    @staticmethod
    def _write_cache(path, data):
        """Write updated cache data back to disk."""
        with open(path, "w") as f:
            json.dump(data, f, indent=2)

    def _print_candidates(self, candidates):
        """Print detailed candidate info for dry-run mode."""
        for category, label in [
            ("needs_metadata", "Missing Metadata"),
            ("reparse_candidates", "Broken Unknown (reparse candidates)"),
            ("needs_bug_details", "Product Bug without bug_details"),
            ("needs_suggested_fix", "Test Bug without suggested_fix"),
        ]:
            items = candidates[category]
            if not items:
                continue
            print(f"\n--- {label} ({len(items)}) ---")
            for path, data in items[:10]:
                test_name = data.get("test_name") or data.get("signature", {}).get("test_name", "?")
                cat = data.get("analysis", {}).get("category", "?")
                has_rm = "Y" if data.get("run_metadata") else "N"
                has_tb = "Y" if data.get("traceback") else "N"
                print(f"  {os.path.basename(path)}  {cat:15}  rm={has_rm} tb={has_tb}  {test_name[:60]}")
            if len(items) > 10:
                print(f"  ... and {len(items) - 10} more")

    def _print_summary(self):
        """Print final summary of changes made."""
        print(f"\n=== Backfill Summary ===")
        print(f"Scanned: {self._stats['scanned']}")
        print(f"Metadata updated: {self._stats['updated_metadata']}")
        print(f"Sessions reparsed: {self._stats['reparsed_sessions']}")
        print(f"Bug details added: {self._stats['updated_bug_details']}")
        print(f"Suggested fixes added: {self._stats['updated_suggested_fix']}")
        print(f"Errors: {self._stats['errors']}")


def main():
    parser = argparse.ArgumentParser(
        description="Backfill cache files with missing fields"
    )
    parser.add_argument(
        "--cache-dir",
        default="~/.ocs-ci/analysis_cache",
        help="Cache directory to process",
    )
    parser.add_argument(
        "--upstream-repo",
        default=DEFAULT_UPSTREAM_REPO,
        help="Path to upstream ocs-ci bare repo",
    )
    parser.add_argument(
        "--sessions-dir",
        default=DEFAULT_SESSIONS_DIR,
        help="Directory containing recorded session files",
    )
    parser.add_argument(
        "--model",
        default="sonnet",
        help="AI model for bug_details/suggested_fix generation",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only scan and report candidates, don't modify anything",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max entries to process per category",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=1.0,
        help="Delay between AI calls in seconds (rate limiting)",
    )
    parser.add_argument(
        "--mode",
        choices=["all", "metadata-only", "reparse-only", "bug-details-only", "fix-only"],
        default="all",
        help="What to backfill",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable debug logging",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
    )

    backfiller = CacheBackfiller(
        cache_dir=args.cache_dir,
        upstream_repo=args.upstream_repo,
        sessions_dir=args.sessions_dir,
        model=args.model,
        delay=args.delay,
    )

    backfiller.run(
        dry_run=args.dry_run,
        limit=args.limit,
        mode=args.mode,
    )


if __name__ == "__main__":
    main()
