"""
Validator for generated test code.

Runs syntax checks, import validation, linting, and pattern checks
to ensure generated tests meet ocs-ci standards.
"""

import ast
import logging
import os
import re
import subprocess
import tempfile
from pathlib import Path
from typing import Optional

from ocs_ci.utility.zstream_test_gen.models import GeneratedTest
from ocs_ci.utility.zstream_test_gen.config import GeneratorConfig

log = logging.getLogger(__name__)

# Required patterns that every generated test must have
REQUIRED_PATTERNS = {
    "squad_marker": (
        r"@(green_squad|red_squad|blue_squad|brown_squad|orange_squad|purple_squad|"
        r"magenta_squad|turquoise_squad)",
        "Missing squad marker decorator",
    ),
    "tier_marker": (
        r"@tier[0-4]",
        "Missing tier marker decorator",
    ),
    "base_class": (
        r"class\s+\w+\((ManageTest|MCGTest|E2ETest|EcosystemTest|BaseTest)\)|@rdr\b|@mdr\b",
        "Test class must inherit from an ocs-ci base class (or use @rdr/@mdr for DR tests)",
    ),
    "logger": (
        r"logger?\s*=\s*logging\.getLogger\(__name__\)",
        "Missing logger initialization: logger = logging.getLogger(__name__)",
    ),
    "docstring": (
        r'"""[\s\S]+?"""',
        "Missing docstring on test class or method",
    ),
    "jira_marker": (
        r'@jira\("DFBUGS-\d+',
        'Missing @jira("DFBUGS-XXXX") marker (import from ocs_ci.framework.pytest_customization.marks)',
    ),
}

# Patterns that indicate hardcoded secrets or sensitive data
SECRET_PATTERNS = [
    (
        r"(?:pull[_-]?secret|auth[_-]?token|api[_-]?key|password|secret[_-]?key)"
        r"\s*[:=]\s*['\"][^'\"]{8,}['\"]",
        "Possible hardcoded secret or credential",
    ),
    (
        r"-----BEGIN\s+(RSA\s+)?PRIVATE\s+KEY-----",
        "Embedded private key",
    ),
    (
        r"-----BEGIN\s+CERTIFICATE-----",
        "Embedded certificate",
    ),
    (
        r"eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}",
        "Possible JWT token",
    ),
    (
        r"['\"]auths['\"]:\s*\{",
        "Possible pull secret (auths block)",
    ),
    (
        r"(?:ghp|gho|ghu|ghs|ghr)_[A-Za-z0-9_]{30,}",
        "GitHub personal access token",
    ),
    (
        r"(?:AKIA|ASIA)[A-Z0-9]{16}",
        "AWS access key",
    ),
    (
        r"(?:cloud\.openshift\.com|sso\.redhat\.com)/[^\s'\"]*token=[^\s'\"]+",
        "Red Hat SSO or OpenShift token URL",
    ),
    (
        r"Bearer\s+[A-Za-z0-9_\-.]{20,}",
        "Hardcoded Bearer token",
    ),
    (
        r"(?:registry\.redhat\.io|quay\.io)/[^\s'\"]*:[^\s'\"]*@",
        "Registry credential in URL",
    ),
]

# Known valid import prefixes in ocs-ci
VALID_IMPORT_PREFIXES = [
    "ocs_ci.",
    "logging",
    "pytest",
    "time",
    "os",
    "re",
    "json",
    "yaml",
    "copy",
    "math",
    "random",
    "string",
    "datetime",
    "collections",
    "concurrent",
    "functools",
    "pathlib",
    "typing",
    "uuid",
    "base64",
    "hashlib",
    "tempfile",
    "textwrap",
    "subprocess",
]


class TestValidator:
    """
    Validates generated ocs-ci test code.

    Args:
        cfg: Generator configuration.

    """

    def __init__(self, cfg: GeneratorConfig):
        self.cfg = cfg
        self.ocsci_root = Path(cfg.ocsci_root)

    def validate(self, test: GeneratedTest) -> GeneratedTest:
        """
        Run all validation checks on a generated test.

        Updates the test's validation_passed and validation_errors fields.

        Args:
            test: The generated test to validate.

        Returns:
            GeneratedTest: The test with validation results updated.

        """
        errors = []
        log.info("Validating generated test for %s", test.bug.bug_id)

        # 1. Syntax check
        syntax_errors = self._check_syntax(test.code)
        if syntax_errors:
            errors.extend(syntax_errors)

        # 2. Import check (only if syntax is valid)
        if not syntax_errors:
            import_errors = self._check_imports(test.code)
            errors.extend(import_errors)

        # 3. Pattern check
        pattern_errors = self._check_patterns(test.code)
        errors.extend(pattern_errors)

        # 4. Duplicate check
        duplicate_errors = self._check_duplicates(test)
        errors.extend(duplicate_errors)

        # 5. Flake8 lint check
        lint_errors = self._run_flake8(test.code)
        errors.extend(lint_errors)

        # 6. Secrets check (blocking)
        secret_errors = self._check_secrets(test.code)
        errors.extend(secret_errors)

        # 7. pytest --collect-only dry-run (non-blocking, warnings only)
        # The ocs-ci conftest has heavy dependencies (pandas, etc.) that may
        # not be installed locally, so collect failures are logged as warnings
        # rather than hard errors.
        if not errors:
            collect_warnings = self._run_pytest_collect(test.code, test.file_path)
            if collect_warnings:
                for w in collect_warnings:
                    log.warning("Collect warning for %s: %s", test.bug.bug_id, w)
                test.collect_warnings = collect_warnings

        test.validation_errors = errors
        test.validation_passed = len(errors) == 0

        if test.validation_passed:
            log.info("Validation passed for %s", test.bug.bug_id)
        else:
            log.warning(
                "Validation failed for %s with %d errors",
                test.bug.bug_id,
                len(errors),
            )
            for error in errors:
                log.debug("  - %s", error)

        return test

    def format_code(self, code: str) -> str:
        """
        Format code using black.

        Args:
            code: Python code to format.

        Returns:
            str: Formatted code, or original if black fails.

        """
        try:
            import black

            mode = black.Mode(
                line_length=120,
                target_versions={black.TargetVersion.PY310},
            )
            return black.format_str(code, mode=mode)
        except ImportError:
            log.warning("black not installed, skipping formatting")
            return code
        except Exception:
            log.warning("black formatting failed", exc_info=True)
            return code

    def _check_syntax(self, code: str) -> list:
        """
        Check Python syntax using ast.parse().

        Args:
            code: Python code to check.

        Returns:
            list[str]: List of syntax error messages.

        """
        try:
            ast.parse(code)
            return []
        except SyntaxError as e:
            return [f"Syntax error at line {e.lineno}: {e.msg}"]

    def _check_imports(self, code: str) -> list:
        """
        Verify that imports reference valid modules.

        Args:
            code: Python code to check.

        Returns:
            list[str]: List of import error messages.

        """
        errors = []
        tree = ast.parse(code)

        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if not self._is_valid_import(alias.name):
                        errors.append(
                            f"Invalid import: '{alias.name}' "
                            f"(not a recognized ocs-ci or stdlib module)"
                        )
            elif isinstance(node, ast.ImportFrom):
                if node.module and not self._is_valid_import(node.module):
                    errors.append(
                        f"Invalid import: 'from {node.module}' "
                        f"(not a recognized ocs-ci or stdlib module)"
                    )
                # For ocs_ci imports, check that the module path exists
                if node.module and node.module.startswith("ocs_ci."):
                    module_path = self._resolve_module_path(node.module)
                    if module_path is None:
                        errors.append(
                            f"Import 'from {node.module}' does not resolve to "
                            f"an existing file in the ocs-ci package"
                        )

        return errors

    def _is_valid_import(self, module_name: str) -> bool:
        """
        Check if a module name has a valid prefix.

        Args:
            module_name: The module name to check.

        Returns:
            bool: True if the import looks valid.

        """
        return any(module_name.startswith(prefix) for prefix in VALID_IMPORT_PREFIXES)

    def _resolve_module_path(self, module_name: str) -> Optional[Path]:
        """
        Resolve an ocs_ci module name to a file path.

        Args:
            module_name: Module name like 'ocs_ci.ocs.constants'.

        Returns:
            Optional[Path]: Path to the module file, or None.

        """
        parts = module_name.split(".")
        # Try as a module file
        module_file = self.ocsci_root / "/".join(parts) / "__init__.py"
        if module_file.exists():
            return module_file

        # Try as a direct .py file
        module_file = self.ocsci_root / "/".join(parts[:-1]) / f"{parts[-1]}.py"
        if module_file.exists():
            return module_file

        # Try the full path as a package
        module_file = self.ocsci_root / "/".join(parts)
        if module_file.exists():
            return module_file

        return None

    def _check_patterns(self, code: str) -> list:
        """
        Check that required patterns are present in the code.

        Args:
            code: Python code to check.

        Returns:
            list[str]: List of pattern error messages.

        """
        errors = []
        for name, (pattern, message) in REQUIRED_PATTERNS.items():
            if not re.search(pattern, code):
                errors.append(f"Pattern check failed: {message}")

        # Anti-pattern checks
        if re.search(r"@bugzilla", code):
            errors.append(
                "Anti-pattern: @bugzilla is deprecated. Use @jira('DFBUGS-XXXX') instead"
            )
        if re.search(r"@pytest\.mark\.usefixtures", code):
            errors.append(
                "Anti-pattern: @pytest.mark.usefixtures is not used in ocs-ci. "
                "Pass fixtures as method parameters instead"
            )
        if re.search(r"^\s+yield\b", code, re.MULTILINE):
            errors.append(
                "Anti-pattern: yield-based fixture teardown is not used in ocs-ci. "
                "Use request.addfinalizer() instead"
            )

        return errors

    def _check_duplicates(self, test: GeneratedTest) -> list:
        """
        Check if a test with the same name already exists.

        Args:
            test: The generated test to check.

        Returns:
            list[str]: List of duplicate error messages.

        """
        errors = []

        # Extract class names and method names from the generated code
        try:
            tree = ast.parse(test.code)
        except SyntaxError:
            return []  # Syntax errors are caught elsewhere

        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                class_name = node.name
                # Search for this class name in existing test files
                target_dir = self.ocsci_root / test.file_path.rsplit("/", 1)[0]
                if target_dir.exists():
                    for test_file in target_dir.rglob("test_*.py"):
                        try:
                            content = test_file.read_text(errors="ignore")
                            if f"class {class_name}" in content:
                                rel_path = str(test_file.relative_to(self.ocsci_root))
                                errors.append(
                                    f"Duplicate class name '{class_name}' "
                                    f"already exists in {rel_path}"
                                )
                        except OSError:
                            continue

        return errors

    def _check_secrets(self, code: str) -> list:
        """
        Scan code for hardcoded secrets, credentials, and sensitive data.

        Args:
            code: Python code to scan.

        Returns:
            list[str]: List of secret-related error messages.

        """
        errors = []
        for pattern, description in SECRET_PATTERNS:
            matches = re.finditer(pattern, code, re.IGNORECASE)
            for match in matches:
                line_num = code[: match.start()].count("\n") + 1
                snippet = match.group()[:40]
                errors.append(
                    f"SECRET DETECTED at line {line_num}: {description} "
                    f"(matched: '{snippet}...')"
                )
        return errors

    @staticmethod
    def scan_content_for_secrets(content: str) -> list:
        """
        Scan arbitrary content (PR body, commit message) for secrets.

        This is a static method so it can be called from the publisher
        without a full validator instance.

        Args:
            content: Text to scan.

        Returns:
            list[str]: Descriptions of detected secrets.

        """
        findings = []
        for pattern, description in SECRET_PATTERNS:
            if re.search(pattern, content, re.IGNORECASE):
                findings.append(description)
        return findings

    def _run_flake8(self, code: str) -> list:
        """
        Run flake8 on the generated code.

        Args:
            code: Python code to lint.

        Returns:
            list[str]: List of lint error messages (filtered to important ones).

        """
        try:
            with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
                f.write(code)
                f.flush()
                temp_path = f.name

            result = subprocess.run(
                [
                    "python",
                    "-m",
                    "flake8",
                    "--max-line-length=120",
                    "--ignore=E501,W503,W504,E402",  # Common acceptable violations
                    temp_path,
                ],
                capture_output=True,
                text=True,
                timeout=30,
                cwd=self.cfg.ocsci_root,
            )

            errors = []
            if result.stdout:
                for line in result.stdout.strip().split("\n"):
                    if line.strip():
                        # Clean up the temp file path from the error message
                        clean_line = line.replace(temp_path, "<generated>")
                        # Only report errors (E), not warnings (W)
                        if ":E" in clean_line or ": E" in clean_line:
                            errors.append(f"Lint: {clean_line}")

            return errors
        except (subprocess.SubprocessError, FileNotFoundError):
            log.debug("flake8 not available, skipping lint check")
            return []
        finally:
            try:
                os.unlink(temp_path)
            except OSError:
                pass

    def _run_pytest_collect(self, code: str, file_path: str) -> list:
        """
        Run pytest --collect-only to verify the test is discoverable.

        Writes the generated code to a temp file under the ocs-ci tree
        so that pytest can resolve imports and conftest fixtures.

        Args:
            code: The generated test code.
            file_path: The intended target path relative to ocs-ci root.

        Returns:
            list[str]: Collection error messages, empty if test collects fine.

        """
        target_dir = self.ocsci_root / Path(file_path).parent
        if not target_dir.exists():
            log.debug(
                "Target dir %s does not exist, skipping collect check", target_dir
            )
            return []

        temp_path = None
        try:
            # Write to the actual target directory so conftest.py files are found
            with tempfile.NamedTemporaryFile(
                mode="w",
                suffix=".py",
                prefix="test_zgen_",
                dir=str(target_dir),
                delete=False,
            ) as f:
                f.write(code)
                f.flush()
                temp_path = f.name

            result = subprocess.run(
                [
                    "python",
                    "-m",
                    "pytest",
                    "--collect-only",
                    "-q",
                    "--no-header",
                    temp_path,
                ],
                capture_output=True,
                text=True,
                timeout=30,
                cwd=str(self.ocsci_root),
                env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
            )

            errors = []
            if result.returncode != 0:
                stderr = result.stderr.strip()
                stdout = result.stdout.strip()
                # Filter to meaningful error lines
                for line in (stderr + "\n" + stdout).split("\n"):
                    line = line.strip()
                    if not line:
                        continue
                    if any(
                        marker in line.lower()
                        for marker in (
                            "error",
                            "importerror",
                            "no tests ran",
                            "modulenotfounderror",
                            "attributeerror",
                        )
                    ):
                        clean = line.replace(temp_path, "<generated>")
                        errors.append(f"Collection: {clean}")

                if not errors and result.returncode != 0:
                    errors.append(
                        "Collection: pytest --collect-only failed "
                        f"(exit code {result.returncode})"
                    )

            else:
                # Check that at least one test was collected
                if "no tests ran" in result.stdout.lower():
                    errors.append("Collection: no tests discovered by pytest")

            return errors

        except subprocess.TimeoutExpired:
            log.debug("pytest --collect-only timed out, skipping")
            return []
        except (subprocess.SubprocessError, FileNotFoundError, OSError):
            log.debug("pytest collect check failed", exc_info=True)
            return []
        finally:
            if temp_path:
                try:
                    os.unlink(temp_path)
                except OSError:
                    pass
