"""
Provider/Client Pattern Analyzer

Detects missing context managers and markers in Provider/Client mode code.
Uses AST parsing to identify function calls that require proper context management.

Signed-off-by: Claude Sonnet 4.5 <noreply@anthropic.com>
"""

import ast
import os
import yaml
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass
from pathlib import Path


@dataclass
class Finding:
    """Represents a pattern violation found in code"""

    file_path: str
    line_number: int
    column: int
    severity: str  # 'error', 'warning', 'info'
    rule: str
    message: str
    suggestion: Optional[str] = None
    code_snippet: Optional[str] = None


class ProviderClientAnalyzer:
    """Analyzes Python code for Provider/Client pattern violations"""

    def __init__(self, patterns_file: str = None):
        """
        Initialize the analyzer with pattern rules.

        Args:
            patterns_file: Path to patterns.yaml config file
        """
        if patterns_file is None:
            # Default to patterns.yaml in same directory
            patterns_file = os.path.join(
                os.path.dirname(__file__), "patterns.yaml"
            )

        with open(patterns_file, "r") as f:
            self.patterns = yaml.safe_load(f)

        self.findings: List[Finding] = []

    def analyze_file(self, file_path: str) -> List[Finding]:
        """
        Analyze a single Python file for pattern violations.

        Args:
            file_path: Path to Python file to analyze

        Returns:
            List of findings
        """
        self.findings = []

        # Check if file is in exemption list
        if self._is_exempt(file_path):
            return self.findings

        try:
            with open(file_path, "r") as f:
                source = f.read()
                source_lines = source.splitlines()

            tree = ast.parse(source, filename=file_path)

            # Visit all nodes in the AST
            visitor = ProviderPatternVisitor(
                self.patterns, file_path, source_lines
            )
            visitor.visit(tree)

            self.findings.extend(visitor.findings)

        except SyntaxError as e:
            self.findings.append(
                Finding(
                    file_path=file_path,
                    line_number=e.lineno or 0,
                    column=e.offset or 0,
                    severity="error",
                    rule="syntax-error",
                    message=f"Syntax error in file: {e.msg}",
                )
            )
        except Exception as e:
            self.findings.append(
                Finding(
                    file_path=file_path,
                    line_number=0,
                    column=0,
                    severity="error",
                    rule="analysis-error",
                    message=f"Error analyzing file: {str(e)}",
                )
            )

        return self.findings

    def analyze_diff(self, diff_text: str) -> List[Finding]:
        """
        Analyze git diff for pattern violations in changed lines only.

        Args:
            diff_text: Git diff output

        Returns:
            List of findings in changed code
        """
        # Parse diff to get changed files and line numbers
        changed_files = self._parse_diff(diff_text)

        all_findings = []
        for file_path, changed_lines in changed_files.items():
            if not file_path.endswith(".py"):
                continue

            file_findings = self.analyze_file(file_path)

            # Filter to only findings in changed lines
            for finding in file_findings:
                if finding.line_number in changed_lines:
                    all_findings.append(finding)

        return all_findings

    def _parse_diff(self, diff_text: str) -> Dict[str, set]:
        """
        Parse git diff to extract changed files and line numbers.

        Args:
            diff_text: Git diff output

        Returns:
            Dict mapping file paths to sets of changed line numbers
        """
        changed_files = {}
        current_file = None
        current_line = 0

        for line in diff_text.splitlines():
            if line.startswith("+++"):
                # New file in diff
                file_path = line[6:]  # Remove '+++ b/'
                if file_path != "/dev/null":
                    current_file = file_path
                    changed_files[current_file] = set()

            elif line.startswith("@@"):
                # Line number marker
                # Format: @@ -old_start,old_count +new_start,new_count @@
                parts = line.split()
                if len(parts) >= 3:
                    new_range = parts[2].lstrip("+")
                    if "," in new_range:
                        start, count = new_range.split(",")
                        current_line = int(start)
                    else:
                        current_line = int(new_range)

            elif current_file and (line.startswith("+") and not line.startswith("++")):
                # Added line
                changed_files[current_file].add(current_line)
                current_line += 1

            elif current_file and not line.startswith("-"):
                # Context line (not removed)
                current_line += 1

        return changed_files

    def _is_exempt(self, file_path: str) -> bool:
        """Check if file is exempt from pattern checking"""
        exemptions = self.patterns.get("exemptions", {})
        exempt_paths = exemptions.get("paths", [])

        for exempt_path in exempt_paths:
            if file_path.endswith(exempt_path) or exempt_path in file_path:
                return True

        return False


class ProviderPatternVisitor(ast.NodeVisitor):
    """AST visitor to detect provider/client pattern violations"""

    def __init__(self, patterns: dict, file_path: str, source_lines: List[str]):
        self.patterns = patterns
        self.file_path = file_path
        self.source_lines = source_lines
        self.findings: List[Finding] = []

        # Track context managers in current scope
        self.in_provider_context = False
        self.context_manager_stack = []

        # Track if we're in a test function with provider marker
        self.in_provider_test = False
        self.current_function_decorators = []

    def visit_FunctionDef(self, node: ast.FunctionDef):
        """Visit function definition to check for provider markers"""
        # Save previous state
        prev_decorators = self.current_function_decorators
        prev_in_provider_test = self.in_provider_test

        # Check decorators for provider markers
        self.current_function_decorators = []
        has_provider_marker = False

        for decorator in node.decorator_list:
            decorator_name = self._get_decorator_name(decorator)
            self.current_function_decorators.append(decorator_name)

            if decorator_name in self.patterns.get("provider_markers", []):
                has_provider_marker = True

        self.in_provider_test = has_provider_marker

        # Visit function body
        self.generic_visit(node)

        # Restore previous state
        self.current_function_decorators = prev_decorators
        self.in_provider_test = prev_in_provider_test

    def visit_With(self, node: ast.With):
        """Visit with statement (context manager)"""
        # Check if this is a provider context manager
        for item in node.items:
            context_name = self._get_context_manager_name(item.context_expr)

            if context_name in self.patterns.get("valid_context_managers", []):
                # We're now in a provider context
                prev_state = self.in_provider_context
                self.in_provider_context = True
                self.context_manager_stack.append(context_name)

                # Visit body of with statement
                for child in node.body:
                    self.visit(child)

                # Restore state
                self.context_manager_stack.pop()
                self.in_provider_context = prev_state

                return  # Don't call generic_visit

        # Not a provider context manager, visit normally
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call):
        """Visit function call to check for provider functions"""
        func_name = self._get_call_name(node)

        if func_name in self.patterns.get("provider_functions", []):
            # This is a provider function call
            if not self.in_provider_context and not self.in_provider_test:
                # Missing context manager!
                self._add_finding(
                    node,
                    severity="error",
                    rule="missing-provider-context",
                    message=f"Call to '{func_name}' requires RunWithProviderConfigContextIfAvailable context manager",
                    suggestion=self._generate_context_suggestion(node, func_name),
                )

        # Continue visiting child nodes
        self.generic_visit(node)

    def _get_decorator_name(self, decorator: ast.expr) -> str:
        """Extract decorator name from AST node"""
        if isinstance(decorator, ast.Name):
            return decorator.id
        elif isinstance(decorator, ast.Call):
            return self._get_call_name(decorator)
        return ""

    def _get_context_manager_name(self, node: ast.expr) -> str:
        """Extract context manager name from AST node"""
        if isinstance(node, ast.Call):
            return self._get_call_name(node)
        elif isinstance(node, ast.Name):
            return node.id
        elif isinstance(node, ast.Attribute):
            return node.attr
        return ""

    def _get_call_name(self, node: ast.Call) -> str:
        """Extract function name from call node"""
        if isinstance(node.func, ast.Name):
            return node.func.id
        elif isinstance(node.func, ast.Attribute):
            return node.func.attr
        return ""

    def _add_finding(
        self,
        node: ast.AST,
        severity: str,
        rule: str,
        message: str,
        suggestion: Optional[str] = None,
    ):
        """Add a finding for the given node"""
        line_number = getattr(node, "lineno", 0)
        column = getattr(node, "col_offset", 0)

        # Get code snippet
        code_snippet = None
        if 0 < line_number <= len(self.source_lines):
            code_snippet = self.source_lines[line_number - 1].strip()

        self.findings.append(
            Finding(
                file_path=self.file_path,
                line_number=line_number,
                column=column,
                severity=severity,
                rule=rule,
                message=message,
                suggestion=suggestion,
                code_snippet=code_snippet,
            )
        )

    def _generate_context_suggestion(self, node: ast.Call, func_name: str) -> str:
        """Generate suggestion for fixing missing context manager"""
        indent = " " * getattr(node, "col_offset", 0)
        return f"""Wrap the call in a provider context manager:

{indent}with config.RunWithProviderConfigContextIfAvailable():
{indent}    {func_name}(...)

Or add @runs_on_provider marker to the test function."""


def format_findings(findings: List[Finding]) -> str:
    """
    Format findings as human-readable text.

    Args:
        findings: List of findings to format

    Returns:
        Formatted string
    """
    if not findings:
        return "✓ No issues found!"

    output = []
    output.append(f"\nFound {len(findings)} issue(s):\n")

    for i, finding in enumerate(findings, 1):
        output.append(f"{i}. {finding.file_path}:{finding.line_number}:{finding.column}")
        output.append(f"   [{finding.severity.upper()}] {finding.rule}")
        output.append(f"   {finding.message}")

        if finding.code_snippet:
            output.append(f"   Code: {finding.code_snippet}")

        if finding.suggestion:
            output.append(f"   Suggestion:\n{finding.suggestion}")

        output.append("")

    return "\n".join(output)
