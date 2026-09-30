"""
Test code generator using Claude AI.

Generates ocs-ci test files from bug context and similar test examples.
Supports both Vertex AI and direct Anthropic API.
"""

import logging
import re

from ocs_ci.utility.zstream_test_gen.models import (
    BugInfo,
    Component,
    ExecutionSpec,
    GeneratedTest,
    HelperSpec,
    RovoTestSpec,
    COMPONENT_CONFIG,
)
from ocs_ci.utility.zstream_test_gen.config import GeneratorConfig
from ocs_ci.utility.zstream_test_gen.prompts import (
    SYSTEM_PROMPT,
    BUG_ANALYSIS_PROMPT,
    TEST_GENERATION_PROMPT,
    DR_TEST_GENERATION_PROMPT,
    FIX_VALIDATION_PROMPT,
    HELPER_EXTRACTION_PROMPT,
)

log = logging.getLogger(__name__)


class TestGenerator:
    """
    Generates ocs-ci test code using Claude.

    Args:
        cfg: Generator configuration with Claude credentials.

    """

    def __init__(self, cfg: GeneratorConfig):
        self.cfg = cfg
        self._client = None

    @property
    def client(self):
        """Lazy-initialize the AI client."""
        if self._client is None:
            self._client = self._create_client()
        return self._client

    def _create_client(self):
        """
        Create the appropriate AI client based on configuration.

        Returns:
            An Anthropic client instance.

        """
        import anthropic

        if self.cfg.claude.use_vertex:
            from anthropic import AnthropicVertex

            log.info(
                "Initializing Vertex AI client (project=%s, location=%s)",
                self.cfg.claude.project_id,
                self.cfg.claude.location,
            )
            return AnthropicVertex(
                project_id=self.cfg.claude.project_id,
                region=self.cfg.claude.location,
            )
        else:
            log.info("Initializing Anthropic API client")
            return anthropic.Anthropic(api_key=self.cfg.claude.api_key)

    def analyze_bug(self, bug: BugInfo) -> RovoTestSpec:
        """
        Analyze a bug using Claude to produce a test specification.

        This serves as a fallback when Rovo MCP is not available.
        It analyzes the bug description and upstream PR diff to generate
        root cause, code changes, and verification steps.

        Args:
            bug: The bug to analyze.

        Returns:
            RovoTestSpec: Generated test specification.

        """
        log.info("Analyzing bug %s with Claude", bug.bug_id)

        # Format comments for the prompt
        comments_text = ""
        if bug.comments:
            for i, comment in enumerate(bug.comments[-5:]):  # Last 5 comments
                comments_text += (
                    f"\n--- Comment by {comment.get('author', 'Unknown')} ---\n"
                    f"{comment.get('body', '')}\n"
                )
        else:
            comments_text = "(no comments)"

        prompt = BUG_ANALYSIS_PROMPT.format(
            bug_id=bug.bug_id,
            summary=bug.summary,
            description=bug.description or "(no description)",
            comments=comments_text,
            upstream_pr_url=bug.upstream_pr_url or "(no upstream PR linked)",
            upstream_pr_diff=bug.upstream_pr_diff or "(PR diff not available)",
        )

        response = self._call_claude(prompt)

        # Parse the response into a RovoTestSpec
        spec = RovoTestSpec(raw_response=response)

        # Extract sections from the response
        sections = self._parse_sections(response)
        spec.root_cause = sections.get("root_cause", "")
        spec.code_changes = sections.get("code_changes", "")
        spec.verification_steps = sections.get("verification_steps", "")

        # Extract test name suggestion
        test_name_raw = sections.get("test_name", "")
        if test_name_raw:
            # Clean up: extract just the filename, ensure it starts with test_
            name = re.sub(r"[^a-z0-9_]", "", test_name_raw.strip().lower().split()[0])
            # Strip version fragments (e.g., 422, 422z, odf422)
            name = re.sub(r"_?\d{3,}z?_?", "_", name)
            name = re.sub(r"_?odf_?\d+_?", "_", name)
            name = re.sub(r"_?backport_?", "_", name)
            name = re.sub(r"__+", "_", name).strip("_")
            if not name.startswith("test_"):
                name = "test_" + name
            spec.test_name = name
            log.info("Suggested test name for %s: %s", bug.bug_id, spec.test_name)

        # Build execution spec from AI analysis + deterministic data
        exec_spec = self._build_execution_spec(bug, sections)
        bug.execution_spec = exec_spec

        log.info("Bug analysis complete for %s", bug.bug_id)
        return spec

    def generate_test(
        self,
        bug: BugInfo,
        similar_tests: list,
    ) -> GeneratedTest:
        """
        Generate an ocs-ci test file for a bug.

        Args:
            bug: The bug to generate a test for.
            similar_tests: List of dicts with 'path' and 'content' of similar tests.

        Returns:
            GeneratedTest: The generated test with code and metadata.

        """
        log.info(
            "Generating test for %s (component: %s)", bug.bug_id, bug.component.value
        )

        # Determine test file path and configuration
        component_config = COMPONENT_CONFIG.get(
            bug.component, COMPONENT_CONFIG[Component.UNKNOWN]
        )
        target_dir = component_config["test_dirs"][0]

        # Use the AI-suggested test name if available, otherwise fall back to slug
        spec = bug.rovo_spec
        if spec and spec.test_name:
            test_filename = spec.test_name
            if not test_filename.endswith(".py"):
                test_filename += ".py"
        else:
            test_filename = f"test_verify_{bug.short_slug}.py"
        target_file_path = f"{target_dir}/{test_filename}"

        # Build the version gate string
        version_gate = self._determine_version_gate(bug)

        # Format similar tests for the prompt
        similar_tests_text = self._format_similar_tests(similar_tests)

        # Format upstream PR diff section
        upstream_pr_diff_section = ""
        if bug.upstream_pr_diff:
            upstream_pr_diff_section = f"Diff:\n```\n{bug.upstream_pr_diff}\n```"

        # Get the test specification
        spec = bug.rovo_spec or RovoTestSpec()

        # Choose the appropriate prompt template
        if bug.component == Component.RAMEN:
            prompt = DR_TEST_GENERATION_PROMPT.format(
                bug_id=bug.bug_id,
                summary=bug.summary,
                component=bug.component.value,
                root_cause=spec.root_cause or bug.description or "(see bug summary)",
                code_changes=spec.code_changes or "(see upstream PR)",
                verification_steps=spec.verification_steps
                or "(derive from bug description)",
                upstream_pr_url=bug.upstream_pr_url or "(not available)",
                upstream_pr_diff_section=upstream_pr_diff_section,
                dr_type=(
                    "regional-dr" if "regional" in bug.summary.lower() else "metro-dr"
                ),
                target_file_path=target_file_path,
                similar_tests=similar_tests_text,
            )
        else:
            prompt = TEST_GENERATION_PROMPT.format(
                bug_id=bug.bug_id,
                summary=bug.summary,
                component=bug.component.value,
                root_cause=spec.root_cause or bug.description or "(see bug summary)",
                code_changes=spec.code_changes or "(see upstream PR)",
                verification_steps=spec.verification_steps
                or "(derive from bug description)",
                upstream_pr_url=bug.upstream_pr_url or "(not available)",
                upstream_pr_diff_section=upstream_pr_diff_section,
                base_class=component_config["base_class"],
                squad_marker=component_config["squad_marker"],
                version_gate=version_gate,
                target_file_path=target_file_path,
                similar_tests=similar_tests_text,
            )

        # Generate the test code
        code = self._call_claude(prompt)
        code = self._clean_code_response(code)

        return GeneratedTest(
            bug=bug,
            file_path=target_file_path,
            code=code,
            similar_tests_used=[t["path"] for t in similar_tests],
        )

    def fix_validation_errors(self, test: GeneratedTest) -> str:
        """
        Fix validation errors in generated test code using Claude.

        Args:
            test: The generated test with validation errors.

        Returns:
            str: Fixed code.

        """
        log.info("Fixing validation errors for %s", test.bug.bug_id)

        errors_text = "\n".join(f"- {e}" for e in test.validation_errors)

        prompt = FIX_VALIDATION_PROMPT.format(
            code=test.code,
            errors=errors_text,
        )

        fixed_code = self._call_claude(prompt)
        return self._clean_code_response(fixed_code)

    def extract_helpers(self, test: GeneratedTest) -> GeneratedTest:
        """
        Second-pass: analyze generated test for inline logic that should be helpers.

        Calls Claude to identify raw OCP commands, inline resource manipulation,
        hardcoded strings, and polling patterns. Generates proper helper functions
        and rewrites the test to use them.

        Args:
            test: The validated generated test.

        Returns:
            GeneratedTest: Updated test with helper_specs populated and code
                rewritten to use the new helpers.

        """
        log.info("Extracting helpers for %s", test.bug.bug_id)

        prompt = HELPER_EXTRACTION_PROMPT.format(
            test_code=test.code,
            bug_id=test.bug.bug_id,
            summary=test.bug.summary,
            component=test.bug.component.value,
        )

        response = self._call_claude(prompt)

        helpers, updated_code = self._parse_helper_response(response)

        if helpers:
            test.helper_specs = helpers
            test.code = updated_code
            log.info(
                "Extracted %d helpers for %s: %s",
                len(helpers),
                test.bug.bug_id,
                [h.function_name for h in helpers],
            )
        else:
            log.info("No helpers needed for %s", test.bug.bug_id)

        return test

    def _parse_helper_response(self, response: str) -> tuple:
        """
        Parse Claude's helper extraction response into HelperSpec objects and updated test code.

        Args:
            response: Raw response from the HELPER_EXTRACTION_PROMPT.

        Returns:
            tuple: (list[HelperSpec], str) — helper specs and updated test code.

        """
        helpers = []

        # Split into HELPERS and UPDATED_TEST sections
        helpers_section = ""
        test_section = ""

        if "### UPDATED_TEST" in response:
            parts = response.split("### UPDATED_TEST", 1)
            helpers_section = parts[0]
            test_section = parts[1].strip()
        elif "## UPDATED_TEST" in response:
            parts = response.split("## UPDATED_TEST", 1)
            helpers_section = parts[0]
            test_section = parts[1].strip()
        else:
            return [], self._clean_code_response(response)

        # Parse helper blocks
        helper_blocks = re.split(r"===\s*HELPER\s*===", helpers_section)
        for block in helper_blocks:
            block = block.strip()
            if not block or "NONE" in block.upper():
                continue
            # Remove trailing === END HELPER ===
            block = re.sub(
                r"===\s*END HELPER\s*===.*", "", block, flags=re.DOTALL
            ).strip()
            if not block:
                continue

            helper = self._parse_single_helper(block)
            if helper:
                helpers.append(helper)

        # Clean up the updated test code
        updated_code = self._clean_code_response(test_section)

        return helpers, updated_code

    def _parse_single_helper(self, block: str) -> HelperSpec:
        """
        Parse a single helper block into a HelperSpec.

        Args:
            block: Text of one === HELPER === block.

        Returns:
            HelperSpec: Parsed helper, or None if parsing fails.

        """
        target_file = ""
        function_name = ""
        description = ""
        insertion_point = ""
        code_lines = []
        in_code = False

        for line in block.split("\n"):
            stripped = line.strip()
            if stripped.upper().startswith("TARGET_FILE:"):
                target_file = stripped.split(":", 1)[1].strip()
            elif stripped.upper().startswith("FUNCTION_NAME:"):
                function_name = stripped.split(":", 1)[1].strip()
            elif stripped.upper().startswith("DESCRIPTION:"):
                description = stripped.split(":", 1)[1].strip()
            elif stripped.upper().startswith("INSERTION_POINT:"):
                insertion_point = stripped.split(":", 1)[1].strip()
            elif stripped.upper().startswith("CODE:"):
                in_code = True
            elif in_code:
                code_lines.append(line)

        if not target_file or not function_name:
            return None

        code = "\n".join(code_lines).strip()
        # Remove markdown fences from code
        code = re.sub(r"^```python\s*\n?", "", code)
        code = re.sub(r"^```\s*\n?", "", code)
        code = re.sub(r"\n?```\s*$", "", code)

        if not code:
            return None

        return HelperSpec(
            target_file=target_file,
            function_name=function_name,
            function_code=code.strip() + "\n",
            description=description,
            insertion_point=insertion_point,
        )

    def _call_claude(self, prompt: str) -> str:
        """
        Make a call to Claude and return the response text.

        Args:
            prompt: The user prompt.

        Returns:
            str: Claude's response text.

        """
        message = self.client.messages.create(
            model=self.cfg.claude.model,
            max_tokens=self.cfg.claude.max_tokens,
            temperature=self.cfg.claude.temperature,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        )
        return message.content[0].text

    def _parse_sections(self, text: str) -> dict:
        """
        Parse structured sections from Claude's bug analysis response.

        Args:
            text: The response text with section headers.

        Returns:
            dict: Mapping of section keys to content.

        """
        sections = {}
        current_key = None
        current_lines = []

        for line in text.split("\n"):
            # Check for section headers
            header_match = re.match(
                r"\*?\*?(\d+\.?\s*)?\*?\*?(Root Cause|Code Changes|Verification Steps"
                r"|Environment|Test Name|Execution Context|Platforms|OCP Versions"
                r"|ACM Version|Deploy Modes|Platform Notes)",
                line,
                re.IGNORECASE,
            )
            if header_match:
                # Save previous section
                if current_key:
                    sections[current_key] = "\n".join(current_lines).strip()

                header_name = header_match.group(2).lower().replace(" ", "_")
                current_key = header_name
                current_lines = []
                # Include any text after the header on the same line
                rest = line[header_match.end() :].strip(":* ")
                if rest:
                    current_lines.append(rest)
            elif current_key:
                current_lines.append(line)

        # Save last section
        if current_key:
            sections[current_key] = "\n".join(current_lines).strip()

        return sections

    def _build_execution_spec(self, bug: BugInfo, sections: dict) -> ExecutionSpec:
        """
        Build an ExecutionSpec from AI analysis sections and deterministic bug data.

        ODF versions come from the clone chain (deterministic), while platforms
        and deploy modes come from Claude's analysis of the bug context.

        Args:
            bug: The bug with fix_versions and linked_issues populated.
            sections: Parsed sections from Claude's analysis response.

        Returns:
            ExecutionSpec: The execution specification.

        """
        spec = ExecutionSpec()

        # --- ODF versions: deterministic from fix_versions + clone chain ---
        odf_versions = set()
        for fv in bug.fix_versions:
            match = re.search(r"odf-(\d+\.\d+)", fv.lower())
            if match:
                odf_versions.add(match.group(1))

        # Also extract versions from linked clone bugs
        for link in bug.linked_issues:
            summary = link.get("summary", "").lower()
            match = re.search(r"odf-(\d+\.\d+)", summary)
            if match:
                odf_versions.add(match.group(1))

        # Also check the bug summary itself for backport version
        match = re.search(r"odf-(\d+\.\d+)", bug.summary.lower())
        if match:
            odf_versions.add(match.group(1))

        spec.odf_versions = sorted(odf_versions)

        # --- Platforms: from AI analysis ---
        platforms_raw = sections.get("platforms", "")
        if platforms_raw:
            spec.platforms = self._parse_list_field(platforms_raw)
        if not spec.platforms:
            # Infer from component
            if bug.component == Component.RAMEN:
                spec.platforms = ["AWS", "vSphere"]
            else:
                spec.platforms = ["all (platform-agnostic)"]

        # --- OCP versions: from AI analysis ---
        spec.ocp_versions = sections.get("ocp_versions", "").strip(" \"'*-:")
        if not spec.ocp_versions:
            # Infer minimum OCP from ODF version mapping
            if spec.odf_versions:
                min_odf = spec.odf_versions[0]
                odf_to_ocp = {
                    "4.14": "4.14+",
                    "4.15": "4.15+",
                    "4.16": "4.16+",
                    "4.17": "4.17+",
                    "4.18": "4.18+",
                    "4.19": "4.19+",
                    "4.20": "4.16+",
                    "4.21": "4.17+",
                    "4.22": "4.18+",
                }
                spec.ocp_versions = odf_to_ocp.get(min_odf, "4.14+")

        # --- ACM version: from AI analysis ---
        acm_raw = sections.get("acm_version", "").strip(" \"'*-:")
        if acm_raw and acm_raw.lower() not in (
            "n/a",
            "na",
            "none",
            "not applicable",
            "",
        ):
            spec.acm_version = acm_raw
        elif bug.component == Component.RAMEN:
            spec.acm_version = "2.10+"

        # --- Deploy modes: from AI analysis ---
        deploy_raw = sections.get("deploy_modes", "")
        if deploy_raw:
            spec.deploy_modes = self._parse_list_field(deploy_raw)
        if not spec.deploy_modes:
            spec.deploy_modes = ["standard"]

        # --- Platform notes: from AI analysis ---
        spec.platform_notes = sections.get("platform_notes", "").strip(" \"'*-:")

        log.info(
            "Execution spec for %s: ODF=%s, OCP=%s, platforms=%s",
            bug.bug_id,
            spec.odf_versions,
            spec.ocp_versions,
            spec.platforms,
        )
        return spec

    def _parse_list_field(self, text: str) -> list:
        """
        Parse a comma/newline-separated list from Claude's response.

        Args:
            text: Raw text that may contain a list.

        Returns:
            list[str]: Parsed list items.

        """
        # Remove markdown list markers and split
        text = re.sub(r"^\s*[-*]\s*", "", text, flags=re.MULTILINE)
        items = re.split(r"[,\n]", text)
        result = []
        for item in items:
            clean = item.strip(" \"'*-:")
            if clean and clean.lower() not in ("", "none", "n/a"):
                result.append(clean)
        return result

    def _determine_version_gate(self, bug: BugInfo) -> str:
        """
        Determine the skipif_ocs_version gate for a bug.

        Args:
            bug: The bug to determine version gating for.

        Returns:
            str: Version gate string for the prompt.

        """
        # Extract version from fix version or summary
        for fv in bug.fix_versions:
            match = re.search(r"(\d+\.\d+)", fv)
            if match:
                version = match.group(1)
                return f'@skipif_ocs_version("<{version}")'

        # Try to extract from summary (backport pattern)
        match = re.search(r"odf-(\d+\.\d+)", bug.summary.lower())
        if match:
            version = match.group(1)
            return f'@skipif_ocs_version("<{version}")'

        return "No version gate needed (applies to all versions)"

    def _format_similar_tests(self, similar_tests: list) -> str:
        """
        Format similar test files for inclusion in the prompt.

        Args:
            similar_tests: List of dicts with 'path' and 'content'.

        Returns:
            str: Formatted text of similar tests.

        """
        if not similar_tests:
            return "(no similar tests found -- follow the rules above carefully)"

        parts = []
        for i, test in enumerate(
            similar_tests[:3]
        ):  # Use top 3 to keep prompt size manageable
            parts.append(f"### Example {i + 1}: {test['path']}\n\n{test['content']}")

        return "\n\n".join(parts)

    def _clean_code_response(self, code: str) -> str:
        """
        Clean up Claude's code response.

        Removes markdown fences and leading/trailing whitespace.

        Args:
            code: Raw code response from Claude.

        Returns:
            str: Clean Python code.

        """
        # Remove markdown code fences
        code = re.sub(r"^```python\s*\n?", "", code)
        code = re.sub(r"^```\s*\n?", "", code)
        code = re.sub(r"\n?```\s*$", "", code)

        return code.strip() + "\n"
