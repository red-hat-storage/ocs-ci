import base64
import json
import logging
import os
import shutil
import subprocess
import time
from abc import ABC, abstractmethod

import requests

from ocs_ci.framework import config as ocsci_config
from ocs_ci.ocs.ui.base_ui import BaseUI

logger = logging.getLogger(__name__)


class LLMClient(ABC):
    """
    Abstract base class for LLM backends used in vision-based UI analysis.

    Subclasses must implement ``is_available`` and ``query_screenshot``.
    JSON parsing and multi-screenshot merging are provided by the base class.
    """

    @abstractmethod
    def is_available(self):
        """
        Checks whether the LLM backend is reachable and ready.

        Returns:
            bool: True if the backend can accept queries.
        """

    @abstractmethod
    def query_screenshot(self, screenshot_path, prompt):
        """
        Sends a single image and prompt to the LLM and returns the raw text response.

        Args:
            screenshot_path (str): Path to the screenshot PNG file.
            prompt (str): The prompt to send along with the image.

        Returns:
            str: The LLM's text response.
        """

    def _parse_json_response(self, raw_response):
        """
        Parses a raw LLM response string into a JSON dict.

        Args:
            raw_response (str): Raw text response from the LLM.

        Returns:
            dict: Parsed JSON object.

        Raises:
            ValueError: If the response cannot be parsed as JSON.
        """
        cleaned = raw_response.strip()
        if cleaned.startswith("```"):
            lines = cleaned.split("\n")
            lines = [l for l in lines if not l.strip().startswith("```")]
            cleaned = "\n".join(lines)

        try:
            return json.loads(cleaned)
        except json.JSONDecodeError:
            logger.warning(f"Failed to parse LLM response as JSON: {raw_response}")
            json_start = cleaned.find("{")
            json_end = cleaned.rfind("}") + 1
            if json_start != -1 and json_end > json_start:
                try:
                    return json.loads(cleaned[json_start:json_end])
                except json.JSONDecodeError:
                    pass
            raise ValueError(
                f"Could not parse LLM response as JSON. Raw response: {raw_response}"
            )

    def query_screenshot_json(self, screenshot_paths, prompt):
        """
        Queries the LLM with one or more screenshots and returns merged JSON.

        When multiple screenshot paths are provided, each is queried separately
        with the same prompt, and the resulting JSON dicts are merged. Later
        screenshots fill in keys that were empty or missing from earlier ones.

        Args:
            screenshot_paths (str or list): Path(s) to the screenshot PNG file(s).
            prompt (str): The prompt to send along with the image(s).

        Returns:
            dict: Merged JSON response from all screenshots.
        """
        if isinstance(screenshot_paths, str):
            screenshot_paths = [screenshot_paths]

        json_prompt = (
            f"{prompt}\n\nRespond ONLY with a valid JSON object. "
            "No markdown, no explanation, just JSON."
        )

        merged = {}
        for path in screenshot_paths:
            raw_response = self.query_screenshot(path, json_prompt)
            if not raw_response:
                logger.warning(
                    f"Empty response from LLM for '{os.path.basename(path)}', skipping"
                )
                continue
            try:
                result = self._parse_json_response(raw_response)
                logger.info(f"Parsed JSON from '{os.path.basename(path)}': {result}")
                for key, value in result.items():
                    if key not in merged or not merged[key]:
                        merged[key] = value
            except ValueError as e:
                logger.warning(f"Skipping '{os.path.basename(path)}': {e}")

        if not merged:
            raise ValueError(
                "Could not parse JSON from any of the provided screenshots"
            )
        return merged


class OllamaClient(LLMClient):
    """
    Manages communication with a local ollama instance for vision-based UI analysis.

    TODO: bring ollama and model setup into ocs-ci deployment, so it's available as a fixture and we can ensure
    the model is pulled before tests run.
    Meanwhile using available cloud llm backends.
    """

    def __init__(self, model=None, host=None):
        self.model = model or ocsci_config.UI_SELENIUM.get("llm_model")
        self.host = (
            host or ocsci_config.UI_SELENIUM.get("llm_host", "http://localhost:11434")
        ).rstrip("/")

    def is_available(self):
        """
        Checks if ollama is running and the required model is pulled.

        Returns:
            bool: True if ollama is reachable and the model is available.
        """
        try:
            response = requests.get(f"{self.host}/api/tags", timeout=10)
            if response.status_code != 200:
                logger.warning(
                    f"Ollama returned status {response.status_code} from /api/tags"
                )
                return False
            models_data = response.json()
            available_models = [
                m.get("name", "") for m in models_data.get("models", [])
            ]
            model_found = any(self.model in m for m in available_models)
            if not model_found:
                logger.warning(
                    f"Model '{self.model}' not found in ollama. "
                    f"Available: {available_models}"
                )
            return model_found
        except requests.ConnectionError:
            logger.warning(f"Cannot connect to ollama at {self.host}")
            return False
        except requests.Timeout:
            logger.warning(f"Timeout connecting to ollama at {self.host}")
            return False
        except Exception as e:
            logger.warning(f"Unexpected error checking ollama availability: {e}")
            return False

    def query_screenshot(self, screenshot_path, prompt):
        """
        Sends a single image and prompt to ollama and returns the raw text response.

        Args:
            screenshot_path (str): Path to the screenshot PNG file.
            prompt (str): The prompt to send along with the image.

        Returns:
            str: The LLM's text response.
        """
        with open(screenshot_path, "rb") as f:
            image_b64 = base64.b64encode(f.read()).decode("utf-8")

        payload = {
            "model": self.model,
            "prompt": prompt,
            "images": [image_b64],
            "stream": False,
        }

        logger.info(
            f"Querying ollama model '{self.model}' with screenshot "
            f"'{os.path.basename(screenshot_path)}'"
        )
        response = requests.post(
            f"{self.host}/api/generate",
            json=payload,
            timeout=120,
        )
        response.raise_for_status()
        result = response.json().get("response", "")
        logger.debug(f"Ollama response: {result}")
        return result


class ClaudeClient(LLMClient):
    """
    Uses the Claude CLI (``claude``) as an LLM backend for vision-based UI analysis.

    The CLI must be installed and authenticated on the machine.  Each query
    runs ``claude -p`` in non-interactive single-shot mode with
    ``--allowedTools Read`` so the CLI can read image files from disk.
    """

    # Maps short alias → full model name passed to --model
    VARIANT_MAP = {
        "opus": "claude-opus-4-6",
        "sonnet": "claude-sonnet-4-5",
        "haiku": "claude-haiku-4-5",
    }
    DEFAULT_VARIANT = "sonnet"

    # Environment variables that signal an interactive Claude Code session and
    # must be removed so the CLI runs as a plain subprocess writing to stdout.
    # CLAUDE_CODE_SSE_PORT hijacks stdout to a JetBrains SSE socket, so
    # subprocess.PIPE never receives any output.
    # NOTE: do NOT remove CLAUDE_CODE_USE_VERTEX / ANTHROPIC_VERTEX_PROJECT_ID
    # as those carry the authentication credentials.
    _CLAUDE_ENV_VARS = (
        "CLAUDECODE",
        "CLAUDE_CODE",
        "CLAUDE_CODE_ENTRYPOINT",
        "CLAUDE_CODE_SSE_PORT",
    )

    def __init__(self, model=None):
        self.variant = self.DEFAULT_VARIANT
        if model and ":" in model:
            variant_part = model.split(":", 1)[1]
            if variant_part in self.VARIANT_MAP:
                self.variant = variant_part
            else:
                logger.warning(
                    f"Unknown Claude variant '{variant_part}', "
                    f"falling back to '{self.DEFAULT_VARIANT}'"
                )

    @property
    def model_name(self):
        """Returns the full model name for the current variant."""
        return self.VARIANT_MAP[self.variant]

    def is_available(self):
        """
        Checks if the ``claude`` CLI is installed and responsive.

        Returns:
            bool: True if ``claude --version`` exits with code 0.
        """
        try:
            claude_bin = self._resolve_claude_bin()
        except RuntimeError as e:
            logger.warning(str(e))
            return False
        try:
            proc = subprocess.run(
                [claude_bin, "--version"],
                capture_output=True,
                timeout=15,
                env=self._build_env(),
                stdin=subprocess.DEVNULL,
            )
            if proc.returncode != 0:
                logger.warning(f"'claude --version' exited with code {proc.returncode}")
                return False
            return True
        except (subprocess.TimeoutExpired, OSError) as e:
            logger.warning(f"Claude CLI check failed: {e}")
            return False

    def _build_env(self):
        """
        Returns a copy of the current environment with Claude interactive-mode
        variables removed, so the CLI behaves as a plain subprocess writing to stdout.

        Ensures /opt/homebrew/bin and the gcloud SDK bin dir are in PATH so that
        both the claude binary and gcloud (needed for Vertex AI auth) are findable.
        """
        env = os.environ.copy()

        # Build list of dirs that must be on PATH
        required_paths = ["/opt/homebrew/bin"]

        # Dynamically locate gcloud and add its parent dir
        gcloud_bin = shutil.which("gcloud")
        if gcloud_bin:
            required_paths.append(os.path.dirname(gcloud_bin))

        current_entries = env.get("PATH", "").split(":")
        for p in required_paths:
            if p not in current_entries:
                env["PATH"] = p + ":" + env["PATH"]

        for key in self._CLAUDE_ENV_VARS:
            env.pop(key, None)

        return env

    def _resolve_claude_bin(self):
        """Returns the absolute path to the claude binary."""
        path = shutil.which("claude", path=self._build_env().get("PATH"))
        if not path:
            raise RuntimeError(
                "Claude CLI ('claude') not found. Install it or add it to PATH."
            )
        return path

    def _debug_log(self, msg):
        """Write debug info to a file, readable even when terminal swallows output."""
        try:
            with open("/tmp/claude_client_debug.txt", "a") as f:
                f.write(f"{time.strftime('%H:%M:%S')} {msg}\n")
                f.flush()
        except OSError:
            pass

    def query_screenshot(self, screenshot_path, prompt):
        """
        Sends a screenshot to the Claude CLI for analysis.

        The CLI is invoked with ``--allowedTools Read`` so it can natively
        read the image file from disk.

        Args:
            screenshot_path (str): Path to the screenshot PNG file.
            prompt (str): The prompt to send along with the image.

        Returns:
            str: The LLM's text response.

        Raises:
            RuntimeError: If the CLI fails, times out, or reports an error.
        """
        abs_path = os.path.abspath(screenshot_path)
        self._debug_log(f"query_screenshot called: {abs_path}")

        if not os.path.isfile(abs_path):
            raise FileNotFoundError(f"Screenshot not found: {abs_path}")

        full_prompt = (
            f"Use the Read tool to read the image file at '{abs_path}', "
            f"then answer the following:\n\n{prompt}"
        )
        timeout = 90
        start = time.time()

        claude_bin = self._resolve_claude_bin()
        env = self._build_env()
        self._debug_log(
            f"claude_bin={claude_bin}, CLAUDE vars={[k for k in env if 'CLAUDE' in k]}"
        )

        cmd = [
            claude_bin,
            "-p",
            full_prompt,
            "--output-format",
            "json",
            "--model",
            self.model_name,
            "--allowedTools",
            "Read",
        ]

        logger.info(
            f"Querying Claude CLI (bin={claude_bin}, model={self.model_name}) with screenshot "
            f"'{os.path.basename(screenshot_path)}'"
        )

        try:
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout,
                env=env,
                stdin=subprocess.DEVNULL,
            )
        except subprocess.TimeoutExpired:
            self._debug_log(f"TIMEOUT after {timeout}s")
            raise RuntimeError(
                f"Claude CLI timed out after {timeout}s "
                f"(duration={time.time() - start:.1f}s)"
            )
        except FileNotFoundError:
            self._debug_log(f"FileNotFoundError: {claude_bin}")
            raise RuntimeError(
                f"Claude CLI not executable at '{claude_bin}'. " "Check installation."
            )
        except OSError as e:
            self._debug_log(f"OSError: {e}")
            raise RuntimeError(f"Failed to launch Claude CLI: {e}")

        duration = time.time() - start
        self._debug_log(f"rc={proc.returncode} duration={duration:.1f}s")
        self._debug_log(f"stdout[:300]={proc.stdout[:300]}")
        self._debug_log(f"stderr[:200]={proc.stderr[:200]}")

        if proc.returncode != 0:
            stderr_text = proc.stderr.strip()[:500] if proc.stderr else ""
            stdout_text = proc.stdout.strip()[:500] if proc.stdout else ""
            cmd_display = " ".join(c for c in cmd if c != full_prompt)
            error_msg = (
                f"Claude CLI exited with code {proc.returncode} "
                f"(duration={duration:.1f}s)\n"
                f"  cmd : {cmd_display}\n"
                f"  stdout: {stdout_text}\n"
                f"  stderr: {stderr_text}"
            )
            logger.error(error_msg)
            self._debug_log(f"ERROR: {error_msg}")
            raise RuntimeError(error_msg)

        raw_output = proc.stdout.strip()
        logger.debug(f"Claude CLI raw output: {raw_output[:500]}")

        try:
            response = json.loads(raw_output)
        except json.JSONDecodeError as e:
            self._debug_log(f"JSONDecodeError: {e} raw={raw_output[:200]}")
            raise RuntimeError(
                f"Failed to parse Claude CLI output as JSON: {e}\n"
                f"Raw output: {raw_output[:500]}"
            )

        if response.get("is_error"):
            result_msg = response.get("result", "unknown error")
            self._debug_log(f"is_error=true: {result_msg}")
            raise RuntimeError(
                f"Claude CLI reported an error: {result_msg}. "
                f"Run 'claude login' or set ANTHROPIC_API_KEY."
            )

        result_text = response.get("result", "")
        if not result_text:
            logger.warning(
                f"Claude CLI returned an empty result for "
                f"'{os.path.basename(screenshot_path)}'"
            )

        cost_usd = response.get("total_cost_usd", 0.0)
        num_turns = response.get("num_turns", 1)
        self._debug_log(
            f"SUCCESS cost=${cost_usd:.4f} turns={num_turns} result[:100]={result_text[:100]}"
        )
        logger.info(
            f"Claude CLI completed: model={self.model_name}, "
            f"cost=${cost_usd:.4f}, turns={num_turns}, duration={duration:.1f}s"
        )

        return result_text


def get_llm_client(model=None):
    """
    Factory function that returns the appropriate LLMClient based on the model string.

    Args:
        model (str): Model identifier.  If it starts with ``"claude"`` a
            :class:`ClaudeClient` is returned; otherwise an
            :class:`OllamaClient`.  Falls back to the value of
            ``config.UI_SELENIUM["llm_model"]`` when *model* is ``None``.

    Returns:
        LLMClient: An instance of the selected backend.
    """
    if model is None:
        model = ocsci_config.UI_SELENIUM.get("llm_model")

    if model and model.startswith("claude"):
        return ClaudeClient(model=model)
    return OllamaClient(model=model)


def ask_llm_about_screen(prompt="", model=None):
    """
    Takes screenshots and queries the LLM about them in one call.

    Args:
        prompt (str): The question to ask the LLM about the screenshot.
        model (str): The LLM model to use. If None, reads from config.

    Returns:
        str: The LLM's text response about the screenshot.
    """
    screenshot_paths = BaseUI().take_screenshot_for_llm(name_suffix="llm_query")
    client = get_llm_client(model=model)
    return client.query_screenshot(screenshot_paths[0], prompt)
