import base64
import json
import logging
import os

import requests

from ocs_ci.framework import config as ocsci_config
from ocs_ci.ocs.ui.base_ui import BaseUI

logger = logging.getLogger(__name__)


# TODO: bring ollama and model setup into ocs-ci deployment, so it's available as a fixture and we can ensure
# the model is pulled before tests run.
class OllamaClient:
    """
    Manages communication with a local ollama instance for vision-based UI analysis.

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
            start = cleaned.find("{")
            end = cleaned.rfind("}") + 1
            if start != -1 and end > start:
                try:
                    return json.loads(cleaned[start:end])
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


def ask_llm_about_screen(prompt="", model=None):
    """
    Takes screenshots and queries the LLM about them in one call.

    Args:
        prompt (str): The question to ask the LLM about the screenshot.
        model (str): The ollama model to use. If None, reads from config.

    Returns:
        str: The LLM's text response about the screenshot.
    """
    screenshot_paths = BaseUI().take_screenshot_for_llm(name_suffix="llm_query")
    client = OllamaClient(model=model)
    return client.query_screenshot(screenshot_paths[0], prompt)
