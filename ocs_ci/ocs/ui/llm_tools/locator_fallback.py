import datetime
import json
import logging
import os
import re

from selenium.common import WebDriverException

from ocs_ci.framework import config as ocsci_config
from ocs_ci.helpers.helpers import get_current_test_name

logger = logging.getLogger(__name__)

STAGE_1_PROMPT = """\
You are a Selenium UI test engineer. A UI test failed because this locator \
could not find an element:
  Selector: {selector}
  Type: {by_type}
  Action: {action}
  URL: {url}

Analyze the DOM below and find the target element. Return a replacement locator.
Rules:
1. Prefer XPath over CSS selectors
2. Prefer data-test, aria-label, id attributes
3. Avoid auto-generated class names
4. NEVER use PatternFly prefixes (pf-, pf-v5-, pf-v6-, etc.) in selectors — \
these change across PF versions and break tests
5. Must match exactly one element

Respond with ONLY JSON: {{"selector": "...", "by_type": "xpath"}}

DOM:
{cleaned_html}
"""

STAGE_2_PROMPT = """\
You are a Selenium UI test engineer. A UI test failed because this locator \
could not find an element:
  Selector: {selector}
  Type: {by_type}
  Action: {action}
  URL: {url}

A screenshot of the page is attached. Cross-reference the visual layout with \
the DOM below to identify the target element. Return a replacement locator.
Rules:
1. Prefer XPath over CSS selectors
2. Prefer data-test, aria-label, id attributes
3. Avoid auto-generated class names
4. NEVER use PatternFly prefixes (pf-, pf-v5-, pf-v6-, etc.) in selectors
5. Must match exactly one element

Respond with ONLY JSON: {{"selector": "...", "by_type": "xpath"}}

DOM:
{cleaned_html}
"""

DOM_MAX_CHARS_STAGE_1 = 80000
DOM_MAX_CHARS_STAGE_2 = 40000

STRIP_TAGS_RE = re.compile(
    r"<(script|style|svg|noscript|link|meta)\b[^>]*>.*?</\1>",
    re.DOTALL | re.IGNORECASE,
)
STRIP_SELF_CLOSING_RE = re.compile(
    r"<(link|meta)\b[^>]*/?>",
    re.IGNORECASE,
)
WHITESPACE_RE = re.compile(r"\s{2,}")


class LocatorFallback:
    """
    AI-powered locator fallback for Selenium UI tests.

    When a locator fails, the DOM (and optionally a screenshot) is sent to an
    LLM which generates a replacement locator. Results are cached per-test.
    """

    def __init__(self, driver):
        self.driver = driver
        self._client = None
        self._cache = None
        self._cache_path = None
        self.total_cost_usd = 0.0
        self.total_requests = 0

    @property
    def client(self):
        if self._client is None:
            from ocs_ci.ocs.ui.llm_tools.llm_helper import get_llm_client

            model = ocsci_config.UI_SELENIUM.get("llm_model", "claude:sonnet")
            self._client = get_llm_client(model=model)
        return self._client

    def _get_cache_path(self):
        if self._cache_path is None:
            base_ui_logs_dir = os.path.join(
                os.path.expanduser(ocsci_config.RUN["log_dir"]),
                f"ui_logs_dir_{ocsci_config.RUN['run_id']}",
            )
            test_name = get_current_test_name()
            self._cache_path = os.path.join(
                base_ui_logs_dir,
                f"ai_locator_cache_{test_name}.json",
            )
        return self._cache_path

    def _load_cache(self):
        if self._cache is not None:
            return self._cache
        cache_path = self._get_cache_path()
        if os.path.isfile(cache_path):
            try:
                with open(cache_path, "r") as f:
                    self._cache = json.load(f)
            except (json.JSONDecodeError, OSError):
                self._cache = {}
        else:
            self._cache = {}
        return self._cache

    def _save_cache(self):
        cache_path = self._get_cache_path()
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        with open(cache_path, "w") as f:
            json.dump(self._cache, f, indent=2)

    @staticmethod
    def _cache_key(locator):
        return f"{locator[0]}|{locator[1]}"

    @staticmethod
    def _strip_dom(html, max_chars=DOM_MAX_CHARS_STAGE_1):
        """
        Strips script, style, svg, noscript, link, and meta tags from HTML,
        collapses whitespace, and truncates to max_chars.
        """
        cleaned = STRIP_TAGS_RE.sub("", html)
        cleaned = STRIP_SELF_CLOSING_RE.sub("", cleaned)
        cleaned = WHITESPACE_RE.sub(" ", cleaned)
        if len(cleaned) > max_chars:
            cleaned = cleaned[:max_chars]
        return cleaned

    def _validate_locator(self, selector, by_type):
        """
        Tests whether a locator finds exactly one element on the current page.

        Returns:
            bool: True if exactly one element is found.
        """
        try:
            elements = self.driver.find_elements(by=by_type, value=selector)
            return len(elements) == 1
        except Exception as e:
            logger.debug(f"Locator validation failed: {e}")
            return False

    def _parse_llm_locator(self, raw_response):
        """
        Parses the LLM response into (selector, by_type).

        Returns:
            tuple: (selector, by_type) or None if parsing fails.
        """
        cleaned = raw_response.strip()
        if cleaned.startswith("```"):
            lines = cleaned.split("\n")
            lines = [l for l in lines if not l.strip().startswith("```")]
            cleaned = "\n".join(lines)

        json_start = cleaned.find("{")
        json_end = cleaned.rfind("}") + 1
        if json_start == -1 or json_end <= json_start:
            logger.warning(f"No JSON found in LLM response: {cleaned[:200]}")
            return None

        try:
            data = json.loads(cleaned[json_start:json_end])
        except json.JSONDecodeError:
            logger.warning(f"Failed to parse LLM locator JSON: {cleaned[:200]}")
            return None

        selector = data.get("selector")
        by_type = data.get("by_type")
        if not selector or not by_type:
            logger.warning(f"LLM response missing selector or by_type: {data}")
            return None

        return (selector, by_type)

    def attempt_fallback(self, locator, action="interact"):
        """
        Attempts to find a replacement locator using LLM analysis.

        Args:
            locator (tuple): Original (selector, By) tuple that failed.
            action (str): The action that was being performed (click, send_keys, etc.).

        Returns:
            tuple: (selector, by_type) replacement locator, or None if fallback fails.
        """
        if not ocsci_config.UI_SELENIUM.get("ai_fallback"):
            return None

        selector = locator[0]
        by_type = locator[1]
        cache_key = self._cache_key(locator)

        logger.info(
            "\n"
            "╔══════════════════════════════════════════════════════════════╗\n"
            "║           AI LOCATOR FALLBACK ACTIVATED                      ║\n"
            "╚══════════════════════════════════════════════════════════════╝\n"
            f"  Selector : {selector}\n"
            f"  Type     : {by_type}\n"
            f"  Action   : {action}"
        )

        # use cached updated if available and matching current locator
        cache = self._load_cache()
        if cache_key in cache:
            cached = cache[cache_key]
            cached_selector = cached["new_selector"]
            cached_by_type = cached["new_by_type"]
            if self._validate_locator(cached_selector, cached_by_type):
                logger.info(
                    "\n"
                    "╔══════════════════════════════════════════════════════════════╗\n"
                    "║           AI FALLBACK: CACHE HIT                             ║\n"
                    "╚══════════════════════════════════════════════════════════════╝\n"
                    f"  Cached selector : {cached_selector}\n"
                    f"  Cached type     : {cached_by_type}"
                )
                return cached_selector, cached_by_type
            else:
                logger.info("Cached locator no longer valid, proceeding to LLM query")

        if not self.client.is_available():
            logger.warning("LLM client is not available, skipping AI fallback")
            return None

        try:
            url = self.driver.current_url
        except WebDriverException:
            url = "unknown"

        try:
            raw_html = self.driver.page_source
        except WebDriverException as e:
            logger.error(f"Failed to capture DOM: {e}")
            return None

        cost_before = self.client.total_cost_usd

        result = self._try_stage_1(selector, by_type, action, url, raw_html)
        if result:
            self._cache_result(cache_key, selector, by_type, result, url)
            self._log_cost(cost_before)
            return result

        result = self._try_stage_2(selector, by_type, action, url, raw_html)
        if result:
            self._cache_result(cache_key, selector, by_type, result, url)
            self._log_cost(cost_before)
            return result

        self._log_cost(cost_before)
        logger.warning(
            "\n"
            "╔══════════════════════════════════════════════════════════════╗\n"
            "║           AI FALLBACK: FAILED                                ║\n"
            "╚══════════════════════════════════════════════════════════════╝\n"
            f"  Could not find replacement for: {selector}"
        )
        return None

    def _try_stage_1(self, selector, by_type, action, url, raw_html):
        """Stage 1: DOM-only LLM query."""
        logger.info("AI Fallback Stage 1: DOM-only analysis")
        cleaned_html = self._strip_dom(raw_html, DOM_MAX_CHARS_STAGE_1)

        prompt = STAGE_1_PROMPT.format(
            selector=selector,
            by_type=by_type,
            action=action,
            url=url,
            cleaned_html=cleaned_html,
        )

        try:
            raw_response = self.client.query_dom(prompt)
        except Exception as e:
            logger.warning(f"Stage 1 LLM query failed: {e}")
            return None

        parsed = self._parse_llm_locator(raw_response)
        if not parsed:
            logger.info("Stage 1: LLM did not return a valid locator")
            return None

        new_selector, new_by_type = parsed
        if self._validate_locator(new_selector, new_by_type):
            logger.info(
                "\n"
                "╔══════════════════════════════════════════════════════════════╗\n"
                "║           AI FALLBACK: STAGE 1 SUCCESS                       ║\n"
                "╚══════════════════════════════════════════════════════════════╝\n"
                f"  New selector : {new_selector}\n"
                f"  New type     : {new_by_type}"
            )
            return new_selector, new_by_type

        logger.info(
            f"Stage 1: LLM locator did not match exactly one element: "
            f"{new_selector} ({new_by_type})"
        )
        return None

    def _try_stage_2(self, selector, by_type, action, url, raw_html):
        """Stage 2: DOM + screenshot LLM query."""
        logger.info("AI Fallback Stage 2: DOM + screenshot analysis")
        cleaned_html = self._strip_dom(raw_html, DOM_MAX_CHARS_STAGE_2)

        screenshot_path = self._capture_screenshot()
        if not screenshot_path:
            logger.warning("Stage 2: Failed to capture screenshot, aborting")
            return None

        prompt = STAGE_2_PROMPT.format(
            selector=selector,
            by_type=by_type,
            action=action,
            url=url,
            cleaned_html=cleaned_html,
        )

        try:
            raw_response = self.client.query_screenshot(screenshot_path, prompt)
        except Exception as e:
            logger.warning(f"Stage 2 LLM query failed: {e}")
            return None

        parsed = self._parse_llm_locator(raw_response)
        if not parsed:
            logger.info("Stage 2: LLM did not return a valid locator")
            return None

        new_selector, new_by_type = parsed
        if self._validate_locator(new_selector, new_by_type):
            logger.info(
                "\n"
                "╔══════════════════════════════════════════════════════════════╗\n"
                "║           AI FALLBACK: STAGE 2 SUCCESS                       ║\n"
                "╚══════════════════════════════════════════════════════════════╝\n"
                f"  New selector : {new_selector}\n"
                f"  New type     : {new_by_type}"
            )
            return (new_selector, new_by_type)

        logger.info(
            f"Stage 2: LLM locator did not match exactly one element: "
            f"{new_selector} ({new_by_type})"
        )
        return None

    def _capture_screenshot(self):
        """Captures a screenshot for Stage 2 analysis."""
        base_ui_logs_dir = os.path.join(
            os.path.expanduser(ocsci_config.RUN["log_dir"]),
            f"ui_logs_dir_{ocsci_config.RUN['run_id']}",
        )
        screenshots_dir = os.path.join(
            base_ui_logs_dir,
            "screenshots_ui",
            get_current_test_name(),
        )
        os.makedirs(screenshots_dir, exist_ok=True)

        timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H-%M-%S.%f")
        screenshot_path = os.path.join(
            screenshots_dir,
            f"{timestamp}_ai_fallback.png",
        )
        try:
            self.driver.save_screenshot(screenshot_path)
            return screenshot_path
        except Exception as e:
            logger.error(f"Failed to save screenshot for AI fallback: {e}")
            return None

    def _log_cost(self, cost_before):
        """Logs cost incurred by the current fallback attempt and cumulative totals."""
        attempt_cost = self.client.total_cost_usd - cost_before
        requests_made = self.client.total_requests - self.total_requests
        self.total_cost_usd = self.client.total_cost_usd
        self.total_requests = self.client.total_requests
        if attempt_cost > 0 or requests_made > 0:
            session_cost = ocsci_config.UI_SELENIUM.get("ai_fallback_session_cost", 0.0)
            session_requests = ocsci_config.UI_SELENIUM.get(
                "ai_fallback_session_requests", 0
            )
            session_cost += attempt_cost
            session_requests += requests_made
            ocsci_config.UI_SELENIUM["ai_fallback_session_cost"] = session_cost
            ocsci_config.UI_SELENIUM["ai_fallback_session_requests"] = session_requests
            logger.info(
                "\n"
                "╔══════════════════════════════════════════════════════════════╗\n"
                "║           AI FALLBACK: COST REPORT                           ║\n"
                "╚══════════════════════════════════════════════════════════════╝\n"
                f"  This attempt   : ${attempt_cost:.4f} ({requests_made} request(s))\n"
                f"  Test cumulative: ${self.total_cost_usd:.4f} "
                f"({self.total_requests} total request(s))\n"
                f"  Session total  : ${session_cost:.4f} "
                f"({session_requests} total request(s))"
            )

    def log_cost_summary(self):
        """
        Logs a final cost summary for the entire test.

        Call this at test teardown to get a complete picture of AI fallback
        costs incurred during the test run.
        """
        if self.total_requests > 0:
            logger.info(
                "\n"
                "╔══════════════════════════════════════════════════════════════╗\n"
                "║           AI FALLBACK: FINAL COST SUMMARY                    ║\n"
                "╚══════════════════════════════════════════════════════════════╝\n"
                f"  Total cost    : ${self.total_cost_usd:.4f}\n"
                f"  Total requests: {self.total_requests}"
            )

    def _cache_result(self, cache_key, old_selector, old_by_type, new_locator, url):
        """Saves a successful fallback result to the cache."""
        cache = self._load_cache()
        cache[cache_key] = {
            "old_selector": old_selector,
            "old_by_type": old_by_type,
            "new_selector": new_locator[0],
            "new_by_type": new_locator[1],
            "timestamp": datetime.datetime.now().isoformat(),
            "page_url": url,
        }
        self._cache = cache
        self._save_cache()
        logger.info(f"AI fallback result cached: {self._get_cache_path()}")
