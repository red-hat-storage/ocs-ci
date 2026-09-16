import datetime
import json
import logging
import os
import re

from bs4 import BeautifulSoup, Comment
from selenium.common import WebDriverException

from ocs_ci.framework import config as ocsci_config
from ocs_ci.helpers.helpers import get_current_test_name

logger = logging.getLogger(__name__)

PRESERVED_ATTRIBUTES = {
    "id",
    "class",
    "name",
    "role",
    "aria-label",
    "aria-labelledby",
    "aria-describedby",
    "aria-expanded",
    "aria-haspopup",
    "aria-modal",
    "aria-hidden",
    "data-testid",
    "data-test",
    "data-test-id",
    "data-cy",
    "title",
    "placeholder",
    "value",
    "href",
    "type",
    "disabled",
    "selected",
    "checked",
    "alt",
}

STAGE_1_PROMPT = """\
You are a Selenium UI test engineer debugging a locator failure.

FAILED LOCATOR (treat as a hint about intent, not the answer):
  Selector : {selector}
  Type     : {by_type}
  Action   : {action}
  URL      : {url}

CALL CHAIN — read this to understand what the test was doing when it failed:
{stack_trace}

BEFORE searching the DOM, reason through these three questions:
1. INTENT   — What is the test trying to accomplish? \
(infer from test name, page-object method names, and helper names in the call chain)
2. ELEMENT  — What kind of element is this? \
(e.g. button, input, checkbox, link, list item, table row, dropdown option) \
Use the action ("{action}"), the method names in the trace, and the failed \
locator name as clues. Do not assume the element type from the selector alone.
3. IDENTITY — What stable attributes would this element carry? \
(data-test, aria-label, role, id, type, name — prefer these over class names)

Then search the DOM for an element that matches the inferred intent, kind, and \
identity — even if it looks nothing like the original locator.

Rules:
1. Prefer XPath over CSS selectors
2. Prefer data-test, aria-label, id, role, type attributes
3. Avoid auto-generated class names
4. NEVER use PatternFly prefixes (pf-, pf-v5-, pf-v6-, etc.) — \
these change across PF versions and break tests
5. Must match exactly one element
6. Prefer semantic/structural attributes over position-based selectors

Respond with ONLY JSON: {{"selector": "...", "by_type": "xpath"}}

DOM:
{cleaned_html}
"""

STAGE_2_PROMPT = """\
You are a Selenium UI test engineer debugging a locator failure.

FAILED LOCATOR (treat as a hint about intent, not the answer):
  Selector : {selector}
  Type     : {by_type}
  Action   : {action}
  URL      : {url}

CALL CHAIN — read this to understand what the test was doing when it failed:
{stack_trace}

BEFORE searching the DOM, reason through these three questions:
1. INTENT   — What is the test trying to accomplish? \
(infer from test name, page-object method names, and helper names in the call chain)
2. ELEMENT  — What kind of element is this? \
(e.g. button, input, checkbox, link, list item, table row, dropdown option) \
Use the action ("{action}"), the method names in the trace, and the failed \
locator name as clues. Do not assume the element type from the selector alone.
3. IDENTITY — What stable attributes would this element carry? \
(data-test, aria-label, role, id, type, name — prefer these over class names)

A screenshot of the page is attached. Use it together with the DOM — \
neither source alone is sufficient:
- Screenshot → tells you WHICH page you are on, WHERE the target element \
appears visually, what its visible label/text/icon is, and what kind of \
control it looks like (button, dropdown, text field, list item, etc.)
- DOM       → tells you the actual HTML structure, attributes, and hierarchy \
needed to write a precise XPath
The screenshot shows the full page — it contains many other elements that are \
NOT the target. Use it only to identify the region and visual appearance of \
the one element the test was trying to interact with, then locate that specific \
node in the DOM and build the XPath from its attributes.

Rules:
1. Prefer XPath over CSS selectors
2. Prefer data-test, aria-label, id, role, type attributes
3. Avoid auto-generated class names
4. NEVER use PatternFly prefixes (pf-, pf-v5-, pf-v6-, etc.)
5. Must match exactly one element
6. Prefer semantic/structural attributes over position-based selectors

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


def _locator_cache_dir():
    """Returns the locator_cache/ directory path for the current run."""
    base_ui_logs_dir = os.path.join(
        os.path.expanduser(ocsci_config.RUN["log_dir"]),
        f"ui_logs_dir_{ocsci_config.RUN['run_id']}",
    )
    return os.path.join(base_ui_logs_dir, "locator_cache")


def get_session_cache_path():
    """Returns the path to the session-wide locator cache file."""
    return os.path.join(_locator_cache_dir(), "session_locators_cache.json")


class LocatorFallback:
    """
    AI-powered locator fallback for Selenium UI tests.

    When a locator fails, the DOM (and optionally a screenshot) is sent to an
    LLM which generates a replacement locator. Results are cached per-test and
    accumulated in a session-wide cache for reuse across tests.
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
            test_name = get_current_test_name()
            self._cache_path = os.path.join(
                _locator_cache_dir(),
                f"{test_name}.json",
            )
        return self._cache_path

    @staticmethod
    def _read_json_file(path):
        """Reads a JSON file and returns its contents as a dict, or {} on any error."""
        if not os.path.isfile(path):
            return {}
        try:
            with open(path, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            return {}

    def _load_cache(self):
        if self._cache is not None:
            return self._cache
        session_data = self._read_json_file(get_session_cache_path())
        per_test_data = self._read_json_file(self._get_cache_path())
        self._cache = {**session_data, **per_test_data}
        return self._cache

    def _save_cache(self):
        cache_dir = _locator_cache_dir()
        os.makedirs(cache_dir, exist_ok=True)

        with open(self._get_cache_path(), "w") as f:
            json.dump(self._cache, f, indent=2)

        session_cache_path = get_session_cache_path()
        session_data = self._read_json_file(session_cache_path)
        session_data.update(self._cache)
        with open(session_cache_path, "w") as f:
            json.dump(session_data, f, indent=2)

    @staticmethod
    def _cache_key(locator):
        return f"{locator[0]}|{locator[1]}"

    @staticmethod
    def _clean_soup_dom(soup, max_chars=DOM_MAX_CHARS_STAGE_1):
        """
        Cleans a BeautifulSoup tree by stripping non-essential tags, comments,
        hidden elements, and irrelevant attributes while preserving structural hierarchy.
        """
        # 1. Remove script, style, svg, noscript, link, meta, iframe, etc.
        for tag in soup.find_all(
            ["script", "style", "svg", "noscript", "link", "meta", "template"]
        ):
            tag.decompose()

        # 2. Remove HTML comments
        for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
            comment.extract()

        # 3. Clean attributes on all remaining tags
        for el in soup.find_all(True):
            attrs_to_keep = {}
            for k, v in list(el.attrs.items()):
                k_lower = k.lower()
                if k_lower in PRESERVED_ATTRIBUTES or k_lower.startswith("data-"):
                    if isinstance(v, list):
                        attrs_to_keep[k] = " ".join(v)
                    else:
                        attrs_to_keep[k] = v
            el.attrs = attrs_to_keep

        # Convert back to string and collapse excessive whitespace
        rendered = str(soup)
        rendered = WHITESPACE_RE.sub(" ", rendered)
        if len(rendered) > max_chars:
            rendered = rendered[:max_chars]
        return rendered

    def _extract_intelligent_dom(self, raw_html, max_chars=DOM_MAX_CHARS_STAGE_1):
        """
        Intelligently extracts DOM, prioritizing active modals, dialogs, drawers,
        and open menus. If an active container exists, returns its structured context.
        Otherwise, returns the structured full-page DOM.
        """
        if not raw_html:
            return ""

        try:
            soup = BeautifulSoup(raw_html, "html.parser")
        except Exception as e:
            logger.warning(
                f"BeautifulSoup parsing failed: {e}. Falling back to regex stripping."
            )
            return self._strip_dom(raw_html, max_chars)

        # Priority 1: Active Modals / Dialogs
        modal_selectors = [
            {"role": "dialog"},
            {"aria-modal": "true"},
            {
                "class": re.compile(
                    r"\b(?:pf-v\d+-c-modal-box|modal-dialog|pf-c-modal-box)\b"
                )
            },
        ]
        for criteria in modal_selectors:
            modal = soup.find(attrs=criteria)
            if modal:
                logger.info(
                    "[AI_FALLBACK] Found active modal dialog context for DOM extraction"
                )
                cleaned_modal = self._clean_soup_dom(modal, max_chars)
                if len(cleaned_modal.strip()) > 50:
                    return cleaned_modal

        # Priority 2: Open Menus / Dropdowns / Popovers / Drawers
        menu_selectors = [
            {"role": "menu"},
            {"role": "listbox"},
            {
                "class": re.compile(
                    r"\b(?:pf-v\d+-c-menu|pf-v\d+-c-dropdown__menu|pf-c-dropdown__menu|pf-v\d+-c-drawer__panel)\b"
                )
            },
        ]
        for criteria in menu_selectors:
            menu = soup.find(attrs=criteria)
            if menu:
                logger.info(
                    "[AI_FALLBACK] Found active menu/drawer context for DOM extraction"
                )
                cleaned_menu = self._clean_soup_dom(menu, max_chars)
                if len(cleaned_menu.strip()) > 50:
                    return cleaned_menu

        # Priority 3: Full page structured extraction
        body = soup.find("body") or soup
        return self._clean_soup_dom(body, max_chars)

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

    def _extract_shadow_dom_and_iframes(self, raw_html):
        """
        Safely extracts Shadow DOM content and iframe documents using Selenium WebDriver
        where supported, without permanently altering current frame context.
        """
        extra_contexts = []
        if not self.driver:
            return extra_contexts

        # 1. Shadow DOM roots via JavaScript
        try:
            shadow_script = """
            var shadowHosts = [];
            var all = document.querySelectorAll('*');
            for (var i = 0; i < all.length; i++) {
                if (all[i].shadowRoot) {
                    shadowHosts.push({
                        tag: all[i].tagName.toLowerCase(),
                        id: all[i].id || '',
                        className: all[i].className || '',
                        html: all[i].shadowRoot.innerHTML
                    });
                }
            }
            return shadowHosts;
            """
            shadow_elements = self.driver.execute_script(shadow_script)
            if shadow_elements:
                for idx, item in enumerate(shadow_elements):
                    host_info = (
                        f"<shadow-root host='{item.get('tag')}' id='{item.get('id')}'>"
                    )
                    extra_contexts.append(
                        f"{host_info}\n{item.get('html', '')}\n</shadow-root>"
                    )
        except Exception as e:
            logger.debug(f"Shadow DOM extraction not supported or failed: {e}")

        # 2. Accessible iframes (with safe frame context restoration)
        try:
            iframes = self.driver.find_elements(by="tag name", value="iframe")
            for idx, frame in enumerate(iframes[:3]):  # limit depth to 3 iframes
                try:
                    self.driver.switch_to.frame(frame)
                    try:
                        frame_html = self.driver.page_source
                        if frame_html:
                            extra_contexts.append(
                                f"<iframe-content index='{idx}'>\n{frame_html}\n</iframe-content>"
                            )
                    finally:
                        self.driver.switch_to.default_content()
                except Exception as fe:
                    logger.debug(f"Could not inspect iframe {idx}: {fe}")
                    try:
                        self.driver.switch_to.default_content()
                    except Exception:
                        pass
        except Exception as e:
            logger.debug(f"iframe inspection failed: {e}")

        return extra_contexts

    def _check_page_health(self):
        """
        Performs lightweight health checks on the browser page before invoking LLM.

        Returns:
            bool: True if page is healthy and ready for fallback, False if page is broken/disconnected/unreachable.
        """
        if not self.driver:
            return True

        try:
            # Check readyState
            ready_state = self.driver.execute_script("return document.readyState;")
            if ready_state not in ("complete", "interactive"):
                logger.warning(
                    f"[AI_FALLBACK] Page document.readyState is '{ready_state}', may still be loading"
                )

            # Check title for clear 404 or connection failures
            title = (self.driver.title or "").lower()
            if any(
                err in title
                for err in [
                    "404 not found",
                    "server error",
                    "502 bad gateway",
                    "503 service unavailable",
                    "problem loading page",
                ]
            ):
                logger.warning(
                    f"[AI_FALLBACK] Page title indicates unrecoverable error: '{title}'"
                )
                return False

            url = (self.driver.current_url or "").lower()
            if (
                url.startswith("data:")
                or "about:neterror" in url
                or "about:blank" in url
            ):
                logger.warning(
                    f"[AI_FALLBACK] Browser current_url indicates invalid page state: '{url}'"
                )
                return False

            return True
        except Exception as e:
            logger.debug(f"[AI_FALLBACK] Page health pre-check failed: {e}")
            return True

    def _validate_locator(self, selector, by_type, index=None):
        """
        Tests whether a locator finds valid matching elements on the current page.

        If index is None: requires exactly 1 match (or attempts safe disambiguation if >1).
        If index is provided (int): checks if elements list has at least index + 1 items.

        Returns:
            bool: True if a valid match is found.
        """
        try:
            elements = self.driver.find_elements(by=by_type, value=selector)
            if index is not None:
                return 0 <= index < len(elements)
            return len(elements) == 1
        except Exception as e:
            logger.debug(f"Locator validation failed: {e}")
            return False

    @staticmethod
    def _extract_json_objects(text):
        """
        Extracts top-level JSON object candidates from text by balancing braces,
        properly tracking string literals and escape sequences.

        Args:
            text (str): Input text possibly containing reasoning and JSON blocks.

        Returns:
            list[dict]: List of successfully parsed JSON dictionaries.
        """
        results = []
        if not text:
            return results

        cleaned = text.strip()
        # Strip markdown fences if present
        if "```" in cleaned:
            fence_pattern = re.compile(
                r"```(?:json)?\s*([\s\S]*?)\s*```", re.IGNORECASE
            )
            fence_matches = fence_pattern.findall(cleaned)
            for fm in fence_matches:
                fm_stripped = fm.strip()
                if fm_stripped.startswith("{") and fm_stripped.endswith("}"):
                    try:
                        parsed = json.loads(fm_stripped)
                        if isinstance(parsed, dict):
                            results.append(parsed)
                    except json.JSONDecodeError:
                        pass

        in_string = False
        escape = False
        brace_level = 0
        start_idx = None

        for idx, char in enumerate(cleaned):
            if in_string:
                if escape:
                    escape = False
                elif char == "\\":
                    escape = True
                elif char == '"':
                    in_string = False
            else:
                if char == '"':
                    in_string = True
                elif char == "{":
                    if brace_level == 0:
                        start_idx = idx
                    brace_level += 1
                elif char == "}":
                    if brace_level > 0:
                        brace_level -= 1
                        if brace_level == 0 and start_idx is not None:
                            candidate = cleaned[start_idx : idx + 1]
                            try:
                                parsed = json.loads(candidate)
                                if isinstance(parsed, dict) and parsed not in results:
                                    results.append(parsed)
                            except json.JSONDecodeError:
                                pass
                            start_idx = None

        return results

    def _parse_llm_locator(self, raw_response):
        """
        Parses the LLM response into (selector, by_type) or (selector, by_type, index).

        Accepts JSON surrounded by markdown code fences or conversational reasoning.
        Validates required fields ('selector' and 'by_type') and their types.

        Returns:
            tuple: (selector, by_type) or (selector, by_type, index) or None if parsing fails.
        """
        if not raw_response or not isinstance(raw_response, str):
            return None

        candidates = self._extract_json_objects(raw_response)
        if not candidates:
            logger.warning(
                f"No valid JSON object found in LLM response: {raw_response[:200]}"
            )
            return None

        valid_by_types = {
            "xpath",
            "css",
            "id",
            "name",
            "tag name",
            "class name",
            "link text",
            "partial link text",
        }

        for data in candidates:
            if not isinstance(data, dict):
                continue
            selector = data.get("selector")
            by_type = data.get("by_type")

            if not isinstance(selector, str) or not selector.strip():
                continue
            if not isinstance(by_type, str) or not by_type.strip():
                continue

            by_type_normalized = by_type.strip().lower()
            if by_type_normalized not in valid_by_types:
                # If LLM wrote 'css selector', map to 'css'
                if by_type_normalized in ("css selector", "css_selector"):
                    by_type_normalized = "css"
                else:
                    logger.warning(f"Unrecognized by_type in LLM response: {by_type}")
                    continue

            index = data.get("index")
            if index is not None:
                if isinstance(index, int) and index >= 0:
                    return (selector.strip(), by_type_normalized, index)
                elif isinstance(index, str) and index.isdigit():
                    return (selector.strip(), by_type_normalized, int(index))

            return (selector.strip(), by_type_normalized)

        logger.warning(
            f"LLM response JSON missing valid selector/by_type schema: {candidates}"
        )
        return None

    def attempt_fallback(self, locator, action="interact", stack_trace=None):
        """
        Attempts to find a replacement locator using LLM analysis.

        Args:
            locator (tuple): Original (selector, By) tuple that failed.
            action (str): The action that was being performed (click, send_keys, etc.).
            stack_trace (str): Full Python traceback captured at the point of failure.

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
            f"  selector={selector}  by={by_type}  action={action}"
        )

        # use cached updated if available and matching current locator
        cache = self._load_cache()
        if cache_key in cache:
            cached = cache[cache_key]
            cached_selector = cached["new_selector"]
            cached_by_type = cached["new_by_type"]
            cached_index = cached.get("index")
            if self._validate_locator(
                cached_selector, cached_by_type, index=cached_index
            ):
                logger.info(
                    "[AI_FALLBACK] cache_hit selector=%s by=%s index=%s",
                    cached_selector,
                    cached_by_type,
                    cached_index,
                )
                return cached_selector, cached_by_type
            else:
                logger.info("Cached locator no longer valid, proceeding to LLM query")

        # Page health pre-check
        if not self._check_page_health():
            logger.warning(
                "[AI_FALLBACK] Page health pre-check failed, skipping AI fallback"
            )
            return None

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

        result = self._try_stage_1(
            selector, by_type, action, url, raw_html, stack_trace=stack_trace
        )
        if result:
            self._cache_result(cache_key, selector, by_type, result, url)
            self._log_cost(cost_before)
            return result

        result = self._try_stage_2(
            selector, by_type, action, url, raw_html, stack_trace=stack_trace
        )
        if result:
            self._cache_result(cache_key, selector, by_type, result, url)
            self._log_cost(cost_before)
            return result

        self._log_cost(cost_before)
        logger.warning(
            "[AI_FALLBACK] failed — no replacement found for selector=%s", selector
        )
        return None

    def _try_stage_1(self, selector, by_type, action, url, raw_html, stack_trace=None):
        """Stage 1: DOM-only LLM query."""
        logger.info("[AI_FALLBACK] stage=1 (DOM-only) selector=%s", selector)
        cleaned_html = self._extract_intelligent_dom(raw_html, DOM_MAX_CHARS_STAGE_1)
        extra_contexts = self._extract_shadow_dom_and_iframes(raw_html)
        if extra_contexts:
            cleaned_html += "\n\nSHADOW DOM & IFRAME CONTEXTS:\n" + "\n".join(
                extra_contexts
            )

        prompt = STAGE_1_PROMPT.format(
            selector=selector,
            by_type=by_type,
            action=action,
            url=url,
            stack_trace=stack_trace or "(not available)",
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

        new_selector = parsed[0]
        new_by_type = parsed[1]
        index = parsed[2] if len(parsed) > 2 else None

        if self._validate_locator(new_selector, new_by_type, index=index):
            logger.info(
                "[AI_FALLBACK] stage=1 success new_selector=%s new_by=%s index=%s",
                new_selector,
                new_by_type,
                index,
            )
            return (new_selector, new_by_type)

        logger.info(
            "[AI_FALLBACK] stage=1 no_match selector=%s by=%s index=%s",
            new_selector,
            new_by_type,
            index,
        )
        return None

    def _try_stage_2(self, selector, by_type, action, url, raw_html, stack_trace=None):
        """Stage 2: DOM + screenshot LLM query."""
        logger.info("[AI_FALLBACK] stage=2 (DOM+screenshot) selector=%s", selector)
        cleaned_html = self._extract_intelligent_dom(raw_html, DOM_MAX_CHARS_STAGE_2)
        extra_contexts = self._extract_shadow_dom_and_iframes(raw_html)
        if extra_contexts:
            cleaned_html += "\n\nSHADOW DOM & IFRAME CONTEXTS:\n" + "\n".join(
                extra_contexts
            )

        screenshot_path = self._capture_screenshot()
        if not screenshot_path:
            logger.warning("Stage 2: Failed to capture screenshot, aborting")
            return None

        prompt = STAGE_2_PROMPT.format(
            selector=selector,
            by_type=by_type,
            action=action,
            url=url,
            stack_trace=stack_trace or "(not available)",
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

        new_selector = parsed[0]
        new_by_type = parsed[1]
        index = parsed[2] if len(parsed) > 2 else None

        if self._validate_locator(new_selector, new_by_type, index=index):
            logger.info(
                "[AI_FALLBACK] stage=2 success new_selector=%s new_by=%s index=%s",
                new_selector,
                new_by_type,
                index,
            )
            return (new_selector, new_by_type)

        logger.info(
            "[AI_FALLBACK] stage=2 no_match selector=%s by=%s index=%s",
            new_selector,
            new_by_type,
            index,
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
                "[AI_FALLBACK] cost attempt=$%.4f/%d_req  cumulative=$%.4f/%d_req  session=$%.4f/%d_req",
                attempt_cost,
                requests_made,
                self.total_cost_usd,
                self.total_requests,
                session_cost,
                session_requests,
            )

    def log_cost_summary(self):
        """
        Logs a final cost summary for the entire test.

        Call this at test teardown to get a complete picture of AI fallback
        costs incurred during the test run.
        """
        if self.total_requests > 0:
            logger.info(
                "[AI_FALLBACK] final_summary cost=$%.4f requests=%d",
                self.total_cost_usd,
                self.total_requests,
            )

    def _cache_result(self, cache_key, old_selector, old_by_type, new_locator, url):
        """Saves a successful fallback result to the cache."""
        cache = self._load_cache()
        entry = {
            "old_selector": old_selector,
            "old_by_type": old_by_type,
            "new_selector": new_locator[0],
            "new_by_type": new_locator[1],
            "timestamp": datetime.datetime.now().isoformat(),
            "page_url": url,
            "test_name": get_current_test_name(),
        }
        if len(new_locator) > 2 and new_locator[2] is not None:
            entry["index"] = new_locator[2]
        cache[cache_key] = entry
        self._cache = cache
        self._save_cache()
        logger.info("[AI_FALLBACK] cached result path=%s", self._get_cache_path())
