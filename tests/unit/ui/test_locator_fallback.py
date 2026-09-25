"""
Unit tests for AI Locator Fallback mechanism.
"""

from unittest.mock import MagicMock, patch
import pytest

from ocs_ci.framework import config as ocsci_config
from ocs_ci.ocs.ui.llm_tools.locator_fallback import LocatorFallback


class DummyDriver:
    def __init__(
        self,
        title="OpenShift Web Console",
        current_url="https://console.example.com",
        elements_map=None,
    ):
        self.title = title
        self.current_url = current_url
        self.elements_map = elements_map or {}
        self.page_source = (
            "<html><body><button id='target-btn'>Submit</button></body></html>"
        )

    def find_elements(self, by, value):
        return self.elements_map.get((by, value), [])

    def save_screenshot(self, path):
        return True


@pytest.fixture
def fallback():
    return LocatorFallback(driver=DummyDriver())


# 1. JSON Parsing Tests
def test_parse_pure_json(fallback):
    raw = '{"selector": "//button[@id=\'submit\']", "by_type": "xpath"}'
    result = fallback._parse_llm_locator(raw)
    assert result == ("//button[@id='submit']", "xpath")


def test_parse_json_in_markdown_fences(fallback):
    raw = """```json
{
    "selector": "#submit-btn",
    "by_type": "css"
}
```"""
    result = fallback._parse_llm_locator(raw)
    assert result == ("#submit-btn", "css")


def test_parse_json_preceded_by_reasoning(fallback):
    raw = """Looking at the context, this is a lifecycle wizard and the button is CURRENT_OBJECTS.
{
    "selector": "//button[@id='CURRENT_OBJECTS']",
    "by_type": "xpath"
}"""
    result = fallback._parse_llm_locator(raw)
    assert result == ("//button[@id='CURRENT_OBJECTS']", "xpath")


def test_parse_json_followed_by_reasoning(fallback):
    raw = """{
    "selector": "//input[@name='search']",
    "by_type": "xpath"
}
Hope this locator helps!"""
    result = fallback._parse_llm_locator(raw)
    assert result == ("//input[@name='search']", "xpath")


def test_parse_json_with_whitespace_and_newlines(fallback):
    raw = """

    {
        "selector"  :   "//div[@role='dialog']//button"  ,
        "by_type"   :   "xpath"
    }

    """
    result = fallback._parse_llm_locator(raw)
    assert result == ("//div[@role='dialog']//button", "xpath")


def test_parse_json_with_additional_valid_fields(fallback):
    raw = """{
    "selector": "//a[@href='/home']",
    "by_type": "xpath",
    "confidence": 0.95,
    "explanation": "Direct navigation link"
}"""
    result = fallback._parse_llm_locator(raw)
    assert result == ("//a[@href='/home']", "xpath")


def test_parse_json_with_nested_braces_in_reasoning(fallback):
    raw = """Here is the rationale: the structure {a: {b: 1}} was replaced by a button.
```json
{
    "selector": "//button[@data-test='confirm']",
    "by_type": "xpath"
}
```"""
    result = fallback._parse_llm_locator(raw)
    assert result == ("//button[@data-test='confirm']", "xpath")


def test_parse_malformed_json(fallback):
    raw = "Here is the locator: {selector: '//button', by_type: xpath}"
    result = fallback._parse_llm_locator(raw)
    assert result is None


def test_parse_missing_selector(fallback):
    raw = '{"by_type": "xpath"}'
    result = fallback._parse_llm_locator(raw)
    assert result is None


def test_parse_missing_by_type(fallback):
    raw = '{"selector": "//button"}'
    result = fallback._parse_llm_locator(raw)
    assert result is None


def test_parse_invalid_field_types(fallback):
    raw = '{"selector": 12345, "by_type": "xpath"}'
    result = fallback._parse_llm_locator(raw)
    assert result is None

    raw2 = '{"selector": "//button", "by_type": ["xpath"]}'
    result2 = fallback._parse_llm_locator(raw2)
    assert result2 is None


def test_parse_empty_or_none(fallback):
    assert fallback._parse_llm_locator("") is None
    assert fallback._parse_llm_locator(None) is None
    assert fallback._parse_llm_locator("   \n\t  ") is None


def test_parse_multiple_json_objects(fallback):
    raw = """First attempt: {"wrong": 1}
Actual locator:
{
    "selector": "//button[@id='save']",
    "by_type": "xpath"
}"""
    result = fallback._parse_llm_locator(raw)
    assert result == ("//button[@id='save']", "xpath")


# 2. Strict Unique Locator Validation (Exactly 1 match)
def test_validation_exactly_one_match():
    driver = DummyDriver(
        elements_map={("xpath", "//button[@id='save']"): ["element_1"]}
    )
    fb = LocatorFallback(driver=driver)
    assert fb._validate_locator("//button[@id='save']", "xpath") is True


def test_validation_zero_matches():
    driver = DummyDriver(elements_map={("xpath", "//button[@id='save']"): []})
    fb = LocatorFallback(driver=driver)
    assert fb._validate_locator("//button[@id='save']", "xpath") is False


def test_validation_multiple_matches_rejected():
    driver = DummyDriver(
        elements_map={("xpath", "//button[@id='save']"): ["el1", "el2", "el3"]}
    )
    fb = LocatorFallback(driver=driver)
    # Strict uniqueness: multiple matches must be rejected
    assert fb._validate_locator("//button[@id='save']", "xpath") is False


# 3. Conservative Page Health Pre-Checks
def test_page_health_normal_page(fallback):
    assert fallback._check_page_health() is True


def test_page_health_error_titles():
    for err_title in [
        "404 Not Found",
        "502 Bad Gateway",
        "503 Service Unavailable",
        "Server Error",
    ]:
        driver = DummyDriver(title=err_title)
        fb = LocatorFallback(driver=driver)
        assert fb._check_page_health() is False


def test_page_health_invalid_urls():
    for err_url in ["about:blank", "about:neterror", "data:text/html,<div>Error</div>"]:
        driver = DummyDriver(current_url=err_url)
        fb = LocatorFallback(driver=driver)
        assert fb._check_page_health() is False


# 4. Fallback Execution Flow (Cache, Stage 1, Stage 2, Failure)
@patch.dict(ocsci_config.UI_SELENIUM, {"ai_fallback": True})
def test_fallback_cache_hit(monkeypatch):
    driver = DummyDriver(
        elements_map={("xpath", "//button[@id='cached-btn']"): ["el1"]}
    )
    fb = LocatorFallback(driver=driver)
    monkeypatch.setattr(
        fb,
        "_load_cache",
        lambda: {
            "old-btn|xpath": {
                "old_selector": "old-btn",
                "old_by_type": "xpath",
                "new_selector": "//button[@id='cached-btn']",
                "new_by_type": "xpath",
            }
        },
    )
    result = fb.attempt_fallback(("old-btn", "xpath"))
    assert result == ("//button[@id='cached-btn']", "xpath")


@patch.dict(ocsci_config.UI_SELENIUM, {"ai_fallback": True})
def test_fallback_stage1_success(monkeypatch):
    driver = DummyDriver(
        elements_map={("xpath", "//button[@id='stage1-btn']"): ["el1"]}
    )
    fb = LocatorFallback(driver=driver)
    monkeypatch.setattr(fb, "_load_cache", lambda: {})
    monkeypatch.setattr(fb, "_cache_result", lambda *args, **kwargs: None)
    mock_client = MagicMock()
    mock_client.is_available.return_value = True
    mock_client.total_cost_usd = 0.05
    mock_client.total_requests = 1
    mock_client.query_dom.return_value = (
        '{"selector": "//button[@id=\'stage1-btn\']", "by_type": "xpath"}'
    )
    fb._client = mock_client

    result = fb.attempt_fallback(("broken-btn", "xpath"))
    assert result == ("//button[@id='stage1-btn']", "xpath")


@patch.dict(ocsci_config.UI_SELENIUM, {"ai_fallback": True})
def test_fallback_stage1_fail_stage2_success(monkeypatch):
    driver = DummyDriver(
        elements_map={("xpath", "//button[@id='stage2-btn']"): ["el1"]}
    )
    fb = LocatorFallback(driver=driver)
    monkeypatch.setattr(fb, "_load_cache", lambda: {})
    monkeypatch.setattr(fb, "_cache_result", lambda *args, **kwargs: None)
    monkeypatch.setattr(fb, "_capture_screenshot", lambda: "/tmp/mock_screenshot.png")
    mock_client = MagicMock()
    mock_client.is_available.return_value = True
    mock_client.total_cost_usd = 0.10
    mock_client.total_requests = 2
    # Stage 1 returns invalid locator (not matching driver's elements)
    mock_client.query_dom.return_value = (
        '{"selector": "//button[@id=\'nonexistent\']", "by_type": "xpath"}'
    )
    # Stage 2 returns valid locator
    mock_client.query_screenshot.return_value = (
        '{"selector": "//button[@id=\'stage2-btn\']", "by_type": "xpath"}'
    )
    fb._client = mock_client

    result = fb.attempt_fallback(("broken-btn", "xpath"))
    assert result == ("//button[@id='stage2-btn']", "xpath")


@patch.dict(ocsci_config.UI_SELENIUM, {"ai_fallback": True})
def test_fallback_both_stages_fail(monkeypatch):
    driver = DummyDriver(elements_map={})
    fb = LocatorFallback(driver=driver)
    monkeypatch.setattr(fb, "_load_cache", lambda: {})
    monkeypatch.setattr(fb, "_capture_screenshot", lambda: "/tmp/mock_screenshot.png")
    mock_client = MagicMock()
    mock_client.is_available.return_value = True
    mock_client.total_cost_usd = 0.10
    mock_client.total_requests = 2
    mock_client.query_dom.return_value = (
        '{"selector": "//button[@id=\'none1\']", "by_type": "xpath"}'
    )
    mock_client.query_screenshot.return_value = (
        '{"selector": "//button[@id=\'none2\']", "by_type": "xpath"}'
    )
    fb._client = mock_client

    result = fb.attempt_fallback(("broken-btn", "xpath"))
    assert result is None
