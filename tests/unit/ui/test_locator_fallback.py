"""
Unit tests for AI Locator Fallback mechanism.
"""

import pytest

from ocs_ci.ocs.ui.llm_tools.locator_fallback import LocatorFallback


class DummyDriver:
    pass


@pytest.fixture
def fallback():
    return LocatorFallback(driver=DummyDriver())


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


def test_parse_json_with_index(fallback):
    raw = """{
    "selector": "tr.pf-v5-c-table__tr",
    "by_type": "css",
    "index": 2
}"""
    result = fallback._parse_llm_locator(raw)
    assert result == ("tr.pf-v5-c-table__tr", "css", 2)


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


def test_clean_soup_dom_removes_scripts_and_preserves_attributes(fallback):
    html = """<html><body>
    <script>alert(1);</script>
    <style>.btn { color: red; }</style>
    <!-- comment -->
    <button id="submit-btn" class="pf-v5-c-button" data-test="save" onclick="doSomething()" arbitrary="remove-me">
        Save
    </button>
    </body></html>"""
    cleaned = fallback._strip_dom(html)
    assert "alert" not in cleaned
    assert ".btn {" not in cleaned
    soup_cleaned = fallback._extract_intelligent_dom(html)
    assert "submit-btn" in soup_cleaned
    assert 'data-test="save"' in soup_cleaned
    assert "arbitrary" not in soup_cleaned
    assert "alert" not in soup_cleaned


def test_modal_prioritization(fallback):
    html = """<html><body>
    <div id="main-content">
        <button id="ignored-bg-btn">Background</button>
    </div>
    <div role="dialog" aria-modal="true" class="pf-v5-c-modal-box">
        <h2>Confirm Delete</h2>
        <button id="modal-confirm-btn" data-test="confirm-action">Confirm</button>
    </div>
    </body></html>"""
    extracted = fallback._extract_intelligent_dom(html)
    assert "modal-confirm-btn" in extracted
    assert 'data-test="confirm-action"' in extracted
    # The modal context should be prioritized over the background
    assert "ignored-bg-btn" not in extracted


def test_menu_drawer_prioritization(fallback):
    html = """<html><body>
    <div id="main-content">
        <button id="page-btn">Page</button>
    </div>
    <div role="menu" class="pf-v5-c-menu">
        <ul class="pf-v5-c-menu__list">
            <li role="menuitem"><button id="dropdown-item-edit">Edit</button></li>
            <li role="menuitem"><button id="dropdown-item-delete">Delete</button></li>
        </ul>
    </div>
    </body></html>"""
    extracted = fallback._extract_intelligent_dom(html)
    assert "dropdown-item-edit" in extracted
    assert "dropdown-item-delete" in extracted
    assert "page-btn" not in extracted


def test_page_health_precheck_unhealthy(fallback):
    class BadDriver:
        title = "502 Bad Gateway"
        current_url = "https://console.example.com"

        def execute_script(self, script):
            return "complete"

    unhealthy_fallback = LocatorFallback(driver=BadDriver())
    assert unhealthy_fallback._check_page_health() is False

    class BlankDriver:
        title = "OpenShift"
        current_url = "about:blank"

        def execute_script(self, script):
            return "complete"

    blank_fallback = LocatorFallback(driver=BlankDriver())
    assert blank_fallback._check_page_health() is False


def test_locator_validation_with_index(fallback):
    class MultiElementDriver:
        def find_elements(self, by, value):
            return ["el0", "el1", "el2"]

    val_fallback = LocatorFallback(driver=MultiElementDriver())
    # Without index: 3 elements is not == 1, so returns False
    assert val_fallback._validate_locator(".kebab", "css") is False
    # With index within range: returns True
    assert val_fallback._validate_locator(".kebab", "css", index=1) is True
    # With index out of range: returns False
    assert val_fallback._validate_locator(".kebab", "css", index=5) is False
