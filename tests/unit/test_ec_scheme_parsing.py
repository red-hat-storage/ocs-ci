"""
Unit tests for EC scheme text parsing in _verify_ec_effective_capacity.

Tests the regex extraction of k+m from scheme cell text that may
include badges like "Recommended".

Run: python tests/unit/test_ec_scheme_parsing.py
"""

import re

from ocs_ci.framework.pytest_customization.marks import purple_squad

pytestmark = purple_squad


def parse_scheme(raw_text):
    """
    Mirror of the scheme parsing logic from
    DeploymentUI._verify_ec_effective_capacity.
    """
    match = re.match(r"(\d+\+\d+)", raw_text.strip())
    if not match:
        return None
    scheme_text = match.group(1)
    ki, mi = (int(x) for x in scheme_text.split("+"))
    return scheme_text, ki, mi


def test_clean_scheme():
    assert parse_scheme("2+2") == ("2+2", 2, 2)
    assert parse_scheme("4+2") == ("4+2", 4, 2)


def test_scheme_with_recommended_no_space():
    """The actual bug: '2+2Recommended' from the DOM"""
    assert parse_scheme("2+2Recommended") == ("2+2", 2, 2)


def test_scheme_with_recommended_space():
    assert parse_scheme("2+2 Recommended") == ("2+2", 2, 2)


def test_scheme_with_newline_badge():
    assert parse_scheme("4+2\nRecommended") == ("4+2", 4, 2)


def test_scheme_with_leading_whitespace():
    assert parse_scheme("  2+1") == ("2+1", 2, 1)


def test_large_scheme():
    assert parse_scheme("8+3") == ("8+3", 8, 3)


def test_garbage_returns_none():
    assert parse_scheme("Recommended") is None
    assert parse_scheme("no scheme here") is None
    assert parse_scheme("") is None


def test_scheme_with_trailing_text():
    assert parse_scheme("4+2 (default)") == ("4+2", 4, 2)


def test_capacity_parsing():
    """Test the capacity text parsing that follows scheme parsing"""
    for text, expected in [
        ("7.85 TiB", 7.85),
        ("10.46 TiB", 10.46),
        ("900 GiB", 900.0),
        ("0.5 TiB", 0.5),
    ]:
        value = float(text.split()[0])
        assert value == expected, f"Failed for '{text}': got {value}"


def test_cross_validation_math():
    """Verify the cross-validation formula: total_raw = cap * (k+m) / k"""
    # 2+2 at 7.85 TiB -> total_raw = 7.85 * 4/2 = 15.7 TiB
    ref_cap, ref_k, ref_m = 7.85, 2, 2
    total_raw = ref_cap * (ref_k + ref_m) / ref_k

    # 4+2 should be total_raw * 4/6 = 15.7 * 4/6 = 10.4667 -> 10.47
    expected_4_2 = round(total_raw * 4 / (4 + 2), 2)
    assert expected_4_2 == 10.47

    # 2+1 should be total_raw * 2/3 = 15.7 * 2/3 = 10.4667 -> 10.47
    expected_2_1 = round(total_raw * 2 / (2 + 1), 2)
    assert expected_2_1 == 10.47


if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for t in tests:
        t()
        print(f"  PASS: {t.__name__}")
    print(f"\nAll {len(tests)} tests passed.")
