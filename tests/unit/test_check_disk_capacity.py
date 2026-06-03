"""
Unit tests for the disk capacity check logic used by
DeploymentUI._check_disk_capacity.

Run directly: python tests/unit/test_check_disk_capacity.py
Or via pytest from outside ocs-ci:
    python -m pytest tests/unit/test_check_disk_capacity.py -v -c /dev/null -p no:ocscilib
"""

from unittest.mock import MagicMock


def make_element(text):
    el = MagicMock()
    el.text = text
    return el


def check_disk_capacity(driver, min_capacity_gib):
    """
    Mirror of DeploymentUI._check_disk_capacity core logic.
    Must stay in sync with deployment_ui.py.
    """
    page_text = driver.find_elements(
        "xpath", "//*[contains(text(), 'GiB') or contains(text(), 'TiB')]"
    )
    for el in page_text:
        try:
            text = el.text.strip()
            parts = text.split()
            value = float(parts[0])
            unit = parts[1]
            capacity_gib = value * 1024 if unit == "TiB" else value
            if capacity_gib >= min_capacity_gib:
                return True
        except (ValueError, IndexError):
            continue
    return False


def _drv(*texts):
    d = MagicMock()
    d.find_elements.return_value = [make_element(t) for t in texts]
    return d


def test_exact_match_gib():
    assert check_disk_capacity(_drv("600 GiB"), 600) is True


def test_higher_capacity_passes():
    assert check_disk_capacity(_drv("900 GiB"), 600) is True


def test_lower_capacity_fails():
    assert check_disk_capacity(_drv("300 GiB"), 600) is False


def test_tib_conversion():
    d = _drv("1.5 TiB")
    assert check_disk_capacity(d, 1024) is True
    assert check_disk_capacity(d, 1536) is True
    assert check_disk_capacity(d, 1537) is False


def test_tib_small():
    d = _drv("0.5 TiB")
    assert check_disk_capacity(d, 512) is True
    assert check_disk_capacity(d, 600) is False


def test_no_elements():
    assert check_disk_capacity(_drv(), 100) is False


def test_multiple_elements():
    assert check_disk_capacity(_drv("100 GiB", "900 GiB"), 600) is True


def test_garbage_skipped():
    assert check_disk_capacity(_drv("random GiB text", "600 GiB"), 600) is True


def test_only_garbage():
    assert check_disk_capacity(_drv("no number GiB", "bad TiB"), 100) is False


def test_empty_text():
    assert check_disk_capacity(_drv(""), 100) is False


def test_real_world_900():
    """3 workers x 200GiB + 3 masters x 100GiB = 900, min 600"""
    assert check_disk_capacity(_drv("900 GiB"), 600) is True


def test_real_world_tib():
    """6 x 512GiB = 3 TiB"""
    d = _drv("3 TiB")
    assert check_disk_capacity(d, 3072) is True
    assert check_disk_capacity(d, 3073) is False


if __name__ == "__main__":
    tests = [v for k, v in globals().items() if k.startswith("test_")]
    for t in tests:
        t()
        print(f"  PASS: {t.__name__}")
    print(f"\nAll {len(tests)} tests passed.")
