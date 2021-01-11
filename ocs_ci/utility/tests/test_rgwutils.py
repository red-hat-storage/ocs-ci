import pytest

from ocs_ci.utility import rgwutils


@pytest.mark.parametrize(
    "ocs_version, is_upgrade, version_before_upgrade, expected",
    [
        ("4.4", False, None, 1),
        ("4.4", True, "4.3", 1),
        ("4.5", False, None, 2),
        ("4.5", True, "4.4", 1),
        ("4.6", False, None, 2),
        ("4.6", True, "4.5", 2),
        ("4.7", False, None, 1),
        ("4.7", True, "4.6", 2),
        ("4.8", False, None, 1),
        ("4.8", True, "4.7", 1),
    ],
)
def test_get_rgw_count(ocs_version, is_upgrade, version_before_upgrade, expected):
    rgw_count = rgwutils.get_rgw_count(ocs_version, is_upgrade, version_before_upgrade)
    assert rgw_count == expected
