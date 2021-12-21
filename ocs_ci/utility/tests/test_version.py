from semantic_version import Version

import pytest

from ocs_ci.ocs.exceptions import WrongVersionExpression
from ocs_ci.utility import version


@pytest.mark.parametrize(
    "product_version, only_major_minor, ignore_pre_release, expected",
    [
        ("4.4", False, False, Version.coerce("4.4")),
        ("4.4.5", True, False, version.VERSION_4_4),
        ("4.4.5", True, True, version.VERSION_4_4),
        ("4.4.5-182.ci", True, False, version.VERSION_4_4),
        ("4.4.5-182.ci", False, False, Version.coerce("4.4.5-182.ci")),
    ],
)
def test_get_semantic_version(
    product_version, only_major_minor, ignore_pre_release, expected
):
    """
    This test is suppose to test if the get_semantic_version returns
    expected values for different combinations of paramters and different values
    of string version provided to that function.
    """

    tested_version = version.get_semantic_version(
        product_version, only_major_minor, ignore_pre_release
    )
    assert tested_version == expected


@pytest.mark.parametrize(
    "product_version, only_major_minor, ignore_pre_release, expected",
    [
        ("4.4", False, False, "4.4.0"),
        ("4.4.5", True, False, "4.4"),
        ("4.4.5", True, True, "4.4"),
        ("4.4.5-182.ci", True, False, "4.4"),
        ("4.4.5-182.ci", False, False, "4.4.5-182.ci"),
        ("4.4.5-182.ci", False, True, "4.4.5"),
    ],
)
def test_get_semantic_version_string_values(
    product_version, only_major_minor, ignore_pre_release, expected
):
    """
    This test is suppose to test if the get_semantic_version returns
    expected values which after the sting formatting are the same as the expected.
    Testing all different combinations of parameters and values.
    """
    tested_version = version.get_semantic_version(
        product_version, only_major_minor, ignore_pre_release
    )
    assert f"{tested_version}" == expected


def test_compare_from_get_semantic_version():
    """
    This teest is testing that semantic version comparison works as expected and
    version 4.5.11 is lower than 4.11. Which in the float will not work, but in semantic
    versions it should be fine.
    """
    tested_version = version.get_semantic_version("4.5.11", only_major_minor=True)
    assert tested_version < Version.coerce("4.11")


@pytest.mark.parametrize(
    "expression, expected",
    [
        ("4.1<4.1", False),
        ("4.1>4.1", False),
        ("4.1<=4.1", True),
        ("4.1>=4.1", True),
        ("4.1==4.1", True),
        ("4.1!=4.1", False),
        ("4.1<4.2", True),
        ("4.1>4.2", False),
        ("4.1<=4.2", True),
        ("4.1>=4.2", False),
        ("4.1==4.2", False),
        ("4.1!=4.2", True),
        ("4.2<4.1", False),
        ("4.2>4.1", True),
        ("4.2<=4.1", False),
        ("4.2>=4.1", True),
        ("4.2==4.1", False),
        ("4.2!=4.1", True),
        ("4.10<4.1", False),
        ("4.10>4.1", True),
        ("4.10<=4.1", False),
        ("4.10>=4.1", True),
        ("4.10==4.1", False),
        ("4.10!=4.1", True),
        ("4.1<4.10", True),
        ("4.1>4.10", False),
        ("4.1<=4.10", True),
        ("4.1>=4.10", False),
        ("4.1==4.10", False),
        ("4.1!=4.10", True),
        ("4.10<4.5", False),
        ("4.10>4.5", True),
        ("4.10<=4.5", False),
        ("4.10>=4.5", True),
        ("4.10==4.5", False),
        ("4.10!=4.5", True),
        ("4.5<4.10", True),
        ("4.5>4.10", False),
        ("4.5<=4.10", True),
        ("4.5>=4.10", False),
        ("4.5==4.10", False),
        ("4.5!=4.10", True),
        ("4.11<4.5", False),
        ("4.11>4.5", True),
        ("4.11<=4.5", False),
        ("4.11>=4.5", True),
        ("4.11==4.5", False),
        ("4.11!=4.5", True),
        ("4.5<4.11", True),
        ("4.5>4.11", False),
        ("4.5<=4.11", True),
        ("4.5>=4.11", False),
        ("4.5==4.11", False),
        ("4.5!=4.11", True),
    ],
)
def test_compare_versions(expression, expected):
    """
    This test is suppose to test if the compare_versions returns
    expected values for different expressions.
    """

    assert version.compare_versions(expression) == expected


@pytest.mark.parametrize(
    "expression",
    [
        "4.1??4.1",
        "4.1 ===4.1",
        "Foo version",
        "<4.1",
        "==4.1",
    ],
)
def test_compare_versions_wrong_expression(expression):
    """
    This test is suppose to test if the compare_versions raises
    expected exception for wron expression.
    """

    with pytest.raises(WrongVersionExpression):
        version.compare_versions(expression)
