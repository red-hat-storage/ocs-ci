from os import environ
from ocs_ci.framework.pytest_customization.marks import (
    post_ocs_upgrade,
    post_upgrade,
    ocs_upgrade,
)


@post_upgrade
def test_1():
    """
    This test depends on test_e.py in folder 'e', test_z.py in folder 'z'
    to test our internal upgrade ordering marks.

    Each test within specific folder 'e' and 'z' update same environment
    variable 'RH' with one character in specific order to test the final
    string "Red Hat".

    Any order changes will cause the environment variable to differ.
    """

    assert environ["RH"] != "Red Hat"


@post_ocs_upgrade
def test_2():
    """
    update environment variable 'RH' to include character "T"
    final verification done in test_1.
    """

    environ["RH"] += "T"


@ocs_upgrade
def test_3():
    """
    update environment variable 'RH' to include character "a"
    final verification done in test_1.
    """

    environ["RH"] += "a"
