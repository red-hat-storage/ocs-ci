from os import environ
from ocs_ci.framework.pytest_customization.marks import (
    post_ocp_upgrade, pre_ocs_upgrade, ocp_upgrade
)


@pre_ocs_upgrade
def test_4():
    """
    update environment variable 'RH' to include character "H"
    final verification done in test_1 in upper level folder 'a'.
    """

    environ['RH'] += "H"


@post_ocp_upgrade
def test_5():
    """
    update environment variable 'RH' to include character " "
    final verification done in test_1 in upper level folder 'a'.
    """

    environ['RH'] += " "


@ocp_upgrade
def test_6():
    """
    update environment variable 'RH' to include character "d"
    final verification done in test_1 in upper level folder 'a'.
    """

    environ['RH'] += "d"
