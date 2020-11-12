from os import environ
from ocs_ci.framework.pytest_customization.marks import (
    pre_ocp_upgrade, pre_upgrade)


@pre_ocp_upgrade
def test_7():
    """
    update environment variable 'RH' to include character "e"
    final verification done in test_1 in upper level folder 'a'.
    """

    environ['RH'] += "e"


@pre_upgrade
def test_8():
    """
    update environment variable 'RH' to include character "R"
    final verification done in test_1 in upper level folder 'a'.
    """

    environ['RH'] = "R"
