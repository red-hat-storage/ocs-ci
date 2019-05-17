"""
This plugin allows you to setup all basic configuratio for pytest we need
in our OCS-CI.
"""
import pytest

from ocsci import config as ocsci_conf

__all__ = [
    "pytest_addoption",
]


def pytest_addoption(parser):
    """
    Add necessary options to initialize ART library.
    """
    parser.addoption(
        '--ocs-conf',
        dest='ocs_conf',
        help="Path to config file of OCS_CI",
    )


# To make sure that we call all hooks after all plugins are loaded we run this
# as the last one.
@pytest.mark.trylast
def pytest_configure(config):
    """
    Load config files, and initialize ocs-ci library.
    """
    # TODO:
    # Load ocs_conf data and do the rest configuration here.
    ocs_conf = config.getoption('ocs_conf')
    if not ocs_conf:
        return
    ocsci_conf.ocs_conf_file = ocs_conf
