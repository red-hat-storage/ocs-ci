import pytest
import logging
from ocs_ci.helpers.odf_cli import odf_cli_setup_helper

log = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def odf_cli_setup():
    try:
        odf_cli_runner = odf_cli_setup_helper()
    except RuntimeError as ex:
        pytest.fail(str(ex))

    return odf_cli_runner
