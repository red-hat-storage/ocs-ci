import pytest
import logging
from ocs_ci.helpers.odf_cli import ODFCLIRetriever, ODFCliRunner

log = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def odf_cli_setup():
    odf_cli_retriever = ODFCLIRetriever()

    # Check and download ODF CLI binary if needed
    try:
        assert odf_cli_retriever.check_odf_cli_binary()
    except AssertionError:
        log.warning("ODF CLI binary not found. Attempting to download...")
        odf_cli_retriever.retrieve_odf_cli_binary()
        if not odf_cli_retriever.check_odf_cli_binary():
            pytest.fail("Failed to download ODF CLI binary")

    # Check and initialize ODFCliRunner if needed
    try:
        odf_cli_runner = ODFCliRunner()
        assert odf_cli_runner
    except AssertionError:
        log.warning("ODFCliRunner not initialized. Attempting to initialize...")
        odf_cli_runner = ODFCliRunner()
        if not odf_cli_runner:
            pytest.fail("Failed to initialize ODFCliRunner")

    log.info("ODF CLI binary downloaded and ODFCliRunner initialized successfully")
    return odf_cli_runner
