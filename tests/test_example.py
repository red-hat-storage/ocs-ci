import logging
import pytest

from ocsci import config

logger = logging.getLogger(__name__)


@pytest.mark.run_this
def test_not_run_me_fail_pass():
    logger.info("Hey from test which should pass")
    # This is just example, we will need to load config data and access them
    # directly, this is just example how we can access and share this config
    # in all the tests
    logger.info(f"Conf file is: {config.ocs_conf_file}")
    assert 1 == 1, "This will not reach this message"


# Not applying this marker if you call py.test -m run_this this test won't be
# executed
# @pytest.mark.run_this
def test_not_run_me_fail():
    logger.info("Hey from test which should pass")
    assert 1 == 0, "This will fail and print this message cause of 1 != 0"
