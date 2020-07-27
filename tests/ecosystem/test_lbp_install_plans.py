import logging
import time

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.testlib import tier1, ecosystem
from ocs_ci.ocs.resources.install_plan import get_install_plans_count


logger = logging.getLogger(__name__)


@pytest.mark.last
@tier1
@ecosystem
def test_lbp_install_plans_increase():
    """
    Test if we got increase in install plans
    """
    wait_time = 30
    timeout = 600
    namespace = config.ENV_DATA["cluster_namespace"]
    initial_ip_count = get_install_plans_count(namespace)
    logger.info(f"Initial number of install plans is: {initial_ip_count}")
    while timeout > 0:
        timeout -= wait_time
        logger.info(
            f"Waiting {wait_time} seconds for next check of LBP install"
            f" plans count. Remaining timeout is: {timeout} seconds."
        )
        time.sleep(wait_time)
        ip_count = get_install_plans_count(namespace)
        logger.info(f"Number of install plans is: {ip_count}")
        assert ip_count <= initial_ip_count, "Install plans increased!"
