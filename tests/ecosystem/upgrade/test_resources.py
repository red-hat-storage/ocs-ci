from concurrent.futures import ThreadPoolExecutor
import logging

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    ignore_leftovers, pre_upgrade, post_upgrade
)
from ocs_ci.ocs import constants
from tests import helpers

log = logging.getLogger(__name__)


@pre_upgrade
@ignore_leftovers
def test_start_pre_upgrade_pod_io(pre_upgrade_pods_running_io):
    """
    Confirm that there are pods created before upgrade.
    """
    assert pre_upgrade_pods_running_io


@post_upgrade
@pytest.mark.polarion_id("OCS-1862")
def test_pod_io(
    pre_upgrade_filesystem_pods,
    post_upgrade_filesystem_pods,
    pre_upgrade_block_pods,
    post_upgrade_block_pods,
    fio_project
):
    """
    Test IO on multiple pods at the same time and finish IO on pods that were
    created before upgrade.
    """
    log.info(
        f"Pods using filesystem created before upgrade: "
        f"{pre_upgrade_filesystem_pods}"
    )
    log.info(
        f"Pods using filesystem created after upgrade: "
        f"{post_upgrade_filesystem_pods}"
    )
    log.info(
        f"Pods using block device created before upgrade: "
        f"{pre_upgrade_block_pods}"
    )
    log.info(
        f"Pods using block device created after upgrade: "
        f"{post_upgrade_block_pods}"
    )

    for pod in pods:
        log.info(f"Checking that fio is still running")
        helpers.wait_for_resource_state(pod, constants.STATUS_RUNNING, timeout=180)

    fio_project.delete()
