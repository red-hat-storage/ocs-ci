import logging

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    ignore_leftovers, pre_upgrade, post_upgrade
)
from ocs_ci.ocs import constants
from ocs_ci.ocs import ocp
from tests import helpers

log = logging.getLogger(__name__)


@pre_upgrade
@ignore_leftovers
def test_start_pre_upgrade_pod_io(pre_upgrade_pods_running_io):
    """
    Confirm that there are pods created before upgrade.
    """
    for pod in pre_upgrade_pods_running_io:
        log.info("Waiting for all fio pods to come up")
        helpers.wait_for_resource_state(
            pod,
            constants.STATUS_RUNNING,
            timeout=600
        )


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
    pods = (
        pre_upgrade_block_pods
        + post_upgrade_block_pods
        + pre_upgrade_filesystem_pods
        + post_upgrade_filesystem_pods
    )
    job_obj = ocp.OCP(kind=constants.JOB, namespace=fio_project.namespace)
    for pod in pods:
        log.info(f"Checking that fio is still running")
        helpers.wait_for_resource_state(
            pod,
            constants.STATUS_RUNNING,
            timeout=600
        )
        job_name = pod.get_labels().get('job-name')
        job_obj.delete(resource_name=job_name)
