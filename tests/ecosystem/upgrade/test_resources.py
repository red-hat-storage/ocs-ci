import logging

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    ignore_leftovers, pre_upgrade, post_upgrade,
    skipif_aws_creds_are_missing
)
from ocs_ci.ocs import constants
from ocs_ci.ocs import ocp
from ocs_ci.ocs.resources.pod import wait_for_storage_pods
from ocs_ci.helpers import helpers

log = logging.getLogger(__name__)


@skipif_aws_creds_are_missing
@post_upgrade
@pytest.mark.polarion_id("OCS-2220")
def test_storage_pods_running(multiregion_mirror_setup_session):
    """
    Test that all pods from openshift-storage namespace have status Running
    or Completed after upgrade is completed.

    multiregion_mirror_setup_session fixture is present during this test to
    make sure that NooBaa backing stores from other upgrade tests were
    not yet deleted. This is done to test scenario from BZ 1823775.

    """
    wait_for_storage_pods(timeout=10), 'Some pods were not in expected state'


@pytest.mark.skipif(
    config.RUN.get('io_in_bg'), reason="IO is running by --io-in-bg param"
)
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


@pytest.mark.skipif(
    config.RUN.get('io_in_bg'), reason="IO is running by --io-in-bg param"
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
        log.info("Checking that fio is still running")
        helpers.wait_for_resource_state(
            pod,
            constants.STATUS_RUNNING,
            timeout=600
        )
        job_name = pod.get_labels().get('job-name')
        job_obj.delete(resource_name=job_name)
