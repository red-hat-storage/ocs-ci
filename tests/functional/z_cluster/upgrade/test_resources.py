import logging
import pytest

from ocs_ci.utility import version
from ocs_ci.ocs.resources.pod import get_pod_logs
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    ignore_leftovers,
    pre_upgrade,
    post_upgrade,
    skipif_aws_creds_are_missing,
    bugzilla,
    red_squad,
    brown_squad,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs import ocp
from ocs_ci.ocs.resources.pod import (
    wait_for_storage_pods,
    get_osd_pods,
    get_mon_pods,
    get_mgr_pods,
)
from ocs_ci.helpers import helpers

log = logging.getLogger(__name__)


@skipif_aws_creds_are_missing
@post_upgrade
@brown_squad
@pytest.mark.polarion_id("OCS-2220")
def test_storage_pods_running(multiregion_mirror_setup_session):
    """
    Test that all pods from openshift-storage namespace have status Running
    or Completed after upgrade is completed.

    multiregion_mirror_setup_session fixture is present during this test to
    make sure that NooBaa backing stores from other upgrade tests were
    not yet deleted. This is done to test scenario from BZ 1823775.

    """
    wait_for_storage_pods(timeout=10), "Some pods were not in expected state"


@pytest.mark.skipif(
    True, reason="This IO test case is problematic, and is disabled. See issue: #6108"
)
@pre_upgrade
@brown_squad
@ignore_leftovers
def test_start_pre_upgrade_pod_io(pause_cluster_load, pre_upgrade_pods_running_io):
    """
    Confirm that there are pods created before upgrade.
    """
    for pod in pre_upgrade_pods_running_io:
        log.info("Waiting for all fio pods to come up")
        helpers.wait_for_resource_state(pod, constants.STATUS_RUNNING, timeout=600)


@pytest.mark.skipif(
    True, reason="This IO test case is problematic, and is disabled. See issue: #6108"
)
@post_upgrade
@brown_squad
@pytest.mark.polarion_id("OCS-1862")
def test_pod_io(
    pre_upgrade_filesystem_pods,
    post_upgrade_filesystem_pods,
    pre_upgrade_block_pods,
    post_upgrade_block_pods,
    fio_project,
    resume_cluster_load,
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
        f"Pods using block device created before upgrade: " f"{pre_upgrade_block_pods}"
    )
    log.info(
        f"Pods using block device created after upgrade: " f"{post_upgrade_block_pods}"
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
        helpers.wait_for_resource_state(pod, constants.STATUS_RUNNING, timeout=600)
        job_name = pod.get_labels().get("job-name")
        job_obj.delete(resource_name=job_name)


@post_upgrade
@bugzilla("1974343")
@pytest.mark.polarion_id("OCS-2629")
@brown_squad
def test_pod_log_after_upgrade():
    """
    Check OSD/MON/MGR pod logs after upgrade and verify the expected log exist

    """
    pod_objs = get_osd_pods() + get_mon_pods() + get_mgr_pods()
    pod_names = [osd_pod_obj.name for osd_pod_obj in pod_objs]
    expected_log_after_upgrade = "set uid:gid to 167:167 (ceph:ceph)"
    log.info(
        f"Check that the log '{expected_log_after_upgrade}' "
        f"appears after the osd/mon/mg pod is initialized"
    )
    for pod_name in pod_names:
        pod_logs = get_pod_logs(pod_name=pod_name, all_containers=True)
        assert expected_log_after_upgrade in pod_logs, (
            f"The expected log after upgrade '{expected_log_after_upgrade}' does not exist"
            f" on pod {pod_name}"
        )
    log.info(f"The log '{expected_log_after_upgrade}' appears in all relevant pods.")


@post_upgrade
@bugzilla("1973179")
@pytest.mark.polarion_id("OCS-2666")
@red_squad
def test_noobaa_service_mon_after_ocs_upgrade():
    """
    Verify 'noobaa-service-monitor' does not exist after OCS upgrade.

    Test Procedure:
    1.Upgrade OCS version
    2.Check servicemonitors
    3.Verify 'noobaa-service-monitor' does not exist

    """
    ocs_version = version.get_ocs_version_from_csv(
        only_major_minor=False, ignore_pre_release=True
    )
    if ocs_version <= version.get_semantic_version("4.7.4"):
        pytest.skip("The test is not supported on version less than 4.7.4")
    ocp_obj = ocp.OCP(
        kind=constants.SERVICE_MONITORS, namespace=config.ENV_DATA["cluster_namespace"]
    )
    servicemon = ocp_obj.get()
    servicemonitors = servicemon["items"]
    for servicemonitor in servicemonitors:
        assert (
            servicemonitor["metadata"]["name"] != "noobaa-service-monitor"
        ), "noobaa-service-monitor exist"
    log.info("noobaa-service-monitor does not exist")
