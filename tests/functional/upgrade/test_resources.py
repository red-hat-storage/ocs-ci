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
    red_squad,
    brown_squad,
    mcg,
    purple_squad,
    skipif_external_mode,
    skipif_hci_client,
    skipif_mcg_only,
    jira,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs import ocp
from ocs_ci.ocs.resources.pod import (
    wait_for_storage_pods,
    get_osd_pods,
    get_mon_pods,
    get_mgr_pods,
    get_all_pods,
)
from ocs_ci.helpers import helpers
from ocs_ci.utility import nfs_utils

log = logging.getLogger(__name__)


@skipif_aws_creds_are_missing
@post_upgrade
@mcg
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
def deprecated_test_start_pre_upgrade_pod_io(
    pause_cluster_load, pre_upgrade_pods_running_io
):
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
@pytest.mark.polarion_id("OCS-2666")
@mcg
@red_squad
def deprecated_test_noobaa_service_mon_after_ocs_upgrade():
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


@post_upgrade
@skipif_external_mode
@skipif_hci_client
@skipif_mcg_only
@jira("DFBUGS-5211")
@jira("DFBUGS-7007")
@pytest.mark.polarion_id("OCS-7419")
@purple_squad
def test_blackbox_pod_after_upgrade():
    """
    Check blackbox exporter pod exists after upgrade

    """
    ocs_version = version.get_ocs_version_from_csv(only_major_minor=True)
    if ocs_version <= version.VERSION_4_20:
        pytest.skip("The test is not supported on odf version less than 4.21")
    else:
        odf_semantic_version = version.get_semantic_running_odf_version()
        if odf_semantic_version >= version.get_semantic_version("4.21.7-1"):
            blackbox_label = constants.BLACKBOX_POD_LABEL_422_AND_ABOVE
            expected_label_key = "app"
        else:
            blackbox_label = constants.BLACKBOX_POD_LABEL
            expected_label_key = "app.kubernetes.io/name"
        ocp_obj = ocp.OCP(
            kind=constants.POD,
            namespace=config.ENV_DATA["cluster_namespace"],
            selector=blackbox_label,
        )
        Pods = ocp_obj.get()
        pods = Pods.get("items", [])
        assert pods, "No pods found"

        for pod in pods:
            pod_name = pod["metadata"]["name"]
            labels = pod["metadata"].get("labels", {})
            assert (
                labels.get(expected_label_key) == "odf-blackbox-exporter"
            ), f"Unexpected pod label on {pod_name}"

        log.info("Blackbox exporter pod exists after upgrade")


@post_upgrade
@skipif_external_mode
@skipif_hci_client
@skipif_mcg_only
@brown_squad
def test_nfs_driver_pods_not_deployed_by_default_after_upgrade():
    """
    Verify NFS driver pods (csi-nfsplugin / ctrlplugin / nodeplugin) are not
    deployed by default after upgrading ODF from 4.21 to 4.22.

    Starting with ODF 4.22, NFS CSI driver pods should only be present when
    NFS is explicitly enabled on the StorageCluster. This test confirms that
    a vanilla upgrade from 4.21 does not leave stale NFS driver pods running.

    Steps:
    1. Skip if the post-upgrade ODF version is less than 4.22
    2. Fetch the NFS driver pod selectors for the current version
    3. Assert that no pods matching any NFS selector exist in the
       openshift-storage namespace

    """
    ocs_version = version.get_ocs_version_from_csv(only_major_minor=True)
    if ocs_version < version.VERSION_4_22:
        pytest.skip("Test only applies to ODF 4.22 and above (upgrade from 4.21)")

    nfs_selectors = nfs_utils.provisioner_selectors(nfs_plugins=True)
    namespace = config.ENV_DATA["cluster_namespace"]

    for selector in nfs_selectors:
        pods = get_all_pods(namespace=namespace, selector=[selector.split("=")[1]])
        assert not pods, (
            f"NFS driver pods found with selector '{selector}' after upgrade "
            "to ODF 4.22. NFS driver pods should not be deployed by default "
            "unless NFS is explicitly enabled on the StorageCluster."
        )
        log.info("No NFS driver pods found for selector '%s' — as expected", selector)
