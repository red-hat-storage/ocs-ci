# -*- coding: utf8 -*-
"""
Test cases test_workload_with_checksum and test_workload_with_checksum_verify
were originaly proposed for testing metrics and (partial) cluster shutdown, but
they can be used for any other test which needs to verify that cluster still
preserves data written before some cluster wide distruption like shutdown,
reboot or upgrade.

The basic idea is 1st test deploys a k8s job which writes data on OCS based PV
using fio along with a checksum file, and then the 2nd test verifies that the
data are still there on the PV. For this to work, the PV created during the 1st
test needs to be preserved.

The data are written on a PV via workload_storageutilization_checksum_rbd
fixture, used in test_workload_with_checksum. When the writing finishes, the
checksum is computed and stored along with the data, and finally reclaim policy
of the PV is changed to Retain, so that the PV survives teardwon of the test.
The PV is labeled as ``fixture=workload_storageutilization_checksum_rbd`` so
that the verification test can identify and reuse it.

After the first test finishes, the PV is in Released state and won't be reused,
because it contains claimRef referencing the original PVC.

The test_workload_with_checksum_verify locates the PV created by
test_workload_with_checksum via the label, and then removes it's claimRef so
that the PV changes it's state to Available. The test asks for the PV using PVC
with the same parameters, and executes the checksum verification.

The original idea was to use fio verification feature, but testing showed that
when this fails for some reason or gets stuck, it's hard to debug.
"""
import logging
import pytest
from ocs_ci.framework import config
from ocs_ci.framework.testlib import (
    blue_squad,
    tier2,
    pre_upgrade,
    post_upgrade,
    skipif_managed_service,
    skipif_mcg_only,
    skipif_hci_provider_and_client,
)
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs import fiojob
from ocs_ci.ocs.resources.objectconfigfile import ObjectConfFile

logger = logging.getLogger(__name__)


@blue_squad
@pre_upgrade
@tier2
@skipif_managed_service
@skipif_mcg_only
@skipif_hci_provider_and_client
def test_workload_with_checksum(workload_storageutilization_checksum_rbd):
    """
    Purpose of this test is to have checksum workload fixture executed.
    """
    logger.info("Starting test: Verify checksum workload execution")

    logger.test_step("Validate fio report is available from fixture")
    result_available = workload_storageutilization_checksum_rbd["result"] is not None
    logger.assertion(
        f"Fio report availability: expected=not None, actual={result_available}"
    )
    assert result_available, "fio report should be available"

    logger.test_step("Verify fio job execution details")
    fio = workload_storageutilization_checksum_rbd["result"]["fio"]
    job_count = len(fio["jobs"])
    logger.assertion(f"Fio job count: expected=1, actual={job_count}")
    assert job_count == 1, "single fio job was executed"

    logger.test_step("Verify fio completed without errors")
    error_count = fio["jobs"][0]["error"]
    logger.assertion(f"Fio error count: expected=0, actual={error_count}")
    assert error_count == 0, "no errors should be reported by fio when writing data"

    logger.info("Test passed: Checksum workload executed successfully")


@blue_squad
@post_upgrade
@tier2
@skipif_managed_service
@skipif_hci_provider_and_client
def test_workload_with_checksum_verify(
    tmp_path,
    project,
    fio_pvc_dict,
    fio_job_dict,
    fio_configmap_dict,
):
    """
    Verify that data written by fio during workload storageutilization fixture
    are still present on the persistent volume.

    This test case assumes that test case ``test_workload_with_checksum``
    (which uses the fixture) has been executed already, and that the PV it
    created is still around (the PV is identified via it's label, which
    references the fixture). There is no direct binding between these tests or
    fixtures, so that one can run ``test_workload_with_checksum`` first,
    then do some cluster wide temporary distruptive operation such as reboot,
    temporary shutdown or upgrade, and finally after that run this verification
    test to check that data are still there.

    Note/TODO: this test doesn't delete the PV created by the previous test
    on purpose, so that this test can be executed multiple times (which is
    important feature of this test, eg. it is possible to run it at different
    stages of the cluster wide distruptions). We may need to come up with a way
    to track it and delete it when it's no longer needed though.
    """
    logger.info("Starting test: Verify checksum data persists after disruption")

    fixture_name = "workload_storageutilization_checksum_rbd"
    storage_class_name = "ocs-storagecluster-ceph-rbd"
    pv_label = f"fixture={fixture_name}"
    logger.info(
        f"Looking for PV with label: {pv_label}, storage class: {storage_class_name}"
    )

    logger.test_step("Locate PV containing previously written fio data")
    ocp_pv = ocp.OCP(kind=constants.PV, namespace=project.namespace)
    logger.info("Searching for PV with label %s, where fio stored data", pv_label)
    pv_data = ocp_pv.get(selector=pv_label)

    logger.assertion(f"PV data kind: expected='List', actual={pv_data.get('kind')}")
    assert pv_data["kind"] == "List", "PV data should be a List"

    pv_count = len(pv_data["items"])
    logger.assertion(f"PV count with label {pv_label}: expected=1, actual={pv_count}")
    pv_exists_msg = (
        f"Single PV with label {pv_label} should exists, "
        "so that test can identify where to verify the data."
    )
    assert pv_count == 1, pv_exists_msg

    pv_dict = pv_data["items"][0]
    pv_name = pv_dict["metadata"]["name"]
    logger.info("PV %s was identified, test can continue.", pv_name)

    logger.test_step("Extract and validate PV capacity")
    capacity = pv_dict["spec"]["capacity"]["storage"]
    logger.info("Capacity of PV %s is %s.", pv_name, capacity)

    # Convert the storage capacity spec into number of GiB
    unit = capacity[-2:]
    unit_valid = unit in ("Gi", "Ti")
    logger.assertion(
        f"PV capacity unit: expected='Gi' or 'Ti', actual={unit}, valid={unit_valid}"
    )
    assert unit_valid, "PV size should be within reasonable range"

    if capacity.endswith("Gi"):
        pvc_size = int(capacity[0:-2])
    elif capacity.endswith("Ti"):
        pvc_size = int(capacity[0:-2]) * 2**10
    logger.info(f"Converted PV capacity to PVC size: {pvc_size} GiB")

    logger.test_step("Remove claimRef to make PV available for reuse")
    if "claimRef" in pv_dict["spec"]:
        logger.info("Dropping claimRef from PV %s.", pv_name)
        patch_success = ocp_pv.patch(
            resource_name=pv_name,
            params='[{ "op": "remove", "path": "/spec/claimRef" }]',
            format_type="json",
        )
        logger.assertion(f"ClaimRef removal: expected=True, actual={patch_success}")
        patch_error_msg = (
            "claimRef should be dropped with success, "
            f"otherwise the test can't continue to reuse PV {pv_name}"
        )
        assert patch_success, patch_error_msg
    else:
        logger.info("PV %s is already without claimRef.", pv_name)

    logger.test_step("Configure and create checksum verification job")
    # The job won't be running fio, it will run sha1sum check only.
    container = fio_job_dict["spec"]["template"]["spec"]["containers"][0]
    container["command"] = ["/usr/bin/sha1sum", "-c", "/mnt/target/fio.sha1sum"]
    logger.debug("Configured job to run sha1sum verification instead of fio")

    # we need to use the same PVC configuration to reuse the PV
    fio_pvc_dict["spec"]["storageClassName"] = storage_class_name
    fio_pvc_dict["spec"]["resources"]["requests"]["storage"] = capacity
    logger.debug(f"PVC configured: class={storage_class_name}, capacity={capacity}")

    # put the dicts together into yaml file of the Job
    fio_objs = [fio_pvc_dict, fio_configmap_dict, fio_job_dict]
    job_file = ObjectConfFile(fixture_name, fio_objs, project, tmp_path)

    # compute timeout based on the minimal write speed
    fio_min_mbps = config.ENV_DATA["fio_storageutilization_min_mbps"]
    job_timeout = fiojob.get_timeout(fio_min_mbps, pvc_size)
    # expand job timeout because during execution of this test is high
    # probability that there is more workload executed (from upgrade tests)
    # that slow down write time
    # TODO(fbalak): calculate this from actual work being executed
    job_timeout = job_timeout * 4
    logger.info(
        f"Job timeout calculated: {job_timeout}s (4x base timeout for upgrade workload)"
    )

    # deploy the Job to the cluster and start it
    logger.info("Creating checksum verification job")
    job_file.create()

    logger.test_step("Wait for checksum verification job to complete")
    # Wait for the job to verify data on the volume. If this fails in any way
    # the job won't finish with success in given time, and the error message
    # below will be reported via exception.
    error_msg = (
        "Checksum verification job failed. We weren't able to verify that "
        "data previously written on the PV are still there."
    )
    logger.info(f"Waiting for job completion (timeout={job_timeout}s)")
    pod_name = fiojob.wait_for_job_completion(project.namespace, job_timeout, error_msg)
    logger.info(f"Checksum verification job completed successfully: pod={pod_name}")

    logger.test_step("Retrieve and log verification results")
    ocp_pod = ocp.OCP(kind="Pod", namespace=project.namespace)
    sha1sum_output = ocp_pod.exec_oc_cmd(f"logs {pod_name}", out_yaml_format=False)
    logger.info("sha1sum output: %s", sha1sum_output)

    logger.info("Test passed: Data persisted successfully after disruption")


@blue_squad
@skipif_mcg_only
@tier2
@pytest.mark.polarion_id("OCS-2125")
@skipif_managed_service
def test_workload_rbd_cephfs_10g(
    workload_storageutilization_10g_rbd, workload_storageutilization_10g_cephfs
):
    """
    Test of a workload utilization with constant 10 GiB target.

    In this test we are only checking whether the storage utilization workload
    failed or not. The main point of having this included in tier1 suite is to
    see whether we are able to actually run the fio write workload without any
    direct failure (fio job could fail to be scheduled, fail during writing or
    timeout when write progress is too slow ...).
    """
    logger.info("Starting test: Verify 10 GiB workload on RBD and CephFS")

    logger.test_step("Validate workload fixture results are available")
    rbd_result_available = workload_storageutilization_10g_rbd["result"] is not None
    cephfs_result_available = (
        workload_storageutilization_10g_cephfs["result"] is not None
    )
    logger.assertion(
        f"RBD workload result: expected=not None, actual={rbd_result_available}"
    )
    logger.assertion(
        f"CephFS workload result: expected=not None, actual={cephfs_result_available}"
    )
    assert (
        rbd_result_available
    ), "RBD workload results should be recorded and provided to the test"
    assert (
        cephfs_result_available
    ), "CephFS workload results should be recorded and provided to the test"

    logger.test_step("Validate fio execution details for both volume types")
    fio_reports = (
        ("rbd", workload_storageutilization_10g_rbd["result"]["fio"]),
        ("cephfs", workload_storageutilization_10g_cephfs["result"]["fio"]),
    )

    for vol_type, fio in fio_reports:
        logger.info(f"Checking fio run on {vol_type} volume")

        job_count = len(fio["jobs"])
        logger.assertion(f"{vol_type} fio job count: expected=1, actual={job_count}")
        assert job_count == 1, "single fio job should be executed in each workload run"

        logger.info(
            "fio (version %s) executed %s job on %s volume",
            fio["fio version"],
            fio["jobs"][0]["jobname"],
            vol_type,
        )

        error_count = fio["jobs"][0]["error"]
        logger.assertion(
            f"{vol_type} fio error count: expected=0, actual={error_count}"
        )
        assert (
            error_count == 0
        ), f"no errors should be reported by fio writing on {vol_type} volume"

    logger.info("Test passed: 10 GiB workload succeeded on both RBD and CephFS volumes")
