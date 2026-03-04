# -*- coding: utf8 -*-
"""
Test cases for workload with disruptions.

The test_workload_with_checksum_rbd test writes data using fio with checksum
generation and immediately verifies the checksum to ensure data integrity.
This merged test addresses issues with PV leftovers and test dependencies.

The test uses the workload_storageutilization_checksum_rbd fixture to write
10GB of data with checksum generation, then immediately verifies the checksum
in the same test. All resources including the PV are properly cleaned up after
the test completes.

This approach solves:
- Issue #14213: PV leftover causing conflicts with other tests
- Issue #13839: Test failure when executed multiple times due to multiple PVs
  with the same label
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
@tier2
@skipif_managed_service
@skipif_mcg_only
@skipif_hci_provider_and_client
def test_workload_with_checksum_rbd(
    workload_storageutilization_checksum_rbd,
    tmp_path,
    project,
    fio_job_dict,
):
    """
    Test workload with checksum generation and immediate verification.

    This test writes 10GB of data using fio, generates a checksum file,
    and immediately verifies the checksum to ensure data integrity.
    All resources including the PV are properly cleaned up after the test.

    This merged test addresses:
    - Issue #14213: PV leftover causing conflicts with other tests
    - Issue #13839: Test failure when executed multiple times
    """
    # Verify fio write operation completed successfully
    msg = "fio report should be available"
    assert workload_storageutilization_checksum_rbd["result"] is not None, msg
    fio = workload_storageutilization_checksum_rbd["result"]["fio"]
    assert len(fio["jobs"]) == 1, "single fio job was executed"
    msg = "no errors should be reported by fio when writing data"
    assert fio["jobs"][0]["error"] == 0, msg

    logger.info(
        "fio write completed successfully, proceeding with checksum verification"
    )

    # Now verify the checksum immediately in the same test
    # The job will run sha1sum check on the same PVC
    container = fio_job_dict["spec"]["template"]["spec"]["containers"][0]
    container["command"] = ["/usr/bin/sha1sum", "-c", "/mnt/target/fio.sha1sum"]

    # Create verification job name to avoid conflicts
    fio_job_dict["metadata"]["name"] = "fio-checksum-verify"

    # Create the verification job
    job_file = ObjectConfFile("fio-checksum-verify", [fio_job_dict], project, tmp_path)

    # Deploy the verification job
    job_file.create()

    # Wait for the verification job to complete
    # Use a reasonable timeout for checksum verification (much faster than write)
    verification_timeout = 300  # 5 minutes should be plenty for checksum verification
    error_msg = "Checksum verification job failed. Data integrity check did not pass."
    pod_name = fiojob.wait_for_job_completion(
        project.namespace, verification_timeout, error_msg
    )

    # Provide clear evidence of the verification in the logs
    ocp_pod = ocp.OCP(kind="Pod", namespace=project.namespace)
    sha1sum_output = ocp_pod.exec_oc_cmd(f"logs {pod_name}", out_yaml_format=False)
    logger.info("Checksum verification output: %s", sha1sum_output)

    # Verify that sha1sum reported success (no output means all checksums matched)
    assert "FAILED" not in sha1sum_output, "Checksum verification failed"
    logger.info("Checksum verification passed successfully")


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
    logger.info("checking fio report results as provided by workload fixtures")
    msg = "workload results should be recorded and provided to the test"
    assert workload_storageutilization_10g_rbd["result"] is not None, msg
    assert workload_storageutilization_10g_cephfs["result"] is not None, msg

    fio_reports = (
        ("rbd", workload_storageutilization_10g_rbd["result"]["fio"]),
        ("cephfs", workload_storageutilization_10g_cephfs["result"]["fio"]),
    )
    for vol_type, fio in fio_reports:
        logger.info("starting to check fio run on %s volume", vol_type)
        msg = "single fio job should be executed in each workload run"
        assert len(fio["jobs"]) == 1, msg
        logger.info(
            "fio (version %s) executed %s job on %s volume",
            fio["fio version"],
            fio["jobs"][0]["jobname"],
            vol_type,
        )
        msg = f"no errors should be reported by fio writing on {vol_type} volume"
        assert fio["jobs"][0]["error"] == 0, msg
