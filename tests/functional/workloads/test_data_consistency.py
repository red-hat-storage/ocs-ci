# -*- coding: utf8 -*-

import logging
import os
import time

import pytest
import yaml

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import magenta_squad
from ocs_ci.framework.testlib import tier2
from ocs_ci.ocs import constants
from ocs_ci.ocs import exceptions
from ocs_ci.ocs import ocp
from ocs_ci.ocs.fio_artefacts import get_pvc_dict
from ocs_ci.ocs.node import get_worker_nodes
from ocs_ci.ocs.resources import job
from ocs_ci.ocs.resources import topology
from ocs_ci.ocs.resources.objectconfigfile import ObjectConfFile, link_spec_volume
from ocs_ci.utility.utils import run_cmd, update_container_with_mirrored_image
from ocs_ci.helpers.helpers import storagecluster_independent_check


logger = logging.getLogger(__name__)


@magenta_squad
@tier2
@pytest.mark.polarion_id("OCS-2735")
def test_log_reader_writer_parallel(project, tmp_path):
    """
    Write and read logfile stored on cephfs volume, from all worker nodes of a
    cluster via k8s Deployment, while fetching content of the stored data via
    oc rsync to check the data locally.

    Reproduces BZ 1989301. Test failure means new blocker high priority bug.
    """
    logger.test_step("Prepare PVC configuration for CephFS RWX volume")
    pvc_dict = get_pvc_dict()
    pvc_dict["metadata"]["name"] = "logwriter-cephfs-many"
    pvc_dict["spec"]["accessModes"] = [constants.ACCESS_MODE_RWX]
    if (
        config.ENV_DATA["platform"].lower() not in constants.HCI_PC_OR_MS_PLATFORM
    ) and storagecluster_independent_check():
        sc_name = constants.DEFAULT_EXTERNAL_MODE_STORAGECLASS_CEPHFS
    else:
        sc_name = constants.CEPHFILESYSTEM_SC
    pvc_dict["spec"]["storageClassName"] = sc_name
    pvc_dict["spec"]["resources"]["requests"]["storage"] = "1Gi"
    logger.info(
        f"PVC configured: name={pvc_dict['metadata']['name']}, "
        f"access_mode=RWX, storage_class={sc_name}, size=1Gi"
    )

    logger.test_step("Configure logwriter deployment for all worker nodes")
    with open(constants.LOGWRITER_CEPHFS_REPRODUCER, "r") as deployment_file:
        deploy_dict = yaml.safe_load(deployment_file.read())

    if config.DEPLOYMENT.get("disconnected"):
        logger.info("Updating container image for disconnected environment")
        update_container_with_mirrored_image(deploy_dict["spec"]["template"])

    worker_count = len(get_worker_nodes())
    deploy_dict["spec"]["replicas"] = worker_count
    logger.info(f"Deployment replicas set to match worker node count: {worker_count}")

    topology.drop_topology_constraint(
        deploy_dict["spec"]["template"]["spec"], topology.ZONE_LABEL
    )
    logger.debug("Dropped zone topology constraints")

    try:
        link_spec_volume(
            deploy_dict["spec"]["template"]["spec"],
            "logwriter-cephfs-volume",
            pvc_dict["metadata"]["name"],
        )
        logger.info(f"Linked deployment to PVC: {pvc_dict['metadata']['name']}")
    except Exception as ex:
        error_msg = "LOGWRITER_CEPHFS_REPRODUCER no longer matches code of this test"
        logger.exception(error_msg)
        raise Exception(error_msg) from ex

    logger.test_step("Deploy log reader/writer workload (one pod per worker)")
    workload_file = ObjectConfFile(
        "log_reader_writer_parallel", [pvc_dict, deploy_dict], project, tmp_path
    )
    logger.info(
        f"Starting log reader/writer workload via Deployment: {worker_count} pods"
    )
    workload_file.create()

    logger.info(
        f"Waiting for {deploy_dict['spec']['replicas']} pod(s) to reach Running state"
    )
    ocp_pod = ocp.OCP(kind="Pod", namespace=project.namespace)
    try:
        ocp_pod.wait_for_resource(
            resource_count=deploy_dict["spec"]["replicas"],
            condition=constants.STATUS_RUNNING,
            error_condition=constants.STATUS_ERROR,
            timeout=300,
            sleep=30,
        )
        logger.info(
            f"All {deploy_dict['spec']['replicas']} workload pod(s) are running"
        )
    except Exception as ex:
        error_msg = "unexpected problem with start of the workload, cluster is either misconfigured or broken"
        logger.exception(error_msg)
        logger.debug(workload_file.describe())
        raise exceptions.UnexpectedBehaviour(error_msg) from ex

    logger.test_step(
        "Fetch and validate data from CephFS volume (BZ 1989301 reproducer)"
    )
    number_of_fetches = 120
    allowed_failures = 12
    number_of_failures = 0
    is_local_data_ok = True
    local_dir = tmp_path / "logwriter"
    local_dir.mkdir()
    workload_pods = ocp_pod.get()
    workload_pod_name = workload_pods["items"][0]["metadata"]["name"]
    logger.info(
        f"Fetching and checking data {number_of_fetches} times to detect corruption "
        f"(allowed failures: {allowed_failures}, reproduces BZ 1989301)"
    )
    logger.info(f"Using pod for data fetch: {workload_pod_name}")
    for _ in range(number_of_fetches):
        # fetch data from cephfs volume into the local dir
        oc_cmd = [
            "oc",
            "cp",
            f"{project.namespace}/{workload_pod_name}:/mnt/target",
            str(local_dir) + "/target",
        ]
        try:
            run_cmd(cmd=oc_cmd, timeout=300)
        except Exception as ex:
            number_of_failures += 1
            # in case this fails, we are going to fetch extra evidence, that
            # said such failure is most likely related to OCP or infrastructure
            error_msg = "oc rsync failed: something is wrong with the cluster"
            logger.exception(error_msg)
            logger.debug(workload_file.describe())
            oc_rpm_debug = [
                "oc",
                "rsh",
                "-n",
                project.namespace,
                f"pod/{workload_pod_name}",
                "bash",
                "-c",
                ";".join(
                    [
                        "rpm -qa",
                        "rpm -qaV",
                        "type -a tar",
                        "tar --version",
                        "type -a rsync",
                        "rsync --version",
                    ]
                ),
            ]
            try:
                run_cmd(cmd=oc_rpm_debug, timeout=600)
            except Exception:
                # if fetch of additional evidence fails, log and ignore the
                # exception (so that we can retry if needed)
                logger.exception("failed to fetch additional evidence")
            # in case the rsync run failed because of a container restart,
            # we assume the pod name hasn't changed, and just wait for the
            # container to be running again - unless the number of rsync
            # failures is too high
            if number_of_failures > allowed_failures:
                logger.error("number of ignored rsync failures is too high")
            else:
                ocp_pod.wait_for_resource(
                    resource_count=deploy_dict["spec"]["replicas"],
                    condition=constants.STATUS_RUNNING,
                    timeout=300,
                    sleep=30,
                )
                continue
            logger.debug(
                "before this failure, we ignored %d previous failures",
                number_of_failures,
            )
            raise exceptions.UnexpectedBehaviour(error_msg) from ex
        target_dir = os.path.join(local_dir, "target")
        file_list = os.listdir(target_dir)
        logger.debug(
            f"Checking {len(file_list)} file(s) for null bytes (data corruption)"
        )
        for file_name in file_list:
            with open(os.path.join(target_dir, file_name), "r") as fo:
                data = fo.read()
                if "\0" in data:
                    is_local_data_ok = False
                    logger.error(
                        f"Data corruption detected: null byte found in file {file_name}"
                    )
        logger.assertion(
            f"Data corruption check: files_checked={len(file_list)}, corrupted={not is_local_data_ok}"
        )
        assert is_local_data_ok, "data corruption detected"
        time.sleep(2)

    logger.info(
        f"Data fetch completed: total_fetches={number_of_fetches}, ignored_failures={number_of_failures}"
    )
    logger.debug(f"Number of ignored rsync failures: {number_of_failures}")

    logger.test_step("Run logreader job to validate checksums in log files")
    logger.info(
        "No corruption detected in initial checks, running full checksum validation"
    )
    with open(constants.LOGWRITER_CEPHFS_READER, "r") as job_file:
        job_dict = yaml.safe_load(job_file.read())

    if config.DEPLOYMENT.get("disconnected"):
        logger.info("Updating container image for disconnected environment")
        update_container_with_mirrored_image(job_dict["spec"]["template"])

    topology.drop_topology_constraint(
        job_dict["spec"]["template"]["spec"], topology.ZONE_LABEL
    )

    job_dict["spec"]["completions"] = deploy_dict["spec"]["replicas"]
    job_dict["spec"]["parallelism"] = deploy_dict["spec"]["replicas"]
    logger.info(
        f"Logreader job configured: completions={job_dict['spec']['completions']}, "
        f"parallelism={job_dict['spec']['parallelism']}"
    )

    try:
        link_spec_volume(
            job_dict["spec"]["template"]["spec"],
            "logwriter-cephfs-volume",
            pvc_dict["metadata"]["name"],
        )
        logger.info(f"Linked logreader job to PVC: {pvc_dict['metadata']['name']}")
    except Exception as ex:
        error_msg = "LOGWRITER_CEPHFS_READER no longer matches code of this test"
        logger.exception(error_msg)
        raise Exception(error_msg) from ex

    job_file = ObjectConfFile("log_reader", [job_dict], project, tmp_path)
    logger.info("Starting log reader data validation job to fully check the log data")
    job_file.create()
    logger.test_step("Wait for logreader job to complete")
    logger.info(
        f"Waiting for logreader job to complete: {job_dict['metadata']['name']}"
    )
    try:
        job.wait_for_job_completion(
            job_name=job_dict["metadata"]["name"],
            namespace=project.namespace,
            timeout=300,
            sleep_time=30,
        )
        logger.info("Logreader job completed")
    except exceptions.TimeoutExpiredError:
        error_msg = (
            "verification failed to complete in time: data loss or broken cluster?"
        )
        logger.exception(error_msg)

    logger.test_step("Verify logreader job completed successfully")
    logger.info("Checking the result of data validation job")
    logger.debug(job_file.describe())
    ocp_job = ocp.OCP(
        kind="Job",
        namespace=project.namespace,
        resource_name=job_dict["metadata"]["name"],
    )
    job_status = ocp_job.get()["status"]
    logger.info(f"Data verification job status: {job_status}")

    expected_succeeded = deploy_dict["spec"]["replicas"]
    actual_succeeded = job_status.get("succeeded", 0)
    has_failures = "failed" in job_status

    logger.assertion(
        f"Logreader job result: expected_succeeded={expected_succeeded}, "
        f"actual_succeeded={actual_succeeded}, has_failures={has_failures}"
    )

    if has_failures or actual_succeeded != expected_succeeded:
        error_msg = "possible data corruption: data verification job failed!"
        logger.error(error_msg)
        job.log_output_of_job_pods(
            job_name=job_dict["metadata"]["name"], namespace=project.namespace
        )
        raise Exception(error_msg)

    logger.info(
        f"Data validation successful: all {expected_succeeded} job(s) completed without corruption"
    )
