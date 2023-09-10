# -*- coding: utf8 -*-

import logging
import os
import time

import pytest
import yaml

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization import marks
from ocs_ci.framework.testlib import tier1
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
from ocs_ci.helpers.storageclass_helpers import storageclass_name


logger = logging.getLogger(__name__)


@tier1
@marks.bugzilla("1989301")
@pytest.mark.polarion_id("OCS-2735")
def test_log_reader_writer_parallel(project, tmp_path):
    """
    Write and read logfile stored on cephfs volume, from all worker nodes of a
    cluster via k8s Deployment, while fetching content of the stored data via
    oc rsync to check the data locally.

    Reproduces BZ 1989301. Test failure means new blocker high priority bug.
    """
    pvc_dict = get_pvc_dict()
    # we need to mount the volume on every worker node, so RWX/cephfs
    pvc_dict["metadata"]["name"] = "logwriter-cephfs-many"
    pvc_dict["spec"]["accessModes"] = [constants.ACCESS_MODE_RWX]
    if (
        config.ENV_DATA["platform"].lower() not in constants.MANAGED_SERVICE_PLATFORMS
    ) and storagecluster_independent_check():
        sc_name = storageclass_name(
            constants.OCS_COMPONENTS_MAP["cephfs"], external_mode=True
        )
    else:
        sc_name = storageclass_name(constants.OCS_COMPONENTS_MAP["cephfs"])
    pvc_dict["spec"]["storageClassName"] = sc_name
    # there is no need for lot of storage capacity for this test
    pvc_dict["spec"]["resources"]["requests"]["storage"] = "1Gi"

    # get deployment dict for the reproducer logwriter workload
    with open(constants.LOGWRITER_CEPHFS_REPRODUCER, "r") as deployment_file:
        deploy_dict = yaml.safe_load(deployment_file.read())
    # if we are running in disconnected environment, we need to mirror the
    # container image first, and then use the mirror instead of the original
    if config.DEPLOYMENT.get("disconnected"):
        update_container_with_mirrored_image(deploy_dict["spec"]["template"])
    # we need to match deployment replicas with number of worker nodes
    deploy_dict["spec"]["replicas"] = len(get_worker_nodes())
    # drop topology spread constraints related to zones
    topology.drop_topology_constraint(
        deploy_dict["spec"]["template"]["spec"], topology.ZONE_LABEL
    )
    # and link the deployment with the pvc
    try:
        link_spec_volume(
            deploy_dict["spec"]["template"]["spec"],
            "logwriter-cephfs-volume",
            pvc_dict["metadata"]["name"],
        )
    except Exception as ex:
        error_msg = "LOGWRITER_CEPHFS_REPRODUCER no longer matches code of this test"
        raise Exception(error_msg) from ex

    # prepare k8s yaml file for deployment
    workload_file = ObjectConfFile(
        "log_reader_writer_parallel", [pvc_dict, deploy_dict], project, tmp_path
    )
    # deploy the workload, starting the log reader/writer pods
    logger.info(
        "starting log reader/writer workload via Deployment, one pod per worker"
    )
    workload_file.create()

    logger.info("waiting for all pods of the workload Deployment to run")
    ocp_pod = ocp.OCP(kind="Pod", namespace=project.namespace)
    try:
        ocp_pod.wait_for_resource(
            resource_count=deploy_dict["spec"]["replicas"],
            condition=constants.STATUS_RUNNING,
            error_condition=constants.STATUS_ERROR,
            timeout=300,
            sleep=30,
        )
    except Exception as ex:
        # this is not a problem with feature under test, but with infra,
        # cluster configuration or unrelated bug which must have happened
        # before this test case
        error_msg = "unexpected problem with start of the workload, cluster is either misconfigured or broken"
        logger.exception(error_msg)
        logger.debug(workload_file.describe())
        raise exceptions.UnexpectedBehaviour(error_msg) from ex

    # while the workload is running, we will try to fetch and validate data
    # from the cephfs volume of the workload 120 times (this number of retries
    # is a bit larger than usual number required to reproduce bug from
    # BZ 1989301, but we need to be sure here)
    number_of_fetches = 120
    # if given fetch fail, we will ignore the failure unless the number of
    # failures is too high (this has no direct impact on feature under test,
    # we should be able to detect the bug even with 10% of rsync failures,
    # since data corruption doesn't simply go away ...)
    number_of_failures = 0
    allowed_failures = 12
    is_local_data_ok = True
    local_dir = tmp_path / "logwriter"
    local_dir.mkdir()
    workload_pods = ocp_pod.get()
    workload_pod_name = workload_pods["items"][0]["metadata"]["name"]
    logger.info(
        "while the workload is running, we will fetch and check data from the cephfs volume %d times",
        number_of_fetches,
    )
    for _ in range(number_of_fetches):
        # fetch data from cephfs volume into the local dir
        oc_cmd = [
            "oc",
            "rsync",
            "--loglevel=4",
            "-n",
            project.namespace,
            f"pod/{workload_pod_name}:/mnt/target",
            local_dir,
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
        # look for null bytes in the just fetched local files in target dir,
        # and if these binary bytes are found, the test failed (the bug
        # was reproduced)
        target_dir = os.path.join(local_dir, "target")
        for file_name in os.listdir(target_dir):
            with open(os.path.join(target_dir, file_name), "r") as fo:
                data = fo.read()
                if "\0" in data:
                    is_local_data_ok = False
                    logger.error(
                        "file %s is corrupted: null byte found in a text file",
                        file_name,
                    )
        # is_local_data_ok = False
        assert is_local_data_ok, "data corruption detected"
        time.sleep(2)

    logger.debug("number of ignored rsync failures: %d", number_of_failures)

    # if no obvious problem was detected, run the logreader job to validate
    # checksums in the log files (so that we are 100% sure that nothing went
    # wrong with the IO or the data)
    with open(constants.LOGWRITER_CEPHFS_READER, "r") as job_file:
        job_dict = yaml.safe_load(job_file.read())
    # mirroring for disconnected environment, if necessary
    if config.DEPLOYMENT.get("disconnected"):
        update_container_with_mirrored_image(job_dict["spec"]["template"])
    # drop topology spread constraints related to zones
    topology.drop_topology_constraint(
        job_dict["spec"]["template"]["spec"], topology.ZONE_LABEL
    )
    # we need to match number of jobs with the number used in the workload
    job_dict["spec"]["completions"] = deploy_dict["spec"]["replicas"]
    job_dict["spec"]["parallelism"] = deploy_dict["spec"]["replicas"]
    # and reffer to the correct pvc name
    try:
        link_spec_volume(
            job_dict["spec"]["template"]["spec"],
            "logwriter-cephfs-volume",
            pvc_dict["metadata"]["name"],
        )
    except Exception as ex:
        error_msg = "LOGWRITER_CEPHFS_READER no longer matches code of this test"
        raise Exception(error_msg) from ex
    # prepare k8s yaml file for the job
    job_file = ObjectConfFile("log_reader", [job_dict], project, tmp_path)
    # deploy the job, starting the log reader pods
    logger.info(
        "starting log reader data validation job to fully check the log data",
    )
    job_file.create()
    # wait for the logreader job to complete (this should be rather quick)
    try:
        job.wait_for_job_completion(
            job_name=job_dict["metadata"]["name"],
            namespace=project.namespace,
            timeout=300,
            sleep_time=30,
        )
    except exceptions.TimeoutExpiredError:
        error_msg = (
            "verification failed to complete in time: data loss or broken cluster?"
        )
        logger.exception(error_msg)
    # and then check that the job completed with success
    logger.info("checking the result of data validation job")
    logger.debug(job_file.describe())
    ocp_job = ocp.OCP(
        kind="Job",
        namespace=project.namespace,
        resource_name=job_dict["metadata"]["name"],
    )
    job_status = ocp_job.get()["status"]
    logger.info("last status of data verification job: %s", job_status)
    if (
        "failed" in job_status
        or job_status["succeeded"] != deploy_dict["spec"]["replicas"]
    ):
        error_msg = "possible data corruption: data verification job failed!"
        logger.error(error_msg)
        job.log_output_of_job_pods(
            job_name=job_dict["metadata"]["name"], namespace=project.namespace
        )
        raise Exception(error_msg)
