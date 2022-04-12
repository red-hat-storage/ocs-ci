# -*- coding: utf8 -*-

import logging
import os
import time

import yaml

from ocs_ci.ocs import constants
from ocs_ci.ocs import exceptions
from ocs_ci.ocs import ocp
from ocs_ci.ocs.fio_artefacts import get_pvc_dict
from ocs_ci.ocs.node import get_worker_nodes
from ocs_ci.ocs.resources import job
from ocs_ci.ocs.resources import topology
from ocs_ci.ocs.resources.objectconfigfile import ObjectConfFile, link_spec_volume
from ocs_ci.utility.utils import exec_cmd


logger = logging.getLogger(__name__)


class LogReaderWriterParallel(object):

    """
    This procedure in the test was originally created in the file
    'tests/e2e/workloads/test_data_consistency.py'. I just rearranged it in a class.

    Write and read logfile stored on cephfs volume, from all worker nodes of a
    cluster via k8s Deployment, while fetching content of the stored data via
    oc rsync to check the data locally.

    """

    def __init__(
        self,
        project,
        tmp_path,
        storage_size=2,
        number_of_fetches=100,
        max_num_of_rsync_command_failures=0,
    ):
        """
        Init of the LogReaderWriterParallel object

        Args:
            project (pytest fixture): The project fixture.
            tmp_path (pytest fixture): The tmp_path fixture.
            storage_size (str): The size of the storage in GB. The default value is 2 GB.
            number_of_fetches (int): Number of fetches. The default value is 120.
            max_num_of_rsync_command_failures (int): The maximum number of rsync command failures
                we allow until we throw an exception. Default is 0 - which means it will throw
                an exception in the first rsync command failure.

        """
        self.project = project
        self.tmp_path = tmp_path
        self.number_of_fetches = number_of_fetches
        self.max_num_of_rsync_command_failures = max_num_of_rsync_command_failures
        self.num_of_rsync_command_failures = 0

        self.pvc_dict = get_pvc_dict()
        # we need to mount the volume on every worker node, so RWX/cephfs
        self.pvc_dict["metadata"]["name"] = "logwriter-cephfs-many"
        self.pvc_dict["spec"]["accessModes"] = [constants.ACCESS_MODE_RWX]
        self.pvc_dict["spec"]["storageClassName"] = constants.CEPHFILESYSTEM_SC
        self.pvc_dict["spec"]["resources"]["requests"]["storage"] = f"{storage_size}Gi"

        self.deploy_dict = {}
        self.workload_file = None
        self.ocp_pod = None

        self.local_dir = self.tmp_path / "logwriter"
        self.local_dir.mkdir()

    def log_reader_writer_parallel(self):
        """
        Write and read logfile stored on cephfs volume, from all worker nodes of a
        cluster via k8s Deployment.

        """

        # get deployment dict for the reproducer logwriter workload
        with open(constants.LOGWRITER_CEPHFS_REPRODUCER, "r") as deployment_file:
            self.deploy_dict = yaml.safe_load(deployment_file.read())
        # we need to match deployment replicas with number of worker nodes
        self.deploy_dict["spec"]["replicas"] = len(get_worker_nodes())
        # drop topology spread constraints related to zones
        topology.drop_topology_constraint(
            self.deploy_dict["spec"]["template"]["spec"], topology.ZONE_LABEL
        )
        # and link the deployment with the pvc
        try:
            link_spec_volume(
                self.deploy_dict["spec"]["template"]["spec"],
                "logwriter-cephfs-volume",
                self.pvc_dict["metadata"]["name"],
            )
        except Exception as ex:
            error_msg = (
                "LOGWRITER_CEPHFS_REPRODUCER no longer matches code of this test"
            )
            raise Exception(error_msg) from ex

        # prepare k8s yaml file for deployment
        self.workload_file = ObjectConfFile(
            "log_reader_writer_parallel",
            [self.pvc_dict, self.deploy_dict],
            self.project,
            self.tmp_path,
        )
        # deploy the workload, starting the log reader/writer pods
        logger.info(
            "starting log reader/writer workload via Deployment, one pod per worker"
        )
        self.workload_file.create()

        logger.info("waiting for all pods of the workload Deployment to run")
        self.ocp_pod = ocp.OCP(kind="Pod", namespace=self.project.namespace)
        try:
            self.ocp_pod.wait_for_resource(
                resource_count=self.deploy_dict["spec"]["replicas"],
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
            logger.debug(self.workload_file.describe())
            raise exceptions.UnexpectedBehaviour(error_msg) from ex

    def fetch_and_validate_data(self):
        """
        while the workload is running, we will try to fetch and validate data
        from the cephfs volume of the workload 'number_of_fetches' times.

        """
        is_local_data_ok = True
        logger.info(
            f"while the workload is running, we will fetch and check data from the "
            f"cephfs volume {self.number_of_fetches} times"
        )
        for i in range(self.number_of_fetches):
            logger.info(f"fetch number {i}")
            # fetch data from cephfs volume into the local dir
            self.execute_rsync_command()
            # look for null bytes in the just fetched local files in target dir,
            # and if these binary bytes are found, the test failed (the bug
            # was reproduced)
            target_dir = os.path.join(self.local_dir, "target")
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

        # if no obvious problem was detected, run the logreader job to validate
        # checksums in the log files (so that we are 100% sure that nothing went
        # wrong with the IO or the data)
        with open(constants.LOGWRITER_CEPHFS_READER, "r") as job_file:
            job_dict = yaml.safe_load(job_file.read())
        # drop topology spread constraints related to zones
        topology.drop_topology_constraint(
            job_dict["spec"]["template"]["spec"], topology.ZONE_LABEL
        )
        # we need to match number of jobs with the number used in the workload
        job_dict["spec"]["completions"] = self.deploy_dict["spec"]["replicas"]
        job_dict["spec"]["parallelism"] = self.deploy_dict["spec"]["replicas"]
        # and reffer to the correct pvc name
        try:
            link_spec_volume(
                job_dict["spec"]["template"]["spec"],
                "logwriter-cephfs-volume",
                self.pvc_dict["metadata"]["name"],
            )
        except Exception as ex:
            error_msg = "LOGWRITER_CEPHFS_READER no longer matches code of this test"
            raise Exception(error_msg) from ex
        # prepare k8s yaml file for the job
        job_file = ObjectConfFile("log_reader", [job_dict], self.project, self.tmp_path)
        # deploy the job, starting the log reader pods
        logger.info(
            "starting log reader data validation job to fully check the log data",
        )
        job_file.create()
        # wait for the logreader job to complete (this should be rather quick)
        try:
            job.wait_for_job_completion(
                job_name=job_dict["metadata"]["name"],
                namespace=self.project.namespace,
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
            namespace=self.project.namespace,
            resource_name=job_dict["metadata"]["name"],
        )
        job_status = ocp_job.get()["status"]
        logger.info("last status of data verification job: %s", job_status)
        if (
            "failed" in job_status
            or job_status["succeeded"] != self.deploy_dict["spec"]["replicas"]
        ):
            error_msg = "possible data corruption: data verification job failed!"
            logger.error(error_msg)
            job.log_output_of_job_pods(
                job_name=job_dict["metadata"]["name"], namespace=self.project.namespace
            )
            raise Exception(error_msg)

    def get_num_of_rsync_command_failures(self):
        """
        Get the number of the rsync command failures
        """
        return self.num_of_rsync_command_failures

    def execute_rsync_command(self):
        """
        Execute the rsync command on the workload pod name.
        This method is for internal use.

        """
        workload_pods = self.ocp_pod.get()
        workload_pod_name = workload_pods["items"][0]["metadata"]["name"]
        oc_cmd = [
            "oc",
            "rsync",
            "--loglevel=4",
            "-n",
            self.project.namespace,
            f"pod/{workload_pod_name}:/mnt/target",
            self.local_dir,
        ]
        try:
            exec_cmd(cmd=oc_cmd, timeout=300)
        except Exception as ex:
            # in case this fails, we are going to fetch extra evidence, that
            # said such failure is most likely related to OCP or infrastructure
            error_msg = "oc rsync failed: something is wrong with the cluster"
            if (
                self.num_of_rsync_command_failures
                >= self.max_num_of_rsync_command_failures
            ):
                logger.exception(error_msg)
            else:
                logger.info(error_msg)

            logger.debug(self.workload_file.describe())
            oc_rpm_debug = [
                "oc",
                "rsh",
                "-n",
                self.project.namespace,
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
                exec_cmd(cmd=oc_rpm_debug, timeout=600)
            except Exception as e:
                logger.info(f"The debug command failed due to the error {str(e)}")

            if (
                self.num_of_rsync_command_failures
                >= self.max_num_of_rsync_command_failures
            ):
                logger.warning(
                    "The maximum rsync command failures reached it's limit. "
                    "Throwing an exception..."
                )
                raise exceptions.UnexpectedBehaviour(error_msg) from ex

            self.num_of_rsync_command_failures += 1
