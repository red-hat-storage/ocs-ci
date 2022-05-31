# -*- coding: utf8 -*-

import logging
import yaml

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs import exceptions
from ocs_ci.ocs import ocp
from ocs_ci.ocs.fio_artefacts import get_pvc_dict
from ocs_ci.ocs.node import get_worker_nodes
from ocs_ci.ocs.resources import job
from ocs_ci.ocs.resources import topology
from ocs_ci.ocs.resources.objectconfigfile import ObjectConfFile, link_spec_volume
from ocs_ci.utility.utils import update_container_with_mirrored_image
from ocs_ci.helpers.helpers import storagecluster_independent_check
from ocs_ci.ocs.cluster import is_managed_service_cluster


logger = logging.getLogger(__name__)


class LogReaderWriterParallel(object):

    """
    Write and read logfile stored on cephfs volume, from all worker nodes of a
    cluster via k8s Deployment, while fetching content of the stored data via
    oc rsync to check the data locally.

    TO DO: Update the test after the issue https://github.com/red-hat-storage/ocs-ci/issues/5724
    will be completed.

    """

    def __init__(
        self,
        project,
        tmp_path,
        storage_size=2,
    ):
        """
        Init of the LogReaderWriterParallel object

        Args:
            project (pytest fixture): The project fixture.
            tmp_path (pytest fixture): The tmp_path fixture.
            storage_size (str): The size of the storage in GB. The default value is 2 GB.

        """
        self.project = project
        self.tmp_path = tmp_path

        self.pvc_dict = get_pvc_dict()
        # we need to mount the volume on every worker node, so RWX/cephfs
        self.pvc_dict["metadata"]["name"] = "logwriter-cephfs-many"
        self.pvc_dict["spec"]["accessModes"] = [constants.ACCESS_MODE_RWX]
        if storagecluster_independent_check() and not is_managed_service_cluster():
            sc_name = constants.DEFAULT_EXTERNAL_MODE_STORAGECLASS_CEPHFS
        else:
            sc_name = constants.CEPHFILESYSTEM_SC
        logger.info(f"Storage class name = {sc_name}")
        self.pvc_dict["spec"]["storageClassName"] = sc_name
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

        Raise:
            NotFoundError: When given volume is not found in given spec
            UnexpectedBehaviour: When an unexpected problem with starting the workload occurred

        """

        # get deployment dict for the reproducer logwriter workload
        with open(constants.LOGWRITER_CEPHFS_REPRODUCER, "r") as deployment_file:
            self.deploy_dict = yaml.safe_load(deployment_file.read())
        # if we are running in disconnected environment, we need to mirror the
        # container image first, and then use the mirror instead of the original
        if config.DEPLOYMENT.get("disconnected"):
            update_container_with_mirrored_image(self.deploy_dict["spec"]["template"])
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
        except (exceptions.NotFoundError, KeyError) as ex:
            logger.warning(
                "Failed to link the deployment with the pvc. We may need to check if the "
                "LOGWRITER_CEPHFS_REPRODUCER still matches the code of this test"
            )
            raise ex

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
        While the workload is running, try to validate the data
        from the cephfs volume of the workload.

        Raise:
            NotFoundError: When the given volume is not found in given spec
            Exception: When the data verification job failed

        """
        # if no obvious problem was detected, run the logreader job to validate
        # checksums in the log files (so that we are 100% sure that nothing went
        # wrong with the IO or the data)
        with open(constants.LOGWRITER_CEPHFS_READER, "r") as job_file:
            job_dict = yaml.safe_load(job_file.read())
        # if we are running in disconnected environment, we need to mirror the
        # container image first, and then use the mirror instead of the original
        if config.DEPLOYMENT.get("disconnected"):
            update_container_with_mirrored_image(self.deploy_dict["spec"]["template"])
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
        except (exceptions.NotFoundError, KeyError) as ex:
            logger.warning(
                "Failed to link the deployment with the pvc. We may need to check if the "
                "LOGWRITER_CEPHFS_REPRODUCER still matches the code of this test"
            )
            raise ex

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
            error_msg = "verification failed to complete in time: probably data loss or broken cluster"
            raise Exception(error_msg)
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
