import os
import logging

import pytest

from ocs_ci.framework.testlib import ManageTest, tier1, bugzilla
from ocs_ci.ocs import openshift_ops, ocp
from ocs_ci.ocs.utils import collect_ocs_logs
from ocs_ci.utility.utils import ocsci_log_path, TimeoutSampler

logger = logging.getLogger(__name__)


@tier1
@pytest.mark.polarion_id("OCS-1583")
@bugzilla('1766646')
class TestMustGather(ManageTest):

    @pytest.fixture(autouse=True)
    def init_ocp(self):
        """
        init OCP() object
        """

        self.ocs = openshift_ops.OCP()
        self.ocp_obj = ocp.OCP()

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        def check_for_must_gather_project():
            namespaces = self.ocs.get_projects()
            logger.info(f"namespaces: {namespaces}")
            for project in namespaces:
                logger.info(f"project: {project}")
                if project[:-5] == "openshift-must-gather-":
                    return project

            return False

        def check_for_must_gather_pod():
            must_gather_pods = self.ocs.get_pods(label_selector='app=must-gather')
            if must_gather_pods:
                logger.info(f"must_gather pods: {must_gather_pods}")
                logger.info("pod still exist")
                return True
            else:
                return False

        def finalizer():
            must_gather_pods = self.ocs.get_pods(label_selector='app=must-gather')
            logger.info(f"must_gather_pods: {must_gather_pods} ")
            sample_pods = TimeoutSampler(
                timeout=30, sleep=3, func=check_for_must_gather_pod,
            )
            sample_namespace = TimeoutSampler(
                timeout=30, sleep=3, func=check_for_must_gather_project,
            )
            if sample_pods.wait_for_func_status(result=True):
                for must_gather_pod in must_gather_pods:
                    self.ocp_obj.wait_for_delete(resource_name=must_gather_pod)
                    logger.info(f"deleted pods: {must_gather_pods}")
            if not sample_namespace.wait_for_func_status(result=False):
                must_gather_namespace = check_for_must_gather_project()
                logger.info(f"namespace to delete: {must_gather_namespace}")
                self.ocp_obj.wait_for_delete(resource_name=must_gather_namespace)

        request.addfinalizer(finalizer)

    def test_must_gather(self):
        """
        Tests functionality of: oc adm must-gather

        """

        # Make logs root directory
        logger.info("Creating logs Directory")
        directory = self.make_directory()
        logger.info(f"Creating {directory}_ocs_logs - Done!")

        # Collect OCS logs
        logger.info("Collecting Logs")
        collect_ocs_logs(dir_name=directory, ocp=False)
        logger.info("Collecting logs - Done!")

        # Compare running pods list to "/pods" subdirectories
        logger.info("Checking logs tree")
        logs = self.get_log_directories(directory)
        pods = self.get_ocs_pods()
        logger.info(f"Logs: {logs}")
        logger.info(f"pods list: {pods}")
        assert set(sorted(logs)) == set(sorted(pods)), (
            "List of openshift-storage pods are not equal to list of logs directories"
            f"list of pods: {pods}"
            f"list of log directories: {logs}"
        )

        # 2nd test: Verify logs file are not empty
        logs_dir_list = self.search_log_files(directory)
        assert self.check_file_size(logs_dir_list), (
            "One or more log file are empty"
        )

    def make_directory(self):
        """
        Check if directory to store must gather logs already exist
        and use new directory if so.

        Returns:
            str: Logs directory

        """
        index = 1
        directory = ocsci_log_path()
        while os.path.isdir(directory + "_ocs_logs"):
            index += 1
            directory = ocsci_log_path() + f"_{index}"

        return directory

    def get_log_directories(self, directory):
        """
        Get list of subdirectories contains openshift-storage pod's logs

        Args:
            directory: (str): location of must gather logs

        Returns:
            list: Subdirectories of "pods" directory

        """
        dir_name = self.locate_pods_directory(directory)
        list_dir = os.listdir(dir_name)

        return list_dir

    def get_ocs_pods(self):
        """
        Get list of openshift-storage pods

        Returns:
            list: pods in openshift-storage namespace

        """

        pods = self.ocs.get_pods(namespace='openshift-storage')

        return pods

    def locate_pods_directory(self, root_directory):
        """
        Find full path of 'pods' subdirectory

        Args:
            root_directory: (str): location of must gather logs

        Returns:
            str: Full path of 'pods' subdirectory, if exist

        """
        for dir_name, subdir_list, files_list in os.walk(root_directory + "_ocs_logs"):
            if dir_name[-4:] == "pods":
                return dir_name

        logger.info("could not find \'pods\' directory")

    def search_log_files(self, directory):
        """

        Args:
            directory: (str): location of must gather logs

        Returns:
            list: list contain full path of each "logs" subdirectory

        """
        pods_dir = self.locate_pods_directory(directory)
        logger.info(f"pods dir: {pods_dir}")
        logs_dir_list = list()
        for dir_name, subdir_list, files_list in os.walk(pods_dir):
            if dir_name[-4:] == "logs":
                logs_dir_list.append(dir_name)

        return logs_dir_list

    def check_file_size(self, logs_dir_list):
        """
        Check if log file "current.log" is empty or not

        Args:
            logs_dir_list: (list): Contain full path of each "logs" subdirectory

        Returns:
            bool: False - if one or more log file is empty

        """
        for log_dir in logs_dir_list:
            log_file = log_dir + "/current.log"
            if os.path.getsize(log_file) > 0:
                logger.info(f"file {log_file} size: {os.path.getsize(log_file)}")

            else:
                logger.info(f"log file {log_file} empty!")
                return False
