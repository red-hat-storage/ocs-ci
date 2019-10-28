import os
import logging
import time

import pytest

from ocs_ci.framework.testlib import ManageTest, tier1
from ocs_ci.ocs import openshift_ops, ocp
from ocs_ci.ocs.utils import collect_ocs_logs

logger = logging.getLogger(__name__)


@tier1
@pytest.mark.polarion_id("OCS-1583")
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

        def finalizer():
            must_gather_pods = self.ocs.get_pods(label_selector='app=must-gather')
            logger.info(f"must_gather_pods: {must_gather_pods} ")
            if must_gather_pods:
                for pod_to_del in must_gather_pods:
                    self.ocp_obj.wait_for_delete(resource_name=pod_to_del)
                    logger.info(f"deleted pods: {pod_to_del}")
                    time.sleep(3)

        request.addfinalizer(finalizer)

    def test_must_gather(self):
        """
        Tests functionality of: oc adm must-gather

        """

        # Make logs root directory
        logger.info("Creating logs Directory")
        directory = self.make_directory()
        logger.info(f"Creating {directory} - Done!")

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
        assert set(sorted(logs)) == set(sorted(pods))

        # 2nd test: Verify logs file are not empty
        self.search_log_files(directory)

    def make_directory(self):
        """
        Check if directory to store must gather logs already exist
        and use new directory if so.

        Returns:
            str: Logs directory

        """
        index = 1
        directory = f"/tmp/mg_test{index}"
        while os.path.isdir(directory + "_ocs_logs"):
            index += 1
            directory = f"/tmp/mg_test{index}"

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
        pods_list = list()
        for a_pod in pods:
            if "example" not in a_pod:
                pods_list.append(a_pod)

        return pods_list

    def locate_pods_directory(self, root_directory):
        """

        Args:
            root_directory: (str):

        Returns:

        """
        for dir_name, subdir_list, files_list in os.walk(root_directory + "_ocs_logs"):
            if dir_name[-4:] == "pods":
                return dir_name

            else:
                logger.info("could not find \'pods\' directory")

    def search_log_files(self, directory):
        pods_dir = self.locate_pods_directory(directory)
        logger.info(f"pods dir {pods_dir}")
