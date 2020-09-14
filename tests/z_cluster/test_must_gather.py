import os
import logging
import re   # This is part of workaround for BZ-1766646, to be removed when fixed

import pytest

from ocs_ci.framework.testlib import ManageTest, tier1
from ocs_ci.ocs import ocp, constants
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.utils import collect_ocs_logs
from ocs_ci.utility.utils import ocsci_log_path, TimeoutSampler

logger = logging.getLogger(__name__)


@tier1
@pytest.mark.polarion_id("OCS-1583")
class TestMustGather(ManageTest):

    @pytest.fixture(autouse=True)
    def init_ocp(self):
        """
        init OCP() object
        """
        self.ocp_obj = ocp.OCP()

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        def check_for_must_gather_project():
            projects = ocp.OCP(kind='Namespace').get().get('items')
            namespaces = [each.get('metadata').get('name') for each in projects]
            logger.info(f"namespaces: {namespaces}")
            for project in namespaces:
                logger.info(f"project: {project}")
                if "openshift-must-gather" in project:
                    return project

            return False

        def check_for_must_gather_pod():
            must_gather_pods = pod.get_all_pods(
                selector_label='app=must-gather'
            )
            if must_gather_pods:
                logger.info(f"must_gather pods: {must_gather_pods}")
                logger.info("pod still exist")
                return True
            else:
                return False

        def finalizer():
            must_gather_pods = pod.get_all_pods(
                selector_label='app=must-gather'
            )
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
        # Fetch pod details
        pods = pod.get_all_pods(namespace='openshift-storage')
        pods = [each.name for each in pods]

        # Make logs root directory
        logger.info("Creating logs Directory")
        directory = self.make_directory()
        logger.info(f"Creating {directory}_ocs_logs - Done!")

        # Collect OCS logs
        logger.info("Collecting Logs")
        collect_ocs_logs(dir_name=directory, ocp=False)
        logger.info("Collecting logs - Done!")

        # Compare running pods list to "/pods" subdirectories
        must_gather_helper = re.compile(r'must-gather-.*.-helper')
        logger.info("Checking logs tree")
        logs = [
            logs for logs in self.get_log_directories(directory) if not (
                must_gather_helper.match(logs)
            )
        ]
        logger.info(f"Logs: {logs}")
        logger.info(f"pods list: {pods}")
        assert set(sorted(logs)) == set(sorted(pods)), (
            f"List of openshift-storage pods are not equal to list of logs "
            f"directories list of pods: {pods} list of log directories: {logs}"
        )

        # 2nd test: Verify logs file are not empty
        logs_dir_list = self.search_log_files(directory)
        assert self.check_file_size(logs_dir_list), (
            "One or more log file are empty"
        )

        # Find must_gather_commands directory for verification
        for dir_root, dirs, files in os.walk(directory + "_ocs_logs"):
            if os.path.basename(dir_root) == 'must_gather_commands':
                logger.info(
                    f"Found must_gather_commands directory - {dir_root}"
                )
                assert 'json_output' in dirs, (
                    "json_output directory is not present in "
                    "must_gather_commands directory."
                )
                assert files, (
                    "No files present in must_gather_commands directory."
                )
                cmd_files_path = [
                    os.path.join(dir_root, file_name) for file_name in files
                ]
                json_output_dir = os.path.join(dir_root, 'json_output')
                break

        # Verify that command output files are present as expected
        assert set(constants.MUST_GATHER_COMMANDS).issubset(files), (
            f"Actual and expected commands output files are not matching.\n"
            f"Actual: {files}\nExpected: {constants.MUST_GATHER_COMMANDS}"
        )
        if sorted(constants.MUST_GATHER_COMMANDS_JSON) != sorted(files):
            logger.warning(
                "There are more actual must gather commands than expected"
            )

        # Verify that files for command output in json are present as expected
        commands_json = os.listdir(json_output_dir)
        assert set(constants.MUST_GATHER_COMMANDS_JSON).issubset(commands_json), (
            f"Actual and expected json output commands files are not "
            f"matching.\nActual: {commands_json}\n"
            f"Expected: {constants.MUST_GATHER_COMMANDS_JSON}"
        )
        if sorted(constants.MUST_GATHER_COMMANDS_JSON) != sorted(commands_json):
            logger.warning(
                "There are more actual must gather commands than expected"
            )

        # Verify that command output files are not empty
        empty_files = []
        json_cmd_files_path = [
            os.path.join(json_output_dir, file_name) for file_name in commands_json
        ]
        for file_path in cmd_files_path + json_cmd_files_path:
            if not os.path.getsize(file_path) > 0:
                empty_files.append(file_path)
        assert not empty_files, f"These files are empty: {empty_files}"

    def make_directory(self):
        """
        Checks if directory that contains must gather logs already exist
        and use new directory if so.

        Returns:
            str: Logs directory

        """
        index = 1
        directory = ocsci_log_path()
        while os.path.isdir(directory + "_ocs_logs"):
            index += 1
            directory = ocsci_log_path() + f"_{index}"

        try:
            os.path.exists(directory)
            logger.info(f'Directory created successfully'
                        f'in path {directory}')
            return directory
        except FileNotFoundError:
            logger.error("Failed to create logs directory")
            raise

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

    def locate_pods_directory(self, root_directory):
        """
        Find full path of 'pods' subdirectory

        Args:
            root_directory: (str): location of must gather logs

        Returns:
            str: Full path of 'pods' subdirectory, if exist

        """
        for dir_name, subdir_list, files_list in os.walk(root_directory + "_ocs_logs"):
            logger.debug(f'dir_name: {dir_name}')
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
        # Workaround for BZ-1766646 Beginning:
        known_missing_logs = [
            re.compile(r'.*rook-ceph-osd-prepare-ocs-deviceset.*blkdevmapper'),
            re.compile(r'.*rook-ceph-osd-\d-.*blkdevmapper'),
            re.compile(r'.*rook-ceph-drain-canary-.*sleep')
        ]
        for log_dir in logs_dir_list:
            log_file = log_dir + "/current.log"
            if os.path.getsize(log_file) > 0:
                logger.info(f"file {log_file} size: {os.path.getsize(log_file)}")

            elif any(regex.match(log_file) for regex in known_missing_logs):
                logger.info(f"known issue: {log_file} is an empty log!")

            else:
                logger.info(f"log file {log_file} empty!")
                return False

        return True
