import os
import logging
import shutil
import tempfile
import re
from pathlib import Path

from ocs_ci.helpers.helpers import storagecluster_independent_check
from ocs_ci.ocs.resources.pod import get_all_pods, get_pod_node, get_pod_obj
from ocs_ci.ocs.utils import collect_ocs_logs, get_pod_name_by_pattern
from ocs_ci.ocs.must_gather.const_must_gather import GATHER_COMMANDS_VERSION
from ocs_ci.ocs.ocp import get_ocs_parsed_version
from ocs_ci.ocs.constants import OPENSHIFT_STORAGE_NAMESPACE, must_gather_pod_label
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.ocs.exceptions import TimeoutExpiredError


logger = logging.getLogger(__name__)


class MustGather(object):
    """
    MustGather Class

    """

    def __init__(self):
        self.type_log = None
        self.root = None
        self.files_path = dict()
        self.empty_files = list()
        self.files_not_exist = list()
        self.files_content_issue = list()

    @property
    def log_type(self):
        return self.type_log

    @log_type.setter
    def log_type(self, type_log):
        if not isinstance(type_log, str):
            raise ValueError("log type arg must be a string")
        self.type_log = type_log

    def collect_must_gather(self):
        """
        Collect ocs_must_gather and copy the logs to a temporary folder.

        """
        temp_folder = tempfile.mkdtemp()
        collect_ocs_logs(dir_name=temp_folder, ocp=False)
        self.root = temp_folder + "_ocs_logs"

    def search_file_path(self):
        """
        Search File Path

        """
        version = get_ocs_parsed_version()
        if self.type_log == "OTHERS" and storagecluster_independent_check():
            files = GATHER_COMMANDS_VERSION[version]["OTHERS_EXTERNAL"]
        else:
            files = GATHER_COMMANDS_VERSION[version][self.type_log]
        for file in files:
            self.files_not_exist.append(file)
            for dir_name, subdir_list, files_list in os.walk(self.root):
                if file in files_list:
                    self.files_path[file] = os.path.join(dir_name, file)
                    self.files_not_exist.remove(file)
                    break

    def validate_file_size(self):
        """
        Validate the file is not empty

        """
        for path, subdirs, files in os.walk(self.root):
            for file in files:
                file_path = os.path.join(path, file)
                if Path(file_path).stat().st_size == 0:
                    logger.error(f"log file {file} empty!")
                    self.empty_files.append(file)

    def validate_expected_files(self):
        """
        Make sure all the relevant files exist

        """
        self.search_file_path()
        self.verify_noobaa_diagnostics()
        for file, file_path in self.files_path.items():
            if not Path(file_path).is_file():
                self.files_not_exist.append(file)
            elif Path(file_path).stat().st_size == 0:
                self.empty_files.append(file)
            elif re.search(r"\.yaml$", file):
                with open(file_path, "r") as f:
                    if "kind" not in f.read().lower():
                        self.files_content_issue.append(file)

    def compare_running_pods(self):
        """
        Compare running pods list to "/pods" subdirectories

        """
        if self.type_log != "OTHERS":
            return
        pod_objs = get_all_pods(namespace=OPENSHIFT_STORAGE_NAMESPACE)
        pod_names = []
        logging.info("Get pod names on openshift-storage project")
        for pod in pod_objs:
            pattern = self.check_pod_name_pattern(pod.name)
            if pattern is False:
                pod_names.append(pod.name)

        for dir_name, subdir_list, files_list in os.walk(self.root):
            if re.search("openshift-storage/pods$", dir_name):
                pod_path = dir_name
                break

        pod_files = []
        logging.info("Get pod names on openshift-storage/pods directory")
        for pod_file in os.listdir(pod_path):
            pattern = self.check_pod_name_pattern(pod_file)
            if pattern is False:
                pod_files.append(pod_file)

        diff = list(set(pod_files) - set(pod_names)) + list(
            set(pod_names) - set(pod_files)
        )
        assert set(sorted(pod_files)) == set(sorted(pod_names)), (
            f"List of openshift-storage pods are not equal to list of logs\n"
            f"directories list of pods: {pod_names} list of log directories: {pod_files}\n"
            f"The difference between pod files and actual pods is: {diff}\n"
        )

    def check_pod_name_pattern(self, pod_name):
        """
        check Pod Name Pattern

        Args:
            pod_name (str): pod name

        return:
            bool: True if match pattern, False otherwise

        """
        regular_ex_list = [
            "must-gather-.*.-helper",
            r"compute-*",
            r"ip-*",
            r"j-*",
            r"argo-*",
            r"vmware-*",
        ]
        for regular_ex in regular_ex_list:
            if re.match(regular_ex, pod_name) is not None:
                return True
        return False

    def print_invalid_files(self):
        """
        Print Invalid Files

        """
        if any([self.empty_files, self.files_not_exist, self.files_content_issue]):
            error = (
                f"Files don't exist:\n{self.files_not_exist}\n"
                f"Empty files:\n{self.empty_files}\n"
                f"Content issues:\n{self.files_content_issue}"
            )
            self.empty_files = list()
            self.files_not_exist = list()
            self.files_content_issue = list()
            raise Exception(error)

    def verify_noobaa_diagnostics(self):
        """
        Verify noobaa diagnostics folder exist

        """
        if self.type_log == "OTHERS" and get_ocs_parsed_version() >= 4.6:
            flag = False
            logger.info("Verify noobaa_diagnostics folder exist")
            for path, subdirs, files in os.walk(self.root):
                for file in files:
                    if re.search(r"noobaa_diagnostics_.*.tar.gz", file):
                        flag = True
            if not flag:
                logger.error("noobaa_diagnostics.tar.gz does not exist")
                self.files_not_exist.append("noobaa_diagnostics.tar.gz")

    def get_must_gather_pod(self):
        """

        :return:
        """
        self.mg_pod = get_pod_name_by_pattern(
            pattern=must_gather_pod_label, namespace=OPENSHIFT_STORAGE_NAMESPACE
        )
        if len(self.mg_pod) > 0:
            return True
        else:
            return False

    def restart_node_where_must_gather_pod_running(self, nodes):
        """
        Restart node where must-gather pod running


        """
        sample = TimeoutSampler(
            timeout=20,
            sleep=2,
            func=self.get_must_gather_pod,
        )
        if not sample.wait_for_func_status(result=True):
            raise TimeoutExpiredError("must gather pod does not found after 20 seconds")

        logger.info(f"Find the worker node where the {self.mg_pod} is running")
        mg_pod_obj = get_pod_obj(self.mg_pod[0], namespace=OPENSHIFT_STORAGE_NAMESPACE)
        node = get_pod_node(mg_pod_obj)
        logger.info(f"Stop and start the worker node: {node}")
        nodes.restart_nodes_by_stop_and_start([node])

    def validate_must_gather(self):
        """
        Validate must-gather

        """
        self.validate_file_size()
        self.validate_expected_files()
        self.print_invalid_files()
        self.compare_running_pods()

    def cleanup(self):
        """
        Delete temporary folder.

        """
        logger.info(f"Delete must gather folder {self.root}")
        if re.search("_ocs_logs$", self.root):
            shutil.rmtree(path=self.root, ignore_errors=False, onerror=None)
