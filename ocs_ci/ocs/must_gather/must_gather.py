import os
import logging
import shutil
import tempfile
import re
import tarfile
from pathlib import Path

from ocs_ci.framework import config
from ocs_ci.helpers.helpers import storagecluster_independent_check
from ocs_ci.ocs.resources.pod import get_all_pods
from ocs_ci.ocs.utils import collect_ocs_logs
from ocs_ci.ocs.must_gather.const_must_gather import (
    GATHER_COMMANDS_VERSION,
    GATHER_COMMANDS_LOG,
)
from ocs_ci.utility import version
from ocs_ci.ocs.constants import (
    OPENSHIFT_STORAGE_NAMESPACE,
    MANAGED_SERVICE_PLATFORMS,
)


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
        self.ocs_version = version.get_semantic_ocs_version_from_config()

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
        ocs_version = float(
            f"{version.get_ocs_version_from_csv(only_major_minor=True)}"
        )
        if (
            self.type_log == "OTHERS"
            and config.ENV_DATA["platform"] in MANAGED_SERVICE_PLATFORMS
        ):
            files = GATHER_COMMANDS_VERSION[ocs_version]["OTHERS_MANAGED_SERVICES"]
        elif self.type_log == "OTHERS" and storagecluster_independent_check():
            files = GATHER_COMMANDS_VERSION[ocs_version]["OTHERS_EXTERNAL"]
        else:
            files = GATHER_COMMANDS_VERSION[ocs_version][self.type_log]
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
        if self.type_log != "OTHERS":
            return
        for path, subdirs, files in os.walk(self.root):
            for file in files:
                file_path = os.path.join(path, file)
                if (
                    Path(file_path).stat().st_size == 0
                    and "noobaa-db-pg-0-init.log" not in file_path
                ):
                    logger.error(f"log file {file} empty!")
                    self.empty_files.append(file)

    def validate_expected_files(self):
        """
        Make sure all the relevant files exist

        """
        self.search_file_path()
        self.verify_ceph_file_content()
        for file, file_path in self.files_path.items():
            if not Path(file_path).is_file():
                self.files_not_exist.append(file)
            elif re.search(r"\.yaml$", file):
                with open(file_path, "r") as f:
                    if "kind" not in f.read().lower():
                        self.files_content_issue.append(file)

    def verify_ceph_file_content(self):
        """
        Verify ceph command does not return an error
        https://bugzilla.redhat.com/show_bug.cgi?id=2014849
        https://bugzilla.redhat.com/show_bug.cgi?id=2021427

        """
        if self.type_log != "CEPH" or self.ocs_version < version.VERSION_4_9:
            return
        pattern = re.compile("exit code [1-9]+")
        for root, dirs, files in os.walk(self.root):
            for file in files:
                try:
                    with open(os.path.join(root, file), "r") as f:
                        data_file = f.read()
                    exit_code_error = pattern.findall(data_file.lower())
                    if len(exit_code_error) > 0 and "gather-debug" not in file:
                        self.files_content_issue.append(os.path.join(root, file))
                except Exception as e:
                    logger.error(f"There is no option to read {file}, error: {e}")

    def print_must_gather_debug(self) -> None:
        try:
            with open(os.path.join(self.root, GATHER_COMMANDS_LOG), "r") as f:
                logger.info(f.readlines())
        except FileNotFoundError:
            logger.error("File not found")

    def compare_running_pods(self):
        """
        Compare running pods list to "/pods" subdirectories

        """
        if self.type_log != "OTHERS":
            return
        pod_objs = get_all_pods(namespace=OPENSHIFT_STORAGE_NAMESPACE)
        pod_names = []
        logger.info("Get pod names on openshift-storage project")
        for pod in pod_objs:
            pattern = self.check_pod_name_pattern(pod.name)
            if pattern is False:
                pod_names.append(pod.name)

        for dir_name, subdir_list, files_list in os.walk(self.root):
            if re.search("openshift-storage/pods$", dir_name):
                pod_path = dir_name
                break

        pod_files = []
        logger.info("Get pod names on openshift-storage/pods directory")
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
            r"^compute-*",
            r"^ip-*",
            r"^j-*",
            r"^argo-*",
            r"^vmware-*",
            "^must-gather",
            r"-debug$",
        ]
        for regular_ex in regular_ex_list:
            if re.search(regular_ex, pod_name) is not None:
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
        ocs_version = version.get_ocs_version_from_csv(only_major_minor=True)
        if self.type_log == "OTHERS" and ocs_version >= version.VERSION_4_6:
            flag = False
            logger.info("Verify noobaa_diagnostics folder exist")
            for path, subdirs, files in os.walk(self.root):
                for file in files:
                    if re.search(r"noobaa_diagnostics_.*.tar.gz", file):
                        flag = True
                        logger.info(f"Extract noobaa_diagnostics dir {file}")
                        path_noobaa_diag = os.path.join(path, file)
                        files_noobaa_diag = tarfile.open(path_noobaa_diag)
                        files_noobaa_diag.extractall(path)
                        break
            if not flag:
                logger.error("noobaa_diagnostics.tar.gz does not exist")
                self.files_not_exist.append("noobaa_diagnostics.tar.gz")

    def validate_must_gather(self):
        """
        Validate must-gather

        """
        if config.ENV_DATA["platform"] not in MANAGED_SERVICE_PLATFORMS:
            self.verify_noobaa_diagnostics()
        self.validate_file_size()
        self.validate_expected_files()
        self.print_invalid_files()
        self.compare_running_pods()
        self.print_must_gather_debug()

    def cleanup(self):
        """
        Delete temporary folder.

        """
        logger.info(f"Delete must gather folder {self.root}")
        if re.search("_ocs_logs$", self.root):
            shutil.rmtree(path=self.root, ignore_errors=False, onerror=None)
