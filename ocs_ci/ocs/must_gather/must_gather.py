import os
import logging
import shutil
import tempfile
import re
from pathlib import Path

from ocs_ci.ocs.resources.pod import get_all_pods
from ocs_ci.ocs.utils import collect_ocs_logs
from ocs_ci.ocs.must_gather.const_must_gather import (
    GATHER_COMMANDS_CEPH,
    GATHER_COMMANDS_JSON,
    GATHER_COMMANDS_OC_OUTPUT
)


logger = logging.getLogger(__name__)
TYPE_LOG = {
    'CEPH': GATHER_COMMANDS_CEPH,
    'JSON': GATHER_COMMANDS_JSON,
    'OC_OUTPUT': GATHER_COMMANDS_OC_OUTPUT
}


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
        return self.num_of_builds

    @log_type.setter
    def log_type(self, type_log):
        if not isinstance(type_log, str):
            raise ValueError('log type arg must be an string')
        self.type_log = type_log

    def collect_must_gather(self):
        """
        Collect ocs_must_gather and copy the logs to a temporary folder.

        """
        temp_folder = tempfile.mkdtemp()
        collect_ocs_logs(dir_name=temp_folder, ocp=False)
        self.root = temp_folder + '_ocs_logs'

    def search_file_path(self):
        """
        Search File Path

        """
        files = TYPE_LOG[self.type_log]
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
        for file, file_path in self.files_path.items():
            if not Path(file_path).is_file():
                self.files_not_exist.append(file)
            elif Path(file_path).stat().st_size == 0:
                self.empty_files.append(file)
            elif re.search(r'\.yaml$', file):
                with open(file_path, 'r') as f:
                    if 'kind' not in f.read().lower():
                        self.files_content_issue.append(file)

    def compare_running_pods(self):
        """
        Compare running pods list to "/pods" subdirectories

        """
        must_gather_helper = re.compile(r'must-gather-.*.-helper')
        pod_objs = get_all_pods(namespace='openshift-storage')
        pod_names = []
        for pod in pod_objs:
            if not must_gather_helper.match(pod.name):
                pod_names.append(pod.name)

        for dir_name, subdir_list, files_list in os.walk(self.root):
            if re.search('openshift-storage/pods$', dir_name):
                pod_path = dir_name

        pod_files = []
        for pod_file in os.listdir(pod_path):
            if not must_gather_helper.match(pod_file):
                pod_files.append(pod_file)

        assert set(sorted(pod_files)) == set(sorted(pod_names)), (
            f"List of openshift-storage pods are not equal to list of logs "
            f"directories list of pods: {pod_names} list of log directories: {pod_files}"
        )

    def cleanup(self):
        """
        Delete temporary folder.

        """
        if re.search('_ocs_logs$', self.temp_folder):
            shutil.rmtree(
                path=self.temp_folder,
                ignore_errors=False,
                onerror=None
            )

    def print_invalid_files(self):
        """
        Print Invalid Files

        """
        error = ''
        if len(self.empty_files) + len(self.files_not_exist) + len(
            self.files_content_issue
        ) != 0:
            for file in self.files_not_exist:
                error += f'file not exist:{file}\n'
            for file in self.empty_files:
                error += f'empty file:{file}\n'
            for file in self.files_content_issue:
                error += f'content issue:{file}\n'

            raise FileNotFoundError(error)

    def validate_must_gather(self):
        """
        Validate must_gather

        """
        self.collect_must_gather()
        self.validate_file_size()
        self.validate_expected_files()
        self.print_invalid_files()
        self.compare_running_pods()
