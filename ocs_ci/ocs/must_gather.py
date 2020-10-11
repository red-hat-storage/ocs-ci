import os
import logging
import shutil
import tempfile

from pathlib import Path
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.utils import collect_ocs_logs
from ocs_ci.ocs.const_must_gather import must_gather_files


logger = logging.getLogger(__name__)


class MustGather(object):
    """
    MustGather Class
    """
    def __init__(self):
        self.root = None
        self.pod_path = None
        self.temp_folder = None
        self.empty_files = list()
        self.files_not_exist = list()

    def collect_must_gather(self):
        """
        Collect ocs_must_gather and copy the logs to a temporary folder.

        """
        self.temp_folder = tempfile.mkdtemp()
        collect_ocs_logs(dir_name=self.temp_folder, ocp=False)
        self.temp_folder += '_ocs_logs'
        directory = os.path.join(self.temp_folder, 'ocs_must_gather')
        for i in range(2):
            folder = os.listdir(directory)
            self.root = os.path.join(directory, folder[0])

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
        for file in must_gather_files:
            file_path = os.path.join(self.root, file)
            if not Path(file_path).is_file():
                logger.error(f"log file {file} does not exist!")
                self.files_not_exist.append(file)
            elif Path(file_path).stat().st_size == 0:
                logger.error(f"log file {file} empty!")
                self.empty_files.append(file)

    def compare_running_pods(self):
        """
        Compare running pods list to "/pods" subdirectories

        """
        pods_obj = pod.get_all_pods(namespace='openshift-storage')
        pods_name = [pod.name for pod in pods_obj]
        for dir_name, subdir_list, files_list in os.walk(self.root):
            if dir_name[-22:] == "openshift-storage/pods":
                self.pod_path = dir_name

        pod_files = [pod_file for pod_file in os.listdir(self.pod_path)]

        assert set(sorted(pod_files)) == set(sorted(pods_name)), (
            f"List of openshift-storage pods are not equal to list of logs "
            f"directories list of pods: {pods_name} list of log directories: {pod_files}"
        )

    def cleanup(self):
        """
        Delete temporary folder.

        """
        if self.temp_folder[-9:] == '_ocs_logs':
            shutil.rmtree(
                path=self.temp_folder,
                ignore_errors=False,
                onerror=None
            )

    def print_invalid_files(self):
        """
        Print Invalid Files

        """
        if len(self.empty_files) > 0:
            for file in self.empty_files:
                logger.error(f'empty file:{file}')
        if len(self.files_not_exist) > 0:
            for file in self.files_not_exist:
                logger.error(f'empty file:{file}')

        if len(self.empty_files) == 0 or len(self.files_not_exist) == 0:
            raise ValueError

    def validate_must_gather(self):
        """
        Validate must_gather

        """
        self.collect_must_gather()
        self.validate_file_size()
        self.validate_expected_files()
        self.print_invalid_files()
        self.compare_running_pods()
