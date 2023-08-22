import logging
import subprocess
import tempfile

from ocs_ci.ocs.resources.pod import get_ceph_tools_pod
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.framework.testlib import ManageTest, bugzilla
from subprocess import run, CalledProcessError

log = logging.getLogger(__name__)


@bugzilla(2067168)
class TestCephfsTop(ManageTest):
    """
    Check cephfs-top output

    """

    def teardown(self):
        """
        Make sure mgr stats disabled.
        """
        self.toolbox_pod.exec_ceph_cmd("ceph mgr module disable stats")
        output1 = self.toolbox_pod.exec_ceph_cmd("ceph mgr module ls")
        if "stats" in output1["enabled_modules"]:
            raise CommandFailed("mgr stats module is still enabled")
        log.info("mgr stats disabled")

    def capture_output_to_file(self, command, output_file):
        """
        Capture the output of command in a file.

        Args:
            command (str): command to run on pod
            output_file (str): file name
        """
        try:
            with open(output_file, "w") as f:
                run(command, stdout=f, stderr=subprocess.STDOUT, shell=True, check=True)
                log.info("Command executed successfully.")
        except CalledProcessError as e:
            log.info(f"Command failed with exit code {e.returncode}.")
        except Exception as e:
            log.info(f"Error: {e}")

    def test_cephfs_top_command(self):
        """
        Steps:
        1. Enable mgr stats.
        2. Check the stats in mgr ls.
        3. Check cephfs-top output for filesystems available.
        """

        # Enable mgr stats
        self.toolbox_pod = get_ceph_tools_pod()
        self.toolbox_pod.exec_ceph_cmd("ceph mgr module enable stats")

        # Check the stats in mgr ls
        output = self.toolbox_pod.exec_ceph_cmd("ceph mgr module ls")
        if "stats" not in output["enabled_modules"]:
            raise CommandFailed("mgr stats module is not enabled")

        # Check cephfs-top output
        toolbox_pod_name = self.toolbox_pod.name
        output_file_path = tempfile.NamedTemporaryFile(
            mode="w+", prefix="test_", suffix=".yaml", delete=False
        )
        output_file_path = output_file_path.name
        command = (
            f"oc -n openshift-storage rsh {toolbox_pod_name} "
            f"timeout 2m cephfs-top --conffile /etc/ceph/ceph.conf --id admin"
        )
        self.capture_output_to_file(command, output_file_path)

        assert (
            "Filesystem" in open(output_file_path).read()
        ), "cephfs-top output didn't have Filesystem"
        cephfilesystem_name = "ocs-storagecluster-cephfilesystem"

        assert (
            cephfilesystem_name in open(output_file_path).read()
        ), f"cephfs-top output doen't contain the {cephfilesystem_name}"
