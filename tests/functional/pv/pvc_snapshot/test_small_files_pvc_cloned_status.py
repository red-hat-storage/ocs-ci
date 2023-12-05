import logging
import os
import urllib.request
import pytest

from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import (
    E2ETest,
    tier2,
    polarion_id,
    bugzilla,
)

log = logging.getLogger(__name__)


@green_squad
@tier2
@polarion_id("OCS-3946")
class TestPvcClonedStatusOfSmallSizeFiles(E2ETest):
    """
    Tests to verify restored PVC status bound or not if
    files size are smaller.
    """

    pvc_size = 1
    pod_path = "/mnt"
    linux_tar = "linux-4.4.tar.xz"

    @pytest.fixture()
    def pvc(self, pvc_factory_class):
        # Create a RWX Cephfs PVC
        self.pvc_obj = pvc_factory_class(
            interface=constants.CEPHFILESYSTEM,
            access_mode=constants.ACCESS_MODE_RWX,
            size=self.pvc_size,
        )

    @pytest.fixture()
    def pod(self, pod_factory_class):
        self.pod_obj = pod_factory_class(
            pvc=self.pvc_obj,
            pod_dict_path=constants.PERF_POD_YAML,
        )

    @pytest.fixture()
    def copy_files(self):
        kernel_url = f"https://cdn.kernel.org/pub/linux/kernel/v4.x/{self.linux_tar}"
        download_path = "tmp"
        dir_path = os.path.join(os.getcwd(), download_path)
        file_path = os.path.join(dir_path, self.linux_tar)
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
        urllib.request.urlretrieve(kernel_url, file_path)

        pod_name = self.pod_obj.name
        ocp = OCP(namespace=self.pvc_obj.namespace)

        rsh_cmd = f"cp {dir_path} {pod_name}:{self.pod_path}"
        ocp.exec_oc_cmd(rsh_cmd)

        rsh_cmd = f"tar xvf {self.pod_path}/tmp/{self.linux_tar} -C {self.pod_path}/tmp"
        self.pod_obj.exec_sh_cmd_on_pod(command=rsh_cmd)

        rsh_cmd = f"ls -laR {self.pod_path}/tmp/linux*|wc -l"
        self.original_total_files = self.pod_obj.exec_sh_cmd_on_pod(command=rsh_cmd)
        log.info(f"original_total_files: {self.original_total_files}")

    @pytest.fixture()
    def create_snapshot(self, snapshot_factory):
        log.info(f"Creating snapshot from {self.pvc_obj.name}")
        self.snapshot = snapshot_factory(
            self.pvc_obj, snapshot_name=f"{self.pvc_obj.name}--snapshot"
        )

    @pytest.fixture()
    def snapshot_restore(self, snapshot_restore_factory):
        log.info(f"Creating restore from snapshot {self.snapshot.name}")
        self.pvc_restore = snapshot_restore_factory(
            snapshot_obj=self.snapshot,
            restore_pvc_name=f"{self.pvc_obj.name}--restore",
            storageclass=self.pvc_obj.backed_sc,
            size=str(self.pvc_size * 1024 * 1024 * 1024),
            restore_pvc_yaml=constants.CSI_CEPHFS_PVC_RESTORE_YAML,
            access_mode=constants.ACCESS_MODE_RWX,
            timeout=960,
        )

    @bugzilla("2039265")
    def test_pvc_status_after_clone(
        self, pvc, pod, copy_files, create_snapshot, snapshot_restore, pod_factory
    ):
        """
        1. Create 1GiB PVC
        2. Attach PVC to an application pod
        3. Download Linux kernel and untar it to the directory where PVC is mounted
        4. Take a snapshot of the PVC.
        5. Create a new PVC out of that snapshot.
        6. Verify PVC state
        7. Attach pod to restored pvc

        Args:
            pvc: A fixture to create new pvc
            pod: A fixture to create new pod
            copy_files: Copy linux.tar to pod
            create_snapshot: A fixture to create new snapshot
            snapshot_restore: A fixture to restore pvc from snapshot
        """

        log.info(f"Attaching pod to pvc restore {self.pvc_restore.name}")
        restored_pod_obj = pod_factory(
            pvc=self.pvc_restore, pod_dict_path=constants.PERF_POD_YAML
        )

        command = f"ls -laR {self.pod_path}/tmp/linux*|wc -l"
        restored_total_files = restored_pod_obj.exec_sh_cmd_on_pod(command=command)
        log.info(f"restored_total_files: {restored_total_files}")

        log.info(f"original_total_files: {self.original_total_files}")
        assert (
            self.original_total_files == restored_total_files
        ), f"Total number of files present in {self.pod_obj} are not same as {restored_pod_obj}"
