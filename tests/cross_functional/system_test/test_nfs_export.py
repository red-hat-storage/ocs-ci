"""
For all the ceph-nfs exported volumes user can write IO from within and outside the OCP cluster
The ongoing IO of NFS exports can continue without problems when the NFS pod hosting node is rebooted.
Data integrity is maintained when a chain of clone/snapshot/resize operations are performed
"""

import logging
import os
import socket
import time

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import skipif_ocs_version
from ocs_ci.framework.testlib import (
    E2ETest,
    skipif_ocp_version,
    skipif_managed_service,
    skipif_disconnected_cluster,
    skipif_proxy_cluster,
)

from ocs_ci.helpers import helpers
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.exceptions import (
    CommandFailed,
    ResourceWrongStatusException,
    ConfigurationError,
)
from ocs_ci.ocs.node import (
    wait_for_nodes_status,
    get_nodes,
)
from ocs_ci.ocs.resources import ocs, pod
from ocs_ci.ocs.resources.fips import check_fips_enabled
from ocs_ci.ocs.resources.pod import get_pod_node
from ocs_ci.ocs.resources.storage_cluster import (
    in_transit_encryption_verification,
    set_in_transit_encryption,
    get_in_transit_encryption_config_state,
)
from ocs_ci.utility import utils, nfs_utils
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import exec_cmd

log = logging.getLogger(__name__)
# Error message to look in a command output
ERRMSG = "Error in command"


@skipif_ocs_version("<4.11")
@skipif_ocp_version("<4.11")
@skipif_managed_service
@skipif_disconnected_cluster
@skipif_proxy_cluster
class TestNfsExport(E2ETest):
    """
    For all the ceph-nfs exported volumes user can write IO from within and and outside of the OCP cluster
    """

    def checks(self):
        """
        Fixture to verify cluster is with FIPS and hugepages enabled
        """

        try:
            check_fips_enabled()
        except Exception as FipsNotInstalledException:
            log.info(f"Handled prometheuous pod exception {FipsNotInstalledException}")

        nodes = get_nodes()
        for node in nodes:
            assert (
                node.get()["status"]["allocatable"]["hugepages-2Mi"] == "64Mi"
            ), f"Huge pages is not applied on {node.name}"

        if not get_in_transit_encryption_config_state():
            if config.ENV_DATA.get("in_transit_encryption"):
                pytest.fail(
                    "In-transit encryption is not enabled on the setup while it was supposed to be."
                )
            else:
                set_in_transit_encryption()
        log.info("Verifying the in-transit encryption is enable on setup.")
        assert (
            in_transit_encryption_verification()
        ), "In transit encryption verification failed"

    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Initialize Sanity instance

        """
        self.sanity_helpers = Sanity()

    @pytest.fixture(autouse=True)
    def setup_teardown(self, request):
        """
        Setup-Teardown for the nfs
        Steps:
        ---Setup---
        1:- Create objects for storage_cluster, configmap, pod, pv, pvc, service and storageclass
        2:- Enable nfs feature
        3:- Create loadbalancer service for nfs

        ---Teardown---
        1:- Disable nfs feature
        2:- Delete ocs nfs Service
        """
        log.info("Setup")
        self.storage_cluster_obj = ocp.OCP(
            kind="Storagecluster", namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        self.config_map_obj = ocp.OCP(
            kind="Configmap", namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        self.pod_obj = ocp.OCP(
            kind="Pod", namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        platform = config.ENV_DATA.get("platform", "").lower()
        self.nfs_sc = "ocs-storagecluster-ceph-nfs"
        self.sc = ocs.OCS(kind=constants.STORAGECLASS, metadata={"name": self.nfs_sc})
        self.run_id = config.RUN.get("run_id")
        self.test_folder = f"mnt/test_nfs_{self.run_id}"
        log.info(f"nfs mount point out of cluster is----- {self.test_folder}")
        self.nfs_client_ip = config.ENV_DATA.get("nfs_client_ip")
        log.info(f"nfs_client_ip is: {self.nfs_client_ip}")

        self.nfs_client_user = config.ENV_DATA.get("nfs_client_user")
        log.info(f"nfs_client_user is: {self.nfs_client_user}")

        self.nfs_client_private_key = os.path.expanduser(
            config.ENV_DATA.get("nfs_client_private_key")
            or config.DEPLOYMENT["ssh_key_private"]
        )
        # Enable nfs feature
        log.info("----Enable nfs----")
        nfs_ganesha_pod_name = nfs_utils.nfs_enable(
            self.storage_cluster_obj,
            self.config_map_obj,
            self.pod_obj,
            constants.OPENSHIFT_STORAGE_NAMESPACE,
        )

        if platform == constants.AWS_PLATFORM:
            # Create loadbalancer service for nfs
            self.hostname_add = nfs_utils.create_nfs_load_balancer_service(
                self.storage_cluster_obj,
            )
        yield

        log.info("-----Teardown-----")
        # Disable nfs feature
        nfs_utils.nfs_disable(
            self.storage_cluster_obj,
            self.config_map_obj,
            self.pod_obj,
            self.sc,
            nfs_ganesha_pod_name,
        )
        if platform == constants.AWS_PLATFORM:
            # Delete ocs nfs Service
            nfs_utils.delete_nfs_load_balancer_service(
                self.storage_cluster_obj,
            )

    def teardown(self):
        """
        Check if any nfs idle mount is available out of cluster
        and remove those.
        """
        if self.con:
            retcode, stdout, _ = self.con.exec_cmd(
                "findmnt -t nfs4 " + self.test_folder
            )
            if stdout:
                log.info("unmounting existing nfs mount")
                nfs_utils.unmount(self.con, self.test_folder)
            log.info("Delete mount point")
            _, _, _ = self.con.exec_cmd("rm -rf " + self.test_folder)

    def outcluster_nfs_export(self, nfs_pvc_obj, pod_obj):
        # Fetch sharing details for the nfs pvc
        fetch_vol_name_cmd = (
            "get pvc " + nfs_pvc_obj.name + " --output jsonpath='{.spec.volumeName}'"
        )
        vol_name = nfs_pvc_obj.exec_oc_cmd(fetch_vol_name_cmd)
        log.info(f"For pvc {nfs_pvc_obj.name} volume name is, {vol_name}")
        fetch_pv_share_cmd = (
            "get pv "
            + vol_name
            + " --output jsonpath='{.spec.csi.volumeAttributes.share}'"
        )
        share_details = nfs_pvc_obj.exec_oc_cmd(fetch_pv_share_cmd)
        log.info(f"Share details is, {share_details}")

        file_name = pod_obj.name
        # Run IO
        pod_obj.run_io(
            storage_type="fs",
            size="4G",
            fio_filename=file_name,
            runtime=60,
        )
        log.info("IO started on all pods")

        # Wait for IO completion
        fio_result = pod_obj.get_fio_results()
        log.info("IO completed on all pods")
        err_count = fio_result.get("jobs")[0].get("error")
        assert err_count == 0, (
            f"IO error on pod {pod_obj.name}. " f"FIO result: {fio_result}"
        )
        # Verify presence of the file
        file_path = pod.get_file_path(pod_obj, file_name)
        log.info(f"Actual file path on the pod {file_path}")
        assert pod.check_file_existence(
            pod_obj, file_path
        ), f"File {file_name} doesn't exist"
        log.info(f"File {file_name} exists in {pod_obj.name}")

        # Create /var/lib/www/html/index.html file inside the pod
        command = (
            "bash -c "
            + '"echo '
            + "'hello world'"
            + '  > /var/lib/www/html/index.html"'
        )
        pod_obj.exec_cmd_on_pod(
            command=command,
            out_yaml_format=False,
        )

        # Connect the external client using the share path and ingress address
        export_nfs_external_cmd = (
            "sudo mount -t nfs4 -o proto=tcp "
            + self.hostname_add
            + ":"
            + share_details
            + " "
            + self.test_folder
        )

        result = retry(
            (CommandFailed),
            tries=25,
            delay=10,
        )(utils.exec_cmd(cmd=export_nfs_external_cmd))
        assert result.returncode == 0

        # Verify able to read exported volume
        command = f"cat {self.test_folder}/index.html"
        result = utils.exec_cmd(cmd=command)
        stdout = result.stdout.decode().rstrip()
        log.info(stdout)
        assert stdout == "hello world"

        command = f"sudo chmod 666 {self.test_folder}/index.html"
        result = utils.exec_cmd(cmd=command)
        assert result.returncode == 0

        # Verify able to write to the exported volume
        command = (
            "bash -c "
            + '"echo '
            + "'test_writing'"
            + f'  >> {self.test_folder}/index.html"'
        )
        result = utils.exec_cmd(cmd=command)
        assert result.returncode == 0

        command = f"cat {self.test_folder}/index.html"
        result = utils.exec_cmd(cmd=command)
        stdout = result.stdout.decode().rstrip()
        log.info(stdout)
        assert stdout == "hello world" + """\n""" + "test_writing"

        # Able to read updated /var/lib/www/html/index.html file from inside the pod
        command = "bash -c " + '"cat ' + ' /var/lib/www/html/index.html"'
        result = pod_obj.exec_cmd_on_pod(
            command=command,
            out_yaml_format=False,
        )
        assert result.rstrip() == "hello world" + """\n""" + "test_writing"

        # unmount
        result = retry(
            (CommandFailed),
            tries=28,
            delay=10,
        )(utils.exec_cmd(cmd="sudo umount -l " + self.test_folder))
        assert result.returncode == 0

        # the NFS Client VM might not be healthy, so rebooting it and re-trying
        @property
        @retry((TimeoutError, socket.gaierror), tries=3, delay=60, backoff=1)
        def con(self):
            """
            Create connection to NFS Client VM, if not accessible, try to restart it.
            """
            if (
                not hasattr(self, "__nfs_client_connection")
                or not self.__nfs_client_connection
            ):
                try:
                    self.__nfs_client_connection = self.get_nfs_client_connection(
                        re_try=False
                    )
                except (TimeoutError, socket.gaierror):
                    nfs_client_vm_cloud = config.ENV_DATA.get("nfs_client_vm_cloud")
                    nfs_client_vm_name = config.ENV_DATA.get("nfs_client_vm_name")
                    if not nfs_client_vm_cloud or not nfs_client_vm_name:
                        raise ConfigurationError(
                            "NFS Client VM is not accessible and ENV_DATA nfs_client_vm_cloud and/or "
                            "nfs_client_vm_name parameters are not configured to be able to "
                            "automatically reboot the NFS Client VM."
                        )
                    cmd = f"openstack --os-cloud {nfs_client_vm_cloud} server reboot --hard --wait {nfs_client_vm_name}"
                    exec_cmd(cmd)

                    time.sleep(60)
                    self.__nfs_client_connection = self.get_nfs_client_connection()
            return self.__nfs_client_connection

    def test_nfs_export(
        self,
        pod_factory,
        nodes,
        snapshot_factory,
        snapshot_restore_factory,
        pvc_clone_factory,
    ):
        """
        This test is to validate NFS export using a PVC mounted on an app pod (in-cluster)

        """
        # incluster_nfs_export
        # Check cephnfs resource running
        log.info("Checking CephNFS resource status...")
        cephnfs_resource_status = self.storage_cluster_obj.exec_oc_cmd(
            "get CephNFS ocs-storagecluster-cephnfs --output jsonpath='{.status.phase}'"
        )
        assert cephnfs_resource_status == "Ready", "CephNFS resource is not ready"

        # Create nfs pvcs with storageclass ocs-storagecluster-ceph-nfs
        log.info("Creating NFS PVC")
        nfs_pvc_obj = helpers.create_pvc(
            sc_name=self.nfs_sc,
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
            size="5Gi",
            do_reload=True,
            access_mode=constants.ACCESS_MODE_RWO,
            volume_mode="Filesystem",
        )

        # Create nginx pod with nfs pvcs mounted
        pod_obj = pod_factory(
            interface=constants.CEPHFILESYSTEM,
            pvc=nfs_pvc_obj,
            status=constants.STATUS_RUNNING,
        )

        file_name = pod_obj.name
        # Run IO on pod
        pod_obj.run_io(
            storage_type="fs",
            size="4G",
            fio_filename=file_name,
            runtime=60,
        )
        log.info("IO started on all pods")
        # Wait for IO completion
        fio_result = pod_obj.get_fio_results()
        log.info("IO completed on all pods")
        err_count = fio_result.get("jobs")[0].get("error")
        assert err_count == 0, (
            f"IO error on pod {pod_obj.name}. " f"FIO result: {fio_result}"
        )
        file_path = pod.get_file_path(pod_obj, file_name)
        log.info(f"Actual file path on the pod {file_path}")

        # Restart pod node
        node = get_pod_node(pod_obj=pod_obj)
        nodes.restart_nodes(nodes=[node], wait=False)
        wait_for_nodes_status([node.name], constants.STATUS_READY, timeout=420)
        # Validate all nodes and services are in READY state and up
        retry(
            (CommandFailed, TimeoutError, AssertionError, ResourceWrongStatusException),
            tries=25,
            delay=15,
        )(ocp.wait_for_cluster_connectivity(tries=60))
        retry(
            (CommandFailed, TimeoutError, AssertionError, ResourceWrongStatusException),
            tries=25,
            delay=15,
        )(wait_for_nodes_status(timeout=1800))

        # Verify presence of the file
        assert pod.check_file_existence(
            pod_obj, file_path
        ), f"File {file_name} doesn't exist"
        log.info(f"File {file_name} exists in {pod_obj.name}")
        self.sanity_helpers.health_check(tries=120)

        # Test export consumed from outside the cluster
        self.outcluster_nfs_export(nfs_pvc_obj, pod_obj)

        # Take snapshot of PVCs
        log.info(f"Creating snapshot of PVC {nfs_pvc_obj.name}")
        snap_obj = snapshot_factory(nfs_pvc_obj, wait=False)
        snap_obj.md5sum = nfs_pvc_obj.md5sum
        log.info(f"Created snapshot of PVC {nfs_pvc_obj.name}")
        # Verify snapshots are ready
        log.info("Verify snapshots are ready")
        snap_obj.ocp.wait_for_resource(
            condition="true",
            resource_name=snap_obj.name,
            column=constants.STATUS_READYTOUSE,
            timeout=180,
        )
        snap_obj.reload()
        log.info("Verified: Snapshot is Ready")
        pod_obj_1 = pod_factory(
            interface=constants.CEPHFILESYSTEM, pvc=snap_obj, status=""
        )
        self.outcluster_nfs_export(snap_obj, pod_obj_1)

        # Clone PVCs
        log.info(f"Creating clone of PVC {snap_obj.name}")
        clone_obj = pvc_clone_factory(
            pvc_obj=snap_obj, status="", volume_mode=constants.VOLUME_MODE_FILESYSTEM
        )
        clone_obj.md5sum = snap_obj.md5sum
        log.info(f"Created clone of PVC {snap_obj.name}")

        log.info("Wait for cloned PVCs to reach Bound state and verify size")
        helpers.wait_for_resource_state(
            resource=clone_obj, state=constants.STATUS_BOUND, timeout=180
        )
        pod_obj_2 = pod_factory(
            interface=constants.CEPHFILESYSTEM, pvc=clone_obj, status=""
        )
        self.outcluster_nfs_export(clone_obj, pod_obj_2)

        pvc_size_expand = 8
        log.info(f"Expanding cloned and restored PVCs to {pvc_size_expand}GiB")
        for pvc_obj in [clone_obj, snap_obj]:
            log.info(
                f"Expanding size of PVC {pvc_obj.name} to "
                f"{pvc_size_expand}GiB from {pvc_obj.size}"
            )
            pvc_obj.resize_pvc(pvc_size_expand, True)
            log.info(
                f"Verified: Size of all cloned and restored PVCs are expanded to "
                f"{pvc_size_expand}GiB"
            )
            snap_obj_2 = snapshot_factory(pvc_obj, wait=False)
            pod_obj_3 = pod_factory(
                interface=constants.CEPHFILESYSTEM, pvc=snap_obj_2, status=""
            )
            self.outcluster_nfs_export(snap_obj_2, pod_obj_3)

        # Deletion of Pods and PVCs
        log.info("Deleting pod")
        pod_obj.delete()
        pod_obj.ocp.wait_for_delete(
            pod_obj.name, 180
        ), f"Pod {pod_obj.name} is not deleted"

        pv_obj = nfs_pvc_obj.backed_pv_obj
        log.info(f"pv object-----{pv_obj}")

        log.info("Deleting PVC")
        nfs_pvc_obj.delete()
        nfs_pvc_obj.ocp.wait_for_delete(
            resource_name=nfs_pvc_obj.name
        ), f"PVC {nfs_pvc_obj.name} is not deleted"
        log.info(f"Verified: PVC {nfs_pvc_obj.name} is deleted.")

        log.info("Check nfs pv is deleted")
        pv_obj.ocp.wait_for_delete(resource_name=pv_obj.name, timeout=180)
