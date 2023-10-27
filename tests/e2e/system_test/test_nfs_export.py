"""
For all the ceph-nfs exported volumes user can write IO from within and and outside of the OCP cluster
The on-going IO of NFS exports can continue without problems when the NFS pod hosting node is rebooted.
Data integrity is maintained when a chain of clone/snapshot/resize operations are performed
"""
import pytest
import logging
import time

from ocs_ci.utility import utils, nfs_utils
from ocs_ci.ocs import ocp
from ocs_ci.helpers import helpers
from ocs_ci.framework.testlib import (
    skipif_ocp_version,
    skipif_managed_service,
    skipif_disconnected_cluster,
    skipif_proxy_cluster,
)

from ocs_ci.ocs.resources import ocs
from ocs_ci.utility.retry import retry
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources import pod
from ocs_ci.framework.testlib import E2ETest
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import skipif_ocs_version
from ocs_ci.ocs.resources.storage_cluster import (
    in_transit_encryption_verification,
    set_in_transit_encryption,
    get_in_transit_encryption_config_state,
)
from ocs_ci.ocs.resources.pod import get_pod_node
from ocs_ci.ocs.node import (
    wait_for_nodes_status,
    drain_nodes,
    unschedule_nodes,
    schedule_nodes,
)


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

    @pytest.fixture(autouse=True)
    def checks(self):
        # This test is skipped due to https://issues.redhat.com/browse/ENTMQST-3422
        """
        try:
            check_fips_enabled()
        except Exception as e:
            logger.info(f"Handled prometheuous pod exception {e}")

        nodes = get_nodes()
        for node in nodes:
            assert (
                node.get()["status"]["allocatable"]["hugepages-2Mi"] == "64Mi"
            ), f"Huge pages enabled on {node.name}"

        """
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

    @pytest.fixture(scope="class", autouse=True)
    def setup(self, request):
        """
        Setup-Teardown for the class

        Steps:
        ---Setup---
        1:- Create objects for storage_cluster, configmap, pod, pv, pvc, service and storageclass
        2:- Fetch number of cephfsplugin and cephfsplugin_provisioner pods running
        3:- Enable nfs feature
        4:- Create loadbalancer service for nfs
        5:- Create snapshot and test nfs export
        6:- Create clone and test nfs export
        7:-
        8:-
        ---Teardown---
        1:- Disable nfs feature
        2:- Delete ocs nfs Service

        """
        self = request.node.cls
        log.info("Setup")
        self.namespace = constants.OPENSHIFT_STORAGE_NAMESPACE
        self.storage_cluster_obj = ocp.OCP(
            kind="Storagecluster", namespace=self.namespace
        )
        self.config_map_obj = ocp.OCP(kind="Configmap", namespace=self.namespace)
        self.pod_obj = ocp.OCP(kind="Pod", namespace=self.namespace)
        self.nfs_sc = "ocs-storagecluster-ceph-nfs"
        self.sc = ocs.OCS(kind=constants.STORAGECLASS, metadata={"name": self.nfs_sc})
        # Enable nfs feature
        log.info("Enable nfs")
        self.nfs_ganesha_pod_name = nfs_utils.nfs_enable(
            self.storage_cluster_obj,
            self.config_map_obj,
            self.pod_obj,
            self.namespace,
        )

        log.info("Teardown")

    def teardown(self):
        # Disable nfs feature
        nfs_utils.nfs_disable(
            self.storage_cluster_obj,
            self.config_map_obj,
            self.pod_obj,
            self.sc,
            self.nfs_ganesha_pod_name,
        )

    def outcluster_nfs_export(self, nfs_pvc_obj, pod_obj):
        # Fetch sharing details for the nfs pvc
        fetch_vol_name_cmd = (
            "get pvc " + nfs_pvc_obj.name + " --output jsonpath='{.spec.volumeName}'"
        )
        vol_name = self.pvc_obj.exec_oc_cmd(fetch_vol_name_cmd)
        log.info(f"For pvc {nfs_pvc_obj.name} volume name is, {vol_name}")
        fetch_pv_share_cmd = (
            "get pv "
            + vol_name
            + " --output jsonpath='{.spec.csi.volumeAttributes.share}'"
        )
        share_details = self.pv_obj.exec_oc_cmd(fetch_pv_share_cmd)
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
            tries=200,
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
            tries=300,
            delay=10,
        )(utils.exec_cmd(cmd="sudo umount -l " + self.test_folder))
        assert result.returncode == 0

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

        Steps:
        1:- Create nfs pvcs with storageclass ocs-storagecluster-ceph-nfs
        2:- Create pods with nfs pvcs mounted
        3:- Run IO
        4:- Wait for IO completion
        5:- Verify presence of the file
        6:- Deletion of Pods and PVCs

        """
        # Create nfs pvcs with storageclass ocs-storagecluster-ceph-nfs
        nfs_pvc_obj = helpers.create_pvc(
            sc_name=self.nfs_sc,
            namespace=self.namespace,
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

        # Restart pod node
        node = get_pod_node(pod_obj=self.pod_obj)
        node_name = get_pod_node(pod_obj=self.pod_obj).name
        self.sanity_helpers.health_check(cluster_check=False, tries=60)

        unschedule_nodes([node_name])
        drain_nodes([node_name])
        nodes.restart_nodes([node], wait=False)
        waiting_time = 30
        log.info(f"Waiting for {waiting_time} seconds")
        time.sleep(waiting_time)
        schedule_nodes([node_name])
        wait_for_nodes_status(status=constants.NODE_READY, timeout=180)

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
