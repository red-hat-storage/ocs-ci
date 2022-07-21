import pytest
import logging

from ocs_ci.utility import utils, nfs_utils, version
from ocs_ci.ocs import constants, ocp
from ocs_ci.helpers import helpers
from ocs_ci.ocs.ocs_upgrade import run_ocs_upgrade
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    ManageTest,
    tier1,
    skipif_ocp_version,
    skipif_managed_service,
    skipif_disconnected_cluster,
    skipif_proxy_cluster,
)

# from ocs_ci.ocs.resources.pod import get_all_pods
from ocs_ci.ocs.resources import pod, ocs

from ocs_ci.utility.retry import retry
from ocs_ci.ocs.exceptions import CommandFailed


log = logging.getLogger(__name__)
# Error message to look in a command output
ERRMSG = "Error in command"
NAMESPACE = "openshift-storage"
STORAGE_CLUSTER_OBJ = ocp.OCP(kind="Storagecluster", namespace=NAMESPACE)
CONFIG_MAP_OBJ = ocp.OCP(kind="Configmap", namespace=NAMESPACE)
POD_OBJ = ocp.OCP(kind="Pod", namespace=NAMESPACE)
SERVICE_OBJ = ocp.OCP(kind="Service", namespace=NAMESPACE)
PVC_OBJ = ocp.OCP(kind=constants.PVC, namespace=NAMESPACE)
PV_OBJ = ocp.OCP(kind=constants.PV, namespace=NAMESPACE)
NFS_SC = "ocs-storagecluster-ceph-nfs"
SC = ocs.OCS(kind=constants.STORAGECLASS, metadata={"name": NFS_SC})
TEST_FOLDER = "test_nfs"


@tier1
@skipif_ocs_version("<4.10")
@skipif_ocp_version("<4.10")
@skipif_managed_service
@skipif_disconnected_cluster
@skipif_proxy_cluster
class TestNfsUnavailableInLowerODFversions(ManageTest):
    """
    Test nfs feature is not available for ODF cluster version below 4.11

    """

    def test_nfs_not_available_before_odf_4_11(self):
        """
        This test is to validate nfs feature is not available for ODF cluster version below 4.11

        Steps:
        1:- Check ocs version < 4.11
        2:- Checks cephnfs resources not available by default
        3:- Enable nfs feature for storage-cluster using patch command
        4:- Checks cephnfs resources not available
        5:- Upgrade OCS to >=4.11
        6:- Enable nfs feature
        7:- Check cephnfs resource running
        8:- Disable nfs feature

        """
        ocs_version = version.get_ocs_version_from_csv(
            only_major_minor=False, ignore_pre_release=True
        )
        log.info(f"odf version---- {ocs_version}")
        if ocs_version < version.VERSION_4_11:
            log.info("ODF version is <4.11")
        else:
            log.warning("ODF version is not <4.11")

        # Checks cephnfs resources not available by default
        cephnfs_resource = STORAGE_CLUSTER_OBJ.exec_oc_cmd("get cephnfs")
        if cephnfs_resource is None:
            log.info(f"No resources found in openshift-storage namespace.")
        else:
            log.error("nfs feature is enabled by default")

        nfs_spec_enable = '{"spec": {"nfs":{"enable": true}}}'

        # Enable nfs feature for storage-cluster using patch command
        assert STORAGE_CLUSTER_OBJ.patch(
            resource_name="ocs-storagecluster",
            params=nfs_spec_enable,
            format_type="merge",
        ), "storagecluster.ocs.openshift.io/ocs-storagecluster not patched"

        # Checks cephnfs resources not available
        cephnfs_resource = STORAGE_CLUSTER_OBJ.exec_oc_cmd("get cephnfs")
        if cephnfs_resource is None:
            log.info(f"No resources found in openshift-storage namespace.")
        else:
            log.error(f"nfs feature is enabled for ODF cluster version, {ocs_version}")

        # Upgrade OCS
        run_ocs_upgrade()
        ocs_version = version.get_ocs_version_from_csv(
            only_major_minor=False, ignore_pre_release=True
        )
        log.info(f"odf version---- {ocs_version}")
        if ocs_version > version.VERSION_4_10:
            log.info("ODF version is greater than 4.10")
        else:
            log.warning("ODF version is not greater than 4.10")

        # Enable nfs feature
        log.info("----Enable nfs----")
        nfs_ganesha_pod_name = nfs_utils.nfs_enable(
            STORAGE_CLUSTER_OBJ,
            CONFIG_MAP_OBJ,
            POD_OBJ,
            NAMESPACE,
        )

        # Check cephnfs resource running
        cephnfs_resource_status = STORAGE_CLUSTER_OBJ.exec_oc_cmd(
            "get CephNFS ocs-storagecluster-cephnfs --output jsonpath='{.status.phase}'"
        )
        assert cephnfs_resource_status == "Ready"

        # Disable nfs feature
        nfs_utils.nfs_disable(
            STORAGE_CLUSTER_OBJ,
            CONFIG_MAP_OBJ,
            POD_OBJ,
            SC,
            nfs_ganesha_pod_name,
        )


@tier1
@skipif_ocs_version("<4.11")
@skipif_ocp_version("<4.11")
@skipif_managed_service
@skipif_disconnected_cluster
@skipif_proxy_cluster
class TestNfsFeatureNegativeTests(ManageTest):
    """
    Test negative scenarios for nfs feature for ODF 4.11

    """

    @pytest.fixture(scope="class", autouse=True)
    def setup_teardown(self, request):
        """
        Setup-Teardown for the class

        Steps:
        ---Setup---
        1:- Create objects for storage_cluster, configmap, pod, pv, pvc, service and storageclass
        2:- Fetch number of cephfsplugin and cephfsplugin_provisioner pods running
        3:- Enable nfs feature
        4:- Create loadbalancer service for nfs
        ---Teardown---
        5:- Disable nfs feature
        6:- Delete ocs nfs Service

        """
        self = request.node.cls
        log.info("-----Setup-----")
        utils.exec_cmd(cmd="mkdir -p " + TEST_FOLDER)

        # Enable nfs feature
        log.info("----Enable nfs----")
        nfs_ganesha_pod_name = nfs_utils.nfs_enable(
            STORAGE_CLUSTER_OBJ,
            CONFIG_MAP_OBJ,
            POD_OBJ,
            NAMESPACE,
        )

        # Create loadbalancer service for nfs
        self.hostname_add = nfs_utils.create_nfs_load_balancer_service(
            STORAGE_CLUSTER_OBJ,
        )

        yield

        log.info("-----Teardown-----")
        # Disable nfs feature
        nfs_utils.nfs_disable(
            STORAGE_CLUSTER_OBJ,
            CONFIG_MAP_OBJ,
            POD_OBJ,
            SC,
            nfs_ganesha_pod_name,
        )
        # Delete ocs nfs Service
        nfs_utils.delete_nfs_load_balancer_service(
            STORAGE_CLUSTER_OBJ,
        )

        utils.exec_cmd(cmd="rm -rf " + TEST_FOLDER)

    def teardown(self):
        """
        Test tear down
        """
        rook_csi_config_enable = '{"data":{"ROOK_CSI_ENABLE_NFS": "true"}}'
        # Enable ROOK_CSI_ENABLE_NFS via patch request
        assert CONFIG_MAP_OBJ.patch(
            resource_name="rook-ceph-operator-config",
            params=rook_csi_config_enable,
            format_type="merge",
        ), "configmap/rook-ceph-operator-config not patched"

    def test_nfs_volumes_creation_deletion_for_nfs_plugin_pods_down(
        self,
        pod_factory,
    ):
        """
        This test is to validate creation and deletion of NFS volumes when NFS plugin pods are down

        Steps:
        1:- Create nfs pvcs with storageclass ocs-storagecluster-ceph-nfs
        2:- Create pods with nfs pvcs mounted
        3:- Run IO
        4:- Wait for IO completion
        5:- Verify presence of the file
        6:- Deletion of Pods and PVCs

        """
        rook_csi_config_disable = '{"data":{"ROOK_CSI_ENABLE_NFS": "false"}}'

        # Create nfs pvcs with storageclass ocs-storagecluster-ceph-nfs
        nfs_pvc_obj = helpers.create_pvc(
            sc_name=NFS_SC,
            namespace=NAMESPACE,
            size="5Gi",
            do_reload=True,
            access_mode=constants.ACCESS_MODE_RWO,
            volume_mode="Filesystem",
        )

        helpers.wait_for_resource_state(nfs_pvc_obj, constants.STATUS_BOUND)
        nfs_pvc_obj.reload()

        # Disable ROOK_CSI_ENABLE_NFS via patch request
        assert CONFIG_MAP_OBJ.patch(
            resource_name="rook-ceph-operator-config",
            params=rook_csi_config_disable,
            format_type="merge",
        ), "configmap/rook-ceph-operator-config not patched"

        # Create nfs pvcs with storageclass ocs-storagecluster-ceph-nfs
        nfs_pvc_obj_2 = helpers.create_pvc(
            sc_name=NFS_SC,
            namespace=NAMESPACE,
            size="5Gi",
            do_reload=True,
            access_mode=constants.ACCESS_MODE_RWO,
            volume_mode="Filesystem",
        )

        helpers.wait_for_resource_state(nfs_pvc_obj_2, constants.STATUS_PENDING)
        failure_str = """waiting for a volume to be created, either by external provisioner
         "openshift-storage.nfs.csi.ceph.com" or manually created by system administrator"""
        if failure_str in nfs_pvc_obj.describe():
            log.info(f"nfs pvc is in Pending state")
        else:
            log.error(f"nfs PVC mounted successfully")

        timeout = 120
        log.info("Deleting nfs PVC in Bound")
        nfs_pvc_obj.delete()
        assert helpers.wait_for_resource_state(
            nfs_pvc_obj.name, constants.STATUS_TERMINATING, timeout
        ), (
            f"The pvc {nfs_pvc_obj.name,constants} didn't reach the status {constants.STATUS_TERMINATING} "
            f"after {timeout} seconds"
        )

    def test_nfs_volumes_mount_unmount_incluster_for_nfs_plugin_pods_down(
        self,
        pod_factory,
    ):
        """
        This test is to validate NFS volumes mount/unmount for an in-cluster consumer when NFS plugin pods are down

        Steps:
        1:- Create nfs pvcs with storageclass ocs-storagecluster-ceph-nfs
        2:- Create pods with nfs pvcs mounted
        3:- Run IO
        4:- Wait for IO completion
        5:- Verify presence of the file
        6:- Deletion of Pods and PVCs

        """
        rook_csi_config_disable = '{"data":{"ROOK_CSI_ENABLE_NFS": "false"}}'

        # Create nfs pvcs with storageclass ocs-storagecluster-ceph-nfs
        nfs_pvc_obj = helpers.create_pvc(
            sc_name=NFS_SC,
            namespace=NAMESPACE,
            size="5Gi",
            do_reload=True,
            access_mode=constants.ACCESS_MODE_RWO,
            volume_mode="Filesystem",
        )

        helpers.wait_for_resource_state(nfs_pvc_obj, constants.STATUS_BOUND)
        nfs_pvc_obj.reload()

        # Create nginx pod with nfs pvcs mounted
        pod_obj = pod_factory(
            interface=constants.CEPHFILESYSTEM,
            pvc=nfs_pvc_obj,
            status=constants.STATUS_RUNNING,
        )

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

        # Disable ROOK_CSI_ENABLE_NFS via patch request
        assert CONFIG_MAP_OBJ.patch(
            resource_name="rook-ceph-operator-config",
            params=rook_csi_config_disable,
            format_type="merge",
        ), "configmap/rook-ceph-operator-config not patched"

        # Create nfs pvcs with storageclass ocs-storagecluster-ceph-nfs
        pod_obj_2 = pod_factory(
            interface=constants.CEPHFILESYSTEM,
            pvc=nfs_pvc_obj,
            status=constants.STATUS_CONTAINER_CREATING,
        )

        failure_str = "attachdetach-controller  AttachVolume.Attach failed for volume"
        if failure_str in pod_obj_2.describe():
            log.info(f"nfs volume mount failed")
        else:
            log.error(f"nfs volume mounted successfully")

    def test_nfs_volumes_mount_unmount_outcluster_for_nfs_plugin_pods_down(
        self,
        pod_factory,
    ):
        """
        This test is to validate NFS volumes mount/unmount for an out-cluster consumer when NFS plugin pods are down

        Steps:
        1:- Create nfs pvcs with storageclass ocs-storagecluster-ceph-nfs
        2:- Create pods with nfs pvcs mounted
        3:- Run IO
        4:- Wait for IO completion
        5:- Verify presence of the file
        6:- Deletion of Pods and PVCs

        """
        rook_csi_config_disable = '{"data":{"ROOK_CSI_ENABLE_NFS": "false"}}'

        # Create nfs pvcs with storageclass ocs-storagecluster-ceph-nfs
        nfs_pvc_obj = helpers.create_pvc(
            sc_name=NFS_SC,
            namespace=NAMESPACE,
            size="5Gi",
            do_reload=True,
            access_mode=constants.ACCESS_MODE_RWO,
            volume_mode="Filesystem",
        )

        helpers.wait_for_resource_state(nfs_pvc_obj, constants.STATUS_BOUND)
        nfs_pvc_obj.reload()

        # Create nginx pod with nfs pvcs mounted
        pod_obj = pod_factory(
            interface=constants.CEPHFILESYSTEM,
            pvc=nfs_pvc_obj,
            status=constants.STATUS_RUNNING,
        )

        file_name = pod_obj.name

        # Fetch sharing details for the nfs pvc
        fetch_vol_name_cmd = (
            "get pvc " + nfs_pvc_obj.name + " --output jsonpath='{.spec.volumeName}'"
        )
        vol_name = PVC_OBJ.exec_oc_cmd(fetch_vol_name_cmd)
        log.info(f"For pvc {nfs_pvc_obj.name} volume name is, {vol_name}")
        fetch_pv_share_cmd = (
            "get pv "
            + vol_name
            + " --output jsonpath='{.spec.csi.volumeAttributes.share}'"
        )
        share_details = PV_OBJ.exec_oc_cmd(fetch_pv_share_cmd)
        log.info(f"Share details is, {share_details}")

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
            + TEST_FOLDER
        )

        result = retry(
            (CommandFailed),
            tries=200,
            delay=10,
        )(utils.exec_cmd(cmd=export_nfs_external_cmd))
        assert result.returncode == 0

        # Verify able to read exported volume
        command = f"cat {TEST_FOLDER}/index.html"
        result = utils.exec_cmd(cmd=command)
        stdout = result.stdout.decode().rstrip()
        log.info(stdout)
        assert stdout == "hello world"

        # Disable ROOK_CSI_ENABLE_NFS via patch request
        assert CONFIG_MAP_OBJ.patch(
            resource_name="rook-ceph-operator-config",
            params=rook_csi_config_disable,
            format_type="merge",
        ), "configmap/rook-ceph-operator-config not patched"

        # Create nfs pvcs with storageclass ocs-storagecluster-ceph-nfs
        pod_obj_2 = pod_factory(
            interface=constants.CEPHFILESYSTEM,
            pvc=nfs_pvc_obj,
            status=constants.STATUS_CONTAINER_CREATING,
        )

        failure_str = "attachdetach-controller  AttachVolume.Attach failed for volume"
        if failure_str in pod_obj_2.describe():
            log.info(f"nfs volume mount failed")
        else:
            log.error(f"nfs volume mounted successfully")

        # Create /var/lib/www/html/index.html file inside the pod
        command = (
            "bash -c "
            + '"echo '
            + f"'I am pod,{pod_obj.name}'"
            + '  >> /var/lib/www/html/index.html"'
        )
        pod_obj.exec_cmd_on_pod(
            command=command,
            out_yaml_format=False,
        )

        # Verify able to read exported volume
        command = f"cat {TEST_FOLDER}/index.html"
        result = utils.exec_cmd(cmd=command)
        stdout = result.stdout.decode().rstrip()
        log.info(stdout)
        assert stdout == "hello world" + """\n""" + f"I am pod,{pod_obj.name}"

        # unmount
        result = retry(
            (CommandFailed),
            tries=300,
            delay=10,
        )(utils.exec_cmd(cmd="sudo umount -l " + TEST_FOLDER))
        assert result.returncode == 0
