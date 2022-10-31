import pytest
import logging

from ocs_ci.utility import metadata_utils
from ocs_ci.ocs import constants, ocp
from ocs_ci.helpers import helpers
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    ManageTest,
    tier1,
    skipif_ocp_version,
    skipif_managed_service,
    skipif_disconnected_cluster,
    skipif_proxy_cluster,
)

from ocs_ci.ocs.resources import pod


log = logging.getLogger(__name__)
# Error message to look in a command output
ERRMSG = "Error in command"


@tier1
@skipif_ocs_version("<4.12")
@skipif_ocp_version("<4.12")
@skipif_managed_service
@skipif_disconnected_cluster
@skipif_proxy_cluster
class TestDefaultMetadataDisabled(ManageTest):
    """
    Test metadata feature disabled by default for ODF 4.12

    """

    def test_metadata_not_enabled_by_default(self):
        """
        This test is to validate metadata feature is not enabled by default for  ODF(4.12) clusters

        Steps:
        1:- Check CSI_ENABLE_METADATA flag unavailable by default in rook-ceph-operator-config
        and setmetadata is unavailable for csi-cephfsplugin-provisioner and csi-rbdplugin-provisioner pods
        """
        config_map_obj = ocp.OCP(kind="Configmap", namespace="openshift-storage")
        pod_obj = ocp.OCP(kind="Pod", namespace="openshift-storage")

        # enable metadata flag not available by default
        metadata_flag = config_map_obj.exec_oc_cmd("get CSI_ENABLE_METADATA")
        log.info(f"metadata flag----{metadata_flag}")

        # Check 'setmatadata' is not set for csi-cephfsplugin-provisioner and csi-rbdplugin-provisioner pods
        plugin_provisioner_pod_objs = pod.get_all_pods(
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
            selector=["csi-cephfsplugin-provisioner", "csi-rbdplugin-provisioner"],
        )
        log.info(f"plugin provisioner pods-----{plugin_provisioner_pod_objs}")

        for plugin_provisioner_pod in plugin_provisioner_pod_objs:
            args = pod_obj.exec_oc_cmd(
                "get pod "
                + plugin_provisioner_pod.name
                + " --output jsonpath='{.spec.containers[4].args}'"
            )
        assert (
            "--setmetadata=true" not in args
        ), "'setmatadata' is set for csi-cephfsplugin-provisioner and csi-rbdplugin-provisioner pods"


@tier1
@skipif_ocs_version("<4.12")
@skipif_ocp_version("<4.12")
@skipif_managed_service
@skipif_disconnected_cluster
@skipif_proxy_cluster
class TestMetadataForCephfs(ManageTest):
    """
    This test class consists of tests to verify cephfs metadata for
    1. a newly created CephFS PVC
    2. Create a clone
    3. Create a volume snapshot
    4. Restore volume from snapshot
    """

    @pytest.fixture(scope="class", autouse=True)
    def setup_teardown(self, request, project_factory_class):
        """
        Setup-Teardown for the class

        Steps:
        ---Setup---
        1:- Create a project
        2:- Enable metadata feature
        ---Teardown---
        3:- Disable metadata feature

        """
        self = request.node.cls
        log.info("-----Setup-----")
        self.project_name = "test-metadata"
        project_factory_class(project_name=self.project_name)
        self.namespace = "openshift-storage"
        self.config_map_obj = ocp.OCP(kind="Configmap", namespace=self.namespace)
        self.pod_obj = ocp.OCP(kind="Pod", namespace=self.namespace)
        self.toolbox = pod.get_ceph_tools_pod()

        self.pvc_obj = ocp.OCP(kind=constants.PVC, namespace=self.namespace)
        self.pv_obj = ocp.OCP(kind=constants.PV, namespace=self.namespace)

        # Enable metadata feature
        log.info("----Enable metadata----")
        self.cluster_name = metadata_utils.enable_metadata(
            self.config_map_obj,
            self.pod_obj,
        )
        log.info(f"cluster name is ----- {self.cluster_name}")
        yield

        log.info("-----Teardown-----")
        # Disable metadata feature
        metadata_utils.disable_metadata(
            self.config_map_obj,
            self.pod_obj,
        )

    def test_verify_metadata_for_cephfs(self):
        """
        This test case verifies the cephfs metadata created on the subvolume for,
        1. a newly created CephFS PVC
        2. Create a clone
        3. Create a volume snapshot
        4. Restore volume from snapshot

        """
        cephfs_subvolumes = self.toolbox.exec_cmd_on_pod(
            f"ceph fs subvolume ls ocs-storagecluster-cephfilesystem --group_name csi"
        )
        log.info(f"available cephfs subvolumes-----{cephfs_subvolumes}")

        # Create pvc object
        pvc_obj = helpers.create_pvc(
            sc_name=constants.DEFAULT_STORAGECLASS_CEPHFS,
            namespace=self.project_name,
            do_reload=True,
            size="1Gi",
        )
        helpers.wait_for_resource_state(pvc_obj, constants.STATUS_BOUND, timeout=600)
        updated_cephfs_subvolumes = self.toolbox.exec_cmd_on_pod(
            f"ceph fs subvolume ls ocs-storagecluster-cephfilesystem --group_name csi"
        )
        log.info(f"updated cephfs subvolume-----{updated_cephfs_subvolumes}")
        for sv in updated_cephfs_subvolumes:
            if sv not in cephfs_subvolumes:
                created_cephfs_subvolume = sv
                log.info(
                    f"created cephfs subvolume-----{created_cephfs_subvolume['name']}"
                )
                break
        metadata = self.toolbox.exec_cmd_on_pod(
            f"ceph fs subvolume metadata ls ocs-storagecluster-cephfilesystem {created_cephfs_subvolume['name']}"
            + " --group_name=csi --format=json"
        )
        log.info(f"metadata----{metadata}")
        metadata_utils.validate_metadata(
            metadata=metadata,
            clustername=self.cluster_name,
            pv_name=pvc_obj.backed_pv_obj.name,
            pvc_name=pvc_obj.name,
            namespace=self.project_name,
        )

    def test_verify_metadata_for_rbd(self):
        """
        This test case verifies the metadata created for the rbd image for,
        1. a newly created RBD PVC
        2. Create a clone
        3. Create a volume snapshot
        4. Restore volume from snapshot

        """

        rbd_cephblockpool = self.toolbox.exec_cmd_on_pod(
            f"rbd ls ocs-storagecluster-cephblockpool"
        ).split()
        log.info(f"available rbd cephblockpool-----{rbd_cephblockpool}")

        # Create pvc object
        pvc_obj = helpers.create_pvc(
            sc_name=constants.DEFAULT_STORAGECLASS_RBD,
            namespace=self.project_name,
            do_reload=True,
            size="1Gi",
        )
        helpers.wait_for_resource_state(pvc_obj, constants.STATUS_BOUND, timeout=600)

        updated_rbd_cephblockpool = self.toolbox.exec_cmd_on_pod(
            f"rbd ls ocs-storagecluster-cephblockpool"
        ).split()
        log.info(f"updated rbd cephblockpool-----{updated_rbd_cephblockpool}")
        for sv in updated_rbd_cephblockpool:
            if sv not in rbd_cephblockpool:
                created_rbd_cephblockpool = sv
                log.info(f"created cephfs subvolume-----{created_rbd_cephblockpool}")
                break
        metadata = self.toolbox.exec_cmd_on_pod(
            f"rbd image-meta list ocs-storagecluster-cephblockpool/{created_rbd_cephblockpool}",
            out_yaml_format=False,
        )
        # metadata = {rbd_metadata[i]: rbd_metadata[i + 1] for i in range(0, len(rbd_metadata), 2)}

        log.info(f"metadata----{metadata}")
        metadata_utils.validate_metadata(
            metadata=metadata,
            clustername=self.cluster_name,
            pv_name=pvc_obj.backed_pv_obj.name,
            pvc_name=pvc_obj.name,
            namespace=self.project_name,
        )
