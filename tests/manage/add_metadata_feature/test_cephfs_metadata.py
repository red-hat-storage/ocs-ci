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
        metadata_flag = config_map_obj.exec_oc_cmd(
            "get cm rook-ceph-operator-config --output  jsonpath='{.data.CSI_ENABLE_METADATA}'"
        )
        log.info(f"metadata flag----{metadata_flag}")
        if metadata_flag is None:
            log.info("metadata feature is not enabled by default.")
        else:
            log.error("metadata feature is enabled by default")

        # Check 'setmatadata' is not set for csi-cephfsplugin-provisioner and csi-rbdplugin-provisioner pods
        res = metadata_utils.check_setmetadata_availability(pod_obj)
        if res:
            raise AssertionError


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
        self.pv_obj = ocp.OCP(kind=constants.PV, namespace=self.namespace)
        self.toolbox = pod.get_ceph_tools_pod()

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

    @pytest.mark.parametrize(
        argnames=["fs", "sc_name"],
        argvalues=[
            pytest.param(
                "ocs-storagecluster-cephfilesystem",
                constants.DEFAULT_STORAGECLASS_CEPHFS,
            ),
            pytest.param(
                "ocs-storagecluster-cephblockpool", constants.DEFAULT_STORAGECLASS_RBD
            ),
        ],
    )
    def test_verify_metadata_details(
        self, pvc_clone_factory, snapshot_factory, snapshot_restore_factory, fs, sc_name
    ):
        """
        This test case verifies the cephfs metadata created on the subvolume for,
        1. a newly created CephFS PVC
        2. Create a clone
        3. Create a volume snapshot
        4. Restore volume from snapshot

        """
        available_subvolumes = metadata_utils.available_subvolumes(
            sc_name, self.toolbox, fs
        )
        # Create pvc object
        pvc_obj = helpers.create_pvc(
            sc_name=sc_name,
            namespace=self.project_name,
            do_reload=True,
            size="1Gi",
        )
        helpers.wait_for_resource_state(pvc_obj, constants.STATUS_BOUND, timeout=600)

        updated_subvolumes = metadata_utils.available_subvolumes(
            sc_name, self.toolbox, fs
        )

        created_subvolume = metadata_utils.created_subvolume(
            available_subvolumes, updated_subvolumes, sc_name
        )

        metadata = metadata_utils.fetch_metadata(
            sc_name, fs, self.toolbox, created_subvolume
        )
        # metadata validation for new PVC created
        metadata_utils.validate_metadata(
            metadata=metadata,
            clustername=self.cluster_name,
            pv_name=pvc_obj.backed_pv_obj.name,
            pvc_name=pvc_obj.name,
            namespace=self.project_name,
        )
        available_subvolumes = updated_subvolumes

        # Clone the PVC
        clone_pvc_obj = pvc_clone_factory(pvc_obj=pvc_obj)
        updated_subvolumes = metadata_utils.available_subvolumes(
            sc_name, self.toolbox, fs
        )
        created_subvolume = metadata_utils.created_subvolume(
            available_subvolumes, updated_subvolumes, sc_name
        )
        metadata = metadata_utils.fetch_metadata(
            sc_name, fs, self.toolbox, created_subvolume
        )

        # metadata validation for cloned PVC
        metadata_utils.validate_metadata(
            metadata=metadata,
            clustername=self.cluster_name,
            pv_name=clone_pvc_obj.backed_pv_obj.name,
            pvc_name=clone_pvc_obj.name,
            namespace=self.project_name,
        )
        # Create a volume snapshot
        available_subvolumes = updated_subvolumes
        snap_obj = snapshot_factory(clone_pvc_obj, wait=True)
        snap_obj_get = snap_obj.get()
        snapshotcontent_name = snap_obj_get["status"]["boundVolumeSnapshotContentName"]
        updated_subvolumes = metadata_utils.available_subvolumes(
            sc_name, self.toolbox, fs
        )
        metadata = metadata_utils.fetch_metadata(
            sc_name,
            fs,
            self.toolbox,
            created_subvolume,
            snapshot=True,
            available_subvolumes=available_subvolumes,
            updated_subvolumes=updated_subvolumes,
        )

        # metadata validation for snapshot created
        metadata_utils.validate_metadata(
            metadata=metadata,
            clustername=self.cluster_name,
            volumesnapshot_name=snap_obj.name,
            volumesnapshot_content=snapshotcontent_name,
            namespace=self.project_name,
        )
        # Restore volume from snapshot
        available_subvolumes = metadata_utils.available_subvolumes(
            sc_name, self.toolbox, fs
        )
        restored_pvc = snapshot_restore_factory(snapshot_obj=snap_obj)
        updated_subvolumes = metadata_utils.available_subvolumes(
            sc_name, self.toolbox, fs
        )
        created_subvolume = metadata_utils.created_subvolume(
            available_subvolumes, updated_subvolumes, sc_name
        )
        metadata = metadata_utils.fetch_metadata(
            sc_name, fs, self.toolbox, created_subvolume
        )
        # metadata validation for restored snapshot
        metadata_utils.validate_metadata(
            metadata=metadata,
            clustername=self.cluster_name,
            pv_name=restored_pvc.backed_pv_obj.name,
            pvc_name=restored_pvc.name,
            namespace=self.project_name,
        )

    @pytest.mark.parametrize(
        argnames=["fs", "sc_name"],
        argvalues=[
            pytest.param(
                "ocs-storagecluster-cephfilesystem",
                constants.DEFAULT_STORAGECLASS_CEPHFS,
            ),
            pytest.param(
                "ocs-storagecluster-cephblockpool", constants.DEFAULT_STORAGECLASS_RBD
            ),
        ],
    )
    def test_metadata_update_for_PV_Retain(self, fs, sc_name, project_factory_class):
        """
        This test case verifies the cephfs metadata created on the subvolume for,
        1. a newly created CephFS PVC
        2. Create a clone
        3. Create a volume snapshot
        4. Restore volume from snapshot

        """
        # Enable CSI_ENABLE_OMAP_GENERATOR flag
        enable_omap_generator = '{"data":{"CSI_ENABLE_OMAP_GENERATOR": "true"}}'

        # Enable CSI_ENABLE_OMAP_GENERATOR flag for rook-ceph-operator-config using patch command
        assert self.config_map_obj.patch(
            resource_name="rook-ceph-operator-config",
            params=enable_omap_generator,
        ), "configmap/rook-ceph-operator-config not patched"

        available_subvolumes = metadata_utils.available_subvolumes(
            sc_name, self.toolbox, fs
        )
        # Create pvc object
        pvc_obj = helpers.create_pvc(
            sc_name=sc_name,
            namespace=self.project_name,
            do_reload=True,
            size="1Gi",
        )
        helpers.wait_for_resource_state(pvc_obj, constants.STATUS_BOUND, timeout=600)
        pv_name = pvc_obj.backed_pv_obj.name
        log.info(f"pv name is ----- {pv_name}")

        params = '{"spec":{"persistentVolumeReclaimPolicy":"Retain"}}'
        assert self.pv_obj.patch(
            resource_name=pv_name, params=params
        ), f"Failed to change the parameter persistentVolumeReclaimPolicy for {pv_name}"

        updated_subvolumes = metadata_utils.available_subvolumes(
            sc_name, self.toolbox, fs
        )
        created_subvolume = metadata_utils.created_subvolume(
            available_subvolumes, updated_subvolumes, sc_name
        )
        metadata = metadata_utils.fetch_metadata(
            sc_name, fs, self.toolbox, created_subvolume
        )
        # metadata validation for PVC created
        metadata_utils.validate_metadata(
            metadata=metadata,
            clustername=self.cluster_name,
            pv_name=pv_name,
            pvc_name=pvc_obj.name,
            namespace=self.project_name,
        )
        # Delete the PVC
        pvc_obj.delete()
        pvc_obj.ocp.wait_for_delete(
            resource_name=pvc_obj.name, timeout=300
        ), f"PVC {pvc_obj.name} is not deleted"

        # Validate PV for claim pvc_obj is in Released state
        self.pv_obj.wait_for_resource(
            condition=constants.STATUS_RELEASED, resource_name=pv_name
        )
        # Edit restore PV and remove the claimRef section
        log.info(f"Remove the claimRef section from PVC {pv_name}")
        params = '[{"op": "remove", "path": "/spec/claimRef"}]'
        self.pv_obj.patch(resource_name=pv_name, params=params, format_type="json")
        log.info(f"Successfully removed claimRef section from PVC {pv_name}")
        # Validate PV is in Available state
        self.pv_obj.wait_for_resource(
            condition=constants.STATUS_AVAILABLE, resource_name=pv_name
        )
        # Create another pvc
        new_pvc_obj = helpers.create_pvc(
            sc_name=sc_name,
            namespace=self.project_name,
            do_reload=True,
            size="1Gi",
        )
        helpers.wait_for_resource_state(
            new_pvc_obj, constants.STATUS_BOUND, timeout=600
        )
        assert new_pvc_obj.backed_pv_obj.name == pv_name
        metadata = metadata_utils.fetch_metadata(
            sc_name, fs, self.toolbox, created_subvolume
        )
        # metadata validation for new PVC created
        metadata_utils.validate_metadata(
            metadata=metadata,
            clustername=self.cluster_name,
            pv_name=pv_name,
            pvc_name=new_pvc_obj.name,
            namespace=self.project_name,
        )

        # Delete the PVC
        new_pvc_obj.delete()
        new_pvc_obj.ocp.wait_for_delete(
            resource_name=new_pvc_obj.name, timeout=600
        ), f"PVC {new_pvc_obj.name} is not deleted"

        # Validate PV for claim pvc_obj is in Released state
        self.pv_obj.wait_for_resource(
            condition=constants.STATUS_RELEASED, resource_name=pv_name
        )
        # Edit again restore PV and remove the claimRef section
        log.info(f"Remove the claimRef section from PVC {pv_name}")
        params = '[{"op": "remove", "path": "/spec/claimRef"}]'
        self.pv_obj.patch(resource_name=pv_name, params=params, format_type="json")
        log.info(f"Successfully removed claimRef section from PVC {pv_name}")
        # Validate PV is in Available state
        self.pv_obj.wait_for_resource(
            condition=constants.STATUS_AVAILABLE, resource_name=pv_name
        )
        # Create another pvc in a different namespace
        project_factory_class(project_name="project-test")
        pvc_obj_in_dif_namespace = helpers.create_pvc(
            sc_name=sc_name,
            namespace="project-test",
            do_reload=True,
            size="1Gi",
        )
        helpers.wait_for_resource_state(
            pvc_obj_in_dif_namespace, constants.STATUS_BOUND, timeout=600
        )
        assert pvc_obj_in_dif_namespace.backed_pv_obj.name == pv_name
        metadata = metadata_utils.fetch_metadata(
            sc_name, fs, self.toolbox, created_subvolume
        )
        # metadata validation for new PVC created
        metadata_utils.validate_metadata(
            metadata=metadata,
            clustername=self.cluster_name,
            pv_name=pv_name,
            pvc_name=pvc_obj_in_dif_namespace.name,
            namespace="project-test",
        )

    @pytest.mark.parametrize(
        argnames=["flag_value"],
        argvalues=[
            pytest.param(12345678),
            pytest.param("feature3504"),
            pytest.param("add-metadata"),
        ],
    )
    def test_negative_values_for_enable_metadata_flag(self, flag_value):
        """
        Validate negative scenarios by providing various un acceptable values for, CSI_ENABLE_METADATA flag.
        1. numeric value for CSI_ENABLE_METADATA flag
        2. alphanumeric value for CSI_ENABLE_METADATA flag
        3. string values other than 'true/false' for CSI_ENABLE_METADATA flag

        """
        # Set numeric value for CSI_ENABLE_METADATA flag
        numeric_value = '{"data":{"CSI_ENABLE_METADATA": ' + flag_value + "}}"

        # Enable CSI_ENABLE_OMAP_GENERATOR flag for rook-ceph-operator-config using patch command
        assert self.config_map_obj.patch(
            resource_name="rook-ceph-operator-config",
            params=numeric_value,
        ), "configmap/rook-ceph-operator-config not patched"

        res = metadata_utils.check_setmetadata_availability(self.pod_obj)
        if res:
            raise AssertionError
