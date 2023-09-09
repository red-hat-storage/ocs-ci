import pytest
import logging

from ocs_ci.utility import metadata_utils
from ocs_ci.ocs import constants, ocp
from ocs_ci.helpers import helpers
from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.ocs.resources import pod
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    ManageTest,
    tier1,
    tier3,
    skipif_ocp_version,
    skipif_managed_service,
    skipif_disconnected_cluster,
    skipif_proxy_cluster,
    pre_upgrade,
    ignore_leftovers,
    polarion_id,
)
from ocs_ci.helpers.storageclass_helpers import storageclass_name


log = logging.getLogger(__name__)
# Error message to look in a command output
ERRMSG = "Error in command"


@tier1
@skipif_ocs_version(">4.11")
@skipif_ocp_version(">4.11")
@skipif_managed_service
@skipif_disconnected_cluster
@skipif_proxy_cluster
@green_squad
@ignore_leftovers
class TestMetadataUnavailable(ManageTest):
    """
    Test metadata feature is unavailable for ODF < 4.12
    """

    @pytest.mark.parametrize(
        argnames=["fs", "sc_interface"],
        argvalues=[
            pytest.param(
                "ocs-storagecluster-cephfilesystem",
                constants.OCS_COMPONENTS_MAP["cephfs"],
            ),
        ],
    )
    @polarion_id("OCS-4669")
    def test_metadata_feature_unavailable_for_previous_versions(
        self, project_factory_class, sc_interface, fs
    ):
        """
        This test is to validate setmetadata feature is unavailable in previous ODF version

        Steps:
        1:- Check CSI_ENABLE_METADATA flag unavailable in rook-ceph-operator-config
        and not suported in previous ODF versions (<4.12) and setmetadata is unavailable,
        for csi-cephfsplugin-provisioner and csi-rbdplugin-provisioner pods
        """
        sc_name = storageclass_name(sc_interface)
        config_map_obj = ocp.OCP(kind="Configmap", namespace="openshift-storage")
        pod_obj = ocp.OCP(kind="Pod", namespace="openshift-storage")
        toolbox = pod.get_ceph_tools_pod()
        project_factory_class(project_name="test-metadata")
        enable_metadata = '{"data":{"CSI_ENABLE_METADATA": "true"}}'
        assert config_map_obj.patch(
            resource_name="rook-ceph-operator-config",
            params=enable_metadata,
        ), "configmap/rook-ceph-operator-config not patched"

        # metadata flag not available
        metadata_flag = config_map_obj.exec_oc_cmd(
            "get cm rook-ceph-operator-config --output  jsonpath='{.data.CSI_ENABLE_METADATA}'"
        )
        log.info(f"metadata flag----{metadata_flag}")

        # Check 'setmatadata' is not set for csi-cephfsplugin-provisioner and csi-rbdplugin-provisioner pods
        res = metadata_utils.check_setmetadata_availability(pod_obj)
        assert (
            not res
        ), "Error: The metadata is set, while it is expected to be unavailable "
        available_subvolumes = metadata_utils.available_subvolumes(sc_name, toolbox, fs)
        # Create pvc object
        pvc_obj = helpers.create_pvc(
            sc_name=sc_name,
            namespace="test-metadata",
            do_reload=True,
            size="1Gi",
        )
        helpers.wait_for_resource_state(pvc_obj, constants.STATUS_BOUND, timeout=600)

        updated_subvolumes = metadata_utils.available_subvolumes(sc_name, toolbox, fs)

        created_subvolume = metadata_utils.created_subvolume(
            available_subvolumes, updated_subvolumes, sc_name
        )
        metadata = metadata_utils.fetch_metadata(
            sc_name, fs, toolbox, created_subvolume
        )
        # metadata details unavailable for the PVC
        assert metadata == {}, "Error: Metadata details are available for the PVC"

        # Delete the PVC
        pvc_obj.delete()
        pvc_obj.ocp.wait_for_delete(
            resource_name=pvc_obj.name, timeout=300
        ), f"PVC {pvc_obj.name} is not deleted"

    @pre_upgrade
    def test_create_pvc(self, pvc_factory):
        """
        This test is to validate setmetadata feature is unavailable in previous ODF version

        Steps:
        1:- Check CSI_ENABLE_METADATA flag unavailable in rook-ceph-operator-config
        and not suported in previous ODF versions (<4.12) and setmetadata is unavailable,
        for csi-cephfsplugin-provisioner and csi-rbdplugin-provisioner pods
        """
        # create a pvc with cephfs sc
        pvc_obj = pvc_factory(
            interface=constants.CEPHFILESYSTEM, status=constants.STATUS_BOUND
        )
        log.info(f"PVC {pvc_obj.name} created!")
        # Delete the PVC
        pvc_obj.delete()
        pvc_obj.ocp.wait_for_delete(
            resource_name=pvc_obj.name, timeout=300
        ), f"PVC {pvc_obj.name} is not deleted"


@tier1
@skipif_ocs_version("<4.12")
@skipif_ocp_version("<4.12")
@skipif_managed_service
@skipif_disconnected_cluster
@skipif_proxy_cluster
@green_squad
@ignore_leftovers
class TestDefaultMetadataDisabled(ManageTest):
    """
    Test metadata feature disabled by default for ODF 4.12

    """

    @pytest.mark.parametrize(
        argnames=["fs", "sc_interface"],
        argvalues=[
            pytest.param(
                "ocs-storagecluster-cephfilesystem",
                constants.OCS_COMPONENTS_MAP["cephfs"],
            )
        ],
    )
    @polarion_id("OCS-4671")
    @polarion_id("OCS-4674")
    def test_metadata_not_enabled_by_default(
        self, pvc_factory, pvc_clone_factory, fs, sc_interface
    ):
        """
        This test is to validate metadata feature is not enabled by default for  ODF(4.12) clusters

        Steps:
        1:- Check CSI_ENABLE_METADATA flag unavailable by default in rook-ceph-operator-config
        and setmetadata is unavailable for csi-cephfsplugin-provisioner and csi-rbdplugin-provisioner pods
        2. metadata details unavailable for
            1. a newly created RBD PVC
            2. PVC clone

        """
        sc_name = storageclass_name(sc_interface)
        config_map_obj = ocp.OCP(kind="Configmap", namespace="openshift-storage")
        pod_obj = ocp.OCP(kind="Pod", namespace="openshift-storage")
        toolbox = pod.get_ceph_tools_pod()
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
        assert (
            not res
        ), "Error: The metadata is set, while it is expected to be unavailable "
        _ = metadata_utils.available_subvolumes(sc_name, toolbox, fs)
        # create a pvc with cephfs sc
        pvc_obj = pvc_factory(
            interface=constants.CEPHFILESYSTEM, status=constants.STATUS_BOUND
        )
        log.info(f"PVC {pvc_obj.name} created!")
        # create a clone of the PVC
        cloned_pvc_obj = pvc_clone_factory(pvc_obj)
        log.info(f"Clone of PVC {pvc_obj.name} created!")
        updated_subvolumes = metadata_utils.available_subvolumes(sc_name, toolbox, fs)
        for sub_vol in updated_subvolumes:
            metadata = metadata_utils.fetch_metadata(
                sc_name, fs, toolbox, sub_vol["name"]
            )
            assert metadata == {}, "Error: Metadata details are available"
        # Delete PVCs
        pvc_obj.delete()
        pvc_obj.ocp.wait_for_delete(
            resource_name=pvc_obj.name, timeout=300
        ), f"PVC {pvc_obj.name} is not deleted"
        cloned_pvc_obj.delete()
        cloned_pvc_obj.ocp.wait_for_delete(
            resource_name=cloned_pvc_obj.name, timeout=300
        ), f"PVC {cloned_pvc_obj.name} is not deleted"


@skipif_ocs_version("<4.12")
@skipif_ocp_version("<4.12")
@skipif_managed_service
@skipif_disconnected_cluster
@skipif_proxy_cluster
@green_squad
@ignore_leftovers
class TestMetadata(ManageTest):
    """
    This test class consists of tests to verify cephfs metadata for
    1. a newly created CephFS PVC
    2. Create a clone
    3. Create a volume snapshot
    4. Restore volume from snapshot
    """

    @pytest.fixture(autouse=True)
    def setup(self, request, project_factory):
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
        self.project_name = "metadata"
        project_factory(project_name=self.project_name)
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

    def teardown(self):
        log.info("-----Teardown-----")
        # Disable metadata feature
        metadata_utils.disable_metadata(
            self.config_map_obj,
            self.pod_obj,
        )

    @tier1
    @pytest.mark.parametrize(
        argnames=["fs", "sc_interface"],
        argvalues=[
            pytest.param(
                "ocs-storagecluster-cephfilesystem",
                constants.OCS_COMPONENTS_MAP["cephfs"],
                marks=pytest.mark.polarion_id("OCS-4676"),
            ),
            pytest.param(
                "ocs-storagecluster-cephblockpool",
                constants.OCS_COMPONENTS_MAP["blockpools"],
                marks=[
                    pytest.mark.polarion_id("OCS-4679"),
                    pytest.mark.bugzilla("2039269"),
                ],
            ),
        ],
    )
    def test_verify_metadata_details(
        self,
        pvc_clone_factory,
        snapshot_factory,
        snapshot_restore_factory,
        fs,
        sc_interface,
    ):
        """
        This test case verifies the cephfs and rbd metadata created on the subvolume
        for CSI_ENABLE_METADATA flag enabled for,
        1. a newly created PVC
        2. Create a clone
        3. Create a volume snapshot
        4. Restore volume from snapshot

        """
        sc_name = storageclass_name(sc_interface)
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
        clone_pvc_obj = pvc_clone_factory(pvc_obj=pvc_obj, timeout=600)
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
        restored_pvc = snapshot_restore_factory(snapshot_obj=snap_obj, timeout=600)
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
        # Deleted PVCs and PVs
        pvc_obj.delete()
        pvc_obj.ocp.wait_for_delete(
            resource_name=pvc_obj.name, timeout=300
        ), f"PVC {pvc_obj.name} is not deleted"
        clone_pvc_obj.delete()
        clone_pvc_obj.ocp.wait_for_delete(
            resource_name=clone_pvc_obj.name, timeout=300
        ), f"PVC {clone_pvc_obj.name} is not deleted"

    @tier1
    @pytest.mark.parametrize(
        argnames=["fs", "sc_interface"],
        argvalues=[
            pytest.param(
                "ocs-storagecluster-cephfilesystem",
                constants.OCS_COMPONENTS_MAP["cephfs"],
            )
        ],
    )
    @polarion_id("OCS-4673")
    @polarion_id("OCS-4683")
    def test_verify_metadata_details_for_new_pvc_same_named(self, fs, sc_interface):
        """
        This test case verifies the behavior for creating a PVC for CSI_ENABLE_METADATA flag
        enabled and then delete the PVC and created a new PVC with the same name
        Steps:
            1. Create a PVC
            2. Check metadata details
            3. Delete the PVC
            4. Create a new PVC with same name as previous
            5. Validate the metadata created for the new PVC
               is different than previous metadata
        """
        sc_name = storageclass_name(sc_interface)
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
        pvc_name = pvc_obj.name
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
            pvc_name=pvc_name,
            namespace=self.project_name,
        )
        # Delete the PVC
        pvc_obj.delete()
        pvc_obj.ocp.wait_for_delete(
            resource_name=pvc_obj.name, timeout=300
        ), f"PVC {pvc_obj.name} is not deleted"
        available_subvolumes = updated_subvolumes
        # Create a pvc object with same name
        pvc_obj = helpers.create_pvc(
            sc_name=sc_name,
            pvc_name=pvc_name,
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
        metadata_new_pvc = metadata_utils.fetch_metadata(
            sc_name, fs, self.toolbox, created_subvolume
        )
        # metadata validation for new PVC created
        metadata_utils.validate_metadata(
            metadata=metadata_new_pvc,
            clustername=self.cluster_name,
            pv_name=pvc_obj.backed_pv_obj.name,
            pvc_name=pvc_name,
            namespace=self.project_name,
        )
        assert metadata_new_pvc != metadata

        # Deleted PVCs and PVs
        pvc_obj.delete()
        pvc_obj.ocp.wait_for_delete(
            resource_name=pvc_obj.name, timeout=300
        ), f"PVC {pvc_obj.name} is not deleted"

    @tier1
    @pytest.mark.parametrize(
        argnames=["fs", "sc_interface"],
        argvalues=[
            pytest.param(
                "ocs-storagecluster-cephfilesystem",
                constants.OCS_COMPONENTS_MAP["cephfs"],
                marks=pytest.mark.polarion_id("OCS-4677"),
            ),
            pytest.param(
                "ocs-storagecluster-cephblockpool",
                constants.OCS_COMPONENTS_MAP["blockpools"],
                marks=pytest.mark.polarion_id("OCS-4678"),
            ),
        ],
    )
    def test_metadata_details_available_only_when_metadata_flag_enabled(
        self,
        pvc_clone_factory,
        snapshot_factory,
        snapshot_restore_factory,
        fs,
        sc_interface,
    ):
        """
        This test case is to validate that metadata details are available for the operations
        done after enabling CSI_ENABLE_METADATA flag----
        Steps:
        1. enable CSI_ENABLE_METADATA flag create a PVC and check the metadata details
        are available for the added PVC,

        2. disable CSI_ENABLE_METADATA flag and create a clone and snapshot of the volume

        3. Check metadata details will not be available for volume clone and snapshot.

        4. Enable CSI_ENABLE_METADATA flag, restore snapshot

        5. Check metadata details available for created pvc and restored volume but
        no metadata details available for the volume clone and snapshot created

        """
        sc_name = storageclass_name(sc_interface)
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
        # Disable metadata flag
        metadata_utils.disable_metadata(
            self.config_map_obj,
            self.pod_obj,
        )
        available_subvolumes = updated_subvolumes
        # Clone the PVC
        clone_pvc_obj = pvc_clone_factory(pvc_obj=pvc_obj, timeout=600)
        updated_subvolumes = metadata_utils.available_subvolumes(
            sc_name, self.toolbox, fs
        )
        created_subvolume = metadata_utils.created_subvolume(
            available_subvolumes, updated_subvolumes, sc_name
        )
        metadata = metadata_utils.fetch_metadata(
            sc_name, fs, self.toolbox, created_subvolume
        )
        # metadata details unavailable for cloned PVC
        if metadata == {} or metadata is None:
            pass
        else:
            raise AssertionError
        # Create a volume snapshot
        available_subvolumes = updated_subvolumes
        snap_obj = snapshot_factory(clone_pvc_obj, wait=True)

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
        # metadata details unavailable for snapshot created
        if metadata == {} or metadata is None:
            pass
        else:
            raise AssertionError

        # Enable metadata feature
        log.info("----Enable metadata----")
        _ = metadata_utils.enable_metadata(
            self.config_map_obj,
            self.pod_obj,
        )
        # Restore volume from snapshot
        available_subvolumes = metadata_utils.available_subvolumes(
            sc_name, self.toolbox, fs
        )
        restored_pvc = snapshot_restore_factory(snapshot_obj=snap_obj, timeout=600)
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
        # Deleted PVCs and PVs
        pvc_obj.delete()
        pvc_obj.ocp.wait_for_delete(
            resource_name=pvc_obj.name, timeout=300
        ), f"PVC {pvc_obj.name} is not deleted"
        clone_pvc_obj.delete()
        clone_pvc_obj.ocp.wait_for_delete(
            resource_name=clone_pvc_obj.name, timeout=300
        ), f"PVC {clone_pvc_obj.name} is not deleted"

    @tier3
    @pytest.mark.parametrize(
        argnames=["fs", "sc_interface"],
        argvalues=[
            pytest.param(
                "ocs-storagecluster-cephfilesystem",
                constants.OCS_COMPONENTS_MAP["cephfs"],
            ),
            pytest.param(
                "ocs-storagecluster-cephblockpool",
                constants.OCS_COMPONENTS_MAP["blockpools"],
            ),
        ],
    )
    @polarion_id("OCS-4672")
    def test_disable_metadata_flag_after_enabling(self, fs, sc_interface):
        """
        This test case is to validate the behavior for, disable CSI_ENABLE_METADATA flag
        after enabling----
        Steps:
        1. enable CSI_ENABLE_METADATA flag create a PVC and check the metadata details
        are available for the added PVC,

        2. disable CSI_ENABLE_METADATA flag and check for the PVC created with CSI_ENABLE_METADATA
        flag enabled meta data details will be still available.

        3. create another PVC after disabling CSI_ENABLE_METADATA flag,
        and check for this PVC metadata details will not be available.

        4. After disabling CSI_ENABLE_METADATA flag, 'setmatadata' should not be set
        for csi-cephfsplugin-provisioner and csi-rbdplugin-provisioner pods

        """
        sc_name = storageclass_name(sc_interface)
        available_subvolumes = metadata_utils.available_subvolumes(
            sc_name, self.toolbox, fs
        )
        # Create pvc object
        pvc_obj_with_metadata_enabled = helpers.create_pvc(
            sc_name=sc_name,
            namespace=self.project_name,
            do_reload=True,
            size="1Gi",
        )
        helpers.wait_for_resource_state(
            pvc_obj_with_metadata_enabled, constants.STATUS_BOUND, timeout=600
        )

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
            pv_name=pvc_obj_with_metadata_enabled.backed_pv_obj.name,
            pvc_name=pvc_obj_with_metadata_enabled.name,
            namespace=self.project_name,
        )
        # Disable metadata flag
        metadata_utils.disable_metadata(
            self.config_map_obj,
            self.pod_obj,
        )
        # check meta data details still available for the pvc
        metadata = metadata_utils.fetch_metadata(
            sc_name, fs, self.toolbox, created_subvolume
        )
        available_subvolumes = updated_subvolumes
        # create another PVC after disabling CSI_ENABLE_METADATA flag
        pvc_obj_with_metadata_disabled = helpers.create_pvc(
            sc_name=sc_name,
            namespace=self.project_name,
            do_reload=True,
            size="1Gi",
        )
        helpers.wait_for_resource_state(
            pvc_obj_with_metadata_disabled, constants.STATUS_BOUND, timeout=600
        )

        updated_subvolumes = metadata_utils.available_subvolumes(
            sc_name, self.toolbox, fs
        )
        created_subvolume = metadata_utils.created_subvolume(
            available_subvolumes, updated_subvolumes, sc_name
        )
        metadata = metadata_utils.fetch_metadata(
            sc_name, fs, self.toolbox, created_subvolume
        )
        # metadata details unavailable for PVC, pvc_obj_with_metadata_disabled
        if metadata == {} or metadata is None:
            pass
        else:
            raise AssertionError
        # 'setmatadata' unavailable for cephfs and rbd plugin provisioner pods
        res = metadata_utils.check_setmetadata_availability(self.pod_obj)
        assert (
            not res
        ), "Error: The metadata is set, while it is expected to be unavailable "

        # Deleted PVCs and PVs
        pvc_obj_with_metadata_enabled.delete()
        pvc_obj_with_metadata_enabled.ocp.wait_for_delete(
            resource_name=pvc_obj_with_metadata_enabled.name, timeout=300
        ), f"PVC {pvc_obj_with_metadata_enabled.name} is not deleted"
        pvc_obj_with_metadata_disabled.delete()
        pvc_obj_with_metadata_disabled.ocp.wait_for_delete(
            resource_name=pvc_obj_with_metadata_disabled.name, timeout=300
        ), f"PVC {pvc_obj_with_metadata_disabled.name} is not deleted"

    @tier1
    @pytest.mark.parametrize(
        argnames=["fs", "sc_interface"],
        argvalues=[
            pytest.param(
                "ocs-storagecluster-cephblockpool",
                constants.OCS_COMPONENTS_MAP["blockpools"],
            ),
        ],
    )
    @polarion_id("OCS-4680")
    @polarion_id("OCS-4681")
    def test_metadata_update_for_PV_Retain(
        self, fs, sc_interface, project_factory_class
    ):
        """
        This test is to validate metadata is updated after a PVC is deleted by setting ReclaimPloicy: Retain on PV
        and a freshly created PVC in same or different namespace is attached to the old PV for CSI_ENABLE_METADATA
        flag set to true,
        Steps:
            1. Enable CSI_ENABLE_OMAP_GENERATOR flag
            2. Create pvc object
            3. Update persistentVolumeReclaimPolicy:Retain for the PV
            4. validate metadata for PVC created
            5. Delete the PVC
            6. Validate PV for claim pvc_obj is in Released state
            7. Edit restore PV and remove the claimRef section
            8. Validate PV is in Available state
            9. Create another pvc
            10. validate metadata for new PVC created
            11. Delete the PVC
            12. Validate PV for claim pvc_obj is in Released state
            13. Edit restore PV and remove the claimRef section
            14. Validate PV is in Available state
            15. Create another pvc in different namespace
            16. validate metadata for new PVC created

        """
        sc_name = storageclass_name(sc_interface)
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

        # Update persistentVolumeReclaimPolicy:Retain for the PV
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
        dif_namespace = "new-project"
        project_factory_class(project_name=dif_namespace)
        pvc_obj_in_dif_namespace = helpers.create_pvc(
            sc_name=sc_name,
            namespace=dif_namespace,
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
            namespace=dif_namespace,
        )
        pvc_obj_in_dif_namespace.delete()
        pvc_obj_in_dif_namespace.ocp.wait_for_delete(
            resource_name=pvc_obj_in_dif_namespace.name, timeout=600
        ), f"PVC {pvc_obj_in_dif_namespace.name} is not deleted"

    @tier3
    @pytest.mark.parametrize(
        argnames=["flag_value"],
        argvalues=[
            pytest.param("12345678"),
            pytest.param("feature3504"),
            pytest.param("add-metadata"),
        ],
    )
    @polarion_id("OCS-4682")
    def test_negative_values_for_enable_metadata_flag(self, flag_value):
        """
        Validate negative scenarios by providing various un acceptable values for, CSI_ENABLE_METADATA flag.
        1. numeric value for CSI_ENABLE_METADATA flag
        2. alphanumeric value for CSI_ENABLE_METADATA flag
        3. string values other than 'true/false' for CSI_ENABLE_METADATA flag

        Steps:
            1. Set CSI_ENABLE_METADATA flag value as numeric value
            2. Set CSI_ENABLE_METADATA flag value as alphanumeric value
            3. Set string values other than 'true/false' for CSI_ENABLE_METADATA flag
        """
        # Set numeric value for CSI_ENABLE_METADATA flag
        params = '{"data":{"CSI_ENABLE_METADATA": ' + '"' + flag_value + '"' + "}}"
        log.info(f"params ----- {params}")

        # Enable CSI_ENABLE_OMAP_GENERATOR flag for rook-ceph-operator-config using patch command
        assert self.config_map_obj.patch(
            resource_name="rook-ceph-operator-config",
            params=params,
        ), "configmap/rook-ceph-operator-config not patched"

        # Check csi-cephfsplugin provisioner and csi-rbdplugin-provisioner pods are up and running
        assert self.pod_obj.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            selector="app=csi-cephfsplugin-provisioner",
            dont_allow_other_resources=True,
            timeout=60,
        )

        assert self.pod_obj.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            selector="app=csi-rbdplugin-provisioner",
            dont_allow_other_resources=True,
            timeout=60,
        )
        res = metadata_utils.check_setmetadata_availability(self.pod_obj)
        assert (
            not res
        ), "Error: The metadata is set, while it is expected to be unavailable "
