import logging

import pytest

from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    skipif_ocp_version,
    ManageTest,
    tier3,
)
from ocs_ci.helpers import helpers
from ocs_ci.utility import nfs_utils
from ocs_ci.utility import templating

log = logging.getLogger(__name__)

INTERFACE_MAP = {
    constants.CEPHFILESYSTEM: {
        "vgs_class": constants.DEFAULT_VOLUMEGROUPSNAPSHOTCLASS_CEPHFS,
    },
    constants.CEPHBLOCKPOOL: {
        "vgs_class": constants.DEFAULT_VOLUMEGROUPSNAPSHOTCLASS_RBD,
    },
    constants.NFS_STORAGECLASS_NAME: {
        "vgs_class": constants.DEFAULT_VOLUMEGROUPSNAPSHOTCLASS_NFS,
    },
}

# Short timeout: we expect the VGS to never become ready, so there is no point
# waiting the full happy-path duration before the negative assertion succeeds.
NOT_READY_TIMEOUT = 60


@green_squad
@tier3
@skipif_ocs_version("<5.0")
@skipif_ocp_version("<5.0")
@pytest.mark.parametrize(
    argnames=["interface"],
    argvalues=[
        pytest.param(constants.CEPHFILESYSTEM),
        pytest.param(constants.CEPHBLOCKPOOL),
        pytest.param(constants.NFS_STORAGECLASS_NAME),
    ],
)
class TestVolumeGroupSnapshotNonexistentPVC(ManageTest):
    """
    Negative tests to verify VolumeGroupSnapshot behavior when the source
    selector matches no PVC (i.e. a non-existent PVC), for CephFS, RBD, and NFS.
    """

    @pytest.fixture(autouse=True)
    def setup(
        self,
        interface,
        project_factory,
        teardown_factory,
    ):
        """
        Set up resources for the negative VolumeGroupSnapshot test.

        Creates an empty project (no PVCs) so that a VolumeGroupSnapshot whose
        selector targets a non-existent PVC label has nothing to match.
        """
        self.interface_config = INTERFACE_MAP[interface]
        self.teardown_factory = teardown_factory
        self.nfs_ganesha_pod_name = None

        if interface == constants.NFS_STORAGECLASS_NAME:
            self.namespace = constants.OPENSHIFT_STORAGE_NAMESPACE
            self.storage_cluster_obj = ocp.OCP(
                kind=constants.STORAGECLUSTER,
                namespace=self.namespace,
            )
            self.config_map_obj = ocp.OCP(
                kind=constants.CONFIGMAP,
                namespace=self.namespace,
            )
            self.pod_obj = ocp.OCP(kind=constants.POD, namespace=self.namespace)
            self.sc = OCS(
                kind=constants.STORAGECLASS,
                metadata={"name": constants.NFS_STORAGECLASS_NAME},
            )
            self.nfs_ganesha_pod_name = nfs_utils.nfs_enable(
                self.storage_cluster_obj,
                self.config_map_obj,
                self.pod_obj,
                self.namespace,
            )

        self.project_obj = project_factory()
        self.namespace = self.project_obj.namespace

        # Intentionally create no PVCs so the VGS selector matches nothing.

        yield

    def teardown(self):
        """Disable NFS after the NFS parameterized test completes."""
        if self.nfs_ganesha_pod_name:
            nfs_utils.nfs_disable(
                self.storage_cluster_obj,
                self.config_map_obj,
                self.pod_obj,
                self.sc,
                self.nfs_ganesha_pod_name,
            )

    def test_vgs_nonexistent_pvc(self, interface, teardown_factory):
        """
        Test VolumeGroupSnapshot with a non-existent source PVC:

        1. Create a VolumeGroupSnapshot whose selector matches no PVC
        2. Verify the VGS object is created (admitted by the API)
        3. Verify the VGS never becomes ready (READYTOUSE != true)
        4. Verify no individual VolumeSnapshots are created
        5. Verify no VolumeGroupSnapshotContent is created for the VGS

        """
        vgs_class = self.interface_config["vgs_class"]

        log.info(f"Verifying VolumeGroupSnapshotClass {vgs_class} exists")
        vgsc_ocp = ocp.OCP(kind=constants.VOLUMEGROUPSNAPSHOTCLASS)
        assert vgsc_ocp.is_exist(
            resource_name=vgs_class
        ), f"VolumeGroupSnapshotClass {vgs_class} does not exist"

        log.info("Creating VolumeGroupSnapshot targeting a non-existent PVC label")
        vgs_data = templating.load_yaml(constants.CSI_VOLUMEGROUPSNAPSHOT_YAML)
        vgs_name = helpers.create_unique_resource_name("test", "vgs")
        label_value = helpers.create_unique_resource_name("nonexistent", "pvc")
        vgs_data["metadata"]["name"] = vgs_name
        vgs_data["metadata"]["namespace"] = self.namespace
        vgs_data["spec"]["volumeGroupSnapshotClassName"] = vgs_class
        vgs_data["spec"]["source"]["selector"]["matchLabels"] = {
            "app": label_value,
        }
        vgs_obj = OCS(**vgs_data)
        vgs_obj.create(do_reload=True)
        teardown_factory(vgs_obj)

        vgs_ocp = ocp.OCP(kind=constants.VOLUMEGROUPSNAPSHOT, namespace=self.namespace)

        log.info(f"Verifying VolumeGroupSnapshot {vgs_name} was created")
        assert vgs_ocp.is_exist(
            resource_name=vgs_name
        ), f"VolumeGroupSnapshot {vgs_name} was not created"

        log.info(
            f"Verifying VolumeGroupSnapshot {vgs_name} never becomes ready "
            f"(selector matches no PVC)"
        )
        with pytest.raises(TimeoutExpiredError):
            vgs_ocp.wait_for_resource(
                condition="true",
                resource_name=vgs_name,
                column=constants.STATUS_READYTOUSE,
                timeout=NOT_READY_TIMEOUT,
                sleep=5,
            )
        log.info(
            f"VolumeGroupSnapshot {vgs_name} correctly did not reach "
            f"{constants.STATUS_READYTOUSE}=true"
        )
        log.info(f"VolumeGroupSnapshot status: {vgs_obj.get().get('status')}")

        log.info("Verifying no individual VolumeSnapshots were created")
        snap_ocp = ocp.OCP(kind=constants.VOLUMESNAPSHOT, namespace=self.namespace)
        snapshots = snap_ocp.get().get("items", [])
        snap_names = [snap.get("metadata", {}).get("name") for snap in snapshots]
        assert (
            len(snapshots) == 0
        ), f"Expected 0 VolumeSnapshots, found {len(snapshots)}: {snap_names}"

        log.info("Verifying no VolumeGroupSnapshotContent was created for the VGS")
        vgsc_content_ocp = ocp.OCP(kind=constants.VOLUMEGROUPSNAPSHOTCONTENT)
        vgs_contents = [
            item
            for item in vgsc_content_ocp.get().get("items", [])
            if (
                item.get("spec", {}).get("volumeGroupSnapshotRef", {}).get("name")
                == vgs_name
            )
        ]
        assert (
            len(vgs_contents) == 0
        ), "VolumeGroupSnapshotContent should not be created when selector matches no PVC"

        log.info(
            "VolumeGroupSnapshot non-existent PVC negative test completed successfully"
        )
