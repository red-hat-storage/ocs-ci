import logging

import pytest

from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.resources import pvc, pod
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    skipif_ocp_version,
    ManageTest,
    tier1,
)
from ocs_ci.helpers import helpers
from ocs_ci.utility import templating

log = logging.getLogger(__name__)

INTERFACE_MAP = {
    constants.CEPHFILESYSTEM: {
        "sc_name": constants.CEPHFILESYSTEM_SC,
        "vgs_class": constants.DEFAULT_VOLUMEGROUPSNAPSHOTCLASS_CEPHFS,
        "restore_yaml": constants.CSI_CEPHFS_PVC_RESTORE_YAML,
        "access_mode": constants.ACCESS_MODE_RWO,
    },
    constants.CEPHBLOCKPOOL: {
        "sc_name": constants.CEPHBLOCKPOOL_SC,
        "vgs_class": constants.DEFAULT_VOLUMEGROUPSNAPSHOTCLASS_RBD,
        "restore_yaml": constants.CSI_RBD_PVC_RESTORE_YAML,
        "access_mode": constants.ACCESS_MODE_RWO,
    },
    constants.NFS_STORAGECLASS_NAME: {
        "sc_name": constants.NFS_STORAGECLASS_NAME,
        "vgs_class": constants.DEFAULT_VOLUMEGROUPSNAPSHOTCLASS_NFS,
        "restore_yaml": constants.CSI_CEPHFS_PVC_RESTORE_YAML,
        "access_mode": constants.ACCESS_MODE_RWO,
    },
}

PVC_COUNT = 3
PVC_SIZE = 5
VGS_LABEL_KEY = "app"
VGS_LABEL_VALUE = "vgs-test"


@green_squad
@tier1
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
class TestVolumeGroupSnapshot(ManageTest):
    """
    Tests to verify VolumeGroupSnapshot happy path for CephFS, RBD, and NFS
    """

    @pytest.fixture(autouse=True)
    def setup(
        self,
        interface,
        project_factory,
        teardown_factory,
    ):
        """
        Set up resources for the VolumeGroupSnapshot test.

        Creates a project, multiple labeled PVCs, writes data to each,
        and records md5sums for later verification.
        """
        self.interface_config = INTERFACE_MAP[interface]
        self.teardown_factory = teardown_factory

        self.project_obj = project_factory()
        self.namespace = self.project_obj.namespace

        sc_name = self.interface_config["sc_name"]
        access_mode = self.interface_config["access_mode"]

        self.pvc_objs = []
        for i in range(PVC_COUNT):
            pvc_obj = helpers.create_pvc(
                sc_name=sc_name,
                namespace=self.namespace,
                size=f"{PVC_SIZE}Gi",
                access_mode=access_mode,
            )
            helpers.wait_for_resource_state(
                pvc_obj, constants.STATUS_BOUND, timeout=90
            )
            pvc_obj.reload()
            teardown_factory(pvc_obj)
            self.pvc_objs.append(pvc_obj)

        ocp_obj = ocp.OCP(kind=constants.PVC, namespace=self.namespace)
        for pvc_obj in self.pvc_objs:
            ocp_obj.exec_oc_cmd(
                f"label pvc {pvc_obj.name} {VGS_LABEL_KEY}={VGS_LABEL_VALUE}"
            )

        self.md5sums = {}
        for pvc_obj in self.pvc_objs:
            pod_obj = helpers.create_pod(
                pvc_name=pvc_obj.name,
                namespace=self.namespace,
                pod_dict_path=constants.NGINX_POD_YAML,
            )
            helpers.wait_for_resource_state(
                pod_obj, constants.STATUS_RUNNING, timeout=120
            )
            pod_obj.reload()

            file_name = f"testfile_{pvc_obj.name}"
            pod_obj.run_io(
                storage_type="fs", size="100M", fio_filename=file_name
            )
            pod_obj.get_fio_results()

            self.md5sums[pvc_obj.name] = pod.cal_md5sum(pod_obj, file_name)

            pod_obj.delete()
            pod_obj.ocp.wait_for_delete(pod_obj.name, timeout=120)

    def test_volume_group_snapshot(self, interface, teardown_factory):
        """
        Test VolumeGroupSnapshot happy path:

        1. Create VolumeGroupSnapshot targeting labeled PVCs
        2. Verify VGS becomes ready (READYTOUSE = true)
        3. Verify individual VolumeSnapshots are created
        4. Restore PVCs from snapshots and verify data integrity
        5. Delete VolumeGroupSnapshot and verify cleanup

        """
        vgs_class = self.interface_config["vgs_class"]
        restore_yaml = self.interface_config["restore_yaml"]
        sc_name = self.interface_config["sc_name"]

        log.info(f"Verifying VolumeGroupSnapshotClass {vgs_class} exists")
        vgsc_ocp = ocp.OCP(kind=constants.VOLUMEGROUPSNAPSHOTCLASS)
        assert vgsc_ocp.is_exist(resource_name=vgs_class), (
            f"VolumeGroupSnapshotClass {vgs_class} does not exist"
        )

        log.info("Creating VolumeGroupSnapshot")
        vgs_data = templating.load_yaml(constants.CSI_VOLUMEGROUPSNAPSHOT_YAML)
        vgs_name = helpers.create_unique_resource_name("test", "vgs")
        vgs_data["metadata"]["name"] = vgs_name
        vgs_data["metadata"]["namespace"] = self.namespace
        vgs_data["spec"]["volumeGroupSnapshotClassName"] = vgs_class
        vgs_data["spec"]["source"]["selector"]["matchLabels"] = {
            VGS_LABEL_KEY: VGS_LABEL_VALUE,
        }
        vgs_obj = OCS(**vgs_data)
        vgs_obj.create(do_reload=True)
        teardown_factory(vgs_obj)

        vgs_ocp = ocp.OCP(
            kind=constants.VOLUMEGROUPSNAPSHOT, namespace=self.namespace
        )
        log.info(f"Waiting for VolumeGroupSnapshot {vgs_name} to become ready")
        vgs_ocp.wait_for_resource(
            condition="true",
            resource_name=vgs_name,
            column=constants.STATUS_READYTOUSE,
            timeout=300,
        )
        log.info(f"VolumeGroupSnapshot {vgs_name} is ready")

        log.info("Verifying individual VolumeSnapshots were created")
        snap_ocp = ocp.OCP(
            kind=constants.VOLUMESNAPSHOT, namespace=self.namespace
        )
        snapshots = snap_ocp.get().get("items", [])
        assert len(snapshots) == PVC_COUNT, (
            f"Expected {PVC_COUNT} VolumeSnapshots, found {len(snapshots)}"
        )

        for snap in snapshots:
            snap_name = snap["metadata"]["name"]
            snap_ocp.wait_for_resource(
                condition="true",
                resource_name=snap_name,
                column=constants.STATUS_READYTOUSE,
                timeout=120,
            )
        log.info(f"All {PVC_COUNT} VolumeSnapshots are ready")

        log.info("Restoring PVCs from snapshots and verifying data")
        restore_pod_objs = []
        restore_pvc_objs = []
        for snap in snapshots:
            snap_name = snap["metadata"]["name"]
            pvc_source = snap["spec"]["source"]["persistentVolumeClaimName"]
            pvc_size = f"{PVC_SIZE}Gi"

            restore_pvc_name = helpers.create_unique_resource_name(
                "test", "restore-pvc"
            )
            restore_pvc_obj = pvc.create_restore_pvc(
                sc_name=sc_name,
                snap_name=snap_name,
                namespace=self.namespace,
                size=pvc_size,
                pvc_name=restore_pvc_name,
                restore_pvc_yaml=restore_yaml,
            )
            helpers.wait_for_resource_state(
                restore_pvc_obj, constants.STATUS_BOUND, timeout=180
            )
            restore_pvc_obj.reload()
            teardown_factory(restore_pvc_obj)
            restore_pvc_objs.append(restore_pvc_obj)

            restore_pod_obj = helpers.create_pod(
                pvc_name=restore_pvc_obj.name,
                namespace=self.namespace,
                pod_dict_path=constants.NGINX_POD_YAML,
            )
            helpers.wait_for_resource_state(
                restore_pod_obj, constants.STATUS_RUNNING, timeout=120
            )
            restore_pod_obj.reload()
            teardown_factory(restore_pod_obj)
            restore_pod_objs.append(restore_pod_obj)

            file_name = f"testfile_{pvc_source}"
            assert pod.verify_data_integrity(
                restore_pod_obj, file_name, self.md5sums[pvc_source]
            ), f"Data integrity check failed for {pvc_source}"
            log.info(f"Data integrity verified for PVC {pvc_source}")

        log.info(f"Deleting VolumeGroupSnapshot {vgs_name}")
        vgs_obj.delete()
        vgs_ocp.wait_for_delete(resource_name=vgs_name, timeout=180)
        log.info(f"VolumeGroupSnapshot {vgs_name} deleted")

        log.info("Verifying VolumeSnapshots are cleaned up")
        remaining_snaps = snap_ocp.get().get("items", [])
        assert len(remaining_snaps) == 0, (
            f"Expected 0 VolumeSnapshots after VGS deletion, found {len(remaining_snaps)}"
        )

        log.info("Verifying VolumeGroupSnapshotContent is cleaned up")
        vgsc_content_ocp = ocp.OCP(kind=constants.VOLUMEGROUPSNAPSHOTCONTENT)
        vgs_contents = [
            item
            for item in vgsc_content_ocp.get().get("items", [])
            if item.get("spec", {})
            .get("volumeGroupSnapshotRef", {})
            .get("name")
            == vgs_name
        ]
        assert len(vgs_contents) == 0, (
            "VolumeGroupSnapshotContent not cleaned up after VGS deletion"
        )

        log.info("VolumeGroupSnapshot happy path test completed successfully")
