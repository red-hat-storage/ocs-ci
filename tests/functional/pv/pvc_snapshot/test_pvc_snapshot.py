import logging

import pytest

from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import (
    green_squad,
    provider_mode,
    run_on_all_clients_push_missing_configs,
)
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    ManageTest,
    tier1,
    acceptance,
    skipif_ocp_version,
)
from ocs_ci.ocs.resources import pod, pvc
from ocs_ci.helpers import helpers

logger = logging.getLogger(__name__)


@provider_mode
@green_squad
@tier1
@acceptance
@skipif_ocs_version("<4.6")
@skipif_ocp_version("<4.6")
@pytest.mark.parametrize(
    argnames=["interface"],
    argvalues=[
        pytest.param(constants.CEPHBLOCKPOOL, marks=pytest.mark.polarion_id("OCS-251")),
        pytest.param(
            constants.CEPHFILESYSTEM, marks=pytest.mark.polarion_id("OCS-251")
        ),
    ],
)
class TestPvcSnapshot(ManageTest):
    """
    Tests to verify PVC snapshot feature
    """

    @pytest.fixture(autouse=True)
    def setup(self, interface, pvc_factory, pod_factory):
        """
        create resources for the test

        Args:
            interface(str): The type of the interface
                (e.g. CephBlockPool, CephFileSystem)
            pvc_factory: A fixture to create new pvc
            pod_factory: A fixture to create new pod
        """
        self.pvc_obj = pvc_factory(
            interface=interface, size=5, status=constants.STATUS_BOUND
        )
        self.pod_obj = pod_factory(
            interface=interface, pvc=self.pvc_obj, status=constants.STATUS_RUNNING
        )

    @run_on_all_clients_push_missing_configs
    def test_pvc_snapshot(self, interface, teardown_factory, cluster_index):
        """
        1. Run I/O on a pod file.
        2. Calculate md5sum of the file.
        3. Take a snapshot of the PVC.
        4. Create a new PVC out of that snapshot.
        5. Attach a new pod to it.
        6. Verify that the file is present on the new pod also.
        7. Verify that the md5sum of the file on the new pod matches
           with the md5sum of the file on the original pod.

        Args:
            interface(str): The type of the interface
            (e.g. CephBlockPool, CephFileSystem)
            pvc_factory: A fixture to create new pvc
            teardown_factory: A fixture to destroy objects
        """
        logger.test_step(f"Run IO on pod {self.pod_obj.name} and calculate md5sum")
        file_name = self.pod_obj.name
        self.pod_obj.run_io(storage_type="fs", size="1G", fio_filename=file_name)

        # Wait for fio to finish
        fio_result = self.pod_obj.get_fio_results()
        err_count = fio_result.get("jobs")[0].get("error")
        assert err_count == 0, (
            f"IO error on pod {self.pod_obj.name}. " f"FIO result: {fio_result}"
        )
        logger.info(f"IO completed successfully on pod {self.pod_obj.name}")

        # Verify presence of the file
        file_path = pod.get_file_path(self.pod_obj, file_name)
        logger.debug(f"Actual file path on the pod: {file_path}")
        file_exists = pod.check_file_existence(self.pod_obj, file_path)
        logger.assertion(
            f"File {file_name} exists on pod: expected=True, actual={file_exists}"
        )
        assert file_exists, f"File {file_name} doesn't exist"
        logger.info(f"File {file_name} exists in {self.pod_obj.name}")

        # Calculate md5sum
        orig_md5_sum = pod.cal_md5sum(self.pod_obj, file_name)

        logger.test_step(f"Take a snapshot of PVC {self.pvc_obj.name}")
        snap_yaml = constants.CSI_RBD_SNAPSHOT_YAML
        if interface == constants.CEPHFILESYSTEM:
            snap_yaml = constants.CSI_CEPHFS_SNAPSHOT_YAML

        snap_name = helpers.create_unique_resource_name("test", "snapshot")
        snap_obj = pvc.create_pvc_snapshot(
            self.pvc_obj.name,
            snap_yaml,
            snap_name,
            self.pvc_obj.namespace,
            helpers.default_volumesnapshotclass(interface).name,
        )
        snap_obj.ocp.wait_for_resource(
            condition="true",
            resource_name=snap_obj.name,
            column=constants.STATUS_READYTOUSE,
            timeout=60,
        )
        teardown_factory(snap_obj)

        # Same Storage class of the original PVC
        sc_name = self.pvc_obj.backed_sc

        # Size should be same as of the original PVC
        pvc_size = str(self.pvc_obj.size) + "Gi"

        logger.test_step(f"Create new PVC from snapshot {snap_obj.name}")
        restore_pvc_name = helpers.create_unique_resource_name("test", "restore-pvc")
        restore_pvc_yaml = constants.CSI_RBD_PVC_RESTORE_YAML
        if interface == constants.CEPHFILESYSTEM:
            restore_pvc_yaml = constants.CSI_CEPHFS_PVC_RESTORE_YAML

        restore_pvc_obj = pvc.create_restore_pvc(
            sc_name=sc_name,
            snap_name=snap_obj.name,
            namespace=snap_obj.namespace,
            size=pvc_size,
            pvc_name=restore_pvc_name,
            restore_pvc_yaml=restore_pvc_yaml,
        )
        helpers.wait_for_resource_state(
            restore_pvc_obj, constants.STATUS_BOUND, timeout=180
        )
        restore_pvc_obj.reload()
        teardown_factory(restore_pvc_obj)

        logger.test_step("Create and attach pod to the restored PVC")
        restore_pod_obj = helpers.create_pod(
            interface_type=interface,
            pvc_name=restore_pvc_obj.name,
            namespace=snap_obj.namespace,
            pod_dict_path=constants.NGINX_POD_YAML,
        )

        # Confirm that the pod is running
        helpers.wait_for_resource_state(
            resource=restore_pod_obj, state=constants.STATUS_RUNNING, timeout=120
        )
        restore_pod_obj.reload()
        teardown_factory(restore_pod_obj)

        logger.test_step("Verify file exists and data integrity on restored pod")
        file_exists_on_restore = pod.check_file_existence(restore_pod_obj, file_path)
        logger.assertion(
            f"File {file_name} exists on restore pod {restore_pod_obj.name}: "
            f"expected=True, actual={file_exists_on_restore}"
        )
        assert file_exists_on_restore, f"File {file_name} doesn't exist"
        logger.info(f"File {file_name} exists in {restore_pod_obj.name}")

        # Verify that the md5sum matches
        data_integrity = pod.verify_data_integrity(
            restore_pod_obj, file_name, orig_md5_sum
        )
        logger.assertion(
            f"Data integrity check on {restore_pod_obj.name}: expected=True, actual={data_integrity}"
        )
        assert data_integrity, "Data integrity check failed"
        logger.info("Data integrity check passed, md5sum matches")

        logger.test_step("Run IO on restored pod to verify usability")
        restore_pod_obj.run_io(storage_type="fs", size="1G", runtime=20)

        # Wait for fio to finish
        restore_pod_obj.get_fio_results()
        logger.info("IO finished on restored pod")
