import logging

import pytest

from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import (
    skipif_ocs_version, ManageTest, tier1
)
from ocs_ci.ocs.resources import pod, pvc
from tests import helpers

log = logging.getLogger(__name__)


@tier1
@skipif_ocs_version('<4.6')
@pytest.mark.parametrize(
    argnames=["interface"],
    argvalues=[
        pytest.param(
            constants.CEPHBLOCKPOOL, marks=pytest.mark.polarion_id('OCS-251')
        ),
        pytest.param(
            constants.CEPHFILESYSTEM, marks=pytest.mark.polarion_id('OCS-251')
        )
    ]
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
            interface=interface,
            size=5,
            status=constants.STATUS_BOUND
        )
        self.pod_obj = pod_factory(
            interface=interface,
            pvc=self.pvc_obj,
            status=constants.STATUS_RUNNING
        )

    def test_pvc_snapshot(self, interface, teardown_factory):
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
        log.info(f"Running IO on pod {self.pod_obj.name}")
        file_name = self.pod_obj.name
        log.info(f"File created during IO {file_name}")
        self.pod_obj.run_io(
            storage_type='fs', size='1G', fio_filename=file_name
        )

        # Wait for fio to finish
        fio_result = self.pod_obj.get_fio_results()
        err_count = fio_result.get('jobs')[0].get('error')
        assert err_count == 0, (
            f"IO error on pod {self.pod_obj.name}. "
            f"FIO result: {fio_result}"
        )
        log.info(f"Verified IO on pod {self.pod_obj.name}.")

        # Verfiy presence of the file
        file_path = pod.get_file_path(self.pod_obj, file_name)
        log.info(f"Actual file path on the pod {file_path}")
        assert pod.check_file_existence(self.pod_obj, file_path), (
            f"File {file_name} doesn't exist"
        )
        log.info(f"File {file_name} exists in {self.pod_obj.name}")

        # Calculate md5sum
        orig_md5_sum = pod.cal_md5sum(self.pod_obj, file_name)
        # Take a snapshot
        snap_yaml = constants.CSI_RBD_SNAPSHOT_YAML
        if interface == constants.CEPHFILESYSTEM:
            snap_yaml = constants.CSI_CEPHFS_SNAPSHOT_YAML

        snap_name = helpers.create_unique_resource_name(
            'test', 'snapshot'
        )
        snap_obj = pvc.create_pvc_snapshot(
            self.pvc_obj.name,
            snap_yaml,
            snap_name,
            helpers.default_volumesnapshotclass(interface).name,
        )
        snap_obj.ocp.wait_for_resource(
            condition='true', resource_name=snap_obj.name,
            column=constants.STATUS_READYTOUSE, timeout=60
        )
        teardown_factory(snap_obj)

        # Same Storage class of the original PVC
        sc_name = self.pvc_obj.backed_sc

        # Size should be same as of the original PVC
        pvc_size = str(self.pvc_obj.size) + "Gi"

        # Create pvc out of the snapshot
        # Both, the snapshot and the restore PVC should be in same namespace
        restore_pvc_name = helpers.create_unique_resource_name(
            'test', 'restore-pvc'
        )
        restore_pvc_yaml = constants.CSI_RBD_PVC_RESTORE_YAML
        if interface == constants.CEPHFILESYSTEM:
            restore_pvc_yaml = constants.CSI_CEPHFS_PVC_RESTORE_YAML

        restore_pvc_obj = pvc.create_restore_pvc(
            sc_name=sc_name, snap_name=snap_obj.name,
            namespace=snap_obj.namespace, size=pvc_size,
            pvc_name=restore_pvc_name,
            restore_pvc_yaml=restore_pvc_yaml
        )
        helpers.wait_for_resource_state(
            restore_pvc_obj,
            constants.STATUS_BOUND
        )
        restore_pvc_obj.reload()
        teardown_factory(restore_pvc_obj)

        # Create and attach pod to the pvc
        restore_pod_obj = helpers.create_pod(
            interface_type=interface, pvc_name=restore_pvc_obj.name,
            namespace=snap_obj.namespace,
            pod_dict_path=constants.NGINX_POD_YAML
        )

        # Confirm that the pod is running
        helpers.wait_for_resource_state(
            resource=restore_pod_obj,
            state=constants.STATUS_RUNNING
        )
        restore_pod_obj.reload()
        teardown_factory(restore_pod_obj)

        # Verify that the file is present on the new pod
        log.info(
            f"Checking the existence of {file_name} "
            f"on restore pod {restore_pod_obj.name}"
        )
        assert pod.check_file_existence(restore_pod_obj, file_path), (
            f"File {file_name} doesn't exist"
        )
        log.info(f"File {file_name} exists in {restore_pod_obj.name}")

        # Verify that the md5sum matches
        log.info(
            f"Verifying that md5sum of {file_name} "
            f"on pod {self.pod_obj.name} matches with md5sum "
            f"of the same file on restore pod {restore_pod_obj.name}"
        )
        assert pod.verify_data_integrity(
            restore_pod_obj,
            file_name,
            orig_md5_sum
        ), 'Data integrity check failed'
        log.info("Data integrity check passed, md5sum are same")
