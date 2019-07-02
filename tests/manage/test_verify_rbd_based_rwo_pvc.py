import logging
import pytest

from ocs_ci.ocs.resources.pod import run_io_and_verify_mount_point
from ocs_ci.ocs.resources.pvc import PVC
from ocs_ci.ocs.ocp import OCP
from ocs_ci.framework.testlib import ManageTest, tier1
from ocs_ci.utility import templating
from ocs_ci.ocs import constants, defaults
from ocs_ci.ocs.exceptions import (
    TimeoutExpiredError, CommandFailed, UnexpectedBehaviour
)
from tests import helpers
from tests.fixtures import create_ceph_block_pool, create_rbd_secret

log = logging.getLogger(__name__)


@pytest.fixture()
def test_fixture(request):
    """
    Setup and teardown
    """
    self = request.node.cls

    def finalizer():
        teardown(self)
    request.addfinalizer(finalizer)
    setup(self)


def setup(self):
    """
    Create new pod using available PVC
    """
    # Create Storage Class with reclaimPolicy: Delete
    self.sc_obj = helpers.create_storage_class(
        interface_type=constants.CEPHBLOCKPOOL,
        interface_name=self.cbp_obj.name,
        secret_name=self.secret_obj.name,
        reclaim_policy='Delete'
    )

    # Create PVC with 'accessModes' 'ReadWriteOnce'
    pvc_data = templating.load_yaml_to_dict(constants.CSI_PVC_YAML)
    pvc_data['metadata']['name'] = helpers.create_unique_resource_name(
        'test', 'pvc'
    )
    pvc_data['metadata']['namespace'] = defaults.ROOK_CLUSTER_NAMESPACE
    pvc_data['spec']['storageClassName'] = self.sc_obj.name
    pvc_data['spec']['accessModes'] = ['ReadWriteOnce']
    self.pvc_obj = PVC(**pvc_data)
    self.pvc_obj.create()

    # Create two pods
    log.info(f"Creating two pods which use PVC {self.pvc_obj.name}")
    pod_data = templating.load_yaml_to_dict(constants.CSI_RBD_POD_YAML)
    pod_data['metadata']['name'] = helpers.create_unique_resource_name(
        'test', 'pod'
    )
    pod_data['metadata']['namespace'] = defaults.ROOK_CLUSTER_NAMESPACE
    pod_data['spec']['volumes'][0]['persistentVolumeClaim']['claimName'] = (
        self.pvc_obj.name
    )
    self.pod_obj = helpers.create_pod(**pod_data)
    pod_data['metadata']['name'] = helpers.create_unique_resource_name(
        'test', 'pod'
    )
    self.pod_obj2 = helpers.create_pod(wait=False, **pod_data)


def teardown(self):
    """
    Delete Storage Class
    """
    self.sc_obj.delete()


@tier1
@pytest.mark.usefixtures(
    create_rbd_secret.__name__,
    create_ceph_block_pool.__name__,
    test_fixture.__name__
)
@pytest.mark.polarion_id("OCS-533")
class TestRbdBasedRwoPvc(ManageTest):
    """
    Verifies RBD Based RWO Dynamic PVC creation
    """
    def test_rbd_based_rwo_pvc_reclaim_delete(self):
        """
        Verifies RBD Based RWO Dynamic PVC creation with Reclaim policy set to
        Delete

        Steps:
        1. Create two pods using same PVC
        2. Run IO on first pod
        3. Verify second pod is not getting into Running state
        4. Delete first pod
        5. Verify second pod is in Running state
        6. Verify usage of volume in second pod is matching with usage in
           first pod
        7. Run IO on second pod
        8. Delete second pod
        9. Delete PVC
        10. Verify PV associated with deleted PVC is also deleted
        """
        # Run IO on first pod
        log.info(f"Running IO on first pod {self.pod_obj.name}")
        usage = run_io_and_verify_mount_point(
            self.pod_obj, '10M', '100', 'dd_1'
        )
        assert usage, f"IO failed on pod {self.pod_obj.name}"

        # Verify that second pod is not getting into Running state. Check it
        # for some period of time.
        try:
            assert not self.pod_obj2.ocp.wait_for_resource(
                condition='Running', resource_name=self.pod_obj2.name,
            ), "Unexpected: Second pod is in Running state"
        except TimeoutExpiredError:
            log.info(
                f"Verified: Second pod {self.pod_obj2.name} is not in "
                f"Running state"
            )

        # Delete first pod
        self.pod_obj.delete(wait=True)

        # Verify pod is deleted
        try:
            self.pod_obj.get()
            raise UnexpectedBehaviour(
                f"First pod {self.pod_obj.name} is not deleted."
            )
        except CommandFailed as exp:
            assert "not found" in str(exp), (
                "Failed to fetch pod details"
            )
            log.info(f"First pod {self.pod_obj.name} is deleted.")

        # Wait for second pod to be in Running state
        assert self.pod_obj2.ocp.wait_for_resource(
            condition='Running', resource_name=self.pod_obj2.name
        )
        log.info(
            f"Second pod {self.pod_obj2.name} is in Running state after "
            f"the first pod is deleted"
        )

        # Verify that volume usage in second pod is matching with the usage in
        # first pod
        mount_point = self.pod_obj2.exec_cmd_on_pod(command="df -kh")
        mount_point = mount_point.split()
        usage_re = mount_point[mount_point.index('/var/lib/www/html') - 1]
        assert usage_re == usage, (
            "Use percentage in new pod is not matching with old pod"
        )

        # Run IO on new pod
        assert run_io_and_verify_mount_point(
            self.pod_obj2, '10M', '100', 'dd_2'
        ), f"IO failed on second pod {self.pod_obj2.name}"

        # Delete second pod
        self.pod_obj2.delete()

        # Verify pod is deleted
        try:
            self.pod_obj2.get()
            raise UnexpectedBehaviour(
                f"Second pod {self.pod_obj2.name} is not deleted."
            )
        except CommandFailed as exp:
            assert "not found" in str(exp), (
                "Failed to fetch pod details"
            )
            log.info(f"Second pod {self.pod_obj2.name} is deleted.")

        # Get PV name
        self.pvc_obj.reload()
        pv_name = self.pvc_obj.backed_pv

        # Delete PVC
        self.pvc_obj.delete()

        # Verify PVC is deleted
        try:
            self.pvc_obj.get()
            raise UnexpectedBehaviour(
                f"PVC {self.pvc_obj.name} is not deleted."
            )
        except CommandFailed as exp:
            assert "not found" in str(exp), (
                f"Failed to fetch PVC details"
            )
            log.info(f"PVC {self.pvc_obj.name} is deleted.")

        # Verify PV is deleted
        pv_obj = OCP(
            kind=constants.PV, namespace=defaults.ROOK_CLUSTER_NAMESPACE
        )
        pv_info = pv_obj.get(out_yaml_format=False)
        if pv_info:
            assert not (pv_name in pv_info), (
                f"PV {pv_name} exists after deleting PVC {self.pvc_obj.name}"
            )

        # TODO: Verify PV using ceph toolbox. PV should be deleted.
        # Not implemented due to bz 1723656
