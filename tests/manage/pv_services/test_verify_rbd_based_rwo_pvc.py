import logging
import pytest

from ocs_ci.ocs.ocp import OCP
from ocs_ci.framework.testlib import ManageTest, tier1
from ocs_ci.utility import templating
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import (
    TimeoutExpiredError, CommandFailed, UnexpectedBehaviour
)
from tests import helpers

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
    Create new project
    """
    self.namespace = helpers.create_unique_resource_name(
        'test', 'namespace'
    )
    self.project_obj = OCP(kind='Project', namespace=self.namespace)

    assert self.project_obj.new_project(self.namespace), (
        f'Failed to create new project {self.namespace}'
    )


def teardown(self):
    """
    Delete project
    """
    self.project_obj.delete(resource_name=self.namespace)
    # Delete Storage Class
    self.sc_obj.delete()


@tier1
@pytest.mark.usefixtures(
    test_fixture.__name__
)
class TestRbdBasedRwoPvc(ManageTest):
    """
    Verifies RBD Based RWO Dynamic PVC creation
    """
    @pytest.mark.parametrize(
        argnames="reclaim_policy",
        argvalues=[
            pytest.param(
                *["Delete"], marks=pytest.mark.polarion_id("OCS-533")
            ),
            pytest.param(
                *["Retain"], marks=pytest.mark.polarion_id("OCS-530")
            )
        ]
    )
    def test_rbd_based_rwo_pvc(
        self,
        reclaim_policy,
        ceph_block_pool_factory,
        rbd_secret_factory,
        rbd_pvc_factory,
        rbd_pod_factory
    ):
        """
        Verifies RBD Based RWO Dynamic PVC creation with Reclaim policy set to
        Delete/Retain

        Steps:
        1. Create Storage Class with reclaimPolicy: Delete/Retain
        2. Create PVC with 'accessModes' 'ReadWriteOnce'
        3. Create two pods using same PVC
        4. Run IO on first pod
        5. Verify second pod is not getting into Running state
        6. Delete first pod
        7. Verify second pod is in Running state
        8. Verify usage of volume in second pod is matching with usage in
           first pod
        9. Run IO on second pod
        10. Delete second pod
        11. Delete PVC
        12. Verify PV associated with deleted PVC is also deleted/released
        """

        # Create Storage Class with reclaimPolicy: Delete
        cbp_obj = ceph_block_pool_factory()
        self.sc_obj = helpers.create_storage_class(
            interface_type=constants.CEPHBLOCKPOOL,
            interface_name=cbp_obj.name,
            secret_name=rbd_secret_factory().name,
            reclaim_policy=reclaim_policy
        )

        # Create PVC with 'accessModes' 'ReadWriteOnce'
        pvc_data = templating.load_yaml_to_dict(constants.CSI_PVC_YAML)
        pvc_data['metadata']['name'] = helpers.create_unique_resource_name(
            'test', 'pvc'
        )
        pvc_data['metadata']['namespace'] = self.namespace
        pvc_data['spec']['storageClassName'] = self.sc_obj.name
        pvc_data['spec']['accessModes'] = ['ReadWriteOnce']
        pvc_obj = rbd_pvc_factory(storageclass=self.sc_obj, custom_data=pvc_data)

        # Create first pod
        log.info(f"Creating two pods which use PVC {pvc_obj.name}")
        pod_obj = rbd_pod_factory(pvc=pvc_obj)

        node_pod1 = pod_obj.get()['spec']['nodeName']

        # Create second pod
        # Try creating pod until it is on a different node than first pod
        for retry in range(1, 6):
            pod_obj2 = rbd_pod_factory(pvc=pvc_obj)

            node_pod2 = pod_obj2.get()['spec']['nodeName']
            if node_pod1 != node_pod2:
                break
            log.info(
                f"Both pods are on same node. Deleting second pod and "
                f"creating another pod. Retry count:{retry}"
            )
            pod_obj2.delete()
            if retry == 5:
                raise UnexpectedBehaviour(
                    "Second pod is always created on same node as of first "
                    "pod even after trying 5 times."
                )

        # Run IO on first pod
        log.info(f"Running IO on first pod {pod_obj.name}")
        pod_obj.run_io('fs', '1G')
        logging.info(f"Waiting for IO results from pod {pod_obj.name}")
        fio_result = pod_obj.get_fio_results()
        logging.info("IOPs after FIO:")
        logging.info(
            f"Read: {fio_result.get('jobs')[0].get('read').get('iops')}"
        )
        logging.info(
            f"Write: {fio_result.get('jobs')[0].get('write').get('iops')}"
        )

        # Fetch usage details
        mount_point = pod_obj.exec_cmd_on_pod(command="df -kh")
        mount_point = mount_point.split()
        usage = mount_point[mount_point.index('/var/lib/www/html') - 1]

        # Verify that second pod is not getting into Running state. Check it
        # for some period of time.
        try:
            assert not pod_obj2.ocp.wait_for_resource(
                condition='Running', resource_name=pod_obj2.name,
            ), "Unexpected: Second pod is in Running state"
        except TimeoutExpiredError:
            log.info(
                f"Verified: Second pod {pod_obj2.name} is not in "
                f"Running state"
            )

        # Delete first pod
        pod_obj.delete(wait=True)

        # Verify pod is deleted
        try:
            pod_obj.get()
            raise UnexpectedBehaviour(
                f"First pod {pod_obj.name} is not deleted."
            )
        except CommandFailed as exp:
            assert "not found" in str(exp), (
                "Failed to fetch pod details"
            )
            log.info(f"First pod {pod_obj.name} is deleted.")

        # Wait for second pod to be in Running state
        try:
            pod_obj2.ocp.wait_for_resource(
                condition='Running', resource_name=pod_obj2.name, timeout=180
            )
        except TimeoutExpiredError as exp:
            raise TimeoutExpiredError(
                f"Second pod {pod_obj2.name} is not in Running state "
                f"after deleting first pod."
            ) from exp
        log.info(
            f"Second pod {pod_obj2.name} is in Running state after "
            f"deleting the first pod."
        )

        # Verify that volume usage in second pod is matching with the usage in
        # first pod
        mount_point = pod_obj2.exec_cmd_on_pod(command="df -kh")
        mount_point = mount_point.split()
        usage_re = mount_point[mount_point.index('/var/lib/www/html') - 1]
        assert usage_re == usage, (
            "Use percentage in new pod is not matching with old pod"
        )

        # Run IO on second pod
        log.info(f"Running IO on second pod {pod_obj2.name}")
        pod_obj2.run_io('fs', '1G')
        logging.info(f"Waiting for IO results from pod {pod_obj2.name}")
        fio_result = pod_obj2.get_fio_results()
        logging.info("IOPs after FIO:")
        logging.info(
            f"Read: {fio_result.get('jobs')[0].get('read').get('iops')}"
        )
        logging.info(
            f"Write: {fio_result.get('jobs')[0].get('write').get('iops')}"
        )

        # Delete second pod
        pod_obj2.delete()

        # Verify pod is deleted
        try:
            pod_obj2.get()
            raise UnexpectedBehaviour(
                f"Second pod {pod_obj2.name} is not deleted."
            )
        except CommandFailed as exp:
            assert "not found" in str(exp), (
                "Failed to fetch pod details"
            )
            log.info(f"Second pod {pod_obj2.name} is deleted.")

        # Get PV name
        pvc_obj.reload()
        pv_name = pvc_obj.backed_pv

        # Delete PVC
        pvc_obj.delete()

        # Verify PVC is deleted
        try:
            pvc_obj.get()
            raise UnexpectedBehaviour(
                f"PVC {pvc_obj.name} is not deleted."
            )
        except CommandFailed as exp:
            assert "not found" in str(exp), (
                "Failed to verify PVC deletion."
            )
            log.info(f"PVC {pvc_obj.name} is deleted.")

        pv_obj = OCP(
            kind=constants.PV, namespace=self.namespace
        )

        if reclaim_policy == "Delete":
            # Verify PV is deleted
            for pv_info in TimeoutSampler(
                30, 2, pv_obj.get, out_yaml_format=False
            ):
                if pv_name not in pv_info:
                    break
                log.warning(
                    f"PV {pv_name} exists after deleting PVC {pvc_obj.name}. "
                    f"Checking again."
                )

            # TODO: Verify PV using ceph toolbox. PV should be deleted.
            # Blocked by bz 1723656

        elif reclaim_policy == "Retain":
            # Wait for PV to be in Released state
            assert pv_obj.wait_for_resource(
                condition='Released', resource_name=pv_name
            )
            log.info(f"PV {pv_name} is in Released state")

            # TODO: Delete PV from backend and verify
            # Blocked by bz 1723656
            pv_obj.delete(resource_name=pv_name)
