import logging
from concurrent.futures import ThreadPoolExecutor
import pytest

from ocs_ci.framework.testlib import ManageTest, tier4
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pvc import delete_pvcs
from ocs_ci.ocs.resources.pod import get_fio_rw_iops
from tests import helpers, disruption_helpers
from tests.fixtures import (
    create_rbd_storageclass, create_ceph_block_pool,
    create_cephfs_storageclass, create_rbd_secret, create_cephfs_secret,
    create_project, create_pvcs
)

log = logging.getLogger(__name__)


@pytest.fixture()
def test_fixture(request):
    """
    Setup and teardown
    """
    cls_ref = request.node.cls
    cls_ref.pvc_objs_new = []
    cls_ref.pod_objs = []

    def finalizer():
        # Delete pods
        for pod_obj in cls_ref.pod_objs:
            pod_obj.delete()

        # Delete newly created PVCs
        delete_pvcs(cls_ref.pvc_objs_new)

    request.addfinalizer(finalizer)


@pytest.mark.usefixtures(create_project.__name__)
class OperationsBase(ManageTest):
    """
    Base class for PVC related disruption tests
    """
    num_of_pvcs = 10
    pvc_size = '3Gi'
    pvc_size_int = 3
    pvc_num_for_io_pods = 5
    num_of_new_pvcs = 5

    def operations_base(self, resource_to_delete):
        """
        Delete resource 'resource_to_delete' while PVCs creation, Pods
        creation and IO operation are progressing.
        Verifies PVCs can be re-used by creating new pods.

        Steps:
        1. Create pods for running IO and verify they are Running.
        2. Start creating more pods.
        3. Start creating new PVCs.
        4. Start IO on pods created in Step 1.
        5. Delete the resource 'resource_to_delete'.
        6. Verify that PVCs created in Step 3 are in Bound state.
        7. Verify that pods created in Step 2 are Running.
        8. Verify IO results.
        9. Delete pods created in Steps 1 and 2.
        10. Use all PVCs to create new pods. One PVC for one pod.
        11. Start IO on all pods created in Step 10.
        12. Verify IO results.
        """
        # Separate the available PVCs
        pvc_objs_for_io_pods = self.pvc_objs[0:self.pvc_num_for_io_pods]
        pvc_objs_new_pods = self.pvc_objs[self.pvc_num_for_io_pods:]

        executor = ThreadPoolExecutor(max_workers=2)

        disruption = disruption_helpers.Disruptions()
        disruption.set_resource(resource=resource_to_delete)

        # Create pods for running IO
        io_pods = helpers.create_pods(
            pvc_objs_list=pvc_objs_for_io_pods, interface_type=self.interface,
            desired_status=constants.STATUS_RUNNING, wait=True,
            namespace=self.namespace
        )

        # Updating self.pod_objs for the purpose of teardown
        self.pod_objs.extend(io_pods)

        # Do setup for running IO on pods
        log.info("Setting up pods for running IO")
        for pod_obj in io_pods:
            pod_obj.workload_setup(storage_type='fs')
        log.info("Setup for running IO is completed on pods")

        # Start creating new pods
        log.info("Start creating new pods.")
        bulk_pod_create = executor.submit(
            helpers.create_pods, pvc_objs_list=pvc_objs_new_pods,
            interface_type=self.interface, wait=False,
            namespace=self.namespace
        )

        # Start creation of new PVCs
        log.info("Start creating new PVCs.")
        bulk_pvc_create = executor.submit(
            helpers.create_multiple_pvcs, sc_name=self.sc_obj.name,
            namespace=self.namespace, number_of_pvc=self.num_of_new_pvcs,
            size=self.pvc_size, wait=False
        )

        # Start IO on each pod
        log.info("Start IO on pods")
        for pod_obj in io_pods:
            pod_obj.run_io(storage_type='fs', size=f'{self.pvc_size_int - 1}G')
        log.info("IO started on all pods.")

        # Delete the resource
        disruption.delete_resource()

        # Getting result of PVC creation as list of PVC objects
        pvc_objs_new = bulk_pvc_create.result()

        # Updating self.pvc_objs_new for the purpose of teardown
        self.pvc_objs_new.extend(pvc_objs_new)

        # Verify PVCs are Bound
        for pvc_obj in pvc_objs_new:
            assert pvc_obj.ocp.wait_for_resource(
                condition=constants.STATUS_BOUND, resource_name=pvc_obj.name,
                timeout=240, sleep=10
            ), (
                f"Wait timeout: PVC {pvc_obj.name} is not in 'Bound' status"
            )
        log.info("Verified: New PVCs are Bound.")

        # Getting result of pods creation as list of Pod objects
        pod_objs_new = bulk_pod_create.result()

        # Updating self.pod_objs for the purpose of teardown
        self.pod_objs.extend(pod_objs_new)

        # Verify new pods are Running
        for pod_obj in pod_objs_new:
            assert pod_obj.ocp.wait_for_resource(
                condition=constants.STATUS_RUNNING,
                resource_name=pod_obj.name, timeout=240, sleep=10
            ), (
                f"Wait timeout: Pod {pod_obj.name} is not in 'Running' "
                f"state even after 120 seconds."
            )
        log.info("Verified: All pods are Running.")

        # Verify IO
        log.info("Fetching IO results.")
        for pod_obj in io_pods:
            get_fio_rw_iops(pod_obj)
        log.info("Verified IO result on pods.")

        # Delete pods
        all_pod_objs = io_pods + pod_objs_new
        for pod_obj in all_pod_objs:
            pod_obj.delete(wait=False)

        # Verify pods are deleted
        for pod_obj in all_pod_objs:
            pod_obj.ocp.wait_for_delete(resource_name=pod_obj.name)

        # Updating self.pod_objs for the purpose of teardown
        self.pod_objs.clear()

        # Verify that PVCs are reusable by creating new pods
        all_pvc_objs = self.pvc_objs + pvc_objs_new
        pod_objs_re = helpers.create_pods(
            pvc_objs_list=all_pvc_objs, interface_type=self.interface,
            desired_status=constants.STATUS_RUNNING, wait=True,
            namespace=self.namespace
        )
        log.info("Successfully created new pods using all PVCs.")

        # Updating self.pod_objs for the purpose of teardown
        self.pod_objs.extend(pod_objs_re)

        # Run IO on each of the newly created pods
        for pod_obj in pod_objs_re:
            pod_obj.run_io(
                storage_type='fs', size='100M', runtime=10,
                fio_filename='fio-file-retest'
            )

        log.info("Fetching IO results from newly created pods")
        for pod_obj in pod_objs_re:
            get_fio_rw_iops(pod_obj)
        log.info("Verified IO result on newly created pods.")


@tier4
@pytest.mark.usefixtures(
    create_rbd_secret.__name__,
    create_ceph_block_pool.__name__,
    create_rbd_storageclass.__name__,
    create_pvcs.__name__,
    test_fixture.__name__
)
class TestResourceDeletionMultiOperationsRBD(OperationsBase):
    """
    Test class for RBD
    """
    interface = constants.CEPHBLOCKPOOL

    @pytest.mark.parametrize(
        argnames="resource_to_delete",
        argvalues=[
            pytest.param(
                *['mgr'],
                marks=pytest.mark.polarion_id("OCS-735")
            ),
            pytest.param(
                *['mon'],
                marks=pytest.mark.polarion_id("OCS-736")
            ),
            pytest.param(
                *['osd'],
                marks=pytest.mark.polarion_id("OCS-737")
            )

        ]
    )
    def test_resource_deletion_during_pvc_pod_creation_and_io_block(
        self, resource_to_delete
    ):
        """
        Delete resource while PVC creation, Pods creation, IO are progressing.
        RBD PVC
        """
        self.operations_base(resource_to_delete)


@tier4
@pytest.mark.usefixtures(
    create_cephfs_secret.__name__,
    create_cephfs_storageclass.__name__,
    create_pvcs.__name__,
    test_fixture.__name__
)
class TestResourceDeletionMultiOperationsCephFS(OperationsBase):
    """
    Test class for CephFS
    """
    interface = constants.CEPHFILESYSTEM

    @pytest.mark.parametrize(
        argnames="resource_to_delete",
        argvalues=[
            pytest.param(
                *['mgr'],
                marks=pytest.mark.polarion_id("OCS-738")
            ),
            pytest.param(
                *['mon'],
                marks=pytest.mark.polarion_id("OCS-739")
            ),
            pytest.param(
                *['osd'],
                marks=pytest.mark.polarion_id("OCS-740")
            ),
            pytest.param(
                *['mds'],
                marks=pytest.mark.polarion_id("OCS-741")
            )
        ]
    )
    def test_resource_deletion_during_pvc_pod_creation_and_io_file(
        self, resource_to_delete
    ):
        """
        Delete resource while PVC creation, Pods creation, IO are progressing.
        CephFS PVC
        """
        self.operations_base(resource_to_delete)
