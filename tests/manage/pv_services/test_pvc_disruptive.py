import logging
from concurrent.futures import ThreadPoolExecutor
import pytest

from ocs_ci.framework.testlib import ManageTest, tier4
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pvc import get_all_pvcs
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.utility.utils import TimeoutSampler
from tests import helpers, disruption_helpers


logger = logging.getLogger(__name__)

DISRUPTION_OPS = disruption_helpers.Disruptions()


@tier4
@pytest.mark.parametrize(
    argnames=["interface", "operation_to_disrupt", "resource_to_delete"],
    argvalues=[
        pytest.param(
            *[constants.CEPHBLOCKPOOL, 'create_pvc', 'mgr'],
            marks=pytest.mark.polarion_id("OCS-568")
        ),
        pytest.param(
            *[constants.CEPHBLOCKPOOL, 'create_pod', 'mgr'],
            marks=pytest.mark.polarion_id("OCS-569")
        ),
        pytest.param(
            *[constants.CEPHBLOCKPOOL, 'run_io', 'mgr'],
            marks=pytest.mark.polarion_id("OCS-570")
        ),
        pytest.param(
            *[constants.CEPHBLOCKPOOL, 'create_pvc', 'mon'],
            marks=pytest.mark.polarion_id("OCS-561")
        ),
        pytest.param(
            *[constants.CEPHBLOCKPOOL, 'create_pod', 'mon'],
            marks=pytest.mark.polarion_id("OCS-562")
        ),
        pytest.param(
            *[constants.CEPHBLOCKPOOL, 'run_io', 'mon'],
            marks=pytest.mark.polarion_id("OCS-563")
        ),
        pytest.param(
            *[constants.CEPHBLOCKPOOL, 'create_pvc', 'osd'],
            marks=pytest.mark.polarion_id("OCS-565")
        ),
        pytest.param(
            *[constants.CEPHBLOCKPOOL, 'create_pod', 'osd'],
            marks=pytest.mark.polarion_id("OCS-554")
        ),
        pytest.param(
            *[constants.CEPHBLOCKPOOL, 'run_io', 'osd'],
            marks=pytest.mark.polarion_id("OCS-566")
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, 'create_pvc', 'mgr'],
            marks=pytest.mark.polarion_id("OCS-555")
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, 'create_pod', 'mgr'],
            marks=pytest.mark.polarion_id("OCS-558")
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, 'run_io', 'mgr'],
            marks=pytest.mark.polarion_id("OCS-559")
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, 'create_pvc', 'mon'],
            marks=pytest.mark.polarion_id("OCS-560")
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, 'create_pod', 'mon'],
            marks=pytest.mark.polarion_id("OCS-550")
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, 'run_io', 'mon'],
            marks=pytest.mark.polarion_id("OCS-551")
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, 'create_pvc', 'osd'],
            marks=pytest.mark.polarion_id("OCS-552")
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, 'create_pod', 'osd'],
            marks=pytest.mark.polarion_id("OCS-553")
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, 'run_io', 'osd'],
            marks=pytest.mark.polarion_id("OCS-549")
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, 'create_pvc', 'mds'],
            marks=pytest.mark.polarion_id("OCS-564")
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, 'create_pod', 'mds'],
            marks=pytest.mark.polarion_id("OCS-567")
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, 'run_io', 'mds'],
            marks=pytest.mark.polarion_id("OCS-556")
        )
    ]
)
class TestPVCDisruption(ManageTest):
    """
    Base class for PVC related disruption tests
    """
    @pytest.fixture(autouse=True)
    def setup(self, interface, storageclass_factory, project_factory):
        """
        Create StorageClass and Project for the test

        Returns:
            OCS: An OCS instance of the storage class
            OCP: An OCP instance of project
        """
        self.sc_obj = storageclass_factory(interface=interface)
        self.proj_obj = project_factory()

    def verify_resource_creation(self, func_to_use, previous_num, namespace):
        """
        Wait for new resources to be created.

        Args:
            func_to_use (function): Function to be used to fetch resource info
            previous_num (int): Previous number of resources
            namespace (str): The namespace to look in

        Returns:
            bool: True if resource creation has started.
                  False in case of timeout.
        """
        try:
            for sample in TimeoutSampler(10, 1, func_to_use, namespace):
                if func_to_use == get_all_pvcs:
                    current_num = len(sample['items'])
                else:
                    current_num = len(sample)
                if current_num > previous_num:
                    return True
        except TimeoutExpiredError:
            return False

    def pods_creation(self, pvc_objs, pod_factory, interface):
        """
        Create pods

        Args:
            pvc_objs (list): List of ocs_ci.ocs.resources.pvc.PVC instances
            pvc_objs (function): Function to be used for creating pods
            interface (int): Interface type

        Returns:
            list: list of Pod objects
        """
        pod_objs = []

        # Create one pod using each RWO PVC and two pods using each RWX PVC
        for pvc_obj in pvc_objs:
            if pvc_obj.access_mode == constants.ACCESS_MODE_RWX:
                pod_obj = pod_factory(
                    interface=interface, pvc=pvc_obj, status=""
                )
                pod_objs.append(pod_obj)
            pod_obj = pod_factory(interface=interface, pvc=pvc_obj, status="")
            pod_objs.append(pod_obj)

        return pod_objs

    def test_pvc_disruptive(
        self, interface, operation_to_disrupt, resource_to_delete,
        multi_pvc_factory, pod_factory
    ):
        """
        Base function for PVC disruptive tests.
        Deletion of 'resource_to_delete' will be introduced while
        'operation_to_disrupt' is progressing.
        """
        num_of_pvc = 6
        namespace = self.proj_obj.namespace

        # Fetch the number of Pods and PVCs
        initial_num_of_pods = len(pod.get_all_pods(namespace=namespace))
        initial_num_of_pvc = len(
            get_all_pvcs(namespace=namespace)['items']
        )

        executor = ThreadPoolExecutor(max_workers=(2 * num_of_pvc))

        DISRUPTION_OPS.set_resource(resource=resource_to_delete)

        access_modes = [constants.ACCESS_MODE_RWO]
        if interface == constants.CEPHFILESYSTEM:
            access_modes.append(constants.ACCESS_MODE_RWX)

        # Start creation of PVCs
        bulk_pvc_create = executor.submit(
            multi_pvc_factory, interface=interface,
            project=self.proj_obj, storageclass=self.sc_obj, size=5,
            access_modes=access_modes,
            access_modes_selection='distribute_random',
            status=constants.STATUS_BOUND, num_of_pvc=num_of_pvc,
            wait_each=False
        )

        if operation_to_disrupt == 'create_pvc':
            # Ensure PVCs are being created before deleting the resource
            ret = self.verify_resource_creation(
                get_all_pvcs, initial_num_of_pvc, namespace
            )
            assert ret, "Wait timeout: PVCs are not being created."
            logging.info(
                f"PVCs creation has started."
            )
            DISRUPTION_OPS.delete_resource()

        pvc_objs = bulk_pvc_create.result()

        # Confirm that PVCs are Bound
        for pvc_obj in pvc_objs:
            helpers.wait_for_resource_state(
                resource=pvc_obj, state=constants.STATUS_BOUND, timeout=120
            )
            pvc_obj.reload()
        logging.info("Verified: PVCs are Bound.")

        # Start creating pods
        bulk_pod_create = executor.submit(
            self.pods_creation, pvc_objs, pod_factory, interface
        )

        if operation_to_disrupt == 'create_pod':
            # Ensure that pods are being created before deleting the resource
            ret = self.verify_resource_creation(
                pod.get_all_pods, initial_num_of_pods, namespace
            )
            assert ret, "Wait timeout: Pods are not being created."
            logging.info(
                f"Pods creation has started."
            )
            DISRUPTION_OPS.delete_resource()

        pod_objs = bulk_pod_create.result()

        # Verify pods are Running
        for pod_obj in pod_objs:
            helpers.wait_for_resource_state(
                resource=pod_obj, state=constants.STATUS_RUNNING
            )
            pod_obj.reload()
        logging.info("Verified: All pods are Running.")

        # Do setup on pods for running IO
        logger.info("Setting up pods for running IO.")
        for pod_obj in pod_objs:
            executor.submit(pod_obj.workload_setup, storage_type='fs')

        # Wait for setup on pods to complete
        for pod_obj in pod_objs:
            for sample in TimeoutSampler(
                180, 2, getattr, pod_obj, 'wl_setup_done'
            ):
                if sample:
                    logger.info(
                        f"Setup for running IO is completed on pod "
                        f"{pod_obj.name}."
                    )
                    break
        logger.info("Setup for running IO is completed on all pods.")

        # Start IO on each pod
        for pod_obj in pod_objs:
            pod_obj.run_io(
                storage_type='fs', size='1G', runtime=10,
                fio_filename=f'{pod_obj.name}_io_file1'
            )
        logging.info("FIO started on all pods.")

        if operation_to_disrupt == 'run_io':
            DISRUPTION_OPS.delete_resource()

        logging.info("Fetching FIO results.")
        for pod_obj in pod_objs:
            fio_result = pod_obj.get_fio_results()
            err_count = fio_result.get('jobs')[0].get('error')
            assert err_count == 0, (
                f"FIO error on pod {pod_obj.name}. FIO result: {fio_result}"
            )
        logging.info("Verified FIO result on pods.")

        # Delete pods
        for pod_obj in pod_objs:
            pod_obj.delete(wait=True)
        for pod_obj in pod_objs:
            pod_obj.ocp.wait_for_delete(pod_obj.name)

        # Verify that PVCs are reusable by creating new pods
        create_pods = executor.submit(
            self.pods_creation, pvc_objs, pod_factory, interface
        )
        pod_objs = create_pods.result()

        # Verify new pods are Running
        for pod_obj in pod_objs:
            helpers.wait_for_resource_state(
                resource=pod_obj, state=constants.STATUS_RUNNING
            )
            pod_obj.reload()
        logging.info("Verified: All new pods are Running.")

        # Run IO on each of the new pods
        for pod_obj in pod_objs:
            pod_obj.run_io(
                storage_type='fs', size='1G', runtime=10,
                fio_filename=f'{pod_obj.name}_io_file2'
            )

        logging.info("Fetching FIO results from new pods")
        for pod_obj in pod_objs:
            fio_result = pod_obj.get_fio_results()
            err_count = fio_result.get('jobs')[0].get('error')
            assert err_count == 0, (
                f"FIO error on pod {pod_obj.name}. FIO result: {fio_result}"
            )
        logging.info("Verified FIO result on new pods.")
