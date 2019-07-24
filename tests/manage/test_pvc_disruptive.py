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


class BaseDisruption(ManageTest):
    """
    Base class for PVC related disruption tests
    """
    sc_obj = None
    pod_obj = None
    pvc_obj = None
    storage_type = None
    namespace = None

    def verify_resource_creation(self, func_to_use, previous_num):
        """
        Wait for new resources to be created.

        Args:
            func_to_use (function): Function to be used to fetch resource info
            previous_num (int): Previous number of resources

        Returns:
            bool: True if resource creation has started.
                  False in case of timeout.
        """
        try:
            for sample in TimeoutSampler(10, 1, func_to_use, self.namespace):
                if func_to_use == get_all_pvcs:
                    current_num = len(sample['items'])
                else:
                    current_num = len(sample)
                if current_num > previous_num:
                    return True
        except TimeoutExpiredError:
            return False

    def disruptive_base(self, operation_to_disrupt, resource_to_delete):
        """
        Base function for PVC disruptive tests.
        Deletion of 'resource_to_delete' will be introduced while
        'operation_to_disrupt' is progressing.
        """
        # Fetch the number of Pods and PVCs
        initial_num_of_pods = len(pod.get_all_pods(namespace=self.namespace))
        initial_num_of_pvc = len(
            get_all_pvcs(namespace=self.namespace)['items']
        )

        executor = ThreadPoolExecutor(max_workers=1)

        DISRUPTION_OPS.set_resource(resource=resource_to_delete)

        # Start creation of multiple PVCs. Create 5 PVCs
        bulk_pvc_create = executor.submit(
            helpers.create_multiple_pvcs, sc_name=self.sc_obj.name,
            namespace=self.namespace, number_of_pvc=5
        )

        if operation_to_disrupt == 'create_pvc':
            # Ensure PVCs are being created before deleting the resource
            ret = self.verify_resource_creation(
                get_all_pvcs, initial_num_of_pvc
            )
            assert ret, "Wait timeout: PVCs are not being created."
            logging.info(
                f"PVCs creation has started."
            )
            DISRUPTION_OPS.delete_resource()

        pvc_objs = bulk_pvc_create.result()

        # Verify PVCs are Bound
        for pvc_obj in pvc_objs:
            assert pvc_obj.ocp.wait_for_resource(
                condition=constants.STATUS_BOUND, resource_name=pvc_obj.name,
                timeout=120
            ), (
                f"Wait timeout: PVC {pvc_obj.name} is not in 'Bound' status "
                f"even after 120 seconds."
            )
        logging.info("Verified: PVCs are Bound.")

        # Start creating pods
        bulk_pod_create = executor.submit(
            helpers.create_pods, pvc_objs_list=pvc_objs,
            interface_type=self.interface, wait=False,
            namespace=self.namespace
        )

        if operation_to_disrupt == 'create_pod':
            # Ensure that pods are being created before deleting the resource
            ret = self.verify_resource_creation(
                pod.get_all_pods, initial_num_of_pods
            )
            assert ret, "Wait timeout: Pods are not being created."
            logging.info(
                f"Pods creation has started."
            )
            DISRUPTION_OPS.delete_resource()

        pod_objs = bulk_pod_create.result()

        # Verify pods are Running
        for pod_obj in pod_objs:
            assert pod_obj.ocp.wait_for_resource(
                condition=constants.STATUS_RUNNING,
                resource_name=pod_obj.name, timeout=120
            ), (
                f"Wait timeout: Pod {pod_obj.name} is not in 'Running' "
                f"state even after 120 seconds."
            )
        logging.info("Verified: All pods are Running.")

        # Start IO on each pod
        for pod_obj in pod_objs:
            pod_obj.run_io(
                storage_type='fs', size='1G', runtime=10,
                fio_filename='fio-file1'
            )
        logging.info("FIO started on all pods.")

        if operation_to_disrupt == 'run_io':
            DISRUPTION_OPS.delete_resource()

        logging.info("Fetching FIO results.")
        for pod_obj in pod_objs:
            fio_result = pod_obj.get_fio_results()
            logging.info(f"IOPs after FIO on pod {pod_obj.name}:")
            logging.info(
                f"Read: {fio_result.get('jobs')[0].get('read').get('iops')}"
            )
            logging.info(
                f"Write: {fio_result.get('jobs')[0].get('write').get('iops')}"
            )
        logging.info("Verified FIO result on pods.")

        # Delete pods
        for pod_obj in pod_objs:
            pod_obj.delete(wait=True)

        # Verify that PVCs are reusable by creating new pods
        create_pods = executor.submit(
            helpers.create_pods, pvc_objs_list=pvc_objs,
            interface_type=self.interface, wait=False, namespace=self.namespace
        )
        pod_objs = create_pods.result()

        # Verify new pods are Running
        for pod_obj in pod_objs:
            assert pod_obj.ocp.wait_for_resource(
                condition=constants.STATUS_RUNNING,
                resource_name=pod_obj.name, timeout=120
            ), (
                f"Wait timeout: Pod {pod_obj.name} is not in 'Running' "
                f"state even after 120 seconds."
            )
        logging.info("Verified: All new pods are Running.")

        # Run IO on each of the new pods
        for pod_obj in pod_objs:
            pod_obj.run_io(
                storage_type='fs', size='1G', runtime=10,
                fio_filename='fio-file2'
            )

        logging.info("Fetching FIO results from new pods")
        for pod_obj in pod_objs:
            fio_result = pod_obj.get_fio_results()
            logging.info(f"IOPs after FIO on pod {pod_obj.name}:")
            logging.info(
                f"Read: {fio_result.get('jobs')[0].get('read').get('iops')}"
            )
            logging.info(
                f"Write: {fio_result.get('jobs')[0].get('write').get('iops')}"
            )
        logging.info("Verified FIO result on new pods.")

        # Delete new pods
        for pod_obj in pod_objs:
            pod_obj.delete()

        # Delete PVCs
        for pvc_obj in pvc_objs:
            pvc_obj.delete()


@tier4
class TestRBDDisruption(BaseDisruption):
    """
    RBD PVC related disruption tests class
    """
    interface = constants.CEPHBLOCKPOOL

    @pytest.mark.parametrize(
        argnames=["operation_to_disrupt", "resource_to_delete"],
        argvalues=[
            pytest.param(
                *['create_pvc', 'mgr'],
                marks=pytest.mark.polarion_id("OCS-568")
            ),
            pytest.param(
                *['create_pod', 'mgr'],
                marks=pytest.mark.polarion_id("OCS-569")
            ),
            pytest.param(
                *['run_io', 'mgr'], marks=pytest.mark.polarion_id("OCS-570")
            ),
            pytest.param(
                *['create_pvc', 'mon'],
                marks=pytest.mark.polarion_id("OCS-561")
            ),
            pytest.param(
                *['create_pod', 'mon'],
                marks=pytest.mark.polarion_id("OCS-562")
            ),
            pytest.param(
                *['run_io', 'mon'], marks=pytest.mark.polarion_id("OCS-563")
            ),
            pytest.param(
                *['create_pvc', 'osd'],
                marks=pytest.mark.polarion_id("OCS-565")
            ),
            pytest.param(
                *['create_pod', 'osd'],
                marks=pytest.mark.polarion_id("OCS-554")
            ),
            pytest.param(
                *['run_io', 'osd'], marks=pytest.mark.polarion_id("OCS-566")
            )

        ]
    )
    def test_disruptive_block(
            self, operation_to_disrupt, resource_to_delete, cephfs_storageclass_factory):
        """
        RBD PVC related disruption tests class method
        """
        self.sc_obj = cephfs_storageclass_factory()
        self.namespace = self.sc_obj.namespace
        self.disruptive_base(operation_to_disrupt, resource_to_delete)


@tier4
class TestFSDisruption(BaseDisruption):
    """
    CephFS PVC related disruption tests class
    """
    interface = constants.CEPHFILESYSTEM

    @pytest.mark.parametrize(
        argnames=["operation_to_disrupt", "resource_to_delete"],
        argvalues=[
            pytest.param(
                *['create_pvc', 'mgr'],
                marks=pytest.mark.polarion_id("OCS-555")
            ),
            pytest.param(
                *['create_pod', 'mgr'],
                marks=pytest.mark.polarion_id("OCS-558")
            ),
            pytest.param(
                *['run_io', 'mgr'], marks=pytest.mark.polarion_id("OCS-559")
            ),
            pytest.param(
                *['create_pvc', 'mon'],
                marks=pytest.mark.polarion_id("OCS-560")
            ),
            pytest.param(
                *['create_pod', 'mon'],
                marks=pytest.mark.polarion_id("OCS-550")
            ),
            pytest.param(
                *['run_io', 'mon'], marks=pytest.mark.polarion_id("OCS-551")
            ),
            pytest.param(
                *['create_pvc', 'osd'],
                marks=pytest.mark.polarion_id("OCS-552")
            ),
            pytest.param(
                *['create_pod', 'osd'],
                marks=pytest.mark.polarion_id("OCS-553")
            ),
            pytest.param(
                *['run_io', 'osd'], marks=pytest.mark.polarion_id("OCS-549")
            ),
            pytest.param(
                *['create_pvc', 'mds'],
                marks=pytest.mark.polarion_id("OCS-564")
            ),
            pytest.param(
                *['create_pod', 'mds'],
                marks=pytest.mark.polarion_id("OCS-567")
            ),
            pytest.param(
                *['run_io', 'mds'], marks=pytest.mark.polarion_id("OCS-556")
            )
        ]
    )
    def test_disruptive_file(
            self,
            operation_to_disrupt,
            resource_to_delete,
            rbd_storageclass_factory
    ):
        """
        CephFS PVC related disruption tests class method
        """
        self.sc_obj = rbd_storageclass_factory()
        self.namespace = self.sc_obj.namespace
        self.disruptive_base(operation_to_disrupt, resource_to_delete)
