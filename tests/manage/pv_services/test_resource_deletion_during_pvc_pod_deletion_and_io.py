import logging
from concurrent.futures import ThreadPoolExecutor
import pytest

from ocs_ci.framework.testlib import ManageTest, tier4
from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pvc import get_all_pvcs, delete_pvcs
from ocs_ci.ocs.resources.pod import get_all_pods, get_fio_rw_iops
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.ocs.resources.pod import (
    get_mds_pods, get_mon_pods, get_mgr_pods, get_osd_pods
)
from ocs_ci.utility.utils import TimeoutSampler, ceph_health_check, run_cmd
from tests.helpers import (
    verify_volume_deleted_in_backend, wait_for_resource_state
)
from tests import disruption_helpers

log = logging.getLogger(__name__)


class DisruptionBase(ManageTest):
    """
    Base class for disruptive operations
    """
    num_of_pvcs = 25
    pvc_size = 3

    def verify_resource_deletion(self, func_to_use, previous_num):
        """
        Wait for resource deletion to start

        Args:
            func_to_use (function): Function to be used to fetch resource info
            previous_num (int): Previous number of resources

        Returns:
            bool: True if resource deletion has started.
                  False in case of timeout.
        """
        try:
            for sample in TimeoutSampler(
                30, 0.01, func_to_use, self.namespace
            ):
                if func_to_use == get_all_pvcs:
                    current_num = len(sample['items'])
                else:
                    current_num = len(sample)
                if current_num < previous_num:
                    return True
        except TimeoutExpiredError:
            return False

    def delete_pods(self, pods_to_delete):
        """
        Delete pods
        """
        for pod_obj in pods_to_delete:
            pod_obj.delete(wait=False)
        return True

    def disruptive_base(
        self, interface, resource_to_delete
    ):
        """
        Base function for disruptive tests.
        Deletion of 'resource_to_delete' will be introduced while
        'operation_to_disrupt' is progressing.
        """
        num_of_pods = 10
        num_of_io_pods = 5
        pods_to_delete = self.pod_objs[:num_of_pods]
        io_pods = self.pod_objs[num_of_pods:num_of_pods + num_of_io_pods]
        pods_for_pvc = self.pod_objs[num_of_pods + num_of_io_pods:]
        pvcs_to_delete = self.pvc_objs[num_of_pods + num_of_io_pods:]
        pod_functions = {
            'mds': get_mds_pods, 'mon': get_mon_pods, 'mgr': get_mgr_pods,
            'osd': get_osd_pods
        }
        disruption = disruption_helpers.Disruptions()
        disruption.set_resource(resource=resource_to_delete)
        executor = ThreadPoolExecutor(max_workers=len(self.pod_objs))

        # Get number of pods of type 'resource_to_delete'
        num_of_resource_to_delete = len(pod_functions[resource_to_delete]())

        # Fetch the number of Pods and PVCs
        initial_num_of_pods = len(get_all_pods(namespace=self.namespace))
        initial_num_of_pvc = len(
            get_all_pvcs(namespace=self.namespace)['items']
        )

        # Fetch PV names to verify after deletion
        pv_objs = []
        for pvc_obj in pvcs_to_delete:
            pvc_obj.reload()
            pv_objs.append(pvc_obj.backed_pv_obj)

        # Fetch volume details from pods for the purpose of verification
        node_pv_dict = {}
        for pod_obj in pods_to_delete:
            pod_info = pod_obj.get()
            node = pod_info['spec']['nodeName']
            pvc = pod_info['spec']['volumes'][0]['persistentVolumeClaim']['claimName']
            for pvc_obj in self.pvc_objs:
                if pvc_obj.name == pvc:
                    pvc_obj.reload()
                    pv = pvc_obj.backed_pv
                    break
            if node in node_pv_dict:
                node_pv_dict[node].append(pv)
            else:
                node_pv_dict[node] = [pv]

        # Fetch image uuid associated with PVCs to be deleted
        pvc_uuid_map = {}
        for pvc_obj in pvcs_to_delete:
            pvc_uuid_map[pvc_obj.name] = pvc_obj.image_uuid
        log.info("Fetched image uuid associated with each PVC")

        # Do setup on pods for running IO
        log.info("Setting up pods for running IO.")
        for pod_obj in self.pod_objs:
            executor.submit(pod_obj.workload_setup, storage_type='fs')

        # Wait for setup on pods to complete
        for pod_obj in self.pod_objs:
            for sample in TimeoutSampler(
                100, 2, getattr, pod_obj, 'wl_setup_done'
            ):
                if sample:
                    log.info(
                        f"Setup for running IO is completed on pod "
                        f"{pod_obj.name}."
                    )
                    break
        log.info("Setup for running IO is completed on all pods.")

        # Start IO on pods having PVCs to delete to load data
        log.info("Starting IO on pods having PVCs to delete.")
        for pod_obj in pods_for_pvc:
            pod_obj.run_io(storage_type='fs', size=f'{self.pvc_size - 1}G')
        log.info("IO started on pods having PVCs to delete.")

        log.info("Fetching IO results from the pods having PVCs to delete.")
        for pod_obj in pods_for_pvc:
            get_fio_rw_iops(pod_obj)
        log.info("Verified IO result on pods having PVCs to delete.")

        # Delete pods having PVCs to delete.
        assert self.delete_pods(pods_for_pvc), (
            "Couldn't delete pods which are having PVCs to delete."
        )
        for pod_obj in pods_for_pvc:
            assert pod_obj.ocp.wait_for_delete(pod_obj.name), (
                f"Pod {pod_obj.name} is not deleted"
            )
        logging.info("Verified: Deleted pods which are having PVCs to delete.")

        # Start IO on pods to be deleted
        log.info("Starting IO on pods to be deleted.")
        for pod_obj in pods_to_delete:
            pod_obj.run_io(storage_type='fs', size=f'{self.pvc_size - 1}G')
        log.info("IO started on pods to be deleted.")

        # Start deleting PVCs
        pvc_bulk_delete = executor.submit(delete_pvcs, pvcs_to_delete)
        log.info("Started deleting PVCs")

        # Start deleting pods
        pod_bulk_delete = executor.submit(self.delete_pods, pods_to_delete)
        log.info("Started deleting pods")

        # Start IO on IO pods
        for pod_obj in io_pods:
            pod_obj.run_io(storage_type='fs', size=f'{self.pvc_size - 1}G')
        log.info("Started IO on IO pods")

        # Verify pvc deletion has started
        pvc_deleting = executor.submit(
            self.verify_resource_deletion, get_all_pvcs, initial_num_of_pvc
        )

        # Verify pod deletion has started
        pod_deleting = executor.submit(
            self.verify_resource_deletion, get_all_pods, initial_num_of_pods
        )

        assert pvc_deleting.result(), (
            "Wait timeout: PVCs are not being deleted."
        )
        logging.info(
            f"PVCs deletion has started."
        )

        assert pod_deleting.result(), (
            "Wait timeout: Pods are not being deleted."
        )
        logging.info(
            f"Pods deletion has started."
        )

        # Delete pod of type 'resource_to_delete'
        disruption.delete_resource()

        pods_deleted = pod_bulk_delete.result()
        assert pods_deleted, "Deletion of pods failed."

        # Verify pods are deleted
        for pod_obj in pods_to_delete:
            assert pod_obj.ocp.wait_for_delete(pod_obj.name), (
                f"Pod {pod_obj.name} is not deleted"
            )
        logging.info("Verified: Pods are deleted.")

        # Verify that the mount point is removed from nodes after deleting pod
        for node, pvs in node_pv_dict.items():
            cmd = f'oc debug nodes/{node} -- df'
            df_on_node = run_cmd(cmd)
            for pv in pvs:
                assert pv not in df_on_node, (
                    f"{pv} is still present on node {node} after "
                    f"deleting the pods."
                )
        log.info(
            "Verified: mount points are removed from nodes after deleting "
            "the pods"
        )

        pvcs_deleted = pvc_bulk_delete.result()
        assert pvcs_deleted, "Deletion of PVCs failed."

        # Verify PVCs are deleted
        for pvc_obj in pvcs_to_delete:
            assert pvc_obj.ocp.wait_for_delete(pvc_obj.name), (
                f"PVC {pvc_obj.name} is not deleted"
            )
        logging.info("Verified: PVCs are deleted.")

        # Verify PVs are deleted
        for pv_obj in pv_objs:
            assert pv_obj.ocp.wait_for_delete(
                resource_name=pv_obj.name, timeout=300
            ), (
                f"PV {pv_obj.name} is not deleted"
            )
        logging.info("Verified: PVs are deleted.")

        # Verify PV using ceph toolbox. Image/Subvolume should be deleted.
        for pvc_name, uuid in pvc_uuid_map.items():
            if interface == constants.CEPHBLOCKPOOL:
                ret = verify_volume_deleted_in_backend(
                    interface=interface, image_uuid=uuid,
                    pool_name=self.sc_obj.ceph_pool.name
                )
            if interface == constants.CEPHFILESYSTEM:
                ret = verify_volume_deleted_in_backend(
                    interface=interface, image_uuid=uuid
                )
            assert ret, (
                f"Volume associated with PVC {pvc_name} still exists "
                f"in backend"
            )

        log.info("Fetching IO results from the pods.")
        for pod_obj in io_pods:
            get_fio_rw_iops(pod_obj)
        log.info("Verified IO result on pods.")

        # Verify number of pods of type 'resource_to_delete'
        final_num_resource_to_delete = len(pod_functions[resource_to_delete]())
        assert final_num_resource_to_delete == num_of_resource_to_delete, (
            f"Total number of {resource_to_delete} pods is not matching with "
            f"initial value. Total number of pods before deleting a pod: "
            f"{num_of_resource_to_delete}. Total number of pods present now: "
            f"{final_num_resource_to_delete}"
        )

        # Check ceph status
        ceph_health_check(namespace=config.ENV_DATA['cluster_namespace'])
        log.info("Ceph cluster health is OK")


@tier4
@pytest.mark.parametrize(
    argnames=['interface', 'resource_to_delete'],
    argvalues=[
        pytest.param(
            *[constants.CEPHBLOCKPOOL, 'mgr'],
            marks=pytest.mark.polarion_id("OCS-810")
        ),
        pytest.param(
            *[constants.CEPHBLOCKPOOL, 'mon'],
            marks=pytest.mark.polarion_id("OCS-811")
        ),
        pytest.param(
            *[constants.CEPHBLOCKPOOL, 'osd'],
            marks=pytest.mark.polarion_id("OCS-812")
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, 'mgr'],
            marks=pytest.mark.polarion_id("OCS-813")
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, 'mon'],
            marks=pytest.mark.polarion_id("OCS-814")
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, 'osd'],
            marks=pytest.mark.polarion_id("OCS-815")
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, 'mds'],
            marks=pytest.mark.polarion_id("OCS-816")
        )
    ]
)
class TestResourceDeletionDuringMultipleDeleteOperations(DisruptionBase):
    """
    Delete ceph/rook pod while deletion of PVCs, pods and IO are progressing
    """
    @pytest.fixture()
    def setup_base(
        self, interface, multi_pvc_factory, pod_factory
    ):
        """
        Create PVCs and pods
        """
        pvc_objs = multi_pvc_factory(
            interface=interface,
            project=None,
            storageclass=None,
            size=self.pvc_size,
            access_mode=constants.ACCESS_MODE_RWO,
            status=constants.STATUS_BOUND,
            num_of_pvc=self.num_of_pvcs,
            wait_each=False
        )

        pod_objs = []
        for pvc_obj in pvc_objs:
            pod_obj = pod_factory(pvc=pvc_obj, status="")
            pod_objs.append(pod_obj)
        for pod_obj in pod_objs:
            wait_for_resource_state(
                resource=pod_obj, state=constants.STATUS_RUNNING
            )

        return pvc_objs, pod_objs

    def test_disruptive_during_pod_pvc_deletion_and_io(
        self, interface, resource_to_delete,
        setup_base
    ):
        """
        Delete ceph/rook pod while PVCs deletion, pods deletion and IO are
        progressing
        """
        self.pvc_objs, self.pod_objs = setup_base
        self.sc_obj = self.pvc_objs[0].storageclass
        self.namespace = self.pvc_objs[0].project.namespace
        self.disruptive_base(
            interface, resource_to_delete
        )
