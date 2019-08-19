import logging
from concurrent.futures import ThreadPoolExecutor
import pytest

from ocs_ci.framework.testlib import ManageTest, tier4
from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pvc import get_all_pvcs, delete_pvcs
from ocs_ci.ocs.resources.pod import get_all_pods
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.utility.utils import TimeoutSampler, ceph_health_check, run_cmd
from ocs_ci.ocs.resources.pod import (
    get_mds_pods, get_mon_pods, get_mgr_pods, get_osd_pods
)
from tests.helpers import verify_volume_deleted_in_backend
from tests import disruption_helpers
from tests.fixtures import (
    create_rbd_storageclass, create_ceph_block_pool,
    create_cephfs_storageclass, create_rbd_secret, create_cephfs_secret,
    create_project, create_pvcs, create_pods
)

log = logging.getLogger(__name__)


class DisruptionBase(ManageTest):
    """
    Base class for disruptive operations
    """
    num_of_pvcs = 10
    pvc_size = '3Gi'
    pvc_size_int = 3

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
                10, 0.01, func_to_use, self.namespace
            ):
                if func_to_use == get_all_pvcs:
                    current_num = len(sample['items'])
                else:
                    current_num = len(sample)
                if current_num < previous_num:
                    return True
        except TimeoutExpiredError:
            return False

    def delete_pods(self):
        """
        Delete pods
        """
        for pod_obj in self.pod_objs:
            pod_obj.delete(wait=False)
        return True

    def disruptive_base(self, operation_to_disrupt, resource_to_delete):
        """
        Base function for disruptive tests.
        Deletion of 'resource_to_delete' will be introduced while
        'operation_to_disrupt' is progressing.
        """
        pod_functions = {
            'mds': get_mds_pods, 'mon': get_mon_pods, 'mgr': get_mgr_pods,
            'osd': get_osd_pods
        }
        disruption = disruption_helpers.Disruptions()
        disruption.set_resource(resource=resource_to_delete)
        executor = ThreadPoolExecutor(max_workers=1)

        # Get number of pods of type 'resource_to_delete'
        num_of_resource_to_delete = len(pod_functions[resource_to_delete]())

        # Fetch the number of Pods and PVCs
        initial_num_of_pods = len(get_all_pods(namespace=self.namespace))
        initial_num_of_pvc = len(
            get_all_pvcs(namespace=self.namespace)['items']
        )

        # Fetch PV names
        pv_objs = []
        for pvc_obj in self.pvc_objs:
            pvc_obj.reload()
            pv_objs.append(pvc_obj.backed_pv_obj)

        # Fetch volume details from pods for the purpose of verification
        node_pv_dict = {}
        for pod_obj in self.pod_objs:
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

        # Do setup for running IO on pods
        log.info("Setting up pods for running IO")
        for pod_obj in self.pod_objs:
            pod_obj.workload_setup(storage_type='fs')
        log.info("Setup for running IO is completed on pods")

        # Start IO on each pod
        log.info("Starting IO on pods")
        for pod_obj in self.pod_objs:
            pod_obj.run_io(storage_type='fs', size=f'{self.pvc_size_int - 1}G')
        log.info("IO started on all pods.")

        # Start deleting pods
        pod_bulk_delete = executor.submit(self.delete_pods)

        if operation_to_disrupt == 'delete_pods':
            ret = self.verify_resource_deletion(
                get_all_pods, initial_num_of_pods
            )
            assert ret, "Wait timeout: Pods are not being deleted."
            logging.info(
                f"Pods deletion has started."
            )
            disruption.delete_resource()

        pods_deleted = pod_bulk_delete.result()

        assert pods_deleted, "Deletion of pods failed."

        # Verify pods are deleted
        for pod_obj in self.pod_objs:
            assert pod_obj.ocp.wait_for_delete(pod_obj.name), (
                f"Pod {pod_obj.name} is not deleted"
            )
        logging.info("Verified: Pods are deleted.")
        self.pod_objs.clear()

        # Verify that the mount point is removed from nodes after deleting pod
        for node, pvs in node_pv_dict.items():
            cmd = f'oc debug nodes/{node} -- df'
            df_on_node = run_cmd(cmd)
            for pv in pvs:
                assert pv not in df_on_node, (
                    f"{pv} is still present on node {node} after "
                    f"deleting the pods."
                )

        # Fetch image uuid associated with PVCs
        pvc_uuid_map = {}
        for pvc_obj in self.pvc_objs:
            pvc_uuid_map[pvc_obj.name] = pvc_obj.image_uuid

        # Start deleting PVCs
        pvc_bulk_delete = executor.submit(delete_pvcs, self.pvc_objs)

        if operation_to_disrupt == 'delete_pvcs':
            ret = self.verify_resource_deletion(
                get_all_pvcs, initial_num_of_pvc
            )
            assert ret, "Wait timeout: PVCs are not being deleted."
            logging.info(
                f"PVCs deletion has started."
            )
            disruption.delete_resource()

        pvcs_deleted = pvc_bulk_delete.result()

        assert pvcs_deleted, "Deletion of PVCs failed."

        # Verify PVCs are deleted
        for pvc_obj in self.pvc_objs:
            assert pvc_obj.ocp.wait_for_delete(pvc_obj.name), (
                f"PVC {pvc_obj.name} is not deleted"
            )
        logging.info("Verified: PVCs are deleted.")
        self.pvc_objs.clear()

        # Verify PVs are deleted
        for pv_obj in pv_objs:
            assert pv_obj.ocp.wait_for_delete(pv_obj.name), (
                f"PV {pv_obj.name} is not deleted"
            )
        logging.info("Verified: PVs are deleted.")

        # Verify PV using ceph toolbox. Image/Subvolume should be deleted.
        for pvc_name, uuid in pvc_uuid_map.items():
            if self.interface == constants.CEPHBLOCKPOOL:
                ret = verify_volume_deleted_in_backend(
                    interface=self.interface, image_uuid=uuid,
                    pool_name=self.cbp_obj.name
                )
            if self.interface == constants.CEPHFILESYSTEM:
                ret = verify_volume_deleted_in_backend(
                    interface=self.interface, image_uuid=uuid
                )
            assert ret, (
                f"Volume associated with PVC {pvc_name} still exists "
                f"in backend"
            )

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


@pytest.mark.usefixtures(
    create_rbd_secret.__name__,
    create_ceph_block_pool.__name__,
    create_rbd_storageclass.__name__,
    create_project.__name__,
    create_pvcs.__name__,
    create_pods.__name__
)
@tier4
class TestDeleteResourceRBD(DisruptionBase):
    """
    RBD related disruption tests class
    """
    interface = constants.CEPHBLOCKPOOL

    @pytest.mark.parametrize(
        argnames=['operation_to_disrupt', 'resource_to_delete'],
        argvalues=[
            pytest.param(
                *['delete_pvcs', 'mgr'],
                marks=pytest.mark.polarion_id("OCS-922")
            ),
            pytest.param(
                *['delete_pods', 'mgr'],
                marks=pytest.mark.polarion_id("OCS-923")
            ),
            pytest.param(
                *['delete_pvcs', 'mon'],
                marks=pytest.mark.polarion_id("OCS-914")
            ),
            pytest.param(
                *['delete_pods', 'mon'],
                marks=pytest.mark.polarion_id("OCS-911")
            ),
            pytest.param(
                *['delete_pvcs', 'osd'],
                marks=pytest.mark.polarion_id("OCS-912")
            ),
            pytest.param(
                *['delete_pods', 'osd'],
                marks=pytest.mark.polarion_id("OCS-913")
            )

        ]
    )
    def test_disruptive_during_deletion_block(
            self, operation_to_disrupt, resource_to_delete
    ):
        """
        Delete ceph/rook pod while deletion of PVCs/pods is progressing-RBD
        """
        self.disruptive_base(operation_to_disrupt, resource_to_delete)


@pytest.mark.usefixtures(
    create_cephfs_secret.__name__,
    create_cephfs_storageclass.__name__,
    create_project.__name__,
    create_pvcs.__name__,
    create_pods.__name__
)
@tier4
class TestDeleteResourceCephFS(DisruptionBase):
    """
    CephFS related disruption tests class
    """
    interface = constants.CEPHFILESYSTEM

    @pytest.mark.parametrize(
        argnames=['operation_to_disrupt', 'resource_to_delete'],
        argvalues=[
            pytest.param(
                *['delete_pvcs', 'mgr'],
                marks=pytest.mark.polarion_id("OCS-920")
            ),
            pytest.param(
                *['delete_pods', 'mgr'],
                marks=pytest.mark.polarion_id("OCS-915")
            ),
            pytest.param(
                *['delete_pvcs', 'mon'],
                marks=pytest.mark.polarion_id("OCS-918")
            ),
            pytest.param(
                *['delete_pods', 'mon'],
                marks=pytest.mark.polarion_id("OCS-919")
            ),
            pytest.param(
                *['delete_pvcs', 'osd'],
                marks=pytest.mark.polarion_id("OCS-924")
            ),
            pytest.param(
                *['delete_pods', 'osd'],
                marks=pytest.mark.polarion_id("OCS-917")
            ),
            pytest.param(
                *['delete_pvcs', 'mds'],
                marks=pytest.mark.polarion_id("OCS-916")
            ),
            pytest.param(
                *['delete_pods', 'mds'],
                marks=pytest.mark.polarion_id("OCS-921")
            )

        ]
    )
    def test_disruptive_during_deletion_file(
            self, operation_to_disrupt, resource_to_delete
    ):
        """
        Delete ceph/rook pod while deletion of PVCs/pods is progressing-CephFS
        """
        self.disruptive_base(operation_to_disrupt, resource_to_delete)
