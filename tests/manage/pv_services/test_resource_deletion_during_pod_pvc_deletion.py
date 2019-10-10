import logging
from concurrent.futures import ThreadPoolExecutor
import pytest
from functools import partial

from ocs_ci.framework.testlib import ManageTest, tier4
from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pvc import get_all_pvcs, delete_pvcs
from ocs_ci.ocs.resources.pod import get_all_pods
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.utility.utils import TimeoutSampler, ceph_health_check, run_cmd
from ocs_ci.ocs.resources.pod import (
    get_mds_pods, get_mon_pods, get_mgr_pods, get_osd_pods, get_plugin_pods,
    get_rbdfsplugin_provisioner_pods, get_cephfsplugin_provisioner_pods
)
from tests.helpers import verify_volume_deleted_in_backend
from tests import disruption_helpers

log = logging.getLogger(__name__)


class DisruptionBase(ManageTest):
    """
    Base class for disruptive operations
    """
    num_of_pvcs = 10
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

    def delete_pods(self):
        """
        Delete pods
        """
        for pod_obj in self.pod_objs:
            pod_obj.delete(wait=False)
        return True

    def disruptive_base(
        self, interface, operation_to_disrupt, resource_to_delete
    ):
        """
        Base function for disruptive tests.
        Deletion of 'resource_to_delete' will be introduced while
        'operation_to_disrupt' is progressing.
        """
        pod_functions = {
            'mds': partial(get_mds_pods), 'mon': partial(get_mon_pods),
            'mgr': partial(get_mgr_pods), 'osd': partial(get_osd_pods),
            'rbdplugin': partial(get_plugin_pods, interface=interface),
            'cephfsplugin': partial(get_plugin_pods, interface=interface),
            'cephfsplugin_provisioner': partial(
                get_cephfsplugin_provisioner_pods
            ),
            'rbdplugin_provisioner': partial(get_rbdfsplugin_provisioner_pods)
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

        # Start IO on each pod. RWX PVC will be used on two pods. So split the
        # size accordingly
        log.info("Starting IO on pods")
        for pod_obj in self.pod_objs:
            if pod_obj.pvc.access_mode == constants.ACCESS_MODE_RWX:
                io_size = int((self.pvc_size - 1) / 2)
            else:
                io_size = self.pvc_size - 1
            pod_obj.run_io(
                storage_type='fs', size=f'{io_size}G',
                fio_filename=f'{pod_obj.name}_io'
            )
        log.info("IO started on all pods.")

        # Start deleting pods
        pod_bulk_delete = executor.submit(self.delete_pods)

        if operation_to_disrupt == 'delete_pods':
            ret = self.verify_resource_deletion(
                get_all_pods, initial_num_of_pods
            )
            assert ret, "Wait timeout: Pods are not being deleted."
            log.info(f"Pods deletion has started.")
            disruption.delete_resource()

        pods_deleted = pod_bulk_delete.result()

        assert pods_deleted, "Deletion of pods failed."

        # Verify pods are deleted
        for pod_obj in self.pod_objs:
            assert pod_obj.ocp.wait_for_delete(pod_obj.name, 180), (
                f"Pod {pod_obj.name} is not deleted"
            )
        log.info("Verified: Pods are deleted.")

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

        # Fetch image uuid associated with PVCs
        pvc_uuid_map = {}
        for pvc_obj in self.pvc_objs:
            pvc_uuid_map[pvc_obj.name] = pvc_obj.image_uuid
        log.info("Fetched image uuid associated with each PVC")

        # Start deleting PVCs
        pvc_bulk_delete = executor.submit(delete_pvcs, self.pvc_objs)

        if operation_to_disrupt == 'delete_pvcs':
            ret = self.verify_resource_deletion(
                get_all_pvcs, initial_num_of_pvc
            )
            assert ret, "Wait timeout: PVCs are not being deleted."
            log.info(f"PVCs deletion has started.")
            disruption.delete_resource()

        pvcs_deleted = pvc_bulk_delete.result()

        assert pvcs_deleted, "Deletion of PVCs failed."

        # Verify PVCs are deleted
        for pvc_obj in self.pvc_objs:
            assert pvc_obj.ocp.wait_for_delete(pvc_obj.name), (
                f"PVC {pvc_obj.name} is not deleted"
            )
        log.info("Verified: PVCs are deleted.")

        # Verify PVs are deleted
        for pv_obj in pv_objs:
            assert pv_obj.ocp.wait_for_delete(pv_obj.name, 120), (
                f"PV {pv_obj.name} is not deleted"
            )
        log.info("Verified: PVs are deleted.")

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
    argnames=['interface', 'operation_to_disrupt', 'resource_to_delete'],
    argvalues=[
        pytest.param(
            *[constants.CEPHBLOCKPOOL, 'delete_pvcs', 'mgr'],
            marks=pytest.mark.polarion_id("OCS-922")
        ),
        pytest.param(
            *[constants.CEPHBLOCKPOOL, 'delete_pods', 'mgr'],
            marks=pytest.mark.polarion_id("OCS-923")
        ),
        pytest.param(
            *[constants.CEPHBLOCKPOOL, 'delete_pvcs', 'mon'],
            marks=pytest.mark.polarion_id("OCS-914")
        ),
        pytest.param(
            *[constants.CEPHBLOCKPOOL, 'delete_pods', 'mon'],
            marks=pytest.mark.polarion_id("OCS-911")
        ),
        pytest.param(
            *[constants.CEPHBLOCKPOOL, 'delete_pvcs', 'osd'],
            marks=pytest.mark.polarion_id("OCS-912")
        ),
        pytest.param(
            *[constants.CEPHBLOCKPOOL, 'delete_pods', 'osd'],
            marks=pytest.mark.polarion_id("OCS-913")
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, 'delete_pvcs', 'mgr'],
            marks=pytest.mark.polarion_id("OCS-920")
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, 'delete_pods', 'mgr'],
            marks=pytest.mark.polarion_id("OCS-915")
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, 'delete_pvcs', 'mon'],
            marks=pytest.mark.polarion_id("OCS-918")
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, 'delete_pods', 'mon'],
            marks=pytest.mark.polarion_id("OCS-919")
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, 'delete_pvcs', 'osd'],
            marks=pytest.mark.polarion_id("OCS-924")
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, 'delete_pods', 'osd'],
            marks=pytest.mark.polarion_id("OCS-917")
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, 'delete_pvcs', 'mds'],
            marks=pytest.mark.polarion_id("OCS-916")
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, 'delete_pods', 'mds'],
            marks=pytest.mark.polarion_id("OCS-921")
        ),
        pytest.param(
            *[constants.CEPHBLOCKPOOL, 'delete_pods', 'rbdplugin'],
            marks=[pytest.mark.polarion_id("OCS-1009"), pytest.mark.bugzilla(
                '1752487'
            )]),
        pytest.param(
            *[constants.CEPHFILESYSTEM, 'delete_pods', 'cephfsplugin'],
            marks=pytest.mark.polarion_id("OCS-1018")
        ),
        pytest.param(
            *[constants.CEPHBLOCKPOOL, 'delete_pvcs', 'rbdplugin_provisioner'],
            marks=pytest.mark.polarion_id("OCS-944")
        ),
        pytest.param(
            *[constants.CEPHBLOCKPOOL, 'delete_pods', 'rbdplugin_provisioner'],
            marks=pytest.mark.polarion_id("OCS-943")
        ),
        pytest.param(
            *[
                constants.CEPHFILESYSTEM, 'delete_pvcs',
                'cephfsplugin_provisioner'
            ],
            marks=pytest.mark.polarion_id("OCS-951")
        ),
        pytest.param(
            *[
                constants.CEPHFILESYSTEM, 'delete_pods',
                'cephfsplugin_provisioner'
            ],
            marks=pytest.mark.polarion_id("OCS-950")
        )
    ]
)
class TestDeleteResourceDuringPodPvcDeletion(DisruptionBase):
    """
    Delete ceph/rook pod while deletion of PVCs/pods is progressing
    """
    @pytest.fixture()
    def setup_base(
        self, interface, multi_pvc_factory, pod_factory
    ):
        """
        Create PVCs and pods
        """
        access_modes = [constants.ACCESS_MODE_RWO]
        if interface == constants.CEPHFILESYSTEM:
            access_modes.append(constants.ACCESS_MODE_RWX)
        pvc_objs = multi_pvc_factory(
            interface=interface,
            project=None,
            storageclass=None,
            size=self.pvc_size,
            access_modes=access_modes,
            status=constants.STATUS_BOUND,
            num_of_pvc=self.num_of_pvcs,
            wait_each=False
        )

        pod_objs = []

        # Create one pod using each RWO PVC and two pods using each RWX PVC
        for pvc_obj in pvc_objs:
            if pvc_obj.access_mode == constants.ACCESS_MODE_RWX:
                pod_obj = pod_factory(
                    interface=interface, pvc=pvc_obj,
                    status=constants.STATUS_RUNNING
                )
                pod_objs.append(pod_obj)
            pod_obj = pod_factory(
                interface=interface, pvc=pvc_obj,
                status=constants.STATUS_RUNNING
            )
            pod_objs.append(pod_obj)

        log.info(f"Created {len(pod_objs)} pods.")
        return pvc_objs, pod_objs

    def test_disruptive_during_pod_pvc_deletion(
        self, interface, operation_to_disrupt, resource_to_delete,
        setup_base
    ):
        """
        Delete ceph/rook pod while deletion of PVCs/pods is progressing
        """
        self.pvc_objs, self.pod_objs = setup_base
        self.sc_obj = self.pvc_objs[0].storageclass
        self.namespace = self.pvc_objs[0].project.namespace
        self.disruptive_base(
            interface, operation_to_disrupt, resource_to_delete
        )
