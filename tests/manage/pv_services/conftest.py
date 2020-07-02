import logging
import pytest

from ocs_ci.ocs import constants
from ocs_ci.ocs.resources import pod

log = logging.getLogger(__name__)


@pytest.fixture()
def create_pvcs_and_pods(
    multi_pvc_factory, pod_factory, service_account_factory
):
    """
    Create rbd, cephfs PVCs and dc pods. To be used for test cases which need
    rbd and cephfs PVCs with different access modes.

    """
    def factory(
        pvc_size=3,
        access_modes_rbd=None,
        access_modes_cephfs=None,
        num_of_rbd_pvc=None,
        num_of_cephfs_pvc=None
    ):
        """
        Args:
            pvc_size (int): The requested size for the PVC in GB
            access_modes_rbd (list): List of access modes. One of the
                access modes will be chosen for creating each PVC. To specify
                volume mode, append volume mode in the access mode name
                separated by '-'. Default is set as
                ['ReadWriteOnce', 'ReadWriteOnce-Block', 'ReadWriteMany-Block']
            access_modes_cephfs (list): List of access modes.
                One of the access modes will be chosen for creating each PVC.
                Default is set as ['ReadWriteOnce', 'ReadWriteMany']
            num_of_rbd_pvc (int): Number of rbd PVCs to be created. Value
                should be greater than or equal to the number of elements in
                the list 'access_modes_rbd'
            num_of_cephfs_pvc (int): Number of cephfs PVCs to be created.
                Value should be greater than or equal to the number of
                elements in the list 'access_modes_cephfs'
        Returns:
            list: OCS instance of pods
        """

        access_modes_rbd = access_modes_rbd or [
            constants.ACCESS_MODE_RWO, f'{constants.ACCESS_MODE_RWO}-Block',
            f'{constants.ACCESS_MODE_RWX}-Block'
        ]

        access_modes_cephfs = access_modes_cephfs or [
            constants.ACCESS_MODE_RWO, constants.ACCESS_MODE_RWX
        ]

        num_of_rbd_pvc = num_of_rbd_pvc or len(access_modes_rbd)
        num_of_cephfs_pvc = num_of_cephfs_pvc or len(access_modes_cephfs)

        pvcs_rbd = multi_pvc_factory(
            interface=constants.CEPHBLOCKPOOL, size=pvc_size,
            access_modes=access_modes_rbd,
            status=constants.STATUS_BOUND, num_of_pvc=num_of_rbd_pvc,
            timeout=180
        )

        project = pvcs_rbd[0].project

        pvcs_cephfs = multi_pvc_factory(
            interface=constants.CEPHFILESYSTEM, project=project,
            size=pvc_size, access_modes=access_modes_cephfs,
            status=constants.STATUS_BOUND, num_of_pvc=num_of_cephfs_pvc,
            timeout=180
        )
        pvcs = pvcs_cephfs + pvcs_rbd

        # Set volume mode on PVC objects
        for pvc_obj in pvcs:
            pvc_info = pvc_obj.get()
            setattr(pvc_obj, 'volume_mode', pvc_info['spec']['volumeMode'])

        sa_obj = service_account_factory(project=project)

        pods_dc = []
        for pvc_obj in pvcs:
            if constants.CEPHFS_INTERFACE in pvc_obj.storageclass.name:
                interface = constants.CEPHFILESYSTEM
            else:
                interface = constants.CEPHBLOCKPOOL
            # Create pods. Create 2 pods if PVC access mode is RWX
            pod_dc_objs = [
                pod_factory(
                    interface=interface, pvc=pvc_obj,
                    pod_dict_path=constants.FEDORA_DC_YAML,
                    raw_block_pv=pvc_obj.volume_mode == 'Block',
                    deployment_config=True, service_account=sa_obj,
                ) for _ in range(
                    int(pvc_obj.access_mode != constants.ACCESS_MODE_RWX), 2
                )
            ]
            pods_dc.extend(pod_dc_objs)

        pods = []
        for pod_dc in pods_dc:
            pods.extend(pod.get_all_pods(
                namespace=project.namespace, selector=[pod_dc.name],
                selector_label='name'
            ))

        log.info(
            f"Created {len(pvcs_cephfs)} cephfs PVCs and {len(pvcs_rbd)} rbd "
            f"PVCs. Created {len(pods)} pods. "
        )
        return pvcs, pods

    return factory
