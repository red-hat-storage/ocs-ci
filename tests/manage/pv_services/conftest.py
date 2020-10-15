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
        pods_for_rwx=1,
        access_modes_rbd=None,
        access_modes_cephfs=None,
        num_of_rbd_pvc=None,
        num_of_cephfs_pvc=None,
        replica_count=1,
        deployment_config=False
    ):
        """
        Args:
            pvc_size (int): The requested size for the PVC in GB
            pods_for_rwx (int): Number of pods to be created if PVC
                access mode is RWX
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
            replica_count (int): The replica count for deployment config
            deployment_config (bool): True for DeploymentConfig creation,
                False otherwise
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
        for pvc_obj in pvcs_rbd:
            pvc_obj.interface = constants.CEPHBLOCKPOOL

        project = pvcs_rbd[0].project

        pvcs_cephfs = multi_pvc_factory(
            interface=constants.CEPHFILESYSTEM, project=project,
            size=pvc_size, access_modes=access_modes_cephfs,
            status=constants.STATUS_BOUND, num_of_pvc=num_of_cephfs_pvc,
            timeout=180
        )
        for pvc_obj in pvcs_cephfs:
            pvc_obj.interface = constants.CEPHFILESYSTEM

        pvcs = pvcs_cephfs + pvcs_rbd

        # Set volume mode on PVC objects
        for pvc_obj in pvcs:
            pvc_info = pvc_obj.get()
            setattr(pvc_obj, 'volume_mode', pvc_info['spec']['volumeMode'])

        sa_obj = service_account_factory(project=project) if deployment_config else None

        pods_dc = []
        pods = []

        # Create pods
        for pvc_obj in pvcs:
            if constants.CEPHFS_INTERFACE in pvc_obj.storageclass.name:
                interface = constants.CEPHFILESYSTEM
            else:
                interface = constants.CEPHBLOCKPOOL

            # TODO: Remove pod_dict_path variable if issue 2524 is fixed
            if deployment_config:
                pod_dict_path = constants.FEDORA_DC_YAML
            elif pvc_obj.volume_mode == 'Block':
                pod_dict_path = constants.CSI_RBD_RAW_BLOCK_POD_YAML
            else:
                pod_dict_path = ''

            num_pods = pods_for_rwx if pvc_obj.access_mode == constants.ACCESS_MODE_RWX else 1
            for _ in range(num_pods):
                # pod_obj will be a Pod instance if deployment_config=False,
                # otherwise an OCP instance of kind DC
                pod_obj = pod_factory(
                    interface=interface, pvc=pvc_obj,
                    pod_dict_path=pod_dict_path,
                    raw_block_pv=pvc_obj.volume_mode == 'Block',
                    deployment_config=deployment_config,
                    service_account=sa_obj,
                    replica_count=replica_count
                )
                pod_obj.pvc = pvc_obj
                pods_dc.append(pod_obj) if deployment_config else pods.append(pod_obj)

        # Get pod objects if deployment_config is True
        # pods_dc will be an empty list if deployment_config is False
        for pod_dc in pods_dc:
            pod_objs = pod.get_all_pods(
                namespace=project.namespace, selector=[pod_dc.name],
                selector_label='name'
            )
            for pod_obj in pod_objs:
                pod_obj.pvc = pod_dc.pvc
            pods.extend(pod_objs)

        log.info(
            f"Created {len(pvcs_cephfs)} cephfs PVCs and {len(pvcs_rbd)} rbd "
            f"PVCs. Created {len(pods)} pods. "
        )
        return pvcs, pods

    return factory
