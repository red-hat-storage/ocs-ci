import logging
import threading

import pytest

from ocs_ci.ocs import constants

log = logging.getLogger(__name__)


def create_pods(
    interface,
    pvc_factory,
    pod_factory,
    storageclass,
    count,
    access_mode,
    volume_mode=None
):
    """
    Create pods for upgrade testing. pvc_factory and pod_factory have to be
    in the same scope.

    Args:
        interface (str): CephBlockPool or CephFileSystem
        pvc_factory (function): Function for creating PVCs
        pod_factory (function): Function for creating pods
        storageclass (obj): Storageclass to use
        count (int): Number of pods to create
        access_mode (str): ReadWriteOnce, ReadOnlyMany or ReadWriteMany.
            This decides the access mode to be used for the PVC
        volume_mode (str): Volume mode for rbd RWO PVC

    Return:
        list: List of generated pods
    """
    log.info(
        f"Creating {count} pods via {interface} using {access_mode}"
        f" access mode, {volume_mode} volume mode and {storageclass.name}"
        f" storageclass"
    )
    pvcs = [
        pvc_factory(
            storageclass=storageclass,
            access_mode=access_mode,
            volume_mode=volume_mode
        ) for _ in range(count)
    ]
    if volume_mode == constants.VOLUME_MODE_BLOCK:
        pod_dict = constants.CSI_RBD_RAW_BLOCK_POD_YAML
        raw_block_pv = True
    else:
        pod_dict = ''
        raw_block_pv = False
    pods = [
        pod_factory(
            interface=interface,
            pvc=pvc,
            raw_block_pv=raw_block_pv,
            pod_dict_path=pod_dict,
        ) for pvc in pvcs
    ]
    return pods


@pytest.fixture(scope='session')
def pre_upgrade_filesystem_pods(
    request,
    pvc_factory_session,
    pod_factory_session,
    default_storageclasses
):
    """
    Generate RBD and CephFS pods for tests before upgrade is executed.
    These pods use filesystem volume type.

    Returns:
        list: List of pods with RBD and CephFs interface
    """
    pods = []
    for reclaim_policy in (
        constants.RECLAIM_POLICY_DELETE,
        constants.RECLAIM_POLICY_RETAIN
    ):
        rbd_pods = create_pods(
            interface=constants.CEPHBLOCKPOOL,
            pvc_factory=pvc_factory_session,
            pod_factory=pod_factory_session,
            storageclass=default_storageclasses.get(reclaim_policy)[0],
            count=10,
            access_mode=constants.ACCESS_MODE_RWO,
            volume_mode=constants.VOLUME_MODE_FILESYSTEM,
        )
        pods.extend(rbd_pods)

        for access_mode in (
            constants.ACCESS_MODE_RWO,
            constants.ACCESS_MODE_RWX
        ):
            cephfs_pods = create_pods(
                interface=constants.CEPHFILESYSTEM,
                pvc_factory=pvc_factory_session,
                pod_factory=pod_factory_session,
                storageclass=default_storageclasses.get(reclaim_policy)[1],
                count=10,
                access_mode=access_mode,
            )
            pods.extend(cephfs_pods)

    return pods


@pytest.fixture(scope='session')
def pre_upgrade_block_pods(
    request,
    pvc_factory_session,
    pod_factory_session,
    default_storageclasses
):
    """
    Generate RBD pods for tests before upgrade is executed.
    These pods use block volume type.

    Returns:
        list: List of pods with RBD interface
    """
    pods = []
    for reclaim_policy in (
        constants.RECLAIM_POLICY_DELETE,
        constants.RECLAIM_POLICY_RETAIN
    ):
        for access_mode in (
            constants.ACCESS_MODE_RWX,
            constants.ACCESS_MODE_RWO
        ):
            rbd_pods = create_pods(
                interface=constants.CEPHBLOCKPOOL,
                pvc_factory=pvc_factory_session,
                pod_factory=pod_factory_session,
                storageclass=default_storageclasses.get(reclaim_policy)[0],
                count=10,
                access_mode=access_mode,
                volume_mode=constants.VOLUME_MODE_BLOCK,
            )
            pods.extend(rbd_pods)

    return pods


@pytest.fixture
def post_upgrade_filesystem_pods(
    pvc_factory,
    pod_factory,
    default_storageclasses
):
    """
    Generate RBD and CephFS pods for tests after upgrade is executed.
    These pods use filesystem volume type.

    Returns:
        list: List of pods with RBD and CephFS interface
    """
    pods = []
    for reclaim_policy in (
        constants.RECLAIM_POLICY_DELETE,
        constants.RECLAIM_POLICY_RETAIN
    ):
        rbd_pods = create_pods(
            interface=constants.CEPHBLOCKPOOL,
            pvc_factory=pvc_factory,
            pod_factory=pod_factory,
            storageclass=default_storageclasses.get(reclaim_policy)[0],
            count=1,
            access_mode=constants.ACCESS_MODE_RWO,
            volume_mode=constants.VOLUME_MODE_FILESYSTEM,
        )
        pods.extend(rbd_pods)

        for access_mode in (
            constants.ACCESS_MODE_RWO,
            constants.ACCESS_MODE_RWX
        ):
            cephfs_pods = create_pods(
                interface=constants.CEPHFILESYSTEM,
                pvc_factory=pvc_factory,
                pod_factory=pod_factory,
                storageclass=default_storageclasses.get(reclaim_policy)[1],
                count=1,
                access_mode=access_mode,
            )
            pods.extend(cephfs_pods)

    return pods


@pytest.fixture
def post_upgrade_block_pods(
    pvc_factory,
    pod_factory,
    default_storageclasses
):
    """
    Generate RBD pods for tests after upgrade is executed.
    These pods use block volume type.

    Returns:
        list: List of pods with RBD interface
    """
    pods = []
    for reclaim_policy in (
        constants.RECLAIM_POLICY_DELETE,
        constants.RECLAIM_POLICY_RETAIN
    ):
        for access_mode in (
            constants.ACCESS_MODE_RWX,
            constants.ACCESS_MODE_RWO
        ):
            rbd_pods = create_pods(
                interface=constants.CEPHBLOCKPOOL,
                pvc_factory=pvc_factory,
                pod_factory=pod_factory,
                storageclass=default_storageclasses.get(reclaim_policy)[0],
                count=1,
                access_mode=access_mode,
                volume_mode=constants.VOLUME_MODE_BLOCK,
            )
            pods.extend(rbd_pods)

    return pods


@pytest.fixture(scope='session')
def upgrade_fio_file(tmp_path_factory):
    """
    File that controls the state of running fio on pods during upgrade.
    """
    upgrade_fio_file = tmp_path_factory.mktemp('upgrade_testing')
    upgrade_fio_file = upgrade_fio_file.joinpath('fio_status')
    upgrade_fio_file.write_text('running')
    return upgrade_fio_file


@pytest.fixture(scope='session')
def pre_upgrade_pods_running_io(
    pre_upgrade_filesystem_pods,
    pre_upgrade_block_pods,
    upgrade_fio_file
):

    def run_io_in_bg():
        """
        Run IO by executing FIO and deleting the file created for FIO on
        the pod, in a while true loop. Will be running as long as
        the upgrade_fio_file contains string 'running'.
        """
        while upgrade_fio_file.read_text() == 'running':
            for pod in pre_upgrade_filesystem_pods:
                log.warning(f"Running fio on fs pod {pod.name}")
                pod.run_io(
                    storage_type='fs',
                    size='1GB'
                )
            for pod in pre_upgrade_block_pods:
                log.warning(f"Running fio on block pod {pod.name}")
                pod.run_io(
                    storage_type='block',
                    size='1024MB'
                )
            for pod in pre_upgrade_filesystem_pods + pre_upgrade_block_pods:
                result = pod.get_fio_results()
                assert result.get('jobs')[0].get('read').get('iops')
                assert result.get('jobs')[0].get('write').get('iops')

    thread = threading.Thread(target=run_io_in_bg)
    thread.start()
    return pre_upgrade_filesystem_pods + pre_upgrade_block_pods
