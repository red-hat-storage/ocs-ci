import logging
import pytest

from ocs_ci.ocs import constants, resources

log = logging.getLogger(__name__)


def create_pods(interface, pvc_factory, pod_factory, count, access_mode):
    """
    Create pods for upgrade testing. pvc_factory and pod_factory have to be
    in the same scope.

    Args:
        interface (str): CephBlockPool or CephFileSystem
        pvc_factory (function): Function for creating PVCs
        pod_factory (function): Function for creating pods
        count (int): Number of pods to create
        access_mode (str): ReadWriteOnce, ReadOnlyMany or ReadWriteMany.
            This decides the access mode to be used for the PVC.

    Return:
        list: List of generated pods
    """
    # TODO(fbalak): Use proper constants after
    # https://github.com/red-hat-storage/ocs-ci/issues/1056
    # is resolved
    if interface == constants.CEPHBLOCKPOOL:
        sc_name = "ocs-storagecluster-ceph-rbd"
    elif interface == constants.CEPHFILESYSTEM:
        sc_name = "ocs-storagecluster-cephfs"
    else:
        raise AttributeError(f"Interface '{interface}' is invalid")

    log.info(
        f"Creating {count} pods via {interface} using {access_mode}"
        f" access mode and {sc_name} storageclass")
    metadata = {'name': sc_name}
    sc = resources.ocs.OCS(
        kind=constants.STORAGECLASS,
        metadata=metadata
    )
    sc.reload()
    pvcs = [
        pvc_factory(
            storageclass=sc,
            access_mode=access_mode
        ) for _ in range(count)
    ]
    pods = [
        pod_factory(
            interface=interface,
            pvc=pvc
        ) for pvc in pvcs
    ]
    return pods


@pytest.fixture(scope='session')
def pre_upgrade_pods(request, pvc_factory_session, pod_factory_session):
    """
    Generate RBD and CephFS pods for tests before upgrade is executed.

    Returns:
        list: List of pods with RBD interface
    """
    pods = []
    for access_mode in [
        constants.ACCESS_MODE_RWO,
    ]:
        rbd_pods = create_pods(
            interface=constants.CEPHBLOCKPOOL,
            pvc_factory=pvc_factory_session,
            pod_factory=pod_factory_session,
            count=20,
            access_mode=access_mode
        )
        pods.extend(rbd_pods)

    for access_mode in [
        constants.ACCESS_MODE_RWO,
        constants.ACCESS_MODE_RWX
    ]:
        cephfs_pods = create_pods(
            interface=constants.CEPHFILESYSTEM,
            pvc_factory=pvc_factory_session,
            pod_factory=pod_factory_session,
            count=20,
            access_mode=access_mode
        )
        pods.extend(cephfs_pods)

    return pods


@pytest.fixture
def post_upgrade_pods(pvc_factory, pod_factory):
    """
    Generate pods for tests.

    Returns:
        list: List of pods with RBD and CephFS interface
    """
    pods = []
    for access_mode in [
        constants.ACCESS_MODE_RWO,
    ]:
        rbd_pods = create_pods(
            interface=constants.CEPHBLOCKPOOL,
            pvc_factory=pvc_factory,
            pod_factory=pod_factory,
            count=1,
            access_mode=access_mode
        )
        pods.extend(rbd_pods)

    for access_mode in [
        constants.ACCESS_MODE_RWO,
        constants.ACCESS_MODE_RWX
    ]:
        cephfs_pods = create_pods(
            interface=constants.CEPHFILESYSTEM,
            pvc_factory=pvc_factory,
            pod_factory=pod_factory,
            count=1,
            access_mode=access_mode
        )
        pods.extend(cephfs_pods)

    return pods
