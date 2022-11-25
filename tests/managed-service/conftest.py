import logging
import pytest

from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pod import cal_md5sum

logger = logging.getLogger(name=__file__)


@pytest.fixture(scope="session")
def block_pod(pvc_factory_session, pod_factory_session):
    """
    Returns:
        obj: Utilized pod with RBD pvc

    """
    pvc = pvc_factory_session(size=5, interface=constants.CEPHBLOCKPOOL)
    pod = pod_factory_session(pvc=pvc, interface=constants.CEPHBLOCKPOOL)
    logger.info(f"Utilization of RBD PVC {pvc.name} with pod {pod.name} starts")
    pod.run_io(
        storage_type="fs",
        size="4G",
        fio_filename="fio-rand-write",
    )
    pod.get_fio_results()
    logger.info(f"IO finished on pod {pod.name}")
    return pod


@pytest.fixture(scope="session")
def block_md5(block_pod):
    """
    Returns:
        str: md5 of utilized file

    """
    md5 = cal_md5sum(
        pod_obj=block_pod,
        file_name="fio-rand-write",
        block=False,
    )
    logger.info(f"RBD md5: {md5}")
    return md5


@pytest.fixture(scope="session")
def fs_pod(pvc_factory_session, pod_factory_session):
    """
    Returns:
        obj: Utilized pod with Ceph FS pvc

    """
    pvc = pvc_factory_session(size=5, interface=constants.CEPHFILESYSTEM)
    pod = pod_factory_session(pvc=pvc, interface=constants.CEPHFILESYSTEM)
    logger.info(f"Utilization of Ceph FS PVC {pvc.name} with pod {pod.name} starts")
    pod.run_io(
        storage_type="fs",
        size="4G",
        fio_filename="fio-rand-write",
    )
    pod.get_fio_results()
    logger.info(f"IO finished on pod {pod.name}")
    return pod


@pytest.fixture(scope="session")
def fs_md5(fs_pod):
    """
    Returns:
        str: md5 of utilized file

    """
    md5 = cal_md5sum(
        pod_obj=fs_pod,
        file_name="fio-rand-write",
        block=False,
    )
    logger.info(f"Ceph FS md5: {md5}")
    return md5
