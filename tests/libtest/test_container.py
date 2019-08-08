"""
Module to perform IOs with several weights
"""
import logging
import pytest

from ocs_ci.ocs import constants
from ocs_ci.ocs.container import ResourceStor
from ocs_ci.framework.testlib import libtest

log = logging.getLogger(__name__)


@pytest.fixture(scope='class')
def container(request):
    # Create storage class
    log.info("Creating RBD and FS PVC")
    rbd_pvc = ResourceStor(cleanup=True)
    fs_pvc = ResourceStor(interface_type=constants.CEPHFILESYSTEM)
    rbd_pvc.create()
    fs_pvc.create()

    def teardown():
        rbd_pvc.delete()
        fs_pvc.delete()
    request.addfinalizer(teardown)
    return rbd_pvc, fs_pvc


@libtest
def test_container(container):
    """
    simple test to create rbd/fs pvc
    """
    rbd_pvc = container[0]
    log.info(rbd_pvc.pool_name)
