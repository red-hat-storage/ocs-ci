"""
Test to exercise Small File Workload

"""

# Builtin modules
import logging
import time

# 3ed party modules
import pytest

# Local modules
import ocs_ci.ocs.resources.pod as pod
from ocs_ci.ocs.ripsaw import RipSaw
from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import E2ETest, scale
from ocs_ci.ocs.small_file_workload import smallfile_workload

log = logging.getLogger(__name__)


def get_cephfs_data():
    """
    Look through ceph pods and find space usage on all cephfilesystem pods

    Returns:
        Dictionary of byte usage, indexed by pod name.
    """
    ceph_pod = pod.get_ceph_tools_pod()
    ceph_status = ceph_pod.exec_ceph_cmd(ceph_cmd="ceph df")
    ret_value = {}
    for pool in ceph_status["pools"]:
        if "cephfilesystem" in pool["name"]:
            ret_value[pool["name"]] = pool["stats"]["bytes_used"]
    return ret_value


@pytest.fixture(scope="module")
def scale_leaks(request):
    """
    For scale tests, get bytes used by cephfs at the start,
    and compare with the cephfs usage at the finish.
    """

    def teardown():
        """
        After all the ripsaw creation and deltion runs have finished,
        test to see if there was a leak in storage.
        """
        log.info("In scale_leaks teardown")
        time.sleep(120)
        orig_data = pytest.ceph_fs_use
        now_data = get_cephfs_data()
        for entry in now_data:
            log.info(f"{entry} now uses {now_data[entry]} bytes")
            log.info(f"{entry} originally used {now_data[entry]} bytes")
            check = (now_data[entry] - orig_data[entry] < 1000000000,)
            #
            # Maybe we should do some more detailed checking here.
            # For now, report an error if there is 1G that appears to leak.
            #
            errmsg = f"{entry} over 1G larger -- possible leak"
            assert check, errmsg

    pytest.ceph_fs_use = get_cephfs_data()
    request.addfinalizer(teardown)

    return scale_leaks


@pytest.fixture(scope="function")
def ripsaw(request, storageclass_factory):
    def teardown():
        ripsaw.cleanup()
        time.sleep(10)

    request.addfinalizer(teardown)

    ripsaw = RipSaw()

    return ripsaw


@scale
class TestSmallFileWorkloadScale(E2ETest):
    """
    Deploy Ripsaw operator and run different scaletests.
    Call common smallfile_worload routie to run SmallFile workload
    """

    @pytest.mark.parametrize(
        argnames=["file_size", "files", "threads", "samples", "interface"],
        argvalues=[pytest.param(*[16, 1000000, 4, 3, constants.CEPHFILESYSTEM])] * 10,
    )
    def test_scale_smallfile_workload(
        self, ripsaw, es, scale_leaks, file_size, files, threads, samples, interface
    ):
        smallfile_workload(
            ripsaw, es, scale_leaks, file_size, files, threads, samples, interface
        )
