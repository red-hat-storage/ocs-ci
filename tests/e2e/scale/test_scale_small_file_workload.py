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


class cephfs_info(object):
    """
    Keep track of number of tests run, and cephfs data
    at the start of the test.
    """

    def __init__(self):
        self.count = 0
        self.ceph_fs_use = {}


cephfs_data = cephfs_info()


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
        orig_data = cephfs_data.ceph_fs_use
        now_data = get_cephfs_data()
        still_going_down = True
        while still_going_down:
            log.info("Waiting for Ceph to finish cleaning up")
            time.sleep(120)
            new_data = get_cephfs_data()
            still_going_down = False
            for entry in new_data:
                if new_data[entry] < now_data[entry]:
                    still_going_down = True
                    now_data[entry] = new_data[entry]
        for entry in now_data:
            # Leak indicated if over %50 more storage is used.
            log.info(f"{entry} now uses {now_data[entry]} bytes")
            log.info(f"{entry} originally used {orig_data[entry]} bytes")
            ratio = now_data[entry] / orig_data[entry]
            check = ratio < 1.50
            errmsg = f"{entry} over 50% larger -- possible leak"
            assert check, errmsg

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

    # The first half of the tests set a baseline usage to compare with later tests.
    # A leak is now determined based on a percentage increase.
    @pytest.mark.parametrize(
        argnames=["file_size", "files", "threads", "interface"],
        argvalues=[pytest.param(*[16, 1000000, 4, constants.CEPHFILESYSTEM])] * 20,
    )
    def test_scale_smallfile_workload(
        self, ripsaw, es, scale_leaks, file_size, files, threads, interface
    ):
        smallfile_workload(ripsaw, es, file_size, files, threads, 3, interface)
        cephfs_data.count += 1
        run_data = get_cephfs_data()
        for entry in run_data:
            log.info(f"{entry} now uses {run_data[entry]} bytes")
        if cephfs_data.count == 10:
            cephfs_data.ceph_fs_use = run_data
