"""
A test for creating a CephFS
"""
import logging

import pytest

from ocs_ci.ocs import constants, defaults, ocp
from ocs_ci.framework import config
from ocs_ci.framework.testlib import (
    tier1, ManageTest, bugzilla, ignore_leftovers
)
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.utility.utils import TimeoutSampler

log = logging.getLogger(__name__)


@pytest.fixture()
def fs_setup(request):
    """
    Setting up the environment for the test
    """
    def finalizer():
        fs_data = cephfs.get(defaults.CEPHFILESYSTEM_NAME)
        fs_data['spec']['metadataServer']['activeCount'] = (
            original_active_count
        )
        ceph_obj.apply(**fs_data)

    request.addfinalizer(finalizer)
    cephfs = ocp.OCP(
        kind=constants.CEPHFILESYSTEM,
        namespace=config.ENV_DATA['cluster_namespace'],
    )
    fs_data = cephfs.get(defaults.CEPHFILESYSTEM_NAME)
    original_active_count = fs_data.get('spec').get('metadataServer').get('activeCount')
    ceph_obj = OCS(**fs_data)
    return original_active_count, fs_data, ceph_obj


def get_mds_active_count():
    """
    get the active mds count from the system

    Returns:
         tuple: represening active_mds_count(int), pods (pod objects)

    """
    pod = ocp.OCP(
        kind=constants.POD,
        namespace=config.ENV_DATA['cluster_namespace'],
    )
    pods = pod.get(selector='app=rook-ceph-mds')['items']
    cephfs = ocp.OCP(
        kind=constants.CEPHFILESYSTEM,
        namespace=config.ENV_DATA['cluster_namespace'],
    )
    fs_data = cephfs.get(defaults.CEPHFILESYSTEM_NAME)
    new_active_count = fs_data.get('spec').get('metadataServer').get('activeCount')
    return new_active_count, pods


@ignore_leftovers
@bugzilla('1752286')
@tier1
class TestCephFilesystemCreation(ManageTest):
    """
    Testing creation of Ceph FileSystem
    """
    def test_cephfilesystem_creation(
        self,
        fs_setup
    ):
        """
        Creating a Ceph Filesystem
        """
        original_active_count, fs_data, ceph_obj = fs_setup
        new_active_count = original_active_count + 1

        fs_data['spec']['metadataServer']['activeCount'] = (
            new_active_count
        )
        ceph_obj.apply(**fs_data)
        for mdss, pods in TimeoutSampler(
            60, 5, get_mds_active_count,
        ):
            if mdss == new_active_count:
                if len(pods) == (new_active_count * 2):
                    log.info(f"mds and pod count reached: {mdss}, {len(pods)}")
                    return
            log.info(f"Current mds count {mdss}, pod count: {len(pods)}")
        pytest.fail("Failed to increase Active MDS count")
