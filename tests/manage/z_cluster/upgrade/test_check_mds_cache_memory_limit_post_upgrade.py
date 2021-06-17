import logging
import pytest

from ocs_ci.framework.testlib import (
    post_ocs_upgrade,
    ManageTest,
    skipif_ocs_version,
    bugzilla,
)
from ocs_ci.ocs.resources.pod import get_ceph_tools_pod

log = logging.getLogger(__name__)


@post_ocs_upgrade
@skipif_ocs_version("<4.7")
@bugzilla("1951348")
@bugzilla("1944148")
@pytest.mark.polarion_id("OCS-2554")
class TestToCheckMDSCacheMemoryLimit(ManageTest):
    """
    Validate post ocs upgrade mds cache memory limit

    """

    def test_check_mds_cache_memory_limit(self):
        """
        Testcase to check mds cache memory limit post ocs upgrade

        """
        pod_obj = get_ceph_tools_pod()
        ceph_cmd = "ceph config dump"
        ceph_config = pod_obj.exec_ceph_cmd(ceph_cmd=ceph_cmd)
        mds_a_dict = next(
            item
            for item in ceph_config
            if item["section"] == "mds.ocs-storagecluster-cephfilesystem-a"
        )
        mds_b_dict = next(
            item
            for item in ceph_config
            if item["section"] == "mds.ocs-storagecluster-cephfilesystem-b"
        )
        if (mds_a_dict["value"] and mds_b_dict["value"]) == "4294967296":
            log.info(
                f"{mds_a_dict['section']} set value {mds_a_dict['value']} and"
                f" {mds_b_dict['section']} set value {mds_b_dict['value']}"
            )
            log.info("mds_cache_memory_limit is set with a value of 4GB")
        else:
            log.error(f"Ceph config dump output: {ceph_config}")
            log.error(
                f"{mds_a_dict['section']} set value {mds_a_dict['value']} and"
                f"{mds_b_dict['section']} set value {mds_b_dict['value']}"
            )
            log.error("mds_cache_memory_limit is not set with a value of 4GB")
            raise Exception("mds_cache_memory_limit is not set with a value of 4GB")
