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
        if (ceph_config[11]["value"] and ceph_config[12]["value"]) == "4294967296":
            log.info(
                f"{ceph_config[11]['section']} set value {ceph_config[11]['value']} and"
                f" {ceph_config[12]['section']} set value {ceph_config[12]['value']}"
            )
            log.info("mds_cache_memory_limit is set with a value of 4GB")
        else:
            log.error(f"Ceph config dump output: {ceph_config}")
            log.error(
                f"{ceph_config[11]['section']} set value {ceph_config[11]['value']} and"
                f"{ceph_config[12]['section']} set value {ceph_config[12]['value']}"
            )
            log.error("mds_cache_memory_limit is not set with a value of 4GB")
            raise Exception("mds_cache_memory_limit is not set with a value of 4GB")
