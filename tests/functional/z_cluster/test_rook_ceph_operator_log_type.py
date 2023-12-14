import logging
import pytest
import random

from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.ocs.cluster import ceph_health_check
from ocs_ci.ocs.resources.pod import get_osd_pods
from ocs_ci.helpers.helpers import (
    set_configmap_log_level_rook_ceph_operator,
    get_last_log_time_date,
    check_osd_log_exist_on_rook_ceph_operator_pod,
)
from ocs_ci.framework.pytest_customization.marks import brown_squad
from ocs_ci.framework.testlib import (
    ManageTest,
    tier2,
    skipif_ocs_version,
    skipif_external_mode,
    bugzilla,
)

log = logging.getLogger(__name__)


@brown_squad
@tier2
@bugzilla("1962821")
@skipif_ocs_version("<4.8")
@skipif_external_mode
@pytest.mark.polarion_id("OCS-2581")
class TestRookCephOperatorLogType(ManageTest):
    """
    Test Process:
    1.Set ROOK_LOG_LEVEL param to "DEBUG"
    2.Respin OSD pod
    3.Verify logs contain the expected strings
    4.Verify logs do not contain the unexpected strings
    5.Set ROOK_LOG_LEVEL param to "INFO"
    6.Respin OSD pod
    7.Verify logs contain the expected strings
    8.Verify logs do not contain the unexpected strings

    Comment:
    On INFO mode,  expected log type [I],[E]
    On DEBUG mode, expected log type [I],[D],[E]

    """

    def teardown(self):
        set_configmap_log_level_rook_ceph_operator(value="INFO")
        ceph_health_check()

    def test_rook_ceph_operator_log_type(self):
        """
        Test the ability to change the log level in rook-ceph operator dynamically
        without rook-ceph operator pod restart.

        """
        set_configmap_log_level_rook_ceph_operator(value="DEBUG")
        last_log_date_time_obj = get_last_log_time_date()

        log.info("Respin OSD pod")
        osd_pod_objs = get_osd_pods()
        osd_pod_obj = random.choice(osd_pod_objs)
        osd_pod_obj.delete()

        sample = TimeoutSampler(
            timeout=400,
            sleep=20,
            func=check_osd_log_exist_on_rook_ceph_operator_pod,
            last_log_date_time_obj=last_log_date_time_obj,
            expected_strings=["D |", "osd"],
        )
        if not sample.wait_for_func_status(result=True):
            raise ValueError("OSD DEBUG Log does not exist")

        set_configmap_log_level_rook_ceph_operator(value="INFO")
        last_log_date_time_obj = get_last_log_time_date()

        log.info("Respin OSD pod")
        osd_pod_objs = get_osd_pods()
        osd_pod_obj = random.choice(osd_pod_objs)
        osd_pod_obj.delete()

        sample = TimeoutSampler(
            timeout=400,
            sleep=20,
            func=check_osd_log_exist_on_rook_ceph_operator_pod,
            last_log_date_time_obj=last_log_date_time_obj,
            expected_strings=["I |", "osd"],
            unexpected_strings=["D |"],
        )
        if not sample.wait_for_func_status(result=True):
            raise ValueError(
                "OSD INFO Log does not exist or DEBUG Log exist on INFO mode"
            )
