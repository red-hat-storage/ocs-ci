from datetime import datetime
import logging
import re

from ocs_ci.utility.utils import TimeoutSampler, ceph_health_check
from ocs_ci.ocs.resources.pod import (
    get_pod_logs,
    get_osd_pods,
    get_ceph_tools_pod,
    get_operator_pods,
)
from ocs_ci.helpers.helpers import set_configmap_log_level_rook_ceph_operator
from ocs_ci.framework.testlib import (
    ManageTest,
    tier2,
    skipif_ocs_version,
    bugzilla,
)

log = logging.getLogger(__name__)


@tier2
@bugzilla("1962821")
@skipif_ocs_version("<4.8")
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
        tool_pod = get_ceph_tools_pod()
        tool_pod.exec_ceph_cmd(ceph_cmd="ceph crash archive-all", format=None)
        logging.info(
            "Perform Ceph and cluster health checks after silencing the ceph warnings"
        )
        ceph_health_check()

    def test_rook_ceph_operator_log_type(self):
        """
        Test the ability to change the log level in rook-ceph operator dynamically
        without rook-ceph operator pod restart.

        """
        set_configmap_log_level_rook_ceph_operator(value="DEBUG")
        last_log_date_time_obj = self.get_last_log_time_date()

        log.info("Respin OSD pod")
        osd_pods_objs = get_osd_pods()
        osd_pods_objs[0].delete()

        sample = TimeoutSampler(
            timeout=400,
            sleep=20,
            func=self.check_osd_log_exist_on_rook_ceph_operator_pod,
            last_log_date_time_obj=last_log_date_time_obj,
            expected_strings=["D |", "osd"],
        )
        if not sample.wait_for_func_status(result=True):
            raise ValueError("OSD DEBUG Log does not exist")

        set_configmap_log_level_rook_ceph_operator(value="INFO")
        last_log_date_time_obj = self.get_last_log_time_date()

        log.info("Respin OSD pod")
        osd_pods_objs = get_osd_pods()
        osd_pods_objs[0].delete()

        sample = TimeoutSampler(
            timeout=400,
            sleep=20,
            func=self.check_osd_log_exist_on_rook_ceph_operator_pod,
            last_log_date_time_obj=last_log_date_time_obj,
            expected_strings=["I |", "osd"],
            unexpected_strings=["D |"],
        )
        if not sample.wait_for_func_status(result=True):
            raise ValueError(
                "OSD INFO Log does not exist or DEBUG Log exist on INFO mode"
            )

    def get_logs_rook_ceph_operator(self):
        """
        Get logs from a rook_ceph_operator pod

        Returns:
            str: Output from 'oc get logs rook-ceph-operator command

        """
        log.info("Get logs from rook_ceph_operator pod")
        rook_ceph_operator_objs = get_operator_pods()
        return get_pod_logs(pod_name=rook_ceph_operator_objs[0].name)

    def check_osd_log_exist_on_rook_ceph_operator_pod(
        self, last_log_date_time_obj, expected_strings=(), unexpected_strings=()
    ):
        """
        Verify logs contain the expected strings and the logs do not
            contain the unexpected strings

        Args:
            last_log_date_time_obj (datetime obj): type of log
            expected_strings (list): verify the logs contain the expected strings
            unexpected_strings (list): verify the logs do not contain the strings

        Returns:
            bool: True if logs contain the expected strings and the logs do not
            contain the unexpected strings, False otherwise

        """
        log.info("Respin OSD pod")
        osd_pods_objs = get_osd_pods()
        osd_pods_objs[0].delete()
        new_logs = list()
        rook_ceph_operator_logs = self.get_logs_rook_ceph_operator()
        for line in rook_ceph_operator_logs.splitlines():
            if re.search(r"\d{4}-\d{2}-\d{2}", line):
                log_date_time_obj = datetime.strptime(line[:26], "%Y-%m-%d %H:%M:%S.%f")
                if log_date_time_obj > last_log_date_time_obj:
                    new_logs.append(line)
        res_expected = False
        res_unexpected = True
        for new_log in new_logs:
            if all(
                expected_string.lower() in new_log.lower()
                for expected_string in expected_strings
            ):
                res_expected = True
                log.info(f"{new_log} contain expected strings {expected_strings}")
                break
        for new_log in new_logs:
            if any(
                unexpected_string.lower() in new_log.lower()
                for unexpected_string in unexpected_strings
            ):
                log.error(f"{new_log} contain unexpected strings {unexpected_strings}")
                res_unexpected = False
                break
        return res_expected & res_unexpected

    def get_last_log_time_date(self):
        """
        Get last log time

        Returns:
            last_log_date_time_obj (datetime obj): type of log

        """
        log.info("Get last log time")
        rook_ceph_operator_logs = self.get_logs_rook_ceph_operator()
        for line in rook_ceph_operator_logs.splitlines():
            if re.search(r"\d{4}-\d{2}-\d{2}", line):
                last_log_date_time_obj = datetime.strptime(
                    line[:26], "%Y-%m-%d %H:%M:%S.%f"
                )
        return last_log_date_time_obj
