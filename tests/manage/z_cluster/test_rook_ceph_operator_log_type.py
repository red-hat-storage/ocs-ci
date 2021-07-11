from datetime import datetime
import logging
import re

from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs import constants
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.ocs.resources.pod import get_pod_logs, get_osd_pods
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
    3.Set ROOK_LOG_LEVEL param to "INFO"
    4.Find the last log (according to the time)
    5.Respin OSD pod
    6.Find 'osd' string on log
    7.Find all new logs (after switching to INFO mode)
    8.Verify there are no DEBUG logs 'D |'

    Comment:
    On INFO mode,  expected log type [I],[E]
    On DEBUG mode, expected log type [I],[D],[E]

    """

    def teardown(self):
        self.set_configmap_log_level(value="DEBUG")

    def test_rook_ceph_operator_log_type(self):
        """
        test the mechanism to enable and disable debug logs in rook-ceph operator

        """
        self.set_configmap_log_level(value="DEBUG")
        osd_pods_objs = get_osd_pods()
        osd_pods_objs[0].delete()

        self.set_configmap_log_level(value="INFO")

        rook_ceph_operator_logs = self.get_logs_rook_ceph_operator()
        for line in rook_ceph_operator_logs.splitlines():
            if re.search(r"\d{4}-\d{2}-\d{2}", line):
                last_log_date_time_obj = datetime.strptime(
                    line[:26], "%Y-%m-%d %H:%M:%S.%f"
                )

        osd_pods_objs = get_osd_pods()
        osd_pods_objs[0].delete()

        sample = TimeoutSampler(
            timeout=400,
            sleep=20,
            func=self.check_osd_log_exist_on_rook_ceph_operator_pod,
            last_log_date_time_obj=last_log_date_time_obj,
        )
        if not sample.wait_for_func_status(result=True):
            log.error(
                "osd log does not exist on rook_ceph_operator pod after 100 seconds"
            )
            raise TimeoutExpiredError

        for new_log in self.new_logs:
            if "D |" in new_log:
                assert f"DEBUG log appeared in INFO state. {self.new_logs}"

    def get_logs_rook_ceph_operator(self):
        """
        Get logs from a rook_ceph_operator pod

        """
        log.info("Get logs from rook_ceph_operator pod")
        rook_ceph_operator_name = get_pod_name_by_pattern("rook-ceph-operator")
        return get_pod_logs(pod_name=rook_ceph_operator_name[0])

    def set_configmap_log_level(self, value):
        """
        Set ROOK_LOG_LEVEL on configmap of rook-ceph-operator

        Args:
            value (str): type of log

        """
        path = "/data/ROOK_LOG_LEVEL"
        params = f"""[{{"op": "add", "path": "{path}", "value": "{value}"}}]"""
        configmap_obj = OCP(
            kind=constants.CONFIGMAP,
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
            resource_name=constants.ROOK_OPERATOR_CONFIGMAP,
        )
        configmap_obj.patch(params=params, format_type="json")

    def check_osd_log_exist_on_rook_ceph_operator_pod(self, last_log_date_time_obj):
        """
        Check 'osd' string exist on rook_ceph_operator log

        Args:
            last_log_date_time_obj (datetime obj): type of log

        return:
            bool: True if 'osd' string exist on logs, False otherwise

        """
        new_logs = list()
        rook_ceph_operator_logs = self.get_logs_rook_ceph_operator()
        for line in rook_ceph_operator_logs.splitlines():
            if re.search(r"\d{4}-\d{2}-\d{2}", line):
                log_date_time_obj = datetime.strptime(line[:26], "%Y-%m-%d %H:%M:%S.%f")
                if log_date_time_obj > last_log_date_time_obj:
                    new_logs.append(line)
        for new_log in new_logs:
            if "osd" in new_log:
                self.new_logs = new_logs
                return True
        osd_pods_objs = get_osd_pods()
        osd_pods_objs[0].delete()
        log.error(f"osd log does not exist on rook_ceph_operator pod {new_logs}")
        return False
