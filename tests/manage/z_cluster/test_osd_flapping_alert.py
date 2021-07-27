# https://bugzilla.redhat.com/show_bug.cgi?id=1935342
"""

Prometheus alert for OSD restart
This enhancement adds a Prometheus alert to notify if an OpenShift Container
Storage OSD restarts more than 5 times in 5 minutes.
The alert message is as follows:
----
 Storage daemon osd.x has restarted 5 times in the last 5 minutes.
 Please check the pod events or ceph status to find out the cause.
----
x - represent the OSD number

"""

from ocs_ci.framework.testlib import ManageTest
from ocs_ci.ocs.resources.pod import get_osd_pods, delete_pods


# TODO add tier? other fixtures?
class TestOSDAlert(ManageTest):
    def test_osd_flapping_alert(self):
        osd_list = get_osd_pods()
        target_osd = osd_list[1]
        for i in range(5):
            delete_pods(target_osd)

        # TODO find where the alert is coming from
        log_alert = "mashehu"
        expected_alert = "mashehu aher"
        """expected_alert = (
            f"Storage daemon {target_osd.name} has restarted 5 times in the last 5 minutes. Please check "
            f"the pod events or ceph status to find out the cause."
        )"""
        assert log_alert == expected_alert, "Message alert was not displayed"
