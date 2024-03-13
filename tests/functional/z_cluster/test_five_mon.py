import logging
import pytest
import time

from ocs_ci.ocs import ocp, constants
from ocs_ci.framework.pytest_customization.marks import (
    brown_squad,
    ignore_leftovers,
    skipif_less_than_five_workers,
)
from ocs_ci.framework.testlib import (
    ManageTest,
    skipif_ocs_version,
)
from ocs_ci.framework import config
from ocs_ci.ocs.cluster import CephCluster
from ocs_ci.utility import prometheus


log = logging.getLogger(__name__)


def verify_mon_pod_running(pods):
    """
    Verify that all five mon pods are in Running state.

    Returns:
        bool: True if all mon pods are in running state, False otherwise

    """
    ret = pods.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector="app=rook-ceph-mon",
        resource_count=5,
        timeout=660,
    )
    log.info(f"Waited for all mon pods to come up and running {ret}")
    return ret


@brown_squad
@ignore_leftovers
@skipif_less_than_five_workers
@skipif_ocs_version("<4.15")
class TestFiveMonInCluster(ManageTest):
    def test_five_mon_pod_in_cluster(self, threading_lock):
        """

        A Testcase to add five mon pods to the cluster when the failure domain value is greater than five

        This test looks if failure domain is greater than five, if yes it will update the monCount to five
        and will wait for the CephMonLowNumber alert to get cleared

        """
        target_msg = "The current number of Ceph monitors can be increased in order to improve cluster resilience."
        target_label = constants.ALERT_CEPHMONLOWCOUNT

        pods = ocp.OCP(
            kind=constants.POD, namespace=config.ENV_DATA["cluster_namespace"]
        )

        ceph_cluster = CephCluster()

        storagecluster_obj = ocp.OCP(
            resource_name=constants.DEFAULT_CLUSTERNAME,
            namespace=config.ENV_DATA["cluster_namespace"],
            kind=constants.STORAGECLUSTER,
        )

        list_mons = ceph_cluster.get_mons_from_cluster()
        assert len(list_mons) < 5, pytest.skip(
            "INVALID: Mon count is already above three."
        )
        api = prometheus.PrometheusAPI(threading_lock=threading_lock)
        alerts_response = api.get(
            "alerts", payload={"silenced": False, "inhibited": False}
        )
        if not alerts_response.ok:
            log.error(f"got bad response from Prometheus: {alerts_response.text}")
        prometheus_alerts = alerts_response.json()["data"]["alerts"]

        log.info("verifying that alert is generated to update monCount to five")
        try:
            prometheus.check_alert_list(
                label=target_label,
                msg=target_msg,
                alerts=prometheus_alerts,
                states=["firing"],
                severity="info",
                ignore_more_occurences=True,
            )
            test_pass = True
        except AssertionError:
            pytest.fail(
                "Failed to get CephMonLowCount warning when failure domain is updated to five"
            )

        if test_pass:
            params = '{"spec":{"managedResources":{"cephCluster":{"monCount": 5}}}}'
            storagecluster_obj.patch(
                params=params,
                format_type="merge",
            )

            log.info("Verifying that all five mon pods are in running state")
            assert verify_mon_pod_running(
                pods
            ), "All five mon pods are not up and running state"

            ceph_cluster.cluster_health_check(timeout=60)

            measure_end_time = time.time()

            assert len(list_mons) != 5, pytest.skip(
                "INVALID: Mon count is already set to five."
            )
        else:
            # if test got to this point, the alert was found, test PASS
            pytest.fail(
                "Failed to get CephMonLowCount warning when mon count is updated to five"
            )

        log.info(
            "Verify that CephMonLowNumber alert got cleared post updating monCount to 5"
        )
        api.check_alert_cleared(
            label=target_label, measure_end_time=measure_end_time, time_min=300
        )
