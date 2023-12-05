import logging
import pytest

from ocs_ci.framework.testlib import (
    tier3,
    tier4,
    tier4a,
    skipif_managed_service,
    runs_on_provider,
    blue_squad,
)
from ocs_ci.ocs import constants
from ocs_ci.utility import prometheus
from ocs_ci.ocs.ocp import OCP

log = logging.getLogger(__name__)


@blue_squad
@tier4
@tier4a
@pytest.mark.polarion_id("OCS-903")
@skipif_managed_service
@runs_on_provider
def test_corrupt_pg_alerts(measure_corrupt_pg, threading_lock):
    """
    Test that there are appropriate alerts when Placement group
    on one OSD is corrupted.ceph manager
    is unavailable and that this alert is cleared when the manager
    is back online.
    """
    api = prometheus.PrometheusAPI(threading_lock=threading_lock)

    alerts = measure_corrupt_pg.get("prometheus_alerts")
    for target_label, target_msg, target_states, target_severity in [
        (
            constants.ALERT_PGREPAIRTAKINGTOOLONG,
            "Self heal problems detected",
            ["pending"],
            "warning",
        ),
        (
            constants.ALERT_CLUSTERERRORSTATE,
            "Storage cluster is in error state",
            ["pending", "firing"],
            "error",
        ),
    ]:
        prometheus.check_alert_list(
            label=target_label,
            msg=target_msg,
            alerts=alerts,
            states=target_states,
            severity=target_severity,
        )
        # the time to wait is increased because it takes more time for Ceph
        # cluster to resolve its issues
        pg_wait = 360
        api.check_alert_cleared(
            label=target_label,
            measure_end_time=measure_corrupt_pg.get("stop"),
            time_min=pg_wait,
        )


@blue_squad
@tier4
@tier4a
@pytest.mark.polarion_id("OCS-898")
@skipif_managed_service
@runs_on_provider
def test_ceph_health(measure_stop_ceph_osd, measure_corrupt_pg, threading_lock):
    """
    Test that there are appropriate alerts for Ceph health triggered.
    For this check of Ceph Warning state is used measure_stop_ceph_osd
    utilization monitor and for Ceph Error state is used measure_corrupt_pg
    utilization.
    """
    api = prometheus.PrometheusAPI(threading_lock=threading_lock)

    alerts = measure_stop_ceph_osd.get("prometheus_alerts")
    target_label = constants.ALERT_CLUSTERWARNINGSTATE
    target_msg = "Storage cluster is in degraded state"
    target_states = ["pending", "firing"]
    target_severity = "warning"
    prometheus.check_alert_list(
        label=target_label,
        msg=target_msg,
        alerts=alerts,
        states=target_states,
        severity=target_severity,
    )
    api.check_alert_cleared(
        label=target_label,
        measure_end_time=measure_stop_ceph_osd.get("stop"),
    )

    alerts = measure_corrupt_pg.get("prometheus_alerts")
    target_label = constants.ALERT_CLUSTERERRORSTATE
    target_msg = "Storage cluster is in error state"
    target_states = ["pending", "firing"]
    target_severity = "error"
    prometheus.check_alert_list(
        label=target_label,
        msg=target_msg,
        alerts=alerts,
        states=target_states,
        severity=target_severity,
    )
    # the time to wait is increased because it takes more time for Ceph
    # cluster to resolve its issues
    pg_wait = 360
    api.check_alert_cleared(
        label=target_label,
        measure_end_time=measure_corrupt_pg.get("stop"),
        time_min=pg_wait,
    )

class TestCephOSDSlowOps(object):
    @pytest.fixture(scope="function")
    def setup(self, request, pod_factory, multi_pvc_factory):
        """
        Set preconditions to trigger CephOSDSlowOps
        """
        self.test_pass = None
        reduced_osd_complaint_time = 0.1

        set_osd_op_complaint_time(reduced_osd_complaint_time)

        ceph_cluster = CephCluster()

        self.full_osd_ratio = round(get_full_ratio_from_osd_dump(), 2)
        self.full_osd_threshold = self.full_osd_ratio * 100

        # max possible cap to reach CephOSDSlowOps is to fill storage up to threshold; alert should appear much earlier
        pvc_size = ceph_cluster.get_ceph_free_capacity() * self.full_osd_ratio

        # assuming storageutilization speed reduced to less than 1, estimation timeout to fill the storage
        # will be reduced by number of osds. That should be more than enough to trigger an alert,
        # otherwise the failure is legitimate
        storageutilization_min_mbps = config.ENV_DATA[
            "fio_storageutilization_min_mbps"
        ] / len(get_osd_pods())
        self.timeout_sec = get_timeout(storageutilization_min_mbps, int(pvc_size))

        access_modes = [constants.ACCESS_MODE_RWO, constants.ACCESS_MODE_RWX]

        num_of_load_objs = 2
        self.pvc_objs = multi_pvc_factory(
            interface=constants.CEPHFILESYSTEM,
            size=pvc_size / num_of_load_objs,
            access_modes=access_modes,
            status=constants.STATUS_BOUND,
            num_of_pvc=num_of_load_objs,
            wait_each=True,
        )
        self.pod_objs = []

        for pvc_obj in self.pvc_objs:
            pod_obj = pod_factory(
                interface=constants.CEPHFILESYSTEM, pvc=pvc_obj, replica_count=3
            )
            self.pod_objs.append(pod_obj)
            file_name = pod_obj.name
            pod_obj.fillup_fs(size=f"{round(pvc_size * 1024)}M", fio_filename=file_name)
            pod_obj.run_io(
                storage_type="fs",
                size="3G",
                runtime=self.timeout_sec,
                fio_filename=f"{pod_obj.name}_io",
            )

        self.start_workload_time = time.perf_counter()

        def finalizer():
            """
            Set default values for:
              osd_op_complaint_time=30.000000
            """
            # set the osd_op_complaint_time to selected monitor back to default value
            set_osd_op_complaint_time(constants.DEFAULT_OSD_OP_COMPLAINT_TIME)

            # delete resources
            for pod_obj in self.pod_objs:
                pod_obj.delete()
                pod_obj.delete(wait=True)

            for pvc_obj in self.pvc_objs:
                pvc_obj.delete(wait=True)

        request.addfinalizer(finalizer)

    @tier3
    @pytest.mark.polarion_id("OCS-5158")
    @blue_squad
    def test_ceph_osd_slow_ops_alert(self, setup, threading_lock):
        """
        Test to verify bz #1966139, more info about Prometheus alert - #1885441

        CephOSDSlowOps. An Object Storage Device (OSD) with slow requests is every OSD that is not able to service
        the I/O operations per second (IOPS) in the queue within the time defined by the osd_op_complaint_time
        parameter. By default, this parameter is set to 30 seconds.

        1. As precondition test setup is to reduce osd_op_complaint_time to 0.1 to prepare condition
        to get CephOSDSlowOps
        2. Run workload_fio_storageutilization gradually filling up the storage up to full_ratio % in a background
        2.1 Validate the CephOSDSlowOps fired, if so check an alert message and finish the test
        2.2 If CephOSDSlowOps has not been fired while the storage filled up to full_ratio % or time to fill up the
        storage ends - fail the test
        """

        api = PrometheusAPI(threading_lock=threading_lock)

        while get_percent_used_capacity() < self.full_osd_threshold:
            time_passed_sec = time.perf_counter() - self.start_workload_time
            if time_passed_sec > self.timeout_sec:
                pytest.fail("failed to fill the storage in calculated time")

            delay_time = 60
            logger.info(f"sleep {delay_time}s")
            time.sleep(delay_time)

            alerts_response = api.get(
                "alerts", payload={"silenced": False, "inhibited": False}
            )
            if not alerts_response.ok:
                logger.error(
                    f"got bad response from Prometheus: {alerts_response.text}"
                )
                continue
            prometheus_alerts = alerts_response.json()["data"]["alerts"]
            logger.info(f"Prometheus Alerts: {prometheus_alerts}")
            for target_label, target_msg, target_states, target_severity in [
                (
                    constants.ALERT_CEPHOSDSLOWOPS,
                    "OSD requests are taking too long to process.",
                    ["firing"],
                    "warning",
                )
            ]:
                try:
                    prometheus.check_alert_list(
                        label=target_label,
                        msg=target_msg,
                        alerts=prometheus_alerts,
                        states=target_states,
                        severity=target_severity,
                        ignore_more_occurences=True,
                    )
                    self.test_pass = True
                except AssertionError:
                    logger.info(
                        "workload storage utilization job did not finish\n"
                        f"current utilization {round(get_percent_used_capacity(), 1)}p\n"
                        f"time passed since start workload: {round(time.perf_counter() - self.start_workload_time)}s\n"
                        f"timeout = {round(self.timeout_sec)}s"
                    )
            if self.test_pass:
                break
        else:
            # if test got to this point, the alert was found, test PASS
            pytest.fail(
                f"failed to get 'CephOSDSlowOps' while workload filled up the storage to {self.full_osd_ratio} percents"
            )


def teardown_module():
    ocs_obj = OCP()
    ocs_obj.login_as_sa()
