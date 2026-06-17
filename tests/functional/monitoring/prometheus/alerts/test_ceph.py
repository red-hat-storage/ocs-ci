import logging
import pytest
import time

from ocs_ci.framework import config
from ocs_ci.framework.testlib import (
    tier3,
    tier4,
    tier4a,
    skipif_managed_service,
    runs_on_provider,
    blue_squad,
    provider_mode,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.cluster import (
    CephCluster,
    get_percent_used_capacity,
    set_osd_op_complaint_time,
    get_full_ratio_from_osd_dump,
)
from ocs_ci.ocs.fiojob import get_timeout
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import get_osd_pods
from ocs_ci.utility import prometheus

logger = logging.getLogger(__name__)


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
    logger.info(
        "Starting test: Verify corrupt placement group alerts trigger and clear"
    )

    logger.test_step("Initialize Prometheus API and retrieve alerts")
    api = prometheus.PrometheusAPI(threading_lock=threading_lock)
    alerts = measure_corrupt_pg.get("prometheus_alerts")
    logger.info(f"Number of alerts retrieved: {len(alerts) if alerts else 0}")

    alert_configs = [
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
    ]
    logger.info(f"Checking {len(alert_configs)} alert types")

    logger.test_step("Validate and verify clearance for each alert")
    # the time to wait is increased because it takes more time for Ceph
    # cluster to resolve its issues
    pg_wait = 360
    logger.debug(f"PG clearance timeout: {pg_wait}min")

    for i, (target_label, target_msg, target_states, target_severity) in enumerate(
        alert_configs, 1
    ):
        logger.info(
            f"Processing alert {i}/{len(alert_configs)}: {target_label} "
            f"(severity: {target_severity})"
        )

        logger.debug(f"Validating {target_label} with states={target_states}")
        prometheus.check_alert_list(
            label=target_label,
            msg=target_msg,
            alerts=alerts,
            states=target_states,
            severity=target_severity,
        )
        logger.info(f"Alert {target_label} validated successfully")

        logger.debug(f"Verifying {target_label} is cleared (timeout={pg_wait}min)")
        api.check_alert_cleared(
            label=target_label,
            measure_end_time=measure_corrupt_pg.get("stop"),
            time_min=pg_wait,
        )
        logger.info(f"Alert {target_label} cleared successfully")

    logger.info("Test passed: All corrupt PG alerts triggered and cleared as expected")


@provider_mode
@blue_squad
@tier4
@tier4a
@pytest.mark.polarion_id("OCS-898")
@skipif_managed_service
@runs_on_provider
def deprecated_test_ceph_health(
    measure_stop_ceph_osd, measure_corrupt_pg, threading_lock
):
    """
    Test that there are appropriate alerts for Ceph health triggered.
    For this check of Ceph Warning state is used measure_stop_ceph_osd
    utilization monitor and for Ceph Error state is used measure_corrupt_pg
    utilization.
    """
    logger.info("Starting test: Verify Ceph health alerts (warning and error states)")

    logger.test_step("Initialize Prometheus API and calculate clearance timeout")
    api = prometheus.PrometheusAPI(threading_lock=threading_lock)
    # the time to wait is increased because it takes more time for Ceph
    # cluster to resolve its issues
    health_wait = 420
    stop_time = max(measure_stop_ceph_osd.get("stop"), measure_corrupt_pg.get("stop"))
    logger.info(f"Health clearance timeout: {health_wait}min, stop_time: {stop_time}")

    logger.test_step("Validate ClusterWarningState alert from OSD stop")
    alerts = measure_stop_ceph_osd.get("prometheus_alerts")
    logger.info(f"OSD stop alerts retrieved: {len(alerts) if alerts else 0}")

    target_label = constants.ALERT_CLUSTERWARNINGSTATE
    target_msg = "Storage cluster is in degraded state"
    target_states = ["pending", "firing"]
    target_severity = "warning"
    logger.info(f"Checking alert: {target_label} (severity: {target_severity})")

    prometheus.check_alert_list(
        label=target_label,
        msg=target_msg,
        alerts=alerts,
        states=target_states,
        severity=target_severity,
    )
    logger.info(f"Alert {target_label} validated successfully")

    logger.debug(f"Verifying {target_label} is cleared")
    api.check_alert_cleared(
        label=target_label,
        measure_end_time=stop_time,
        time_min=health_wait,
    )
    logger.info(f"Alert {target_label} cleared successfully")

    logger.test_step("Validate ClusterErrorState alert from corrupt PG")
    alerts = measure_corrupt_pg.get("prometheus_alerts")
    logger.info(f"Corrupt PG alerts retrieved: {len(alerts) if alerts else 0}")

    target_label = constants.ALERT_CLUSTERERRORSTATE
    target_msg = "Storage cluster is in error state"
    target_states = ["pending", "firing"]
    target_severity = "error"
    logger.info(f"Checking alert: {target_label} (severity: {target_severity})")

    prometheus.check_alert_list(
        label=target_label,
        msg=target_msg,
        alerts=alerts,
        states=target_states,
        severity=target_severity,
    )
    logger.info(f"Alert {target_label} validated successfully")

    logger.debug(f"Verifying {target_label} is cleared")
    api.check_alert_cleared(
        label=target_label,
        measure_end_time=stop_time,
        time_min=health_wait,
    )
    logger.info(f"Alert {target_label} cleared successfully")

    logger.info("Test passed: Ceph health alerts triggered and cleared correctly")


@runs_on_provider
class TestCephOSDSlowOps(object):
    @pytest.fixture(scope="function")
    def setup(self, request, pod_factory, multi_pvc_factory):
        """
        Set preconditions to trigger CephOSDSlowOps
        """
        logger.info("Setting up test: Configure preconditions for CephOSDSlowOps")

        self.test_pass = None
        reduced_osd_complaint_time = 0.02

        logger.test_step("Reduce OSD complaint time to trigger slow ops alert")
        logger.info(f"Setting osd_op_complaint_time to {reduced_osd_complaint_time}s")
        set_osd_op_complaint_time(reduced_osd_complaint_time)

        logger.test_step("Calculate storage capacity and timeout parameters")
        ceph_cluster = CephCluster()

        self.full_osd_ratio = round(get_full_ratio_from_osd_dump(), 2)
        self.full_osd_threshold = self.full_osd_ratio * 100
        logger.info(
            f"Full OSD ratio: {self.full_osd_ratio}, threshold: {self.full_osd_threshold}%"
        )

        # max possible cap to reach CephOSDSlowOps is to fill storage up to threshold; alert should appear much earlier
        pvc_size = ceph_cluster.get_ceph_free_capacity() * self.full_osd_ratio
        logger.info(f"Target PVC size calculated: {pvc_size} GiB")

        # assuming storageutilization speed reduced to less than 1, estimation timeout to fill the storage
        # will be reduced by number of osds. That should be more than enough to trigger an alert,
        # otherwise the failure is legitimate
        osd_count = len(get_osd_pods())
        storageutilization_min_mbps = (
            config.ENV_DATA["fio_storageutilization_min_mbps"] / osd_count
        )
        self.timeout_sec = get_timeout(storageutilization_min_mbps, int(pvc_size))
        logger.info(
            f"Calculated timeout: {self.timeout_sec}s (OSDs: {osd_count}, min_mbps: {storageutilization_min_mbps})"
        )

        logger.test_step("Create PVCs for workload")
        access_modes = [constants.ACCESS_MODE_RWO, constants.ACCESS_MODE_RWX]
        num_of_load_objs = 2
        logger.info(f"Creating {num_of_load_objs} PVCs with CephFS interface")

        self.pvc_objs = multi_pvc_factory(
            interface=constants.CEPHFILESYSTEM,
            size=pvc_size / num_of_load_objs,
            access_modes=access_modes,
            status=constants.STATUS_BOUND,
            num_of_pvc=num_of_load_objs,
            wait_each=True,
        )
        logger.info(f"Created {len(self.pvc_objs)} PVCs successfully")

        logger.test_step("Create pods and start IO workload")
        self.pod_objs = []

        for i, pvc_obj in enumerate(self.pvc_objs, 1):
            logger.debug(f"Creating pod {i}/{num_of_load_objs} for PVC {pvc_obj.name}")
            pod_obj = pod_factory(
                interface=constants.CEPHFILESYSTEM, pvc=pvc_obj, replica_count=3
            )
            self.pod_objs.append(pod_obj)

            file_name = pod_obj.name
            fillup_size = f"{round(pvc_size * 1024)}M"
            logger.debug(
                f"Filling up filesystem on pod {pod_obj.name} with {fillup_size}"
            )
            pod_obj.fillup_fs(size=fillup_size, fio_filename=file_name)

            logger.debug(
                f"Starting IO workload on pod {pod_obj.name} (runtime: {self.timeout_sec}s)"
            )
            pod_obj.run_io(
                storage_type="fs",
                size="3G",
                runtime=self.timeout_sec,
                fio_filename=f"{pod_obj.name}_io",
            )

        self.start_workload_time = time.perf_counter()
        logger.info(f"All {len(self.pod_objs)} pods created and workload started")

        def finalizer():
            """
            Set default values for:
              osd_op_complaint_time=30.000000
            """
            logger.info(
                "Tearing down test: Restoring OSD settings and cleaning resources"
            )

            # set the osd_op_complaint_time to selected monitor back to default value
            logger.debug(
                f"Restoring osd_op_complaint_time to {constants.DEFAULT_OSD_OP_COMPLAINT_TIME}"
            )
            set_osd_op_complaint_time(constants.DEFAULT_OSD_OP_COMPLAINT_TIME)

            # delete resources
            logger.debug(f"Deleting {len(self.pod_objs)} pods")
            for pod_obj in self.pod_objs:
                pod_obj.delete()
                pod_obj.delete(wait=True)

            logger.debug(f"Deleting {len(self.pvc_objs)} PVCs")
            for pvc_obj in self.pvc_objs:
                pvc_obj.delete(wait=True)

            logger.info("Cleanup completed successfully")

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

        1. As precondition test setup is to reduce osd_op_complaint_time to 0.02 (200 ms) to prepare condition
        to get CephOSDSlowOps
        2. Run workload_fio_storageutilization gradually filling up the storage up to full_ratio % in a background
        2.1 Validate the CephOSDSlowOps fired, if so check an alert message and finish the test
        2.2 If CephOSDSlowOps has not been fired while the storage filled up to full_ratio % or time to fill up the
        storage ends - fail the test
        """
        logger.info(
            "Starting test: Verify CephOSDSlowOps alert triggers during storage utilization"
        )
        logger.info(
            f"Test conditions - Full OSD threshold: {self.full_osd_threshold}%, "
            f"Timeout: {self.timeout_sec}s"
        )

        logger.test_step(
            "Initialize Prometheus API and monitor for CephOSDSlowOps alert"
        )
        api = prometheus.PrometheusAPI(threading_lock=threading_lock)

        target_alert = constants.ALERT_CEPHOSDSLOWOPS
        target_msg = "OSD requests are taking too long to process."
        target_states = ["firing"]
        target_severity = "warning"
        logger.info(
            f"Monitoring for alert: {target_alert} (severity: {target_severity})"
        )

        iteration = 0
        while get_percent_used_capacity() < self.full_osd_threshold:
            iteration += 1
            time_passed_sec = time.perf_counter() - self.start_workload_time
            current_utilization = round(get_percent_used_capacity(), 1)

            logger.debug(
                f"Iteration {iteration}: utilization={current_utilization}%, "
                f"elapsed={round(time_passed_sec)}s, timeout={round(self.timeout_sec)}s"
            )

            if time_passed_sec > self.timeout_sec:
                logger.error(
                    f"Timeout exceeded: {round(time_passed_sec)}s > {round(self.timeout_sec)}s "
                    f"at {current_utilization}% utilization"
                )
                pytest.fail("failed to fill the storage in calculated time")

            delay_time = 60
            logger.debug(f"Sleeping {delay_time}s before next check")
            time.sleep(delay_time)

            logger.debug("Querying Prometheus for current alerts")
            alerts_response = api.get(
                "alerts", payload={"silenced": False, "inhibited": False}
            )
            if not alerts_response.ok:
                logger.error(
                    f"got bad response from Prometheus: {alerts_response.text}"
                )
                continue

            prometheus_alerts = alerts_response.json()["data"]["alerts"]
            logger.debug(f"Prometheus returned {len(prometheus_alerts)} alerts")

            try:
                prometheus.check_alert_list(
                    label=target_alert,
                    msg=target_msg,
                    alerts=prometheus_alerts,
                    states=target_states,
                    severity=target_severity,
                    ignore_more_occurences=True,
                )
                logger.info(
                    f"Alert {target_alert} detected successfully at {current_utilization}% "
                    f"utilization after {round(time_passed_sec)}s"
                )
                self.test_pass = True
            except AssertionError:
                logger.debug(
                    f"Alert {target_alert} not yet fired - "
                    f"utilization: {current_utilization}%, "
                    f"elapsed: {round(time_passed_sec)}s"
                )

            if self.test_pass:
                logger.info("Test passed: CephOSDSlowOps alert triggered as expected")
                break
        else:
            # if test got to this point without detecting the alert, fail
            logger.error(
                f"Failed to detect {target_alert} alert. "
                f"Storage filled to {self.full_osd_ratio * 100}% without alert firing"
            )
            pytest.fail(
                f"failed to get 'CephOSDSlowOps' while workload filled up the storage to {self.full_osd_ratio} percents"
            )


def setup_module(module):
    logger.info("Setting up module: Storing original user for cleanup")
    ocs_obj = OCP()
    module.original_user = ocs_obj.get_user_name()
    logger.info(f"Original user stored: {module.original_user}")


def teardown_module(module):
    logger.info("Tearing down module: Restoring original user")
    ocs_obj = OCP()
    ocs_obj.login_as_user(module.original_user)
    logger.info(f"Restored user: {module.original_user}")
