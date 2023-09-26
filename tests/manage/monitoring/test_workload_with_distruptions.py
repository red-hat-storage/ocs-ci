# -*- coding: utf8 -*-
"""
Test cases test_workload_with_checksum and test_workload_with_checksum_verify
were originaly proposed for testing metrics and (partial) cluster shutdown, but
they can be used for any other test which needs to verify that cluster still
preserves data written before some cluster wide distruption like shutdown,
reboot or upgrade.

The basic idea is 1st test deploys a k8s job which writes data on OCS based PV
using fio along with a checksum file, and then the 2nd test verifies that the
data are still there on the PV. For this to work, the PV created during the 1st
test needs to be preserved.

The data are written on a PV via workload_storageutilization_checksum_rbd
fixture, used in test_workload_with_checksum. When the writing finishes, the
checksum is computed and stored along with the data, and finally reclaim policy
of the PV is changed to Retain, so that the PV survives teardwon of the test.
The PV is labeled as ``fixture=workload_storageutilization_checksum_rbd`` so
that the verification test can identify and reuse it.

After the first test finishes, the PV is in Released state and won't be reused,
because it contains claimRef referencing the original PVC.

The test_workload_with_checksum_verify locates the PV created by
test_workload_with_checksum via the label, and then removes it's claimRef so
that the PV changes it's state to Available. The test asks for the PV using PVC
with the same parameters, and executes the checksum verification.

The original idea was to use fio verification feature, but testing showed that
when this fails for some reason or gets stuck, it's hard to debug.
"""
import logging
import time
import pytest
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import blue_squad, tier3
from ocs_ci.framework.testlib import (
    tier2,
    pre_upgrade,
    post_upgrade,
    skipif_managed_service,
)
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs import fiojob
from ocs_ci.ocs.cluster import (
    CephCluster,
    get_percent_used_capacity,
    set_osd_op_complaint_time,
    get_full_ratio_from_osd_dump,
)
from ocs_ci.ocs.fiojob import get_timeout
from ocs_ci.ocs.resources.objectconfigfile import ObjectConfFile
from ocs_ci.ocs.resources.pod import get_osd_pods
from ocs_ci.utility import prometheus
from ocs_ci.utility.prometheus import PrometheusAPI

logger = logging.getLogger(__name__)


@blue_squad
@pre_upgrade
@tier2
@skipif_managed_service
def test_workload_with_checksum(workload_storageutilization_checksum_rbd):
    """
    Purpose of this test is to have checksum workload fixture executed.
    """
    msg = "fio report should be available"
    assert workload_storageutilization_checksum_rbd["result"] is not None, msg
    fio = workload_storageutilization_checksum_rbd["result"]["fio"]
    assert len(fio["jobs"]) == 1, "single fio job was executed"
    msg = "no errors should be reported by fio when writing data"
    assert fio["jobs"][0]["error"] == 0, msg


@blue_squad
@post_upgrade
@tier2
@skipif_managed_service
def test_workload_with_checksum_verify(
    tmp_path,
    project,
    fio_pvc_dict,
    fio_job_dict,
    fio_configmap_dict,
):
    """
    Verify that data written by fio during workload storageutilization fixture
    are still present on the persistent volume.

    This test case assumes that test case ``test_workload_with_checksum``
    (which uses the fixture) has been executed already, and that the PV it
    created is still around (the PV is identified via it's label, which
    references the fixture). There is no direct binding between these tests or
    fixtures, so that one can run ``test_workload_with_checksum`` first,
    then do some cluster wide temporary distruptive operation such as reboot,
    temporary shutdown or upgrade, and finally after that run this verification
    test to check that data are still there.

    Note/TODO: this test doesn't delete the PV created by the previous test
    on purpose, so that this test can be executed multiple times (which is
    important feature of this test, eg. it is possible to run it at different
    stages of the cluster wide distruptions). We may need to come up with a way
    to track it and delete it when it's no longer needed though.
    """
    fixture_name = "workload_storageutilization_checksum_rbd"
    storage_class_name = "ocs-storagecluster-ceph-rbd"
    pv_label = f"fixture={fixture_name}"

    # find the volume where the data are stored
    ocp_pv = ocp.OCP(kind=constants.PV, namespace=project.namespace)
    logger.info("Searching for PV with label %s, where fio stored data", pv_label)
    pv_data = ocp_pv.get(selector=pv_label)
    assert pv_data["kind"] == "List"
    pv_exists_msg = (
        f"Single PV with label {pv_label} should exists, "
        "so that test can identify where to verify the data."
    )
    assert len(pv_data["items"]) == 1, pv_exists_msg
    pv_dict = pv_data["items"][0]
    pv_name = pv_dict["metadata"]["name"]
    logger.info("PV %s was identified, test can continue.", pv_name)

    # We need to check the PV size so that we can ask for the same via PVC
    capacity = pv_dict["spec"]["capacity"]["storage"]
    logger.info("Capacity of PV %s is %s.", pv_name, capacity)

    # Convert the storage capacity spec into number of GiB
    unit = capacity[-2:]
    assert unit in ("Gi", "Ti"), "PV size should be within reasonable range"
    if capacity.endswith("Gi"):
        pvc_size = int(capacity[0:-2])
    elif capacity.endswith("Ti"):
        pvc_size = int(capacity[0:-2]) * 2**10

    # And we need to drop claimRef, so that the PV will become available again
    if "claimRef" in pv_dict["spec"]:
        logger.info("Dropping claimRef from PV %s.", pv_name)
        patch_success = ocp_pv.patch(
            resource_name=pv_name,
            params='[{ "op": "remove", "path": "/spec/claimRef" }]',
            format_type="json",
        )
        patch_error_msg = (
            "claimRef should be dropped with success, "
            f"otherwise the test can't continue to reuse PV {pv_name}"
        )
        assert patch_success, patch_error_msg
    else:
        logger.info("PV %s is already without claimRef.", pv_name)

    # The job won't be running fio, it will run sha1sum check only.
    container = fio_job_dict["spec"]["template"]["spec"]["containers"][0]
    container["command"] = ["/usr/bin/sha1sum", "-c", "/mnt/target/fio.sha1sum"]
    # we need to use the same PVC configuration to reuse the PV
    fio_pvc_dict["spec"]["storageClassName"] = storage_class_name
    fio_pvc_dict["spec"]["resources"]["requests"]["storage"] = capacity
    # put the dicts together into yaml file of the Job
    fio_objs = [fio_pvc_dict, fio_configmap_dict, fio_job_dict]
    job_file = ObjectConfFile(fixture_name, fio_objs, project, tmp_path)

    # compute timeout based on the minimal write speed
    fio_min_mbps = config.ENV_DATA["fio_storageutilization_min_mbps"]
    job_timeout = fiojob.get_timeout(fio_min_mbps, pvc_size)
    # expand job timeout because during execution of this test is high
    # probability that there is more workload executed (from upgrade tests)
    # that slow down write time
    # TODO(fbalak): calculate this from actual work being executed
    job_timeout = job_timeout * 4

    # deploy the Job to the cluster and start it
    job_file.create()

    # Wait for the job to verify data on the volume. If this fails in any way
    # the job won't finish with success in given time, and the error message
    # below will be reported via exception.
    error_msg = (
        "Checksum verification job failed. We weren't able to verify that "
        "data previously written on the PV are still there."
    )
    pod_name = fiojob.wait_for_job_completion(project.namespace, job_timeout, error_msg)

    # provide clear evidence of the verification in the logs
    ocp_pod = ocp.OCP(kind="Pod", namespace=project.namespace)
    sha1sum_output = ocp_pod.exec_oc_cmd(f"logs {pod_name}", out_yaml_format=False)
    logger.info("sha1sum output: %s", sha1sum_output)


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
    def test_ceph_osd_slow_ops_alert(self, setup):
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

        api = PrometheusAPI()

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
