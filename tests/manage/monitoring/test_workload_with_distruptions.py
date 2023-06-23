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
import random
import pytest
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import blue_squad, tier3
from ocs_ci.framework.testlib import (
    tier2,
    pre_upgrade,
    post_upgrade,
    skipif_managed_service,
)
from ocs_ci.helpers.helpers import wait_for_resource_state
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs import fiojob
from ocs_ci.ocs.fiojob import workload_fio_storageutilization
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.resources.objectconfigfile import ObjectConfFile
from ocs_ci.ocs.resources.pod import get_mon_pods, get_osd_pods, Pod
from ocs_ci.utility import prometheus

logger = logging.getLogger(__name__)


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


def get_mon_pod_by_pvc_name(pvc_name: str):
    """
    Function to get monitor pod by pvc_name label

    Args:
        pvc_name (str): name of the pvc the monitor pod is related to
    """
    mon_pod_ocp = (
        ocp.OCP(
            kind=constants.POD,
            namespace=config.ENV_DATA["cluster_namespace"],
            selector=f"pvc_name={pvc_name}",
        )
        .get()
        .get("items")[0]
    )
    return Pod(**mon_pod_ocp)


class TestCephOSDSlowOps(object):
    @pytest.fixture
    def setup(self, request, pod_factory):
        """
        Set values for:
          osd_op_complaint_time=0.1
        """

        self.mon_pods = get_mon_pods()
        self.mon_pods_num = len(self.mon_pods)
        self.mon_pod_selected = random.choice(self.mon_pods)
        self.pvc_name = self.mon_pod_selected.labels.get("pvc_name")

        def set_osd_op_complaint_time(osd_op_complaint_time_val: float):
            self.mon_pod_selected = get_mon_pod_by_pvc_name(self.pvc_name)
            logger.info(f"Selected mon pod is: {self.mon_pod_selected.name}")
            logger.info(
                f"Setting values: osd_op_complaint_time={osd_op_complaint_time_val}"
            )
            self.ct_pod = pod.get_ceph_tools_pod()
            # mon in the "tell" command should be mon.a / mon.b / mon.c
            mon_id = (
                self.mon_pod_selected.get().get("metadata").get("labels").get("mon")
            )
            self.ct_pod.exec_ceph_cmd(
                ceph_cmd=f"ceph tell mon.{mon_id} injectargs "
                f"--osd_op_complaint_time={osd_op_complaint_time_val}"
            )

            # restart mon pod
            self.mon_pod_selected.delete()
            self.mon_pod_selected = get_mon_pod_by_pvc_name(self.pvc_name)
            wait_for_resource_state(self.mon_pod_selected, constants.STATUS_RUNNING)

        set_osd_op_complaint_time(0.1)
        # reduce storage utilization speed by number of monitors slowed down
        # in order to calculate workload timeout correctly during workload_fio_storageutilization
        fio_storage_util_reduced = constants.DEFAULT_OSD_OP_COMPLAINT_TIME / 3
        config.ENV_DATA["fio_storageutilization_min_mbps"] = fio_storage_util_reduced

        def finalizer():
            """
            Set default values for:
              osd_op_complaint_time=30.000000
            """

            set_osd_op_complaint_time(constants.DEFAULT_OSD_OP_COMPLAINT_TIME)
            config.ENV_DATA[
                "fio_storageutilization_min_mbps"
            ] = constants.DEFAULT_OSD_OP_COMPLAINT_TIME

        request.addfinalizer(finalizer)

    @tier3
    @blue_squad
    def test_ceph_osd_low_ops_alert(
        self,
        setup,
        pvc_factory_session,
        pod_factory_session,
        project,
        fio_pvc_dict,
        fio_job_dict,
        fio_configmap_dict,
        measurement_dir,
        tmp_path,
    ):
        """
        Test to verify bz #1966139

        CephOSDSlowOps. An Object Storage Device (OSD) with slow requests is every OSD that is not able to service
        the I/O operations per second (IOPS) in the queue within the time defined by the osd_op_complaint_time
        parameter. By default, this parameter is set to 30 seconds.

        1. As precondition test setup is to reduce osd_op_complaint_time to 0.1
        to prepare condition to get CephOSDSlowOps
        2. Run workload_fio_storageutilization gradually filling up the storage by 5% and verify the alerts
        2.1 Validate the CephOSDSlowOps fired during workload_fio_storageutilization,
         if so - finish the test, if not continue workload_fio_storageutilization operations
        """
        # starting from 1% fill up the storage until CephOSDSlowOps will be found or storage utilization
        percent_step = 5
        fill_storage_up_to = 97
        test_pass = False
        for i in range(10, fill_storage_up_to + 1, percent_step):
            logger.info(f"running workload storage utilization up to {i}p")
            fixture_name = f"workload_storageutilization_{i}p_rbd"

            workload_operation = workload_fio_storageutilization(
                fixture_name,
                project,
                fio_pvc_dict,
                fio_job_dict,
                fio_configmap_dict,
                measurement_dir,
                tmp_path,
                target_percentage=i / 100.0,
                throw_skip=False,
            )

            if not workload_operation:
                continue

            start_operation = workload_operation.get("start")
            logger.debug(f"workload started {start_operation}")

            stop_operation = workload_operation.get("stop")
            logger.debug(f"workload stopped {stop_operation}")

            osd_pods = get_osd_pods()
            for osd_pod in osd_pods:
                logger.info(f"gathering ops data from {osd_pod.name}")
                name_split = osd_pod.name.split("-")
                osd_index = name_split[3]
                ops = osd_pod.exec_cmd_on_pod(f"ceph daemon osd.{osd_index} ops")
                logger.info(ops)
                historic_ops = osd_pod.exec_cmd_on_pod(
                    f"ceph daemon osd.{osd_index} dump_historic_ops"
                )
                logger.info(historic_ops)

            prometheus_alerts = workload_operation.get("prometheus_alerts")
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
                    test_pass = True
                except AssertionError:
                    if i + percent_step >= fill_storage_up_to:
                        logger.info(
                            f"workload storage utilization {i}p complete. "
                            f"{constants.ALERT_CEPHOSDSLOWOPS} alert not found. "
                            f"Fill the storage to {percent_step}p more"
                        )
                    else:
                        raise

            if test_pass:
                break
