import logging
import pytest
import time

from ocs_ci.ocs.resources.storage_cluster import StorageCluster
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources import pod
from ocs_ci.helpers.helpers import (
    verify_rook_ceph_crashcollector_pods_where_rook_ceph_pods_are_running,
)
from ocs_ci.framework.testlib import (
    tier2,
    ManageTest,
    bugzilla,
    skipif_external_mode,
)

log = logging.getLogger(__name__)


class TestResourceCrashCollector(ManageTest):
    """
    Test for ocs operator honoring resource requests and limits for ceph crashcollector pods

    Test Procedure:
    1.Update storage cluster with CPU and memory for limits abd requests
    2.Wait for the crash collector pod to respin
    3.Check crash collector pod yaml for pod requests and limits value of CPU and memory
    4.The new values of CPU, memory limit and requests should be updated in crash collector pod
    """

    @tier2
    @bugzilla("1962751")
    @skipif_external_mode
    @pytest.mark.polarion_id("OCS-4835")
    @pytest.mark.parametrize(
        argnames=["limit_cpu", "request_cpu", "limit_memory", "request_memory"],
        argvalues=[
            pytest.param(
                *['"50m"', '"40m"', '"80Mi"', '"60Mi"'],
                marks=pytest.mark.polarion_id("OCS-4835"),
            ),
        ],
    )
    def test_resource_request_for_crashcollector_pod(
        self, limit_cpu, request_cpu, limit_memory, request_memory
    ):

        cluster_namespace = constants.OPENSHIFT_STORAGE_NAMESPACE
        assert verify_rook_ceph_crashcollector_pods_where_rook_ceph_pods_are_running()
        storage_cluster = StorageCluster(
            resource_name="ocs-storagecluster",
            namespace=cluster_namespace,
        )

        crash_col_limit = (
            '{"spec": {"resources":{"crashcollector": {"limits":{"cpu":'
            + limit_cpu
            + ',"memory":'
            + limit_memory
            + "}}}}}"
        )
        crash_col_request = (
            '{"spec": {"resources":{"crashcollector": {"requests":{"cpu":'
            + request_cpu
            + ',"memory":'
            + request_memory
            + "}}}}}"
        )
        # Add crash collector pod limit and request values in storage cluster yaml
        assert storage_cluster.patch(
            resource_name="ocs-storagecluster",
            params=crash_col_limit,
            format_type="merge",
        ), "storagecluster.ocs.openshift.io/ocs-storagecluster not patched for limit values"

        assert storage_cluster.patch(
            resource_name="ocs-storagecluster",
            params=crash_col_request,
            format_type="merge",
        ), "storagecluster.ocs.openshift.io/ocs-storagecluster not patched for request values"
        time.sleep(60)

        assert verify_rook_ceph_crashcollector_pods_where_rook_ceph_pods_are_running()

        crashcollector_pod_objs = pod.get_crashcollector_pods()

        # Verify crashcollector CPU and memory
        log.info("Verifying OSD CPU and memory")

        for crashcollector_pod_ob in crashcollector_pod_objs:
            crashcollector_pod_obj = crashcollector_pod_ob.get()
            log.info(crashcollector_pod_obj["spec"])
            for container in crashcollector_pod_obj["spec"]["containers"]:
                assert (
                    container["resources"]["limits"]["cpu"] == limit_cpu[1:-1]
                ), "Crash collector pod container cpu limit is not updated to new value."

                assert (
                    str(container["resources"]["requests"]["cpu"]) == request_cpu[1:-1]
                ), "Crash collector pod container cpu request is not updated to new value."

                assert (
                    str(container["resources"]["limits"]["memory"])
                    == limit_memory[1:-1]
                ), "Crash collector pod container memory limit is not updated to new value."

                assert (
                    str(container["resources"]["requests"]["memory"])
                    == request_memory[1:-1]
                ), "Crash collector pod container memory limit is not updated to new value."

        log.info(
            "OCS operator marshals resource requests and limits for ceph crashcollector pods"
        )
