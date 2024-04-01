import logging
import pytest
import time

from ocs_ci.ocs import ocp, constants
from ocs_ci.framework.pytest_customization.marks import (
    brown_squad,
    post_upgrade,
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
from ocs_ci.ocs.resources.pod import verify_mon_pod_running

log = logging.getLogger(__name__)


@brown_squad
@ignore_leftovers
@skipif_less_than_five_workers
@skipif_ocs_version("<4.15")
class TestFiveMonInCluster(ManageTest):
    @post_ocs_upgrade
    def test_scale_mons_in_cluster_to_five(self, threading_lock):
        """

        A Testcase to add five mon pods to the cluster when the failure domain value is greater than five

        This test looks if failure domain is greater than five, if yes it will update the monCount to five
        and will wait for the CephMonLowNumber alert to get cleared

        """
        mon_count = 5

        target_msg = "The current number of Ceph monitors can be increased in order to improve cluster resilience."
        target_label = constants.ALERT_CEPHMONLOWCOUNT

        ceph_cluster = CephCluster()

        storagecluster_obj = ocp.OCP(
            resource_name=constants.DEFAULT_CLUSTERNAME,
            namespace=config.ENV_DATA["cluster_namespace"],
            kind=constants.STORAGECLUSTER,
        )

        list_mons = ceph_cluster.get_mons_from_cluster()
        assert len(list_mons) < mon_count, pytest.skip(
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
            params_neg = '{"spec":{"managedResources":{"cephCluster":{"monCount": 4}}}}'
            params = '{"spec":{"managedResources":{"cephCluster":{"monCount": 5}}}}'
            assert storagecluster_obj.patch(
                params=params,
                format_type="merge",
            ), log.error("Mon count should not be updated value other than 3 and 5")

            storagecluster_obj.patch(
                params=params_neg,
                format_type="merge",
            )

            log.info("Verifying that all five mon pods are in running state")
            assert verify_mon_pod_running(
                mon_count
            ), "All five mon pods are not up and running state"

            ceph_cluster.cluster_health_check(timeout=60)

            measure_end_time = time.time()

            assert len(list_mons) != mon_count, pytest.skip(
                "INVALID: Mon count is already set to five."
            )
        else:
            # if test got to this point, the alert was found, test PASS
            pytest.fail(
                "Failed to get CephMonLowCount warning when mon count is updated to five"
            )

        log.info(
            f"Verify that CephMonLowNumber alert got cleared post updating monCount to {mon_count}"
        )
        api.check_alert_cleared(
            label=target_label, measure_end_time=measure_end_time, time_min=300
        )

    def test_mon_restart_post_five_mon_update(
        self, nodes, pvc_factory, pod_factory, force, bucket_factory, rgw_bucket_factory
    ):
        """
        Test nodes restart (from the platform layer, i.e, EC2 instances, VMWare VMs) post
        updating the monCount to five

        """
        ceph_cluster = CephCluster()

        storagecluster_obj = ocp.OCP(
            resource_name=constants.DEFAULT_CLUSTERNAME,
            namespace=config.ENV_DATA["cluster_namespace"],
            kind=constants.STORAGECLUSTER,
        )

        pods = ocp.OCP(
            kind=constants.POD, namespace=config.ENV_DATA["cluster_namespace"]
        )

        list_mons = ceph_cluster.get_mons_from_cluster()
        if len(list_mons) < 5:
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

        ocp_nodes = get_node_objs()
        if is_vsphere_ipi_cluster():
            # When using vSphere IPI, we restart the nodes without stopping them.
            # See issue https://github.com/red-hat-storage/ocs-ci/issues/7760.
            nodes.restart_nodes(nodes=ocp_nodes, force=force, wait=False)
            node_names = [n.name for n in ocp_nodes]
            wait_for_nodes_status(node_names, constants.STATUS_READY, timeout=420)
        else:
            nodes.restart_nodes_by_stop_and_start(nodes=ocp_nodes, force=force)

        self.sanity_helpers.health_check()
        self.sanity_helpers.create_resources(
            pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory
        )

    def test_node_maintenance_post_five_mon_update(
        self,
        pvc_factory,
        pod_factory,
        bucket_factory,
        rgw_bucket_factory,
    ):
        """
        - Update monCount to five
        - Maintenance (mark as unscheduable and drain) 2 worker nodes
        - Check cluster functionality by creating resources
          (pools, storageclasses, PVCs, pods - both CephFS and RBD)
        - Mark the node as scheduable
        - Check cluster and Ceph health
        """
        ceph_cluster = CephCluster()

        storagecluster_obj = ocp.OCP(
            resource_name=constants.DEFAULT_CLUSTERNAME,
            namespace=config.ENV_DATA["cluster_namespace"],
            kind=constants.STORAGECLUSTER,
        )

        pods = ocp.OCP(
            kind=constants.POD, namespace=config.ENV_DATA["cluster_namespace"]
        )

        #Update monCount to five
        list_mons = ceph_cluster.get_mons_from_cluster()
        if len(list_mons) < 5:
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

        mon_nodes = []
        mon_pods = pod.get_mon_pods()
        for pod in mon_pods:
            mon_nodes.append(pod.get_pod_node((pod).name))
        assert mon_nodes, "Failed to find a node for the test"
        # check csi-cephfsplugin-provisioner's and csi-rbdplugin-provisioner's
        # are ready, see BZ #2162504
        provis_pods = pod.get_pods_having_label(
            constants.CSI_CEPHFSPLUGIN_PROVISIONER_LABEL,
            config.ENV_DATA["cluster_namespace"],
        )
        provis_pods += pod.get_pods_having_label(
            constants.CSI_RBDPLUGIN_PROVISIONER_LABEL,
            config.ENV_DATA["cluster_namespace"],
        )
        provis_pod_names = [p["metadata"]["name"] for p in provis_pods]

        # Maintenance the node (unschedule and drain)
        log.info(f"Chosen nodes for draining are {mon_nodes[0:2]} ")
        drain_nodes(mon_nodes[0:2])

        # avoid scenario when provisioners yet not been created (6 sec for creation)
        retry(ResourceNotFoundError, tries=3, delay=3, backoff=3)(
            pod.wait_for_pods_to_be_running
        )(pod_names=provis_pod_names, raise_pod_not_found_error=True)

        # Check basic cluster functionality by creating resources
        # (pools, storageclasses, PVCs, pods - both CephFS and RBD),
        # run IO and delete the resources
        self.sanity_helpers.create_resources(
            pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory
        )
        self.sanity_helpers.delete_resources()

        # Mark the node back to schedulable
        schedule_nodes(mon_nodes[0:2])

        # Perform cluster and Ceph health checks
        self.sanity_helpers.health_check(tries=150)

