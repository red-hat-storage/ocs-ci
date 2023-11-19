import logging
import os
from itertools import cycle
import pytest

from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import yellow_squad
from ocs_ci.framework.testlib import (
    ManageTest,
    tier4c,
    ignore_leftover_label,
    multicluster_platform_required,
)
from ocs_ci.ocs.managedservice import patch_consumer_toolbox
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources import csv
from ocs_ci.helpers import disruption_helpers
from ocs_ci.framework import config
from ocs_ci.ocs.resources.storage_cluster import verify_storage_cluster

log = logging.getLogger(__name__)


@yellow_squad
@tier4c
@multicluster_platform_required
@ignore_leftover_label(constants.TOOL_APP_LABEL)
@pytest.mark.polarion_id("OCS-3924")
class TestPodDisruptions(ManageTest):
    """
    Tests to verify pod disruption

    """

    pvc_size = 25

    @pytest.fixture(autouse=True)
    def setup(self, request, create_pvcs_and_pods):
        """
        Prepare pods for the test and add finalizer.

        """
        self.provider_cluster_index = config.get_provider_index()
        self.consumer_indexes = config.get_consumer_indexes_list()
        if config.ENV_DATA["platform"].lower() in constants.MANAGED_SERVICE_PLATFORMS:
            # Get the index of current cluster
            initial_cluster_index = config.cur_index

            def teardown():
                # ocs-operator pod deletion on consumer cluster will trigger rook-ceph-tools pod respin. Patching of
                # rook-ceph-tools pod is done in the test case after ocs-operator pod respin. But if the automatic
                # respin of rook-ceph-tools pod is delayed by few seconds, the patching step in the test case will not
                # run. So doing patch at the end of the test to ensure that the rook-ceph-tools pod on consumers
                # can run ceph command.
                for consumer_index in self.consumer_indexes:
                    config.switch_ctx(consumer_index)
                    patch_consumer_toolbox()
                # Switching cluster context will be done during the test case.
                # Switch back to current cluster context after the test case.
                config.switch_ctx(initial_cluster_index)

            request.addfinalizer(teardown)

        self.io_pods = list()
        for cluster_index in self.consumer_indexes:
            config.switch_ctx(cluster_index)
            consumer_cluster_kubeconfig = os.path.join(
                config.clusters[cluster_index].ENV_DATA["cluster_path"],
                config.clusters[cluster_index].RUN.get("kubeconfig_location"),
            )
            pvcs, io_pods = create_pvcs_and_pods(
                pvc_size=self.pvc_size,
                replica_count=1,
                pod_dict_path=constants.PERF_POD_YAML,
            )
            for pvc_obj in pvcs:
                pvc_obj.ocp.cluster_kubeconfig = consumer_cluster_kubeconfig
            for io_pod in io_pods:
                io_pod.ocp.cluster_kubeconfig = consumer_cluster_kubeconfig
            pvcs[0].project.cluster_kubeconfig = consumer_cluster_kubeconfig
            self.io_pods.extend(io_pods)

    def test_pod_disruptions(self, create_pvcs_and_pods):
        """
        Test to perform pod disruption in consumer and provider cluster

        """
        # List of pods to be disrupted. Using different list for consumer and provider for the easy implementation
        pods_on_consumer = [
            "alertmanager_managed_ocs_alertmanager",
            "ocs_osd_controller_manager",
            "prometheus_managed_ocs_prometheus",
            "prometheus_operator",
            "ocs_operator",
        ]
        pods_on_provider = [
            "alertmanager_managed_ocs_alertmanager",
            "ocs_osd_controller_manager",
            "prometheus_managed_ocs_prometheus",
            "prometheus_operator",
            "ocs_provider_server",
            "ocs_operator",
        ]
        disruption_on_consumer = []
        disruption_on_provider = []

        # Start I/O
        log.info("Starting fio on all pods")
        for pod_obj in self.io_pods:
            if pod_obj.pvc.volume_mode == constants.VOLUME_MODE_BLOCK:
                storage_type = "block"
                direct = 1
            else:
                storage_type = "fs"
                direct = 0
            pod_obj.run_io(
                storage_type=storage_type,
                size="10G",
                fio_filename=f"{pod_obj.name}",
                runtime=320,
                end_fsync=1,
                direct=direct,
                invalidate=0,
                fio_installed=True,
            )

        consumer_index_iter = cycle(self.consumer_indexes)

        # Create Disruptions instance for each pod to be disrupted on consumer
        for pod_type in pods_on_consumer:
            consumer_index = next(consumer_index_iter)
            config.switch_ctx(consumer_index)
            disruption_obj = disruption_helpers.Disruptions()
            # Select each pod to be disrupted from different consumers
            disruption_obj.set_resource(resource=pod_type, cluster_index=consumer_index)
            disruption_obj.index_of_consumer = consumer_index
            disruption_on_consumer.append(disruption_obj)

        # Create Disruptions instance for each pod to be disrupted on provider
        config.switch_to_provider()
        for pod_type in pods_on_provider:
            disruption_obj = disruption_helpers.Disruptions()
            disruption_obj.set_resource(
                resource=pod_type, cluster_index=self.provider_cluster_index
            )
            disruption_on_provider.append(disruption_obj)

        # Delete pods on consumer one at a time
        log.info("Starting pod disruptions on consumer clusters")
        for disruptions_obj in disruption_on_consumer:
            disruptions_obj.delete_resource()
            # ocs-operator respin will trigger rook-ceph-tools pod respin.
            # Patch rook-ceph-tools pod to run ceph commands.
            if disruptions_obj.resource == "ocs_operator":
                config.switch_ctx(disruptions_obj.index_of_consumer)
                patch_consumer_toolbox()

        # Delete pods on provider one at a time
        log.info("Starting pod disruptions on provider cluster")
        for disruptions_obj in disruption_on_provider:
            disruptions_obj.delete_resource()

        log.info("Wait for IO to complete on pods")
        for pod_obj in self.io_pods:
            pod_obj.get_fio_results()
            log.info(f"Verified IO on pod {pod_obj.name}")
        log.info("IO is successful on all pods")

        # Performs different checks in the clusters
        for cluster_index in [self.provider_cluster_index] + self.consumer_indexes:
            config.switch_ctx(cluster_index)

            # Verify managedocs components are Ready
            log.info("Verifying managedocs components state")
            managedocs_obj = OCP(
                kind="managedocs",
                resource_name="managedocs",
                namespace=config.ENV_DATA["cluster_namespace"],
            )
            for component in {"alertmanager", "prometheus", "storageCluster"}:
                assert (
                    managedocs_obj.get()["status"]["components"][component]["state"]
                    == "Ready"
                ), f"{component} status is {managedocs_obj.get()['status']['components'][component]['state']}"

            # Verify storagecluster status
            log.info("Verifying storagecluster status")
            verify_storage_cluster()

            # Verify CSV status
            for managed_csv in {
                constants.OCS_CSV_PREFIX,
                constants.OSD_DEPLOYER,
                constants.OSE_PROMETHEUS_OPERATOR,
            }:
                csvs = csv.get_csvs_start_with_prefix(
                    managed_csv, config.ENV_DATA["cluster_namespace"]
                )
                assert (
                    len(csvs) == 1
                ), f"Unexpected number of CSVs with {managed_csv} prefix: {len(csvs)}"
                csv_name = csvs[0]["metadata"]["name"]
                csv_obj = csv.CSV(
                    resource_name=csv_name,
                    namespace=config.ENV_DATA["cluster_namespace"],
                )
                log.info(f"Check if {csv_name} is in Succeeded phase.")
                csv_obj.wait_for_phase(phase="Succeeded", timeout=600)

            # Verify the phase of ceph cluster
            log.info("Verify the phase of ceph cluster")
            cephcluster = OCP(
                kind="CephCluster", namespace=config.ENV_DATA["cluster_namespace"]
            )
            cephcluster_yaml = cephcluster.get().get("items")[0]
            expected_phase = "Connected"
            if cluster_index == self.provider_cluster_index:
                expected_phase = "Ready"
            assert (
                cephcluster_yaml["status"]["phase"] == expected_phase
            ), f"Status of cephcluster {cephcluster_yaml['metadata']['name']} is {cephcluster_yaml['status']['phase']}"

        # Create PVC and pods on all consumer clusters
        log.info("Creating new PVCs and pods")
        pods = list()
        for cluster_index in self.consumer_indexes:
            config.switch_ctx(cluster_index)
            consumer_cluster_kubeconfig = os.path.join(
                config.clusters[cluster_index].ENV_DATA["cluster_path"],
                config.clusters[cluster_index].RUN.get("kubeconfig_location"),
            )
            pvcs, io_pods = create_pvcs_and_pods(
                pvc_size=self.pvc_size,
                replica_count=1,
                pod_dict_path=constants.PERF_POD_YAML,
            )
            for pvc_obj in pvcs:
                pvc_obj.ocp.cluster_kubeconfig = consumer_cluster_kubeconfig
            for io_pod in io_pods:
                io_pod.ocp.cluster_kubeconfig = consumer_cluster_kubeconfig
            pvcs[0].project.cluster_kubeconfig = consumer_cluster_kubeconfig
            pods.extend(io_pods)

        # Run I/O on new pods
        log.info("Running I/O on new pods")
        for pod_obj in pods:
            if pod_obj.pvc.volume_mode == constants.VOLUME_MODE_BLOCK:
                storage_type = "block"
                direct = 1
            else:
                storage_type = "fs"
                direct = 0
            pod_obj.run_io(
                storage_type=storage_type,
                size="10G",
                fio_filename=f"{pod_obj.name}",
                runtime=320,
                end_fsync=1,
                direct=direct,
                invalidate=0,
                fio_installed=True,
            )

        log.info("Wait for I/O to complete on new pods")
        for pod_obj in pods:
            pod_obj.get_fio_results()
            log.info(f"Verified IO on the new pod {pod_obj.name}")
        log.info("IO is successful on new pods")
