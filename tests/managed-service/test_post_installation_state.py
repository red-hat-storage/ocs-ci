import logging
import pytest

from ocs_ci.framework import config
from ocs_ci.ocs import constants, managedservice, ocp
from ocs_ci.ocs.resources import pod, storage_cluster
from ocs_ci.framework.pytest_customization.marks import yellow_squad
from ocs_ci.framework.testlib import (
    acceptance,
    managed_service_required,
    ManageTest,
    ms_provider_required,
    tier1,
    runs_on_provider,
    bugzilla,
)
from ocs_ci.ocs.exceptions import CommandFailed

log = logging.getLogger(__name__)


@yellow_squad
@managed_service_required
class TestPostInstallationState(ManageTest):
    """
    Post-installation tests for ROSA and OSD clusters
    """

    @acceptance
    @managed_service_required
    def test_post_installation(self):
        storage_cluster.ocs_install_verification()

    @acceptance
    @ms_provider_required
    @pytest.mark.polarion_id("OCS-3909")
    def test_consumers_ceph_resources(self):
        """
        Test that all CephResources of every storageconsumer are in Ready status
        """
        consumer_names = managedservice.get_consumer_names()
        for consumer_name in consumer_names:
            consumer_yaml = ocp.OCP(
                kind="StorageConsumer",
                namespace=config.ENV_DATA["cluster_namespace"],
                resource_name=consumer_name,
            )
            ceph_resources = consumer_yaml.get().get("status")["cephResources"]
            for resource in ceph_resources:
                log.info(
                    f"Verifying Ready status of {resource['name']} resource of {consumer_name}"
                )
                assert (
                    resource["status"] == "Ready"
                ), f"{resource['name']} of {consumer_name} is in status {resource['status']}"

    @acceptance
    @ms_provider_required
    @pytest.mark.polarion_id("OCS-3910")
    def test_consumers_capacity(self):
        """
        Test each storageconsumer's capacity and requested capacity.
        Now only 1Ti value is possible. If more options get added, the test
        will need to get the value from the consumer cluster's config file
        """
        consumer_names = managedservice.get_consumer_names()
        for consumer_name in consumer_names:
            consumer_yaml = ocp.OCP(
                kind="StorageConsumer",
                namespace=config.ENV_DATA["cluster_namespace"],
                resource_name=consumer_name,
            ).get()
            log.info(f"Verifying capacity of {consumer_name}")
            assert consumer_yaml["spec"]["capacity"] in {"1Ti", "1Pi"}
            log.info(f"Verifying granted capacity of {consumer_name}")
            assert (
                consumer_yaml["status"]["grantedCapacity"]
                == consumer_yaml["spec"]["capacity"]
            )

    @tier1
    @pytest.mark.polarion_id("OCS-3917")
    @runs_on_provider
    def test_provider_server_logs(self):
        """
        Test that the logs of ocs-provider-server pod have entries for each consumer
        """
        provider_pod = pod.get_pods_having_label(
            constants.PROVIDER_SERVER_LABEL, config.ENV_DATA["cluster_namespace"]
        )[0]
        provider_logs = pod.get_pod_logs(pod_name=provider_pod["metadata"]["name"])
        log_lines = provider_logs.split("\n")
        consumer_names = managedservice.get_consumer_names()
        for consumer_name in consumer_names:
            expected_log = (
                f'successfully Enabled the StorageConsumer resource "{consumer_name}"'
            )
            log_found = False
            for line in log_lines:
                if expected_log in line:
                    log_found = True
                    log.info(f"'{expected_log}' found in ocs-provider-server logs")
                    break
            assert log_found, f"'{expected_log}' not found in ocs-provider-server logs"

    @tier1
    @pytest.mark.polarion_id("OCS-3918")
    @runs_on_provider
    def test_ceph_clients(self):
        """
        Test that for every consumer there are  the following cephclients in
        the provider cluster: rbd provisioner, rbd node, cephfs provisioner,
        cephfs node, healthchecker.
        """
        cephclients = storage_cluster.get_ceph_clients()
        consumer_names = managedservice.get_consumer_names()
        for consumer_name in consumer_names:
            found_clients = []
            for cephclient in cephclients:
                if (
                    cephclient["metadata"]["annotations"][
                        "ocs.openshift.io.storageconsumer"
                    ]
                    == consumer_name
                ):
                    found_client = (
                        f"{cephclient['metadata']['annotations']['ocs.openshift.io.storageclaim']}-"
                        f"{cephclient['metadata']['annotations']['ocs.openshift.io.cephusertype']}"
                    )
                    log.info(f"Ceph client {found_client} for {consumer_name} found")
                    found_clients.append(found_client)
            for client in {
                "rbd-provisioner",
                "rbd-node",
                "cephfs-provisioner",
                "cephfs-node",
                "global-healthchecker",
            }:
                assert (
                    client in found_clients
                ), f"Ceph client {client} for {consumer_name} not found"

    @tier1
    @pytest.mark.polarion_id("OCS-2694")
    def test_deployer_logs_not_empty(self):
        """
        Test that the logs of manager container of ocs-osd-controller-manager pod are not empty
        """
        deployer_pod = pod.get_pods_having_label(
            constants.MANAGED_CONTROLLER_LABEL, config.ENV_DATA["cluster_namespace"]
        )[0]
        deployer_logs = pod.get_pod_logs(
            pod_name=deployer_pod["metadata"]["name"], container="manager"
        )
        log_lines = deployer_logs.split("\n")
        for line in log_lines:
            if "ERR" in line:
                log.info(f"{line}")
        log.info(f"Deployer log has {len(log_lines)} lines.")
        assert len(log_lines) > 100

    @tier1
    @bugzilla("2117312")
    @runs_on_provider
    @pytest.mark.polarion_id("OCS-2695")
    def test_connection_time_out(self):
        """
        Test that connection from mon pod to external domain is blocked and gets timeout
        """
        mon_pod = pod.get_mon_pods()[0]
        with pytest.raises(CommandFailed) as cmdfailed:
            mon_pod.exec_cmd_on_pod("curl google.com")
        assert "Connection timed out" in str(cmdfailed)
