import logging
import pytest

from ocs_ci.ocs import constants, defaults, managedservice, ocp
from ocs_ci.ocs.resources import pod, storage_cluster
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
    @pytest.mark.parametrize(
        argnames=["resource"],
        argvalues=[
            pytest.param(
                *[constants.CEPHBLOCKPOOL.lower()],
                marks=pytest.mark.polarion_id("OCS-3907"),
            ),
            pytest.param(
                *[constants.CEPHFILESYSTEMSUBVOLUMEGROUP],
                marks=pytest.mark.polarion_id("OCS-3908"),
            ),
        ],
    )
    def test_consumers_connected(self, resource):
        """
        Test run on provider cluster that at least one consumer is connected
        and a unique cephblockpool and subvolumegroup are successfully created
        on the provider cluster for each connected consumer.
        """
        consumer_names = managedservice.get_consumer_names()
        log.info(f"Connected consumer names: {consumer_names}")
        assert consumer_names, "No consumer clusters are connected"
        for consumer_name in consumer_names:
            resource_name = resource + "-" + consumer_name
            resource_yaml = ocp.OCP(
                kind=resource,
                namespace=defaults.ROOK_CLUSTER_NAMESPACE,
                resource_name=resource_name,
            )
            assert resource_yaml.get()["status"]["phase"] == "Ready"

    @tier1
    @pytest.mark.polarion_id("OCS-2694")
    @managed_service_required
    def test_deployer_logs_not_empty(self):
        """
        Test that the logs of manager container of ocs-osd-controller-manager pod are not empty
        """
        deployer_pod = pod.get_pods_having_label(
            constants.MANAGED_CONTROLLER_LABEL, constants.OPENSHIFT_STORAGE_NAMESPACE
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
    @bugzilla("2073025")
    @runs_on_provider
    @pytest.mark.polarion_id("OCS-2695")
    @managed_service_required
    def test_connection_time_out(self):
        """
        Test that connection from mon pod to external domain is blocked and gets timeout
        """
        mon_pod = pod.get_mon_pods()[0]
        with pytest.raises(CommandFailed) as cmdfailed:
            mon_pod.exec_cmd_on_pod("curl google.com")
        assert "Connection timed out" in str(cmdfailed)
