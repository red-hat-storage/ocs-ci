import logging
import pytest

from ocs_ci.ocs import constants
from ocs_ci.ocs.resources import pod, storage_cluster
from ocs_ci.framework.testlib import (
    acceptance,
    managed_service_required,
    ManageTest,
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
    
    @managed_service_required
    def test_post_installation(self):
        storage_cluster.ocs_install_verification()

    @acceptance
    @managed_service_required
    def test_post_installation(self):
        storage_cluster.ocs_install_verification()

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
