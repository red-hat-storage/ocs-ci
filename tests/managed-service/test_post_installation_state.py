import logging
import pytest

from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources import csv, pod
from ocs_ci.framework.testlib import (
    managed_service_required,
    ManageTest,
    tier1,
)

log = logging.getLogger(__name__)


class TestPostInstallation(ManageTest):
    """
    Post-installation tests for ROSA and OSD clusters
    """

    @tier1
    @managed_service_required
    @pytest.mark.parametrize(
        argnames=["csv_prefix"],
        argvalues=[
            pytest.param(constants.OCS_CSV_PREFIX),
            pytest.param(constants.OSD_DEPLOYER),
            pytest.param(constants.OSE_PROMETHEUS_OPERATOR),
        ],
    )
    def test_csv_phase_succeeded(self, csv_prefix):
        """
        Test to verify that ocs-operator, ocs-osd-deployer, ose-prometheus-operator csvs
        are in Succeeded phase
        """
        assert (
            len(
                csv.get_csvs_start_with_prefix(
                    csv_prefix, constants.OPENSHIFT_STORAGE_NAMESPACE
                )
            )
            == 1
        )
        csv_data = csv.get_csvs_start_with_prefix(
            csv_prefix, constants.OPENSHIFT_STORAGE_NAMESPACE
        )[0]
        csv_phase = csv_data["status"]["phase"]
        log.info(f"{csv_prefix} csv is in Phase: {csv_phase}")
        assert csv_phase == "Succeeded"

    @tier1
    @managed_service_required
    @pytest.mark.parametrize(
        argnames=["secret_name"],
        argvalues=[
            pytest.param("ocs-converged-smtp"),
            pytest.param("ocs-converged-deadmanssnitch"),
            pytest.param("ocs-converged-pagerduty"),
        ],
    )
    def test_alerting_secret_existence(self, secret_name):
        """
        Test to verify that ocs-converged-pagerduty, ocs-converged-smtp,
        ocs-converged-deadmanssnitch secrets exist in openshift-storage namespace
        """
        secret_ocp_obj = OCP(
            kind="secret", namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        assert secret_ocp_obj.is_exist(resource_name=secret_name)

    @tier1
    @managed_service_required
    @pytest.mark.parametrize(
        argnames=["component_name"],
        argvalues=[
            pytest.param("alertmanager"),
            pytest.param("prometheus"),
            pytest.param("storageCluster"),
        ],
    )
    def test_managedocs_components_ready(self, component_name):
        """
        Test to verify that managedocs components alertmanager, prometheus, storageCluster
        are in Ready state
        """
        managedocs_obj = OCP(
            kind="managedocs",
            resource_name="managedocs",
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
        )
        assert (
            managedocs_obj.get()["status"]["components"][component_name]["state"]
            == "Ready"
        )

    @tier1
    @managed_service_required
    @pytest.mark.parametrize(
        argnames=["pod_label", "pod_count"],
        argvalues=[
            pytest.param(constants.MANAGED_PROMETHEUS_LABEL, 1),
            pytest.param(constants.MANAGED_ALERTMANAGER_LABEL, 3),
        ],
    )
    def test_alerting_pods(self, pod_label, pod_count):
        """
        Test that 1 prometheus pod and 3 alertmanager pods are in Running state
        """
        pods = pod.get_pods_having_label(
            pod_label, constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        assert len(pods) == pod_count
        for each_pod in pods:
            assert each_pod["status"]["phase"] == constants.STATUS_RUNNING

    @tier1
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
        log.info(f"Deployer logs: {deployer_logs}")
        assert len(deployer_logs) > 1000
