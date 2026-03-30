import pytest

from ocs_ci.framework.pytest_customization.marks import (
    data_replication_separation_required,
    jira,
    yellow_squad,
)
from ocs_ci.framework.testlib import tier1
from ocs_ci.ocs import data_replication_separation


@tier1
@yellow_squad
@data_replication_separation_required
@pytest.mark.polarion_id("OCS-7308")
def test_ceph_pods_have_host_network():
    """
    Test that running ceph pods have set host network.
    """
    results = []
    results.append(
        data_replication_separation.validate_monitor_pods_have_host_network()
    )
    results.append(data_replication_separation.validate_osd_pods_have_host_network())
    results.append(data_replication_separation.validate_rgw_pods_have_host_network())
    results.append(
        data_replication_separation.validate_mgr_and_mdr_pods_have_host_network()
    )
    results.append(
        data_replication_separation.validate_ceph_tool_pods_have_host_network()
    )
    assert all(
        results
    ), "one or more pods has incorrectly set host network, check test error logs to identify which one"


@tier1
@yellow_squad
@data_replication_separation_required
@jira("DFBUGS-4306")
@pytest.mark.polarion_id("OCS-7307")
def test_operator_and_csi_pods_have_host_network():
    """
    Test that running operator and csi pods have set host network.
    """
    results = []
    results.append(
        data_replication_separation.validate_ceph_exporter_pods_have_host_network()
    )
    results.append(
        data_replication_separation.validate_ceph_operator_pods_have_host_network()
    )
    results.append(
        data_replication_separation.validate_metrics_exporter_pods_have_host_network()
    )
    results.append(data_replication_separation.validate_csi_pods_have_host_network())
    assert all(
        results
    ), "one or more pods has incorrectly set host network, check test error logs to identify which one"
