import pytest

from ocs_ci.framework.testlib import tier1
from ocs_ci.ocs import data_replication_separation


@tier1
@pytest.mark.polarion_id("")
def test_ceph_pods_have_host_network():
    """
    Test that running ceph pods have set host network.
    """
    data_replication_separation.validate_monitor_pods_have_host_network()
    data_replication_separation.validate_osd_pods_have_host_network()
    data_replication_separation.validate_rgw_pods_have_host_network()
    data_replication_separation.validate_mgr_and_mdr_pods_have_host_network()
    data_replication_separation.validate_ceph_tool_pods_have_host_network()


@tier1
@pytest.mark.polarion_id("")
def test_operator_and_csi_pods_have_host_network():
    """
    Test that running operator and csi pods have set host network.
    """
    data_replication_separation.validate_ceph_exporter_pods_have_host_network()
    data_replication_separation.validate_ceph_operator_pods_have_host_network()
    data_replication_separation.validate_metrics_exporter_pods_have_host_network()
    data_replication_separation.validate_csi_pods_have_host_network()
