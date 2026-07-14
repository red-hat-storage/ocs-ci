from ocs_ci.framework.pytest_customization.marks import libtest
from ocs_ci.framework.testlib import provider_client_ms_platform_required
from ocs_ci.utility.networking import create_drs_machine_config, create_drs_nad


@libtest
@provider_client_ms_platform_required
def test_create_nad_and_mc():
    """
    Create NAD and MachineConfig required to bridge network interface
    for data replication separation.

    The function will create a namespace in the format "clusters-test-cluster"
    if it doesn't already exist.
    """
    test_cluster_name = "test-cluster"
    create_drs_machine_config()
    create_drs_nad(test_cluster_name)
