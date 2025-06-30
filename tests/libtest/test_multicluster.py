import pytest

from ocs_ci.framework.pytest_customization.marks import libtest, run_on_all_clients


@libtest
@run_on_all_clients
def test_run_on_all_clients_marker(cluster_index):
    pass


@libtest
@run_on_all_clients
@pytest.mark.parametrize("another_param", [1, 2])
def test_run_on_all_clients_marker_with_additional_parameters(
    another_param, cluster_index
):
    pass
