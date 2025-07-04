import pytest
import logging

from ocs_ci.framework.pytest_customization.marks import libtest, run_on_all_clients

logger = logging.getLogger(name=__file__)


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


@libtest
@pytest.mark.parametrize("cluster_index", [1, 2], indirect=True)
def test_cluster_index_fixture(cluster_index):
    logger.info(f"param: {cluster_index}")
