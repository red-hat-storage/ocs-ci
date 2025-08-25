import pytest
import logging

from ocs_ci.framework import config

from ocs_ci.framework.pytest_customization.marks import (
    libtest,
    run_on_all_clients,
    run_on_all_clients_push_missing_configs,
)
from ocs_ci.utility.utils import get_primary_nb_db_pod

logger = logging.getLogger(name=__file__)


@libtest
@run_on_all_clients
def test_run_on_all_clients_marker(cluster_index):
    pass


@libtest
@run_on_all_clients_push_missing_configs
def test_run_on_all_clients_push_missing_config_marker(cluster_index):
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


@libtest
def test_run_with_provider_decorator():
    """
    Test context switching between provider and consumer configurations.
    """
    # Choose any function that will succeed on provider and fail on clients
    provider_only_func = get_primary_nb_db_pod

    # Wrap the provider_only_func with the decorator
    @config.run_with_provider_context_if_available
    def _run_provider_only_func_using_decorator():
        return provider_only_func()

    # Force switch to client
    with config.RunWithFirstConsumerConfigContextIfAvailable():

        # Use the wrapped function to switch back to provider
        # and perform the action
        _run_provider_only_func_using_decorator()

        # Expect to switch back to client and try again
        # expect to fail
        with pytest.raises(Exception):
            provider_only_func()
