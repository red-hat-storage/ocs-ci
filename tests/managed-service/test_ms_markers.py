import logging
import pytest

from ocs_ci.framework.testlib import (
    ManageTest,
    ignore_leftovers,
    managed_service_required,
    skipif_ms_consumer,
    skipif_ms_provider,
    runs_on_provider,
)
from ocs_ci.ocs.cluster import is_ms_consumer_cluster, is_ms_provider_cluster
from ocs_ci.framework import config

logger = logging.getLogger(__name__)


def check_default_cluster_context_index_equal_to_current_index(switch_to_default=True):
    """
    Check that the default index is equal to the current index

    Args:
        switch_to_default(bool): If true, it will switch to the default cluster in case the default index
            is different from the current index. False, otherwise. The default value is true.

    Returns:
        bool: True, if the default index is equal to the current index

    """
    default_index = config.ENV_DATA["default_cluster_context_index"]
    logger.info(f"default index = {default_index}, current index = {config.cur_index}")

    if default_index != config.cur_index:
        logger.warning("The default index is different from the current index")
        if switch_to_default:
            logger.info("Switch to the default index")
            config.switch_ctx(default_index)

        return False
    else:
        logger.info("The default index is equal to the current index")
        return True


@managed_service_required
@ignore_leftovers
class TestManagedServiceMarkers(ManageTest):
    """
    Test that the managed service markers work as expected
    """

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        """
        Check that the default index is equal to the current index
        """

        def finalizer():
            assert check_default_cluster_context_index_equal_to_current_index()

        request.addfinalizer(finalizer)

    def test_default_cluster_context_index_equal_to_current_index(self):
        """
        Test that the default index is equal to the current index. This test should run first
        """
        assert (
            check_default_cluster_context_index_equal_to_current_index()
        ), "The default index is different from the current index"

    @skipif_ms_consumer
    def test_marker_skipif_ms_consumer(self):
        """
        Test that the 'skipif_ms_consumer' marker work as expected
        """
        assert (
            not is_ms_consumer_cluster()
        ), "The cluster is a consumer cluster, even though we have the marker 'skipif_ms_consumer'"
        logger.info("The cluster is not a consumer cluster as expected")

    @skipif_ms_provider
    def test_marker_skipif_ms_provider(self):
        """
        Test that the 'skipif_ms_provider' marker work as expected
        """
        assert (
            not is_ms_provider_cluster()
        ), "The cluster is a provider cluster, even though we have the marker 'skipif_ms_provider'"
        logger.info("The cluster is not a provider cluster as expected")

    @runs_on_provider
    def test_runs_on_provider_marker(self):
        """
        Test that the 'runs_on_provider' marker work as expected
        """
        assert (
            is_ms_provider_cluster()
        ), "The cluster is not a provider cluster, even though we have the marker 'runs_on_provider'"
        logger.info("The cluster is a provider cluster as expected")
