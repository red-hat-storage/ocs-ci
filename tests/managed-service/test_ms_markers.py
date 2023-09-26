import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import yellow_squad
from ocs_ci.framework.testlib import (
    libtest,
    ManageTest,
    ignore_leftovers,
    managed_service_required,
    skipif_ms_consumer,
    skipif_ms_provider,
    runs_on_provider,
)
from ocs_ci.ocs.cluster import (
    is_ms_consumer_cluster,
    is_ms_provider_cluster,
)
from ocs_ci.ocs.managedservice import check_and_change_current_index_to_default_index

logger = logging.getLogger(__name__)


@yellow_squad
@libtest
@managed_service_required
@ignore_leftovers
class TestManagedServiceMarkers(ManageTest):
    """
    Test that the managed service markers work as expected
    """

    @pytest.mark.first
    def test_default_cluster_context_index_equal_to_current_index(self):
        """
        Test that the default cluster index is equal to the current cluster index. This test should run first
        """
        assert (
            check_and_change_current_index_to_default_index()
        ), "The default cluster index is different from the current cluster index"
        logger.info(
            "The default cluster index is equal to the current cluster index as expected"
        )

    @skipif_ms_consumer
    def test_marker_skipif_ms_consumer(self):
        """
        Test that the 'skipif_ms_consumer' marker work as expected
        """
        assert (
            not is_ms_consumer_cluster()
        ), "The cluster is a consumer cluster, even though we have the marker 'skipif_ms_consumer'"
        logger.info("The cluster is not a consumer cluster as expected")

        assert check_and_change_current_index_to_default_index()
        logger.info(
            "The default cluster index is equal to the current cluster index as expected"
        )

    @skipif_ms_provider
    def test_marker_skipif_ms_provider(self):
        """
        Test that the 'skipif_ms_provider' marker work as expected
        """
        assert (
            not is_ms_provider_cluster()
        ), "The cluster is a provider cluster, even though we have the marker 'skipif_ms_provider'"
        logger.info("The cluster is not a provider cluster as expected")

        assert check_and_change_current_index_to_default_index()
        logger.info(
            "The default cluster index is equal to the current cluster index as expected"
        )

    @runs_on_provider
    @pytest.mark.second_to_last
    def test_runs_on_provider_marker(self):
        """
        Test that the 'runs_on_provider' marker work as expected
        """
        assert (
            is_ms_provider_cluster()
        ), "The cluster is not a provider cluster, even though we have the marker 'runs_on_provider'"
        logger.info("The cluster is a provider cluster as expected")

    @pytest.mark.last
    def test_current_index_not_change_after_using_runs_on_provider(self):
        """
        Test that the current cluster index didn't change after using the 'runs_on_provider'
        marker in the previous test.
        """
        assert (
            check_and_change_current_index_to_default_index()
        ), "The current cluster index has changed after using the 'runs_on_provider' marker"
        logger.info(
            "The current cluster index didn't change after using the 'runs_on_provider' marker"
        )
