import logging
import pytest

from ocs_ci.framework.testlib import (
    tier1,
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
    ceph_health_check,
)
from ocs_ci.ocs.managedservice import (
    check_default_cluster_context_index_equal_to_current_index,
)

logger = logging.getLogger(__name__)


@tier1
@managed_service_required
@ignore_leftovers
class TestManagedServiceMarkers(ManageTest):
    """
    Test that the managed service markers work as expected
    """

    @pytest.fixture(autouse=True)
    def teardown(self):
        """
        Check that the Ceph health is OK
        """
        logger.info("Check that the Ceph health is OK")
        ceph_health_check()

    def test_default_cluster_context_index_equal_to_current_index(self):
        """
        Test that the default index is equal to the current index. This test should run first
        """
        assert (
            check_default_cluster_context_index_equal_to_current_index()
        ), "The default index is different from the current index"
        logger.info("The default index is equal to the current index as expected")

    @skipif_ms_consumer
    def test_marker_skipif_ms_consumer(self):
        """
        Test that the 'skipif_ms_consumer' marker work as expected
        """
        assert (
            not is_ms_consumer_cluster()
        ), "The cluster is a consumer cluster, even though we have the marker 'skipif_ms_consumer'"
        logger.info("The cluster is not a consumer cluster as expected")

        assert check_default_cluster_context_index_equal_to_current_index()
        logger.info("The default index is equal to the current index as expected")

    @skipif_ms_provider
    def test_marker_skipif_ms_provider(self):
        """
        Test that the 'skipif_ms_provider' marker work as expected
        """
        assert (
            not is_ms_provider_cluster()
        ), "The cluster is a provider cluster, even though we have the marker 'skipif_ms_provider'"
        logger.info("The cluster is not a provider cluster as expected")

        assert check_default_cluster_context_index_equal_to_current_index()
        logger.info("The default index is equal to the current index as expected")

    @runs_on_provider
    def test_runs_on_provider_marker(self):
        """
        Test that the 'runs_on_provider' marker work as expected
        """
        assert (
            is_ms_provider_cluster()
        ), "The cluster is not a provider cluster, even though we have the marker 'runs_on_provider'"
        logger.info("The cluster is a provider cluster as expected")

    def test_current_index_not_change_after_using_runs_on_provider(self):
        """
        Test that the current index didn't change after using the 'runs_on_provider'
        marker in the previous test.
        """
        assert (
            check_default_cluster_context_index_equal_to_current_index()
        ), "The current index has changed after using the 'runs_on_provider' marker"
        logger.info(
            "The current index didn't change after using the 'runs_on_provider' marker"
        )
