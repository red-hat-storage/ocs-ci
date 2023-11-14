import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import yellow_squad
from ocs_ci.framework.testlib import (
    libtest,
    ManageTest,
    ignore_leftovers,
    hci_provider_and_client_required,
    skipif_hci_client,
    skipif_hci_provider,
    runs_on_provider,
)
from ocs_ci.ocs.cluster import (
    is_hci_client_cluster,
    is_hci_provider_cluster,
)
from ocs_ci.ocs.managedservice import check_and_change_current_index_to_default_index

logger = logging.getLogger(__name__)


@yellow_squad
@libtest
@hci_provider_and_client_required
@ignore_leftovers
class TestHCIProviderClientMarkers(ManageTest):
    """
    Test that the HCI Provider Client  markers work as expected
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

    @skipif_hci_client
    def test_marker_skipif_hci_client(self):
        """
        Test that the 'skipif_hci_client' marker work as expected
        """
        assert (
            not is_hci_client_cluster()
        ), "The cluster is a HCI Client cluster, even though we have the marker 'skipif_hci_client'"
        logger.info("The cluster is not a HCI Client cluster as expected")

        assert check_and_change_current_index_to_default_index()
        logger.info(
            "The default cluster index is equal to the current cluster index as expected"
        )

    @skipif_hci_provider
    def test_marker_skipif_hci_provider(self):
        """
        Test that the 'skipif_hci_provider' marker work as expected
        """
        assert (
            not is_hci_provider_cluster()
        ), "The cluster is a HCI provider cluster, even though we have the marker 'skipif_hci_provider'"
        logger.info("The cluster is not a HCI provider cluster as expected")

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
            is_hci_provider_cluster()
        ), "The cluster is not a HCI provider cluster, even though we have the marker 'runs_on_provider'"
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
