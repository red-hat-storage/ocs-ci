import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import yellow_squad
from ocs_ci.framework.testlib import (
    libtest,
    ManageTest,
    hci_provider_and_client_required,
)
from ocs_ci.ocs.cluster import (
    is_managed_service_cluster,
    is_hci_cluster,
)
from ocs_ci.utility.utils import switch_to_correct_cluster_at_setup
from ocs_ci.helpers.sanity_helpers import Sanity, SanityManagedService
from ocs_ci.ocs.constants import (
    MS_PROVIDER_TYPE,
    HCI_CLIENT,
    HCI_PROVIDER,
    NON_MS_CLUSTER_TYPE,
)

from ocs_ci.ocs.managedservice import (
    check_switch_to_correct_cluster_at_setup,
)

logger = logging.getLogger(__name__)


@yellow_squad
@libtest
class TestSwitchToCorrectIndexAtSetup(ManageTest):
    """
    Test switch to the correct cluster index at setup.
    The class contains test examples of switching to the correct cluster index at setup using the param
    'cluster_type'.

    """

    @pytest.fixture(autouse=True)
    def setup(self, create_scale_pods_and_pvcs_using_kube_job_on_ms_consumers, request):
        switch_to_correct_cluster_at_setup(request)
        # Adding the sanity helpers here to make it similar to a regular test.
        if is_managed_service_cluster():
            self.sanity_helpers = SanityManagedService(
                create_scale_pods_and_pvcs_using_kube_job_on_ms_consumers
            )
        elif is_hci_cluster:
            # TODO: implement hci sanity helpers
            pass
        else:
            self.sanity_helpers = Sanity()

    @hci_provider_and_client_required
    @pytest.mark.parametrize(
        "cluster_type",
        [HCI_CLIENT, HCI_PROVIDER],
    )
    def test_switch_to_correct_cluster_with_hci_cluster_types(self, cluster_type):
        """
        Test switch to the correct cluster index at setup, when we have hci cluster types

        """
        check_switch_to_correct_cluster_at_setup(cluster_type)

    @pytest.mark.parametrize(
        "cluster_type",
        [MS_PROVIDER_TYPE, NON_MS_CLUSTER_TYPE],
    )
    def test_switch_to_correct_cluster_with_provider_and_non_ms_cluster_types(
        self, cluster_type
    ):
        """
        Test switch to the correct cluster index at setup,
        when we have MS provider and non-MS cluster types

        """
        check_switch_to_correct_cluster_at_setup(cluster_type)

    def test_switch_to_correct_cluster_without_cluster_type_param(self):
        """
        Test switch to the correct cluster index at setup, when we don't pass the cluster type param

        """
        check_switch_to_correct_cluster_at_setup()
