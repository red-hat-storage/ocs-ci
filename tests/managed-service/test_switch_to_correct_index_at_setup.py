import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import yellow_squad
from ocs_ci.framework.testlib import (
    libtest,
    ManageTest,
    managed_service_required,
)
from ocs_ci.ocs.cluster import (
    is_managed_service_cluster,
)
from ocs_ci.utility.utils import switch_to_correct_cluster_at_setup
from ocs_ci.helpers.sanity_helpers import Sanity, SanityManagedService
from ocs_ci.ocs.constants import MS_CONSUMER_TYPE, MS_PROVIDER_TYPE, NON_MS_CLUSTER_TYPE

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
        else:
            self.sanity_helpers = Sanity()

    @managed_service_required
    @pytest.mark.parametrize(
        "cluster_type",
        [MS_PROVIDER_TYPE, MS_CONSUMER_TYPE],
    )
    def test_switch_to_correct_cluster_with_ms_cluster_types(self, cluster_type):
        """
        Test switch to the correct cluster index at setup, when we have MS cluster types

        """
        check_switch_to_correct_cluster_at_setup(cluster_type)

    @managed_service_required
    @pytest.mark.parametrize(
        "cluster_type",
        [MS_PROVIDER_TYPE],
    )
    def test_switch_to_correct_cluster_with_provider_cluster_type(self, cluster_type):
        """
        Test switch to the correct cluster index at setup, when we have MS provider cluster type

        """
        check_switch_to_correct_cluster_at_setup(cluster_type)

    @pytest.mark.parametrize(
        "cluster_type",
        [MS_PROVIDER_TYPE, MS_CONSUMER_TYPE, NON_MS_CLUSTER_TYPE],
    )
    def test_switch_to_correct_cluster_with_all_cluster_types(self, cluster_type):
        """
        Test switch to the correct cluster index at setup, when we have all the cluster types

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

    @pytest.mark.parametrize(
        "cluster_type",
        [MS_CONSUMER_TYPE, NON_MS_CLUSTER_TYPE],
    )
    def test_switch_to_correct_cluster_with_consumer_and_non_ms_cluster_types(
        self, cluster_type
    ):
        """
        Test switch to the correct cluster index at setup,
        when we have MS consumer and non-MS cluster types

        """
        check_switch_to_correct_cluster_at_setup(cluster_type)

    def test_switch_to_correct_cluster_without_cluster_type_param(self):
        """
        Test switch to the correct cluster index at setup, when we don't pass the cluster type param

        """
        check_switch_to_correct_cluster_at_setup()

    @pytest.mark.parametrize(
        argnames=["cluster_type", "additional_param"],
        argvalues=[
            pytest.param(*[MS_PROVIDER_TYPE, "common_value"]),
            pytest.param(*[MS_CONSUMER_TYPE, "common_value"]),
            pytest.param(*[NON_MS_CLUSTER_TYPE, "common_value"]),
            pytest.param(*[MS_PROVIDER_TYPE, "provider_value"]),
            pytest.param(*[MS_CONSUMER_TYPE, "consumer_value"]),
        ],
    )
    def test_switch_to_correct_cluster_with_all_cluster_types_with_additional_param(
        self, cluster_type, additional_param
    ):
        """
        Test switch to the correct cluster index at setup when we have all cluster types, and we also pass
        an additional parameter. Some param values we use for all the cluster types, and some we use only
        for specific clusters.

        """
        logger.info(f"additional value = {additional_param}")
        check_switch_to_correct_cluster_at_setup(cluster_type)

    @pytest.mark.parametrize(
        argnames=["cluster_type", "additional_param"],
        argvalues=[
            pytest.param(*[MS_PROVIDER_TYPE, "common_value"]),
            pytest.param(*[MS_CONSUMER_TYPE, "common_value"]),
            pytest.param(*[MS_CONSUMER_TYPE, "consumer_value"]),
        ],
    )
    def test_switch_to_correct_cluster_with_ms_cluster_types_with_additional_param(
        self, cluster_type, additional_param
    ):
        """
        Test switch to the correct cluster index at setup when we have all cluster types, and we also pass
        an additional parameter. Some param values we use for all the cluster types, and some we use only
        for specific clusters.

        """
        logger.info(f"additional value = {additional_param}")
        check_switch_to_correct_cluster_at_setup(cluster_type)
