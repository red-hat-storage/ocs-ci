import logging
from time import sleep
import pytest

from ocs_ci.helpers.sanity_helpers import SanityProviderMode
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import yellow_squad
from ocs_ci.framework.testlib import (
    libtest,
    ManageTest,
    hci_provider_and_client_required,
)
from ocs_ci.utility.utils import switch_to_correct_cluster_at_setup
from ocs_ci.ocs.constants import HCI_PROVIDER, HCI_CLIENT

log = logging.getLogger(__name__)


@yellow_squad
@libtest
@hci_provider_and_client_required
class TestSanityProviderModeWithDefaultParams(ManageTest):
    """
    Test the usage of the 'SanityProviderMode' class when using the default params

    """

    @pytest.fixture(autouse=True)
    def setup(self, request, create_scale_pods_and_pvcs_using_kube_job_on_hci_clients):
        """
        Save the original index, and init the sanity instance
        """
        self.orig_index = config.cur_index
        switch_to_correct_cluster_at_setup(request)
        # Pass the 'create_scale_pods_and_pvcs_using_kube_job_on_hci_clients' factory to the
        # init method and use the default params
        self.sanity_helpers = SanityProviderMode(
            create_scale_pods_and_pvcs_using_kube_job_on_hci_clients
        )

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        """
        Make sure the original index is equal to the current index
        """

        def finalizer():
            log.info("Switch to the original cluster index")
            config.switch_ctx(self.orig_index)

        request.addfinalizer(finalizer)

    @pytest.mark.parametrize(
        argnames=["cluster_type"],
        argvalues=[
            pytest.param(*[HCI_PROVIDER]),
            pytest.param(*[HCI_CLIENT]),
        ],
    )
    def test_sanity_ms(self, cluster_type):
        orig_test_index = config.cur_index
        log.info("Start creating resources for the clients")
        self.sanity_helpers.create_resources_on_clients()
        timeout = 60
        log.info(f"Waiting {timeout} seconds for the IO to be running")
        sleep(timeout)

        log.info("Deleting the resources from the clients")
        self.sanity_helpers.delete_resources_on_clients()
        log.info("Check the cluster health")
        self.sanity_helpers.health_check_provider_mode()

        assert (
            config.cur_index == orig_test_index
        ), "The current index is different from the original test index"
        log.info("The current index is equal to the original test index")


@yellow_squad
@libtest
@hci_provider_and_client_required
class TestSanityProviderModeWithOptionalParams(ManageTest):
    """
    Test the usage of the 'SanityProviderMode' class when passing optional params

    """

    @pytest.fixture(autouse=True)
    def setup(self, create_scale_pods_and_pvcs_using_kube_job_on_hci_clients):
        """
        Save the original index, and init the sanity instance
        """
        self.orig_index = config.cur_index

        first_client_i = config.get_consumer_indexes_list()[0]
        # Pass the 'create_scale_pods_and_pvcs_using_kube_job_on_hci_clients' factory to the
        # init method and use the optional params
        self.sanity_helpers = SanityProviderMode(
            create_scale_pods_and_pvcs_using_kube_job_on_hci_clients,
            scale_count=40,
            pvc_per_pod_count=10,
            start_io=True,
            io_runtime=600,
            max_pvc_size=25,
            client_indices=[first_client_i],
        )

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        """
        Make sure the original index is equal to the current index
        """

        def finalizer():
            log.info("Switch to the original cluster index")
            config.switch_ctx(self.orig_index)

        request.addfinalizer(finalizer)

    def test_sanity_ms_with_optional_params(
        self, create_scale_pods_and_pvcs_using_kube_job_on_hci_clients
    ):
        orig_test_index = config.cur_index
        log.info("Start creating resources for the clients")
        self.sanity_helpers.create_resources_on_clients()
        timeout = 60
        log.info(f"Waiting {timeout} seconds for the IO to be running")
        sleep(timeout)

        log.info("Deleting the resources from the clients")
        self.sanity_helpers.delete_resources_on_clients()
        log.info("Check the cluster health")
        self.sanity_helpers.health_check_provider_mode()

        assert (
            config.cur_index == orig_test_index
        ), "The current index is different from the original test index"
        log.info("The current index is equal to the original test index")
