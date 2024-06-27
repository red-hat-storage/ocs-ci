import logging
import pytest

from ocs_ci.helpers.sanity_helpers import (
    SanityProviderMode,
    Sanity,
    SanityWithInitParams,
)
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import yellow_squad
from ocs_ci.framework.testlib import (
    libtest,
    ManageTest,
)
from ocs_ci.ocs.cluster import is_hci_cluster


log = logging.getLogger(__name__)


@yellow_squad
@libtest
class TestSanityWithCreateResourcesParams(ManageTest):
    """
    Test the usage of the 'Sanity' and 'SanityProviderMode' classes when passing the parameters
    in the Sanity 'create_resources' method. This approach we used in our tests so far.

    """

    @pytest.fixture(autouse=True)
    def setup(self, create_scale_pods_and_pvcs_using_kube_job_on_hci_clients):
        """
        Save the original index, and init the sanity instance
        """
        self.orig_index = config.cur_index

        if is_hci_cluster():
            self.sanity_helpers = SanityProviderMode(
                create_scale_pods_and_pvcs_using_kube_job_on_hci_clients
            )
        else:
            self.sanity_helpers = Sanity()

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        """
        Make sure the original index is equal to the current index
        """

        def finalizer():
            if config.cur_index != self.orig_index:
                log.warning("The current index is not equal to the original index")

            log.info("Switch to the original cluster index")
            config.switch_ctx(self.orig_index)

        request.addfinalizer(finalizer)

    def test_sanity_create_resources(
        self, pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory
    ):
        """
        Test Sanity 'create_resources' method

        """
        self.sanity_helpers.create_resources(
            pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory
        )
        self.sanity_helpers.health_check(cluster_check=True, tries=12)

    def test_sanity_create_and_delete_resources(
        self, pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory
    ):
        """
        Test Sanity 'create_resources' and 'delete_resources' methods

        """
        self.sanity_helpers.create_resources(
            pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory
        )
        self.sanity_helpers.delete_resources()
        self.sanity_helpers.health_check(cluster_check=True, tries=12)


@yellow_squad
@libtest
class TestSanityWithInitParams(ManageTest):
    """
    Test the usage of the 'Sanity' and 'SanityProviderMode' classes when passing the parameters
    in the Sanity 'Init' method

    """

    @pytest.fixture(autouse=True)
    def setup(
        self,
        pvc_factory,
        pod_factory,
        bucket_factory,
        rgw_bucket_factory,
        create_scale_pods_and_pvcs_using_kube_job_on_hci_clients,
    ):
        """
        Save the original index, and init the sanity instance
        """
        self.orig_index = config.cur_index

        if is_hci_cluster():
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
        else:
            self.sanity_helpers = SanityWithInitParams(
                pvc_factory,
                pod_factory,
                bucket_factory,
                rgw_bucket_factory,
                run_io=True,
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

    def test_sanity_create_resources(self):
        """
        Test Sanity 'create_resources' method

        """
        self.sanity_helpers.create_resources()
        self.sanity_helpers.health_check(cluster_check=True, tries=12)

    def test_sanity_create_and_delete_resources(self):
        """
        Test Sanity 'create_resources' and 'delete_resources' methods

        """
        self.sanity_helpers.create_resources()
        self.sanity_helpers.delete_resources()
        self.sanity_helpers.health_check()


@yellow_squad
@libtest
class TestSanityWithDefaultParams(ManageTest):
    """
    Test the usage of the 'Sanity' and 'SanityProviderMode' classes when using the default parameters

    """

    @pytest.fixture(autouse=True)
    def setup(self, request):
        """
        Save the original index, and init the sanity instance
        """
        self.orig_index = config.cur_index

        if is_hci_cluster():
            self.sanity_helpers = SanityProviderMode.init_default(request)
        else:
            self.sanity_helpers = SanityWithInitParams.init_default(request)

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        """
        Make sure the original index is equal to the current index
        """

        def finalizer():
            log.info("Switch to the original cluster index")
            config.switch_ctx(self.orig_index)

        request.addfinalizer(finalizer)

    def test_sanity_create_resources(self):
        """
        Test Sanity 'create_resources' method

        """
        self.sanity_helpers.create_resources()
        self.sanity_helpers.health_check()

    def test_sanity_create_and_delete_resources(self):
        """
        Test Sanity 'create_resources' and 'delete_resources' methods

        """
        self.sanity_helpers.create_resources()
        self.sanity_helpers.delete_resources()
        self.sanity_helpers.health_check()
