import logging
from time import sleep
import pytest

from ocs_ci.helpers.sanity_helpers import SanityManagedService
from ocs_ci.framework import config
from ocs_ci.framework.testlib import (
    libtest,
    ManageTest,
    ignore_leftovers,
    managed_service_required,
)

log = logging.getLogger(__name__)


@libtest
@ignore_leftovers
@managed_service_required
class TestSanityManagedServiceWithDefaultParams(ManageTest):
    """
    Test the usage of the 'SanityManagedService' class when using the default params
    """

    @pytest.fixture(autouse=True)
    def setup(self, create_scale_pods_and_pvcs_using_kube_job_on_ms_consumers):
        """
        Save the original index, and init the sanity instance
        """
        self.orig_index = config.cur_index
        self.sanity_helpers = SanityManagedService()
        # Init the 'create resources' factory with the default params
        self.sanity_helpers.init_create_resources_on_ms_factory(
            create_scale_pods_and_pvcs_using_kube_job_on_ms_consumers
        )

    @pytest.fixture(autouse=True)
    def teardown(self, request, nodes):
        """
        Make sure the original index is equal to the current index
        """

        def finalizer():
            log.info("Switch to the original cluster index")
            config.switch_ctx(self.orig_index)

        request.addfinalizer(finalizer)

    def test_sanity_ms(self):
        log.info("Start creating resources for the MS consumers")
        self.sanity_helpers.create_resources_on_ms_consumers()
        timeout = 60
        log.info(f"Waiting {timeout} seconds for the IO to be running")
        sleep(timeout)

        log.info("Deleting the resources from the MS consumers")
        self.sanity_helpers.delete_resources_on_ms_consumers()
        log.info("Check the cluster health")
        self.sanity_helpers.health_check_ms()

        assert (
            config.cur_index == self.orig_index
        ), "The current index is different from the original index"
        log.info("The The current index is equal to the original index")


@libtest
@ignore_leftovers
@managed_service_required
class TestSanityManagedServiceWithOptionalParams(ManageTest):
    """
    Test the usage of the 'SanityManagedService' class when passing optional params
    """

    @pytest.fixture(autouse=True)
    def setup(self):
        """
        Save the original index, and init the sanity instance
        """
        self.orig_index = config.cur_index
        self.sanity_helpers = SanityManagedService()

    @pytest.fixture(autouse=True)
    def teardown(self, request, nodes):
        """
        Make sure the original index is equal to the current index
        """

        def finalizer():
            log.info("Switch to the original cluster index")
            config.switch_ctx(self.orig_index)

        request.addfinalizer(finalizer)

    def test_sanity_ms_with_optional_params(
        self, create_scale_pods_and_pvcs_using_kube_job_on_ms_consumers
    ):
        consumer_i = config.get_consumer_indexes_list()[0]
        # Init the 'create resources' factory with the optional params
        self.sanity_helpers.init_create_resources_on_ms_factory(
            create_scale_pods_and_pvcs_using_kube_job_on_ms_consumers,
            scale_count=40,
            pvc_per_pod_count=10,
            start_io=True,
            io_runtime=600,
            max_pvc_size=25,
            consumer_indexes=[consumer_i],
        )

        log.info("Start creating resources for the MS consumers")
        self.sanity_helpers.create_resources_on_ms_consumers()
        timeout = 60
        log.info(f"Waiting {timeout} seconds for the IO to be running")
        sleep(timeout)

        log.info("Deleting the resources from the MS consumers")
        self.sanity_helpers.delete_resources_on_ms_consumers()
        log.info("Check the cluster health")
        self.sanity_helpers.health_check_ms()

        assert (
            config.cur_index == self.orig_index
        ), "The current index is different from the original index"
        log.info("The The current index is equal to the original index")
