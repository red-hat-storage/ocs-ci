import pytest
import logging

# import tempfile
# import time


# from ocs_ci.framework import config
from ocs_ci.ocs import constants

# from ocs_ci.deployment.helpers.lso_helpers import setup_local_storage
# from ocs_ci.ocs.node import label_nodes, get_all_nodes, get_node_objs
# from ocs_ci.utility.retry import retry
# from ocs_ci.ocs.ui.validation_ui import ValidationUI
# from ocs_ci.ocs.ui.base_ui import login_ui, close_browser
# from ocs_ci.ocs.utils import (
#     setup_ceph_toolbox,
#     enable_console_plugin,
#     run_cmd,
# )
# from ocs_ci.utility.utils import (
#     wait_for_machineconfigpool_status,
# )
# from ocs_ci.utility import templating, version

# from ocs_ci.deployment.deployment import Deployment, create_catalog_source
# from ocs_ci.deployment.baremetal import clean_disk
# from ocs_ci.ocs.resources.storage_cluster import (
#     verify_storage_cluster,
#     check_storage_client_status,
# )
# from ocs_ci.ocs.resources.catalog_source import CatalogSource
# from ocs_ci.ocs.bucket_utils import check_pv_backingstore_type
# from ocs_ci.ocs.resources import pod
from ocs_ci.helpers.helpers import (
    get_all_storageclass_names,
    verify_block_pool_exists,
    verify_cephblockpool_status,
    check_phase_of_rados_namespace,
)

# from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.helpers.managed_services import verify_storageclient
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    ManageTest,
    tier1,
    skipif_ocp_version,
    skipif_managed_service,
    runs_on_provider,
    skipif_external_mode,
)

log = logging.getLogger(__name__)


@tier1
@skipif_ocs_version("<4.16")
@skipif_ocp_version("<4.16")
@skipif_external_mode
@runs_on_provider
@skipif_managed_service
class TestStorageResourceAllocationAndDistribution(ManageTest):
    @pytest.fixture(scope="class", autouse=True)
    def setup(self, request):
        """
        Setup method for the class

        1. Create storageclient for the provider

        """

    def test_storage_allocation_for_multiple_storageclients_using_same_storageprofile_for_storageclaims(
        self,
    ):
        """
        For multiple clients to the same provider, and for each client StorageClaim is
        created using the same Storage profile name.
        at Provider side:
        1. Verify storageclass creation works as expected
        2. Verify that only one CephBlockPool gets created
        3. Verify for each storageclaim creates a new radosnamespace corresponding to
        the storageprofile check the radosnamespaces are in "Ready" status
        Note: Storageclients are clusterscoped.
        4. Verify data is isolated between the consumers sharing the same blockpool
        """

        # Validate storageclaims are Ready and associated storageclasses are created
        verify_storageclient()

        # Validate cephblockpool created
        assert verify_block_pool_exists(
            constants.DEFAULT_BLOCKPOOL
        ), f"{constants.DEFAULT_BLOCKPOOL} is not created"
        assert verify_cephblockpool_status(), "the cephblockpool is not in Ready phase"

        # Validate radosnamespace created and in 'Ready' status
        assert (
            check_phase_of_rados_namespace()
        ), "The radosnamespace is not in Ready phase"

        # Validate storageclassrequests created
        storage_class_classes = get_all_storageclass_names()
        for storage_class in self.storage_class_claims:
            assert (
                storage_class in storage_class_classes
            ), "Storage classes ae not created as expected"
