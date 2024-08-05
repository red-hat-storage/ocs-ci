import logging
import pytest

from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs import constants
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import black_squad, runs_on_provider
from ocs_ci.framework.testlib import tier1, ui, polarion_id
from ocs_ci.ocs.ui.mcg_ui import NamespaceStoreUI
from ocs_ci.ocs.resources.namespacestore import NamespaceStore


logger = logging.getLogger(__name__)


@black_squad
@runs_on_provider
class TestNamespaceStoreUI(object):
    """
    Test namespace-store via User Interface.

    """

    def teardown(self):
        """
        Delete namespacestore.

        """
        if self.namespace_store_obj is not None:
            self.namespace_store_obj.delete()

    @ui
    @tier1
    @runs_on_provider
    @pytest.mark.bugzilla("2158922")
    @polarion_id("OCS-5125")
    def test_create_namespace_store_ui(self, setup_ui_class_factory, pvc_factory):
        """
        1. Create a new PVC on openshift-storage namespce.
        2. Create namespacestore via ui based on filesystem and mount to new pvc
        3. Verify namespacestore in Ready state
        4. Delete namespacestore
        5. Delete PVC

        """
        setup_ui_class_factory()

        self.namespace_store_obj = None
        openshift_storage_ns_obj = OCP(namespace=config.ENV_DATA["cluster_namespace"])
        pvc_obj = pvc_factory(
            interface=constants.CEPHFILESYSTEM,
            project=openshift_storage_ns_obj,
            storageclass=None,
            size=20,
            access_mode=constants.ACCESS_MODE_RWX,
            status=constants.STATUS_BOUND,
            volume_mode=constants.VOLUME_MODE_FILESYSTEM,
        )
        namespace_store_ui_obj = NamespaceStoreUI()
        namespace_store_ui_obj.create_namespace_store(
            namespace_store_name="my-namespace-store",
            namespace_store_provider="fs",
            namespace_store_pvc_name=pvc_obj.name,
            namespace_store_folder="new",
        )
        self.namespace_store_obj = NamespaceStore(
            name="my-namespace-store", method="oc"
        )
        assert (
            self.namespace_store_obj.verify_health()
        ), f"The namespace_store {self.namespace_store_obj.name} is not in Ready state"
