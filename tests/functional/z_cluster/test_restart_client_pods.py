import logging
import pytest


from ocs_ci.framework.pytest_customization.marks import brown_squad
from ocs_ci.framework.testlib import (
    tier4b,
    ignore_leftovers,
    ManageTest,
    provider_client_platform_required,
)
from ocs_ci.ocs.resources.pod import (
    get_cephfsplugin_provisioner_pods,
    get_rbdfsplugin_provisioner_pods,
    delete_pods,
    get_plugin_pods,
)
from ocs_ci.helpers.sanity_helpers import SanityProviderMode
from ocs_ci.ocs import constants
from ocs_ci.ocs.cluster import client_cluster_health_check
from ocs_ci.framework import config


logger = logging.getLogger(__name__)


@brown_squad
@ignore_leftovers
@provider_client_platform_required
class TestClientPodsRestart(ManageTest):
    """
    Test pods restart scenarios when using the hosted cluster
    """

    @pytest.fixture(autouse=True)
    def setup(self, create_scale_pods_and_pvcs_using_kube_job_on_hci_clients):
        """
        Initialize Sanity instance, and create pods and PVCs factory

        """
        self.orig_index = config.cur_index
        config.switch_to_consumer()
        self.sanity_helpers = SanityProviderMode(
            create_scale_pods_and_pvcs_using_kube_job_on_hci_clients
        )

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        """
        Switch to the original index

        """

        def finalizer():
            logger.info("Switch to the original cluster index")
            config.switch_ctx(self.orig_index)

        request.addfinalizer(finalizer)

    @tier4b
    def test_restart_csi_pods(self):
        """
        Test restart the csi pods on the hosted cluster

        """
        csi_pods = (
            get_cephfsplugin_provisioner_pods() + get_rbdfsplugin_provisioner_pods()
        )
        interfaces = [constants.CEPHBLOCKPOOL, constants.CEPHFILESYSTEM]
        for interface in interfaces:
            plugin_pods = get_plugin_pods(interface)
            csi_pods += plugin_pods

        csi_pod_names = [p.name for p in csi_pods]
        logger.info(f"Restart the csi pods {csi_pod_names}")
        delete_pods(csi_pods)

        self.sanity_helpers.create_resources_on_clients(tries=3, delay=20)
        self.sanity_helpers.delete_resources_on_clients()
        logger.info("Checking that the client cluster health is OK")
        client_cluster_health_check()
