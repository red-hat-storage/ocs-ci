import logging

import pytest

from ocs_ci.deployment.helpers.hypershift_base import (
    get_random_cluster_name,
)
from ocs_ci.framework import config as ocsci_config
from ocs_ci.framework.pytest_customization.marks import (
    tier4b,
)
from ocs_ci.ocs.version import get_ocs_version
from ocs_ci.utility.utils import get_latest_release_version


logger = logging.getLogger(__name__)


@pytest.fixture
def return_to_original_context(request):
    """
    Make sure that original context is restored after the test.
    """
    original_cluster = ocsci_config.cluster_ctx.MULTICLUSTER["multicluster_index"]

    def finalizer():
        logger.info(f"Switching back to original cluster with index {original_cluster}")
        ocsci_config.switch_ctx(original_cluster)

    request.addfinalizer(finalizer)
    yield


class TestStorageClientRemoval(object):
    """
    Test storage client removal
    """

    @tier4b
    def test_remove_orphan_clients_resources(
        self, create_hypershift_clusters, return_to_original_context
    ):
        """
        This test is to remove the orphaned storage client resources

        Steps:
        1. Create hosted client.
        2. Add block and cephfs resources and data on hosted client.
        3. Remove the storage client with `hcp` command.
        4. Verify the storage client and it's resources were removed from Provider.
        """
        cluster_name = get_random_cluster_name()
        odf_version = get_ocs_version()
        ocp_version = get_latest_release_version()
        hosted_clusters_conf_on_provider = {
            "ENV_DATA": {
                "clusters": {
                    cluster_name: {
                        "hosted_cluster_path": f"~/clusters/{cluster_name}/openshift-cluster-dir",
                        "ocp_version": ocp_version,
                        "cpu_cores_per_hosted_cluster": 8,
                        "memory_per_hosted_cluster": "12Gi",
                        "hosted_odf_registry": "quay.io/rhceph-dev/ocs-registry",
                        "hosted_odf_version": odf_version,
                        "setup_storage_client": True,
                        "nodepool_replicas": 2,
                    }
                }
            }
        }

        create_hypershift_clusters(hosted_clusters_conf_on_provider)

        original_cluster_index = ocsci_config.cluster_ctx.MULTICLUSTER[
            "multicluster_index"
        ]
        logger.info(f"Original cluster index: {original_cluster_index}")

        ocsci_config.switch_to_cluster_by_name(cluster_name)

        logger.info(
            f"Switched to cluster with index {ocsci_config.cluster_ctx.MULTICLUSTER['multicluster_index']}"
        )
