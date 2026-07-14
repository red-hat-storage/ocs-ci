from ocs_ci.ocs import constants
from ocs_ci.ocs.acm.acm import import_clusters_with_acm
from ocs_ci.framework.pytest_customization.marks import purple_squad
from ocs_ci.framework.testlib import acm_import
from ocs_ci.framework import config
import logging

from ocs_ci.utility import version
from ocs_ci.utility.utils import run_cmd, wait_for_machineconfigpool_status

logger = logging.getLogger(__name__)


####################################################################################################
# This file is placeholder for calling import ACM as test, until full solution will be implimented #
####################################################################################################


@purple_squad
@acm_import
def test_acm_import():
    def apply_idms(cluster):
        index = cluster.MULTICLUSTER["multicluster_index"]
        cluster_name = cluster.MULTICLUSTER.get("name", f"Cluster-{index}")
        with config.RunWithConfigContext(index):
            config.switch_ctx(index)
            logger.info(
                f"[{cluster_name}] Creating ImageDigestMirrorSet for ACM Deployment"
            )
            run_cmd(f"oc apply -f {constants.ACM_BREW_IDMS_YAML}")

    def wait_for_mcp(cluster):
        index = cluster.MULTICLUSTER["multicluster_index"]
        cluster_name = cluster.MULTICLUSTER.get("name", f"Cluster-{index}")
        with config.RunWithConfigContext(index):
            logger.info(f"[{cluster_name}] Waiting for MachineConfigPool to be updated")
            wait_for_machineconfigpool_status(node_type="all")

    if version.compare_versions(f"{config.ENV_DATA.get('acm_version')} >= 2.14"):
        # Step 1: Apply IDMS to all clusters
        for cluster in config.clusters:
            try:
                apply_idms(cluster)
            except Exception as e:
                logger.error(
                    f"Error applying IDMS on cluster index {cluster.MULTICLUSTER['multicluster_index']}: {e}"
                )

        # Step 2: Wait for MCP update on all clusters
        for cluster in config.clusters:
            try:
                wait_for_mcp(cluster)
            except Exception as e:
                logger.error(
                    f"Error waiting for MCP on cluster index {cluster.MULTICLUSTER['multicluster_index']}: {e}"
                )
    with config.RunWithAcmConfigContext():
        import_clusters_with_acm()
