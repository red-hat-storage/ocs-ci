import logging

from ocs_ci.framework import config
from ocs_ci.framework.testlib import deployment, polarion_id
from ocs_ci.ocs.resources.storage_cluster import ocs_install_verification
from ocs_ci.utility.reporting import get_polarion_id
from ocs_ci.utility.utils import is_cluster_running
from tests.sanity_helpers import Sanity, SanityExternalCluster

log = logging.getLogger(__name__)


@deployment
@polarion_id(get_polarion_id())
def test_deployment(pvc_factory, pod_factory):
    deploy = config.RUN['cli_params'].get('deploy')
    teardown = config.RUN['cli_params'].get('teardown')
    if not teardown or deploy:
        log.info("Verifying OCP cluster is running")
        assert is_cluster_running(config.ENV_DATA['cluster_path'])
        if not config.ENV_DATA['skip_ocs_deployment']:
            ocs_registry_image = config.DEPLOYMENT.get(
                'ocs_registry_image'
            )
            ocs_install_verification(ocs_registry_image=ocs_registry_image)

            # Check basic cluster functionality by creating resources
            # (pools, storageclasses, PVCs, pods - both CephFS and RBD),
            # run IO and delete the resources
            if config.DEPLOYMENT['external_mode']:
                sanity_helpers = SanityExternalCluster()
            else:
                sanity_helpers = Sanity()
            sanity_helpers.health_check()
            sanity_helpers.create_resources(pvc_factory, pod_factory)
            sanity_helpers.delete_resources()

    if teardown:
        log.info(
            "Cluster will be destroyed during teardown part of this test."
        )
