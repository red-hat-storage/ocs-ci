import logging

from ocs_ci.framework import config
from ocs_ci.ocs import defaults
from ocs_ci.framework.testlib import deployment, polarion_id
from ocs_ci.ocs.resources.storage_cluster import ocs_install_verification
from ocs_ci.utility.reporting import get_deployment_polarion_id
from ocs_ci.utility.utils import is_cluster_running
from ocs_ci.ocs.ocp import OCP
from tests.sanity_helpers import Sanity

log = logging.getLogger(__name__)


@deployment
@polarion_id(get_deployment_polarion_id())
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
            nb_eps = config.DEPLOYMENT.get('noobaa_endpoints')
            if nb_eps > 1:
                log.info(f"Scaling up Noobaa endpoints to maximum of {nb_eps}")
                params = f'{{"spec":{{"endpoints":{{"maxCount":{nb_eps},"minCount":1}}}}}}'
                noobaa = OCP(kind='noobaa', namespace=defaults.ROOK_CLUSTER_NAMESPACE)
                noobaa.patch(resource_name='noobaa', params=params, format_type='merge')
            # Check basic cluster functionality by creating resources
            # (pools, storageclasses, PVCs, pods - both CephFS and RBD),
            # run IO and delete the resources
            sanity_helpers = Sanity()
            sanity_helpers.health_check()
            sanity_helpers.create_resources(pvc_factory, pod_factory)
            sanity_helpers.delete_resources()

    if teardown:
        log.info(
            "Cluster will be destroyed during teardown part of this test."
        )
