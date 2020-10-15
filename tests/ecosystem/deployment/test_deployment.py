import logging
import re

from ocs_ci.framework import config
from ocs_ci.framework.testlib import deployment, polarion_id
from ocs_ci.ocs.resources.storage_cluster import ocs_install_verification
from ocs_ci.utility.reporting import get_polarion_id
from ocs_ci.utility.utils import is_cluster_running, run_cmd
from tests.sanity_helpers import Sanity, SanityExternalCluster
from ocs_ci.ocs.node import get_osd_running_nodes
from ocs_ci.ocs.resources.storage_cluster import get_osd_size
from ocs_ci.ocs.exceptions import UnsupportedFeatureError
from ocs_ci.framework.pytest_customization.marks import skipif_no_encrypted


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


@skipif_no_encrypted
@deployment
def test_verify_osd_encryption():
    """
    Verify the OSD is encrypted
    """
    ocs_version = float(config.ENV_DATA['ocs_version'])
    if ocs_version < 4.6:
        error_message = "Encryption at REST can be enabled only on OCS >= 4.6!"
        raise UnsupportedFeatureError(error_message)

    osd_node_names = get_osd_running_nodes()
    osd_size = get_osd_size()
    lsblk_output_list = []
    for worker_node in osd_node_names:
        lsblk_cmd = 'oc debug node/' + worker_node + ' -- chroot /host lsblk'
        log.info(lsblk_cmd)
        out = run_cmd(lsblk_cmd)
        log.info(out)
        lsblk_output_list.append(out)

    for node_output_lsblk in lsblk_output_list:
        node_lsb = node_output_lsblk.split()
        # Search 'crypt' in node_lsb list
        if 'crypt' not in node_lsb:
            raise EnvironmentError('OSD is not encrypted')
        index_crypt = node_lsb.index('crypt')
        encrypted_component_size = int(
            (re.findall(r'\d+', node_lsb[index_crypt - 2]))[0]
        )
        # Verify that OSD is encrypted, and not another component like sda
        if encrypted_component_size != osd_size:
            raise EnvironmentError(
                'The OSD is not encrypted, another mount encrypted.'
            )
