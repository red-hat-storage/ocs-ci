import logging

from ocs_ci.ocs.acm.acm import import_clusters_with_acm, CreateClusterViaACM
from ocs_ci.framework.pytest_customization.marks import purple_squad
from ocs_ci.framework.testlib import acm_import, acm_install
from ocs_ci.framework import config
from ocs_ci.ocs.utils import get_non_acm_cluster_config


####################################################################################################
# This file is placeholder for calling import ACM as test, until full solution will be implimented #
####################################################################################################

log = logging.getLogger(__name__)


@purple_squad
@acm_import
def test_acm_import():
    import_clusters_with_acm()


@purple_squad
@acm_install
def test_acm_create():
    """
    Create ocp cluster via ACM

    """
    non_acm_clusters = get_non_acm_cluster_config()
    non_acm_cluster_objs = dict()
    for non_acm_cluster in non_acm_clusters:
        config.switch_ctx(non_acm_cluster.MULTICLUSTER["multicluster_index"])
        non_acm_cluster_objs[
            non_acm_cluster.MULTICLUSTER["multicluster_index"]
        ] = CreateClusterViaACM()
        non_acm_cluster_objs[
            non_acm_cluster.MULTICLUSTER["multicluster_index"]
        ].create_cluster()

    for context_id, non_acm_cluster_obj in non_acm_cluster_objs.items():
        config.switch_ctx(context_id)
        non_acm_cluster_obj.verify()
