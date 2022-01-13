from ocs_ci.ocs.acm.acm import import_clusters_with_acm
from ocs_ci.ocs.acm.acm import AcmAddClusters

####################################################################################################
# This file is placeholder for calling import ACM as test, until full solution will be implimented #
####################################################################################################
from ocs_ci.ocs.ui.acm_ui import AcmPageNavigator
from tests.conftest import setup_ui


def test_import_acm(setup_ui_acm):
    # import_clusters_with_acm()
    acm_obj = AcmAddClusters(setup_ui)
    acm_obj.install_submariner_ui()