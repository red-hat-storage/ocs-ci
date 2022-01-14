from ocs_ci.ocs.acm.acm import import_clusters_with_acm
from ocs_ci.ocs.acm.acm import AcmAddClusters


####################################################################################################
# This file is placeholder for calling import ACM as test, until full solution will be implimented #
####################################################################################################


def test_import_acm():
    import_clusters_with_acm()


def test_submariner_acm_ui(setup_acm_ui):

    acm_obj = AcmAddClusters(setup_acm_ui)
    acm_obj.install_submariner_ui()
    acm_obj.submariner_validation_ui()
