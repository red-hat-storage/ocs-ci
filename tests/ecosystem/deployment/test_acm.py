from ocs_ci.ocs.acm.acm import import_clusters_with_acm
from ocs_ci.ocs.acm.acm import AcmAddClusters
from ocs_ci.ocs.acm.acm import login_to_acm

####################################################################################################
# This file is placeholder for calling import ACM as test, until full solution will be implimented #
####################################################################################################


def test_import_acm():
    import_clusters_with_acm()
    driver = login_to_acm()
    acm_obj = AcmAddClusters(driver)
    acm_obj.install_submariner_ui()
