from ocs_ci.ocs.acm.acm import import_clusters_with_acm
from ocs_ci.framework.testlib import acm_import

####################################################################################################
# This file is placeholder for calling import ACM as test, until full solution will be implimented #
####################################################################################################


@acm_import
def test_acm_import(setup_acm_ui):
    import_clusters_with_acm()
    # TODO: Install the submariner and run validation by calling the methods:
    # install_submariner_ui() and submarines_validation_ui() from AcmAddClusters Class
