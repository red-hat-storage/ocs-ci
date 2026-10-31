from ocs_ci.ocs.acm.acm import import_clusters_with_acm
from ocs_ci.framework.pytest_customization.marks import purple_squad, zstream_4_18_1, zstream_4_18_1_ocs_operator
from ocs_ci.framework.testlib import acm_import

####################################################################################################
# This file is placeholder for calling import ACM as test, until full solution will be implimented #
####################################################################################################


@purple_squad
@acm_import
@zstream_4_18_1
@zstream_4_18_1_ocs_operator
def test_acm_import():
    import_clusters_with_acm()
