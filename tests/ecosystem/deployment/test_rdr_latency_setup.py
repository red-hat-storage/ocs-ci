from ocs_ci.deployment.multicluster_deployment import setup_rdr_latency
from ocs_ci.framework.pytest_customization.marks import rdr_latency

####################################################################################################
# This file is placeholder for setting up rdr cluster latency #
####################################################################################################


@rdr_latency
def test_rdr_latancy_setup():
    setup_rdr_latency()
