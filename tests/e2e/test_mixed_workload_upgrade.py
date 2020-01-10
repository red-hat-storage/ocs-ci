"""
Module to test mixed workloads with OCP Upgrade
"""
import logging

from ocs_ci.utility.workloads.mixworkloads import MixWorkload
from ocs_ci.utility.utils import run_cmd
from ocs_ci.framework.testlib import workloads

from ocs_ci.ocs import constants
log = logging.getLogger(__name__)


@workloads
def test_mixed_workload():
    log.info("Setting up the workload")
    mix_pgsql_tar_untar = MixWorkload(workload_name=constants.MIX_PGSQL_TAR_YAML)
    mix_pgsql_tar_untar.run()
    run_cmd('oc get pods -o wide -n openshift-logging')
