import logging

import pytest

from ocs_ci.framework.testlib import ocs_upgrade
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs import defaults
from ocs_ci.ocs.ocs_upgrade import run_ocs_upgrade
from ocs_ci.ocs.cluster import CephCluster

logger = logging.getLogger(__name__)


def get_mon_pdb():
    """"""
    # Check for mon pdb
    pdb_obj = OCP(
        kind="pdb",
        resource_name="rook-ceph-mon-pdb",
        namespace=defaults.ROOK_CLUSTER_NAMESPACE,
    )

    disruptions_allowed = pdb_obj.get().get("status").get("disruptionsAllowed")
    min_available_mon = pdb_obj.get().get("spec").get("minAvailable")
    return disruptions_allowed, min_available_mon


@ocs_upgrade
@pytest.mark.polarion_id("xyz")
def test_double_upgrades():
    """
    Tests upgrade procedure of OCS cluster

    """

    ceph_obj = CephCluster()

    run_ocs_upgrade()

    # Check for mon count
    mons_after_upgrade = ceph_obj.get_mons_from_cluster()
    logger.info(f"Mons after upgrade {mons_after_upgrade}")

    disruptions_allowed, min_available_mon = get_mon_pdb()
    logger.info(f"Disruptions allowed {disruptions_allowed}")
    logger.info(f"Minimum available mon count {min_available_mon}")

    assert disruptions_allowed == 1, "Disruption allowed not matching"
    assert min_available_mon == 2, "Min mon count is not matching"
