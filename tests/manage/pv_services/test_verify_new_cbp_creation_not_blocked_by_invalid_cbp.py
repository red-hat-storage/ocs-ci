import logging

from tests import helpers
from ocs_ci.framework.testlib import polarion_id, skipif_ocs_version, tier2

log = logging.getLogger(__name__)


@tier2
@skipif_ocs_version('<4.3')
@polarion_id('OCS-2126')
def test_verify_new_cbp_creation_not_blocked_by_invalid_cbp(teardown_factory):
    """
    Test to verify new ceph block pool can be created without deleting
    ceph block pool having invalid parameters
    Verifies bz 1711814
    """
    log.info("Trying creating ceph block pool with invalid replicated size.")
    cbp_invalid = helpers.create_ceph_block_pool(
        replica_size='none', verify=False
    )
    teardown_factory(cbp_invalid)
    assert not helpers.verify_block_pool_exists(cbp_invalid.name), (
        f"Unexpected: Ceph Block Pool {cbp_invalid.name} created with "
        f"invalid replicated size."
    )
    log.info(
        f"Expected: {cbp_invalid.name} with invalid replicated size is not "
        f"present in pools list"
    )

    cbp_valid = helpers.create_ceph_block_pool(verify=False)
    teardown_factory(cbp_valid)
    assert helpers.verify_block_pool_exists(cbp_valid.name), (
        f"Ceph Block Pool {cbp_valid.name} is not created."
    )
