import logging

from ocs_ci.helpers import helpers
from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import (
    polarion_id,
    skipif_ocs_version,
    tier2,
    skipif_external_mode,
    skipif_hci_provider_and_client,
)

logger = logging.getLogger(__name__)


@green_squad
@tier2
@skipif_ocs_version("<4.3")
@polarion_id("OCS-2130")
@skipif_external_mode
@skipif_hci_provider_and_client
def test_verify_new_cbp_creation_not_blocked_by_invalid_cbp(teardown_factory):
    """
    Test to verify new ceph block pool can be created without deleting
    ceph block pool having invalid parameters
    Verifies bz 1711814
    """
    logger.test_step("Create ceph block pool with invalid failure domain")
    cbp_invalid = helpers.create_ceph_block_pool(
        failure_domain="no-failure-domain", verify=False
    )
    teardown_factory(cbp_invalid)
    logger.assertion(f"Invalid CBP '{cbp_invalid.name}' should not exist in pools list")
    assert not helpers.verify_block_pool_exists(cbp_invalid.name), (
        f"Unexpected: Ceph Block Pool {cbp_invalid.name} created with "
        f"invalid failure domain."
    )
    logger.info(
        f"Expected: {cbp_invalid.name} with invalid failure domain is not "
        f"present in pools list"
    )

    logger.test_step("Create valid ceph block pool and verify it exists")
    cbp_valid = helpers.create_ceph_block_pool(verify=False)
    teardown_factory(cbp_valid)
    logger.assertion(f"Valid CBP '{cbp_valid.name}' should exist in pools list")
    assert helpers.verify_block_pool_exists(
        cbp_valid.name
    ), f"Ceph Block Pool {cbp_valid.name} is not created."
    logger.info(f"Verified: {cbp_valid.name} is created")
