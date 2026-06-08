import logging
import pytest

from ocs_ci.helpers import helpers
from ocs_ci.framework.pytest_customization.marks import green_squad, ec_allowed
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    tier2,
    skipif_external_mode,
    skipif_hci_provider_and_client,
)
from ocs_ci.ocs.cluster import is_ec_pool_supported

log = logging.getLogger(__name__)


@green_squad
@tier2
@skipif_ocs_version("<4.3")
@skipif_external_mode
@skipif_hci_provider_and_client
@pytest.mark.parametrize(
    "erasure_coded",
    [
        pytest.param(False, marks=[pytest.mark.polarion_id("OCS-2130")]),
        pytest.param(
            True,
            marks=[
                ec_allowed,
                pytest.mark.polarion_id("OCS-7981"),
                pytest.mark.skipif(
                    not is_ec_pool_supported(),
                    reason="Erasure coded pools are not supported on this cluster",
                ),
            ],
        ),
    ],
)
def test_verify_new_cbp_creation_not_blocked_by_invalid_cbp(
    erasure_coded, teardown_factory
):
    """
    Test to verify new ceph block pool can be created without deleting
    ceph block pool having invalid parameters
    Verifies bz 1711814
    """
    log.info("Trying creating ceph block pool with invalid failure domain.")
    cbp_invalid = helpers.create_ceph_block_pool(
        failure_domain="no-failure-domain", verify=False
    )
    teardown_factory(cbp_invalid)
    assert not helpers.verify_block_pool_exists(cbp_invalid.name), (
        f"Unexpected: Ceph Block Pool {cbp_invalid.name} created with "
        f"invalid failure domain."
    )
    log.info(
        f"Expected: {cbp_invalid.name} with invalid failure domain is not "
        f"present in pools list"
    )

    log.info("Create valid ceph block pool")
    cbp_valid = helpers.create_ceph_block_pool(
        erasure_coded=erasure_coded, verify=False
    )
    teardown_factory(cbp_valid)
    assert helpers.verify_block_pool_exists(
        cbp_valid.name
    ), f"Ceph Block Pool {cbp_valid.name} is not created."
    log.info(f"Verified: {cbp_valid.name} is created")
