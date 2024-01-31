import logging

from ocs_ci.framework.pytest_customization.marks import (
    tier1,
    red_squad,
    runs_on_provider,
    mcg,
    provider_client_ms_platform_required,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.framework.testlib import polarion_id

log = logging.getLogger(__name__)


@mcg
@red_squad
@runs_on_provider
@provider_client_ms_platform_required
@tier1
@polarion_id("OCS-5415")
def test_verify_backingstore_uses_rgw(mcg_obj_session):
    """
    Validates whether default MCG backingstore uses rgw endpoint
    """
    ceph_object_store = OCP(
        kind=constants.CEPHOBJECTSTORE,
        resource_name="ocs-storagecluster-cephobjectstore",
    ).get()
    log.debug(f"Ceph object store: {ceph_object_store}")
    rgw_endpoint = ceph_object_store["status"]["endpoints"]["secure"][0]
    log.info(
        f"Checking if backingstore noobaa-default-backing-store uses endpoint {rgw_endpoint}"
    )

    # Get default backingstore status
    backingstore_data = mcg_obj_session.exec_mcg_cmd(
        "backingstore status noobaa-default-backing-store"
    ).stdout
    assert f"endpoint: {rgw_endpoint}" in backingstore_data
