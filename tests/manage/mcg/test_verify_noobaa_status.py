import logging
import re

from ocs_ci.framework.pytest_customization.marks import (
    tier1,
    skipif_ocs_version,
    skipif_openshift_dedicated,
    red_squad,
    mcg,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pod import get_pod_logs
from ocs_ci.framework.testlib import polarion_id, bugzilla
from ocs_ci.framework.pytest_customization.marks import skipif_managed_service

log = logging.getLogger(__name__)


@mcg
@red_squad
@tier1
@polarion_id("OCS-2084")
@bugzilla("1799077")
@skipif_openshift_dedicated
@skipif_managed_service
def test_verify_noobaa_status_cli(mcg_obj_session):
    """
    Verify noobaa status output is clean without any errors using the noobaa cli
    """
    # Get noobaa status
    status = mcg_obj_session.exec_mcg_cmd("status").stderr
    for line in status.split("\n"):
        if "Not Found" in line:
            assert "Optional" in line, f"Error in noobaa status output- {line}"
    log.info("Verified: noobaa status does not contain any error.")


@mcg
@red_squad
@tier1
@skipif_ocs_version("<4.8")
@polarion_id("OCS-2748")
@bugzilla("2004130")
@skipif_openshift_dedicated
@skipif_managed_service
def test_verify_noobaa_db_service(mcg_obj_session):
    """
    Validates whether MCG cli and noobaa db logs does not check 'noobaa-db'
    """
    # Get noobaa status
    status = mcg_obj_session.exec_mcg_cmd("status").stderr
    assert (
        'Service "noobaa-db"' not in status
    ), "Error in MCG Cli status regarding non-existent noobaa-db service"
    log.info(
        "Verified: noobaa status does not contain error related to `noobaa-db` service."
    )

    # verify noobaa db logs
    pattern = "Not found: Service noobaa-db"
    noobaa_db_log = get_pod_logs(pod_name=constants.NB_DB_NAME_47_AND_ABOVE)
    assert (
        re.search(pattern=pattern, string=noobaa_db_log) is None
    ), f"Error: {pattern} msg found in the noobaa db logs."
