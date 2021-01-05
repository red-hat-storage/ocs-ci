import logging

from ocs_ci.framework.pytest_customization.marks import tier1
from ocs_ci.framework.testlib import polarion_id, bugzilla

log = logging.getLogger(__name__)


@tier1
@polarion_id("OCS-2084")
@bugzilla("1799077")
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
