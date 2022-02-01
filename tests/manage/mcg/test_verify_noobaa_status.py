import logging
import re

from ocs_ci.framework.pytest_customization.marks import tier1
from ocs_ci.framework.testlib import polarion_id, bugzilla, version
from ocs_ci.framework.pytest_customization.marks import skipif_openshift_dedicated
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pod import get_pod_logs

log = logging.getLogger(__name__)


@tier1
@polarion_id("OCS-2084")
@bugzilla("1799077")
@bugzilla("2004130")
@skipif_openshift_dedicated
def test_verify_noobaa_status_cli(mcg_obj_session):
    """
    Verify noobaa status output is clean without any errors using the noobaa cli
    """
    ocs_version_semantic = version.get_semantic_ocs_version_from_config()

    # Get noobaa status
    status = mcg_obj_session.exec_mcg_cmd("status").stderr
    for line in status.split("\n"):
        if "Not Found" in line:
            assert "Optional" in line, f"Error in noobaa status output- {line}"
        if ocs_version_semantic >= version.VERSION_4_8 and "noobaa-db" in line:
            assert (
                "Old noobaa-db service is logged" in line
            ), f"Error in MCG Cli status output- {line}"
    log.info("Verified: noobaa status does not contain any error.")

    # verify noobaa db logs for #bz2004130
    if ocs_version_semantic >= version.VERSION_4_8:
        pattern = "Not found: Service noobaa-db"
        noobaa_db_log = get_pod_logs(pod_name=constants.NB_DB_NAME_47_AND_ABOVE)
        assert (
            re.search(pattern=pattern, string=noobaa_db_log) is None
        ), f"Error: {pattern} msg found in the noobaa db logs."
