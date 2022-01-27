import logging
import re

from ocs_ci.framework.pytest_customization.marks import tier1
from ocs_ci.framework.testlib import polarion_id, bugzilla, config
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
    # Get noobaa status
    status = mcg_obj_session.exec_mcg_cmd("status").stderr
    for line in status.split("\n"):
        if "Not Found" in line:
            assert "Optional" in line, f"Error in noobaa status output- {line}"
        if float(config.ENV_DATA["ocs_version"]) > 4.7 and "Noobaa-db" in line:
            assert (
                "Old noobaa-db service is logged" in line
            ), f"Error in MCG Cli status output- {line}"
    log.info("Verified: noobaa status does not contain any error.")

    # verify noobaa db logs for #bz2004130
    if float(config.ENV_DATA["ocs_version"]) > 4.7:
        pattern = "Not found: Service noobaa-db"
        noobaa_db_log = get_pod_logs(pod_name=constants.NB_DB_NAME_47_AND_ABOVE)
        assert (
            re.search(pattern=pattern, string=noobaa_db_log) is None
        ), f"Error: {pattern} msg found in the noobaa db logs."
