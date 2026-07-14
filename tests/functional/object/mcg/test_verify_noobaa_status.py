import logging
import re

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    tier1,
    skipif_ocs_version,
    skipif_openshift_dedicated,
    red_squad,
    runs_on_provider,
    mcg,
    skipif_noobaa_external_pgsql,
)
from ocs_ci.ocs.resources.pod import get_pod_logs
from ocs_ci.framework.testlib import polarion_id
from ocs_ci.framework.pytest_customization.marks import skipif_managed_service
from ocs_ci.utility.utils import get_primary_nb_db_pod

log = logging.getLogger(__name__)


@mcg
@red_squad
@runs_on_provider
@tier1
@polarion_id("OCS-2084")
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
@runs_on_provider
@tier1
@skipif_ocs_version("<4.8")
@polarion_id("OCS-2748")
@skipif_openshift_dedicated
@skipif_managed_service
@skipif_noobaa_external_pgsql
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
    with config.RunWithProviderConfigContextIfAvailable():
        primary_nb_db_pod = get_primary_nb_db_pod()
        noobaa_db_log = get_pod_logs(
            pod_name=primary_nb_db_pod.name,
            namespace=config.ENV_DATA["cluster_namespace"],
        )
    assert (
        re.search(pattern=pattern, string=noobaa_db_log) is None
    ), f"Error: {pattern} msg found in the noobaa db logs."
