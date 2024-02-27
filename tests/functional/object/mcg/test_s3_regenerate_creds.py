import logging

from ocs_ci.framework.pytest_customization.marks import (
    tier2,
    bugzilla,
    polarion_id,
    red_squad,
    runs_on_provider,
    mcg,
)
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs import constants

logger = logging.getLogger(__name__)


@tier2
@mcg
@red_squad
@runs_on_provider
@bugzilla("2246328")
@polarion_id("OCS-5216")
def test_s3_regenerate_creds(mcg_obj, project_factory):
    """
    Test s3 regenerate credential

    """

    # create a custom namespace
    proj_name = "reg-project"
    logger.info(f"Creating the project {proj_name}")
    project_factory(project_name=proj_name)

    # create obc in that namespace
    ocp_obj = OCP(kind="obc", namespace=proj_name)
    obc_name = "reg-obc"
    logger.info(f"Creating OBC {obc_name}")
    mcg_obj.exec_mcg_cmd(
        cmd=f"obc create {obc_name} --app-namespace {proj_name}",
        namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
    )
    ocp_obj.get(resource_name=obc_name)

    # regenerate credential
    mcg_obj.exec_mcg_cmd(
        cmd=f"obc regenerate {obc_name} --app-namespace {proj_name}",
        namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
        use_yes=True,
    )
    logger.info("Successfully regenerated s3 credentials")
