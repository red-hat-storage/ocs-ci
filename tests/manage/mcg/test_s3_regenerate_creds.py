import logging

from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs import constants

logger = logging.getLogger(__name__)


def test_s3_regenerate_creds(mcg_obj):
    """
    Test s3 regenerate credential

    """

    # create a custom namespace
    proj_name = "reg_project"
    logger.info(f"Creating project {proj_name}")
    ocp_obj = OCP(kind="obc", namespace=proj_name)
    ocp_obj.new_project(proj_name)

    # create obc in that namespace
    obc_name = "reg_obc"
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
