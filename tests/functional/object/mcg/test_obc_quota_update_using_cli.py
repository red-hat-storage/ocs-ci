import logging

from ocs_ci.framework import config
from ocs_ci.helpers import helpers
from ocs_ci.framework.pytest_customization.marks import (
    tier1,
    jira,
    polarion_id,
    red_squad,
    mcg,
)

logger = logging.getLogger(__name__)


@tier1
@mcg
@red_squad
@jira("DFBUGS-1173")
@polarion_id("OCS-6340")
def test_obc_quota_update_using_cli(mcg_obj):
    """
    Test OBC quota update using mcg cli
    """

    # create obc with quota size
    obc_name = helpers.create_unique_resource_name(
        resource_description="obc", resource_type="cli"
    )
    logger.info(f"Creating OBC {obc_name}")
    data_space = 1
    result = mcg_obj.exec_mcg_cmd(
        cmd=f"obc create {obc_name} --max-size={str(data_space)}Gi --exact=true",
        namespace=config.ENV_DATA["cluster_namespace"],
        use_yes=True,
    ).stdout
    logger.info(result)
    op = result.split("\n")
    current_space_avail = next(
        item.split(":")[1].strip() for item in op if "Data Space Avail" in item
    )
    current_space_avail = current_space_avail.split(" ")
    current_avail_value = float(current_space_avail[0])

    new_data_space = current_avail_value + 1

    # Update the quota size from 1GB to 2 GB
    mcg_obj.exec_mcg_cmd(
        cmd=f"bucket update --max-size={str(new_data_space)}Gi {obc_name}",
        namespace=config.ENV_DATA["cluster_namespace"],
        use_yes=True,
    )

    updated_size_resp = mcg_obj.exec_mcg_cmd(
        cmd=f"bucket status {obc_name}",
        namespace=config.ENV_DATA["cluster_namespace"],
        use_yes=True,
    ).stdout
    logger.info(updated_size_resp)
    op = updated_size_resp.split("\n")
    updated_space_avail = next(
        item.split(":")[1].strip() for item in op if "Data Space Avail" in item
    )
    updated_space_avail = updated_space_avail.split(" ")
    updated_avail_value = float(updated_space_avail[0])

    assert (
        new_data_space == updated_avail_value
    ), "There is mismatch in updated data size and provided data size"
    logger.info("Data size has updated successfully")

    # Delete OBC using MCG cli
    mcg_obj.exec_mcg_cmd(
        cmd=f"obc delete {obc_name}",
        namespace=config.ENV_DATA["cluster_namespace"],
        use_yes=True,
    )
    logger.info("OBC deleted successfully")
