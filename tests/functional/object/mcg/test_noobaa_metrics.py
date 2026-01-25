import logging
import time
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    tier1,
    polarion_id,
    red_squad,
    mcg,
)


from ocs_ci.ocs.bucket_utils import (
    craft_s3_command,
)
from ocs_ci.ocs.constants import AWSCLI_TEST_OBJ_DIR

logger = logging.getLogger(__name__)


@tier1
@mcg
@red_squad
@polarion_id("OCS-7465")
def test_noobaa_space_available_using_cli(mcg_obj, awscli_pod_session, bucket_factory):
    """
    Test that 'bucket update' command with --max-size and --max-object parameters work as expected
    Args:
        mcg_obj (obj): An object representing the current state of the MCG in the cluster
        awscli_pod_session (pod): A pod running the AWSCLI tools
        bucket_factory: Calling this fixture creates a new bucket(s)
    """

    # Create bucket and check original quota status
    bucket_name = bucket_factory(amount=1, interface="CLI")[0].name

    logger.info(f"Creating bucket {bucket_name}")

    quota_status = get_bucket_status_value(mcg_obj, bucket_name, "QuotaStatus")
    assert (
        quota_status == "QUOTA_NOT_SET"
    ), f"Original quota status should be QUOTA_NOT_SET, is {quota_status}"

    # Set max quota size to 2GB and make sure that this worked as expected
    new_data_space = 2
    mcg_obj.exec_mcg_cmd(
        cmd=f"bucket update --max-size={str(new_data_space)}Gi {bucket_name}",
        namespace=config.ENV_DATA["cluster_namespace"],
        use_yes=True,
    )

    space_avail_after_update = get_bucket_status_value(
        mcg_obj, bucket_name, "Data Space Avail"
    )
    space_avail_after_update = float(space_avail_after_update.split(" ")[0])

    logger.info(f"Space available after update {space_avail_after_update}")
    assert (
        float(new_data_space) == space_avail_after_update
    ), "There is mismatch in updated data size and provided data size"
    quota_status = get_bucket_status_value(mcg_obj, bucket_name, "QuotaStatus")
    assert (
        quota_status == "OPTIMAL"
    ), f"Quota status after update should be OPTIMAL, is {quota_status}"
    logger.info("Data size was updated successfully")

    # Copy a file to the bucket and make sure that the available space decreased
    standard_test_obj_list = awscli_pod_session.exec_cmd_on_pod(
        f"ls -A1 {AWSCLI_TEST_OBJ_DIR}"
    ).split(" ")
    file_name = standard_test_obj_list[0]
    logger.info(f"Going to copy file {file_name} to the bucket {bucket_name}")

    cp_command = f"cp {AWSCLI_TEST_OBJ_DIR}{file_name} s3://{bucket_name}/{file_name}"

    awscli_pod_session.exec_cmd_on_pod(
        command=craft_s3_command(cp_command, mcg_obj=mcg_obj),
        out_yaml_format=False,
    )
    time.sleep(180)

    space_avail_after_write = get_bucket_status_value(
        mcg_obj, bucket_name, "Data Space Avail"
    )
    space_avail_after_write = float(space_avail_after_write.split(" ")[0])
    assert (
        space_avail_after_write < space_avail_after_update
    ), f"Available space before write = {space_avail_after_update}, after write = {space_avail_after_write}"

    # Update bucket with --max-objects quota and verify that it worked as expected
    max_objects = 10
    mcg_obj.exec_mcg_cmd(
        cmd=f"bucket update --max-objects={max_objects} {bucket_name}",
        namespace=config.ENV_DATA["cluster_namespace"],
        use_yes=True,
    )
    # one file was already copied, so now available objects number should be (max_objects -1)
    num_objects_avail_after_update = get_bucket_status_value(
        mcg_obj, bucket_name, "Num Objects Avail"
    )
    assert (
        int(num_objects_avail_after_update) == max_objects - 1
    ), f"Number of available objects is {int(num_objects_avail_after_update)}, expected {max_objects - 1}"

    # copy (max-objects-2) files to the bucket
    for file_name in standard_test_obj_list[1 : max_objects - 1]:
        logger.info(f"Going to copy file {file_name} to the bucket {bucket_name}")

        cp_command = (
            f"cp {AWSCLI_TEST_OBJ_DIR}{file_name} s3://{bucket_name}/{file_name}"
        )

        awscli_pod_session.exec_cmd_on_pod(
            command=craft_s3_command(cp_command, mcg_obj=mcg_obj),
            out_yaml_format=False,
        )

    time.sleep(180)

    # Verify that max_objects -1 have been copied, only 1 object is available and quota status is APPROUCHING_QUOTA
    num_objects_after_write = get_bucket_status_value(
        mcg_obj, bucket_name, "Num Objects"
    )
    assert (
        int(num_objects_after_write) == max_objects - 1
    ), f"Number of written objects is {int(num_objects_after_write) }, expected {max_objects -1}"
    num_objects_avail_after_write = get_bucket_status_value(
        mcg_obj, bucket_name, "Num Objects Avail"
    )
    assert (
        int(num_objects_avail_after_write) == 1
    ), f"Number of available objects is {int(num_objects_avail_after_write)}, expected 1"
    quota_status_after_write = get_bucket_status_value(
        mcg_obj, bucket_name, "QuotaStatus"
    )
    assert (
        quota_status_after_write == "APPROUCHING_QUOTA"
    ), f"Quota status is {quota_status_after_write}, expected APPROUCHING_QUOTA"

    # Update again bucket with bigger --max-objects quota and verify that it worked as expected -- quota status is
    # 'Optimal' again
    max_objects = 20
    mcg_obj.exec_mcg_cmd(
        cmd=f"bucket update --max-objects={max_objects} {bucket_name}",
        namespace=config.ENV_DATA["cluster_namespace"],
        use_yes=True,
    )
    time.sleep(180)
    quota_status = get_bucket_status_value(mcg_obj, bucket_name, "QuotaStatus")
    assert (
        quota_status == "OPTIMAL"
    ), f"Quota status after update should be OPTIMAL, is {quota_status}"


def get_bucket_status_value(mcg_obj, bucket_name, key):
    """
    Helper function returning specific bucket status value by key
    Args:
        mcg_obj (obj): An object representing the current state of the MCG in the cluster
        bucket_name (str): Name of the bucket on which ls should be run
        key (str): Key to bucket status value to be returned
    Returns:
        str: value of the status property
    """
    bucket_status = mcg_obj.exec_mcg_cmd(
        cmd=f"bucket status {bucket_name}",
        namespace=config.ENV_DATA["cluster_namespace"],
        use_yes=True,
    ).stdout
    logger.info(f"Status = {bucket_status}")
    op = bucket_status.split("\n")
    value = next(item.split(":")[1].strip() for item in op if key in item)
    return value
