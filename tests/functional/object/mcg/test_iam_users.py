import logging
import random
import string
import secrets

from ocs_ci.framework.pytest_customization.marks import tier2, red_squad, mcg
from ocs_ci.framework.testlib import MCGTest
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.utility.iam_utils import (
    generate_random_iam_path,
    run_iam_command,
    get_user_access_keys,
)

logger = logging.getLogger(__name__)


def rand_str(n=5):
    """
    Generates random string

    Args:
        n (int): String length

    Returns:
        str: random string
    """
    return "".join(secrets.choice(string.ascii_lowercase) for _ in range(n))


@tier2
@mcg
@red_squad
class TestIAMUsers(MCGTest):

    def test_iam_user_actions(self, iam_users_factory, mcg_obj, awscli_pod_session):
        """
        Runs different iam users tests
        The scenario is as following:
        1. Create several users and verify that they are created
        2. Choose a random user, update its path and verify that the update worked
        3. Choose a random user, create, update and delete its access key, verify that everything works as expected
        4. Choose a random user, create multiple access keys and verify that the quota cannot be exceeded
        5. Choose a random user, tag/untag it and verify that works as expected
        6. Delete all the created users

        Args:
            mcg_obj: An MCG class instance
            awscli_pod_session (pod): A pod running the AWSCLI tools

        """
        new_users_num = 3
        new_users_list = iam_users_factory(num=new_users_num)

        list_users_result = run_iam_command(mcg_obj, awscli_pod_session, "list-users")
        existing_usernames = {
            user["UserName"] for user in list_users_result.get("Users", [])
        }
        for name in new_users_list:
            assert name in existing_usernames, f"Failed to add iam user : {name}"
        logger.info("All users added successfully")

        # Test update-user command
        user_to_update = random.choice(new_users_list)
        updated_path = generate_random_iam_path()
        update_user_cmd = (
            f"update-user --user-name {user_to_update} --new-path {updated_path}"
        )
        run_iam_command(mcg_obj, awscli_pod_session, update_user_cmd)

        get_user_cmd = f"get-user --user-name {user_to_update}"
        get_user_result = run_iam_command(mcg_obj, awscli_pod_session, get_user_cmd)
        actual_path = get_user_result.get("User", {}).get("Path")
        assert (
            actual_path == updated_path
        ), f"After update command for {user_to_update}: expected path '{updated_path}', actual path '{actual_path}'"
        logger.info(f"The path of user {user_to_update} updated successfully")

        # Test create-access-key, list-access-keys, update-access-key, delete-access-key commands
        user_for_access_keys = random.choice(new_users_list)
        create_access_key_cmd = f"create-access-key --user-name {user_for_access_keys}"
        run_iam_command(mcg_obj, awscli_pod_session, create_access_key_cmd)

        access_keys = get_user_access_keys(
            mcg_obj, awscli_pod_session, user_for_access_keys
        )
        assert len(access_keys) == 1, (
            f"Expected exactly one access key "
            f"for user {user_for_access_keys}, got {len(access_keys)}"
        )
        assert (
            access_keys[0].get("Status") == "Active"
        ), f"Expected key to be Active, got {access_keys[0].get('Status')}"
        access_key_id = access_keys[0]["AccessKeyId"]
        logger.info(
            f"Access key to the user {user_for_access_keys} created successfully"
        )

        update_access_key_cmd = (
            f"update-access-key --user-name {user_for_access_keys} "
            f"--access-key-id {access_key_id} --status Inactive"
        )
        run_iam_command(mcg_obj, awscli_pod_session, update_access_key_cmd)
        access_keys = get_user_access_keys(
            mcg_obj, awscli_pod_session, user_for_access_keys
        )
        assert (
            access_keys[0].get("Status") == "Inactive"
        ), f"Expected key to be Inactive, got {access_keys[0].get('Status')}"
        logger.info(
            f"Access key for the user {user_for_access_keys} updated successfully"
        )

        delete_access_key_cmd = f"delete-access-key --user-name {user_for_access_keys} --access-key-id {access_key_id}"
        run_iam_command(mcg_obj, awscli_pod_session, delete_access_key_cmd)
        access_keys = get_user_access_keys(
            mcg_obj, awscli_pod_session, user_for_access_keys
        )
        assert (
            not access_keys
        ), f"Expected no access key for user {user_for_access_keys}, got {len(access_keys)}"
        logger.info(
            f"Access key to the user {user_for_access_keys} deleted successfully"
        )

        # Test multiple access keys and verify that the limit (2) cannot be exceeded
        user_for_multiple_access_keys = random.choice(new_users_list)
        create_access_key_cmd = (
            f"create-access-key --user-name {user_for_multiple_access_keys}"
        )
        run_iam_command(mcg_obj, awscli_pod_session, create_access_key_cmd)
        run_iam_command(mcg_obj, awscli_pod_session, create_access_key_cmd)
        access_keys = get_user_access_keys(
            mcg_obj, awscli_pod_session, user_for_multiple_access_keys
        )
        assert len(access_keys) == 2, (
            f"Expected two access keys "
            f"for user {user_for_multiple_access_keys}, got {len(access_keys)}"
        )
        logger.info(
            f"Two access keys for the user {user_for_multiple_access_keys} created successfully"
        )

        try:
            run_iam_command(mcg_obj, awscli_pod_session, create_access_key_cmd)
        except CommandFailed as ex:
            if "Cannot exceed quota for AccessKeysPerUser" in str(ex):
                logger.info("As expected, cannot create access key above the quota (2)")
            else:
                raise ex

        # Test tag-user, list-user-tags, untag-users commands
        user_for_tags = random.choice(new_users_list)
        tags = [  # create two pairs of random tags
            {"Key": f"key_{rand_str()}", "Value": f"value_{rand_str()}"},
            {"Key": f"key_{rand_str()}", "Value": f"value_{rand_str()}"},
        ]
        tags_str = " ".join(f"Key={t['Key']},Value={t['Value']}" for t in tags)
        tag_user_cmd = f"tag-user --user-name {user_for_tags} --tags {tags_str}"
        run_iam_command(mcg_obj, awscli_pod_session, tag_user_cmd)

        list_user_tags_cmd = f"list-user-tags --user-name {user_for_tags}"
        list_user_tags_result = run_iam_command(
            mcg_obj, awscli_pod_session, list_user_tags_cmd
        )

        user_tags_dict = {
            t["Key"]: t["Value"] for t in list_user_tags_result.get("Tags", [])
        }

        for t in tags:  # assert all expected tags exist
            assert t["Key"] in user_tags_dict, f"Missing tag key: {t['Key']}"
            assert (
                user_tags_dict[t["Key"]] == t["Value"]
            ), f"Value mismatch for {t['Key']}: expected {t['Value']}, got {user_tags_dict[t['Key']]}"

        first_key_name = tags[0]["Key"]
        untag_user_cmd = (
            f"untag-user --user-name {user_for_tags} --tag-keys {first_key_name}"
        )
        run_iam_command(mcg_obj, awscli_pod_session, untag_user_cmd)
        list_user_tags_result = run_iam_command(
            mcg_obj, awscli_pod_session, list_user_tags_cmd
        )
        user_tags_dict = {
            t["Key"]: t["Value"] for t in list_user_tags_result.get("Tags", [])
        }

        # assert the deleted tag is absent
        assert (
            first_key_name not in user_tags_dict
        ), f"Tag {first_key_name} should be absent but is present"
        logger.info("User tags work as expected")
