import logging
import random
import string
import secrets
from functools import lru_cache

from ocs_ci.framework.pytest_customization.marks import tier1, red_squad, mcg
from ocs_ci.framework.testlib import MCGTest
from ocs_ci.helpers.performance_lib import run_oc_command
from ocs_ci.framework import config
from ocs_ci.ocs.exceptions import CommandFailed


logger = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def get_base_iam_command(mcg_obj):
    """
    Builds base iam command, to which "a real command" should be added by the caller

    Args:
        mcg_obj: An MCG class instance

    Returns:
        str: base iam command
    """

    # iam_host of the iam is the endpoint of the iam command
    routes = run_oc_command(f"get routes -n {config.ENV_DATA['cluster_namespace']}")

    iam_host = None
    for line in routes[1:]:  # skip header
        parts = line.split()
        if parts[0] == "iam":
            iam_host = parts[1]
            break
    endpoint = "https://" + iam_host

    return (
        f'sh -c "'
        f"AWS_ACCESS_KEY_ID={mcg_obj.access_key_id} "
        f"AWS_SECRET_ACCESS_KEY={mcg_obj.access_key} "
        f"{f'AWS_DEFAULT_REGION={mcg_obj.region} ' if mcg_obj.region else ''}"
        f"aws --endpoint={endpoint} iam --no-verify-ssl "
    )


def run_iam_command(mcg_obj, awscli_pod_session, cmd):
    """
    Builds base iam command, to which "a real command" should be added by the caller

    Args:
        mcg_obj: An MCG class instance
        awscli_pod_session (pod): A pod running the AWSCLI tools
        cmd (str): A command to run

    Returns:
        dict: command result
    """

    full_command = get_base_iam_command(mcg_obj) + cmd + '"'
    return awscli_pod_session.exec_cmd_on_pod(full_command)


def generate_random_iam_path(levels=3, length=8):
    """
        Generates random path of iam user

    Args:
        levels (int): Number of levels (nesting) of the path
        length (int): Length of each part of path

    Returns:
           str: random iam user path
    """

    alphabet = string.ascii_lowercase + string.digits
    parts = [
        "".join(secrets.choice(alphabet) for _ in range(length)) for _ in range(levels)
    ]
    return "/" + "/".join(parts) + "/"


def rand_str(n=5):
    """
    Generates random string

    Args:
        n (int): String length

    Returns:
        str: random string
    """
    return "".join(secrets.choice(string.ascii_lowercase) for _ in range(n))


def get_user_access_keys(mcg_obj, awscli_pod_session, user_name):
    """
    Runs user-access-keys command

    Args:
        mcg_obj: An MCG class instance
        awscli_pod_session (pod): A pod running the AWSCLI tools
        user_name (str): Name of the user whose access keys should be returned

    Returns:
        dict: user-access-keys command result
    """

    list_access_key_cmd = f"list-access-keys --user-name {user_name}"
    list_access_key_result = run_iam_command(
        mcg_obj, awscli_pod_session, list_access_key_cmd
    )
    return list_access_key_result.get("AccessKeyMetadata", [])


@tier1
@mcg
@red_squad
class TestIAMUsers(MCGTest):

    def test_iam_user_actions(self, mcg_obj, awscli_pod_session):
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
        new_users_list = []

        # Test create-user and list-users command
        for _ in range(new_users_num):
            random_name_part = "".join(
                random.choices(string.ascii_lowercase + string.digits, k=8)
            )
            new_user_name = f"iam_user_{random_name_part}"
            new_user_path = generate_random_iam_path()
            create_user_cmd = (
                f"create-user --user-name {new_user_name} --path {new_user_path}"
            )
            run_iam_command(mcg_obj, awscli_pod_session, create_user_cmd)
            logger.info(f"User {new_user_name} created")
            new_users_list.append(new_user_name)

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

        # Now delete all the created users to restore the state before the test
        for user_name in new_users_list:
            access_keys = get_user_access_keys(mcg_obj, awscli_pod_session, user_name)
            for (
                key
            ) in access_keys:  # access keys should be deleted before the user deletion
                access_key_id = key["AccessKeyId"]
                delete_key_cmd = f"delete-access-key --user-name {user_name} --access-key-id {access_key_id}"
                run_iam_command(mcg_obj, awscli_pod_session, delete_key_cmd)

            delete_user_cmd = f"delete-user --user-name {user_name}"
            run_iam_command(mcg_obj, awscli_pod_session, delete_user_cmd)
