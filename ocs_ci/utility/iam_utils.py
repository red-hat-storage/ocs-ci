import secrets
import string


def get_base_iam_command(mcg_obj):
    """
    Builds base iam command, to which "a real command" should be added by the caller

    Args:
        mcg_obj: An MCG class instance

    Returns:
        str: base iam command
    """

    return (
        f'sh -c "'
        f"AWS_ACCESS_KEY_ID={mcg_obj.access_key_id} "
        f"AWS_SECRET_ACCESS_KEY={mcg_obj.access_key} "
        f"{f'AWS_DEFAULT_REGION={mcg_obj.region} ' if mcg_obj.region else ''}"
        f"aws --endpoint={mcg_obj.iam_endpoint} iam --no-verify-ssl "
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
