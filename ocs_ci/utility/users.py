import os
import random
import string

from ocs_ci.ocs import constants
from ocs_ci.utility.utils import exec_cmd


def add_htpasswd_user(username, password, htpasswd_path):
    """
    Create a new user credentials with provided username and password.
    These will be saved in file located on htpasswd_path. The file will
    be created if it doesn't exist.

    Args:
        username (str): Name of a new user
        password (str): Password for a new user
        htpasswd_path (str): Path to httpasswd file

    """
    if os.path.isfile(htpasswd_path):
        cmd = ['htpasswd', '-B', '-b', htpasswd_path, username, password]
    else:
        cmd = ['htpasswd', '-c', '-B', '-b', htpasswd_path, username, password]
    exec_cmd(cmd)


def create_htpasswd_secret(htpasswd_path, replace=False):
    """
    Create or update htpass-secret secret from file located on htpasswd_path.

    Args:
        htpasswd_path (str): Path to httpasswd file
        replace (bool): If secret already exists then this will replace it

    """
    kubeconfig = os.getenv('KUBECONFIG')
    if replace:
        replace = (
            f" --dry-run -o yaml | oc replace --kubeconfig {kubeconfig} -f -"
        )
    else:
        replace = ''

    cmd = (
        f"oc create secret generic htpass-secret "
        f"--from-file=htpasswd={htpasswd_path} -n openshift-config "
        f"--kubeconfig {kubeconfig}{replace}"
    )
    exec_cmd(cmd)


def delete_htpasswd_secret():
    """
    Delete HTPasswd secret.

    """
    cmd = "oc delete secret htpass-secret -n openshift-config"
    exec_cmd(cmd)


def create_htpasswd_idp():
    """
    Create OAuth identity provider of HTPasswd type. It uses htpass-secret
    secret as a source for list of users.

    """
    cmd = f"oc apply -f {constants.HTPASSWD_IDP_YAML}"
    exec_cmd(cmd)


def user_factory(request, htpasswd_path):
    """
    Create a user factory.

    Args:
        request (obj): request fixture
        htpasswd_path (str): Path to htpasswd file

    Returns:
        func: User factory function

    """
    _users = []

    def _factory(
        username=None,
        password=None,
    ):
        """
        Create a new user.

        Args:
            username (str): Username of a new user. If not proided then
                set it to random string
            password (str): Password of a new user. If not provided then
                set it to random string

        Returns:
            tuple: username and password of a new user

        """
        if not username:
            # Generate random username from letters and numbers starting
            # with letter. Length is between 1 and 11 characters.
            username = random.choice(string.ascii_letters)
            username = username + ''.join(
                random.choice(
                    string.ascii_letters + string.digits
                ) for _ in range(random.randint(0, 10))
            )
        if not password:
            # Generate random password from letters, numbers and special
            # characters. Length is between 6 and 15 characters.
            password = ''.join(
                random.choice(
                    string.ascii_letters + string.digits + string.punctuation
                ) for _ in range(random.randint(6, 15))
            )
        add_htpasswd_user(username, password, htpasswd_path)
        if not _users:
            create_htpasswd_secret(htpasswd_path)
        else:
            create_htpasswd_secret(htpasswd_path, replace=True)

        # : is a delimiter in htpasswd file and it will ensure that only full
        # usernames are deleted
        _users.append(f"{username}:")

        return (username, password)

    def _finalizer():
        """
        Delete all users created by the factory

        """
        with open(htpasswd_path) as f:
            htpasswd = f.readlines()
        new_htpasswd = [
            line for line in htpasswd if line.startswith(tuple(_users))
        ]
        with open(htpasswd_path, 'w+') as f:
            for line in new_htpasswd:
                f.write(line)
        create_htpasswd_secret(htpasswd_path, replace=True)

    request.addfinalizer(_finalizer)
    return _factory
