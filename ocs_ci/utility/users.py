import logging
import os
import random
import shlex
import string
import time
from dataclasses import dataclass
from tempfile import NamedTemporaryFile
from ocs_ci.utility.retry import retry, catch_exceptions
from ocs_ci.framework import config
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility.rosa import (
    rosa_create_htpasswd_idp,
    rosa_delete_htpasswd_idp,
    rosa_list_idps,
)
from ocs_ci.utility.utils import exec_cmd, TimeoutSampler, get_random_str
from ocs_ci.utility import templating
from ocs_ci.helpers.helpers import create_resource
from ocs_ci.ocs.resources.ocs import OCS

log = logging.getLogger(__name__)


@dataclass
class DevUser:
    """
    Data class representing a dev user with OpenShift and S3 credentials.

    Fields:
        username: OpenShift username for console login.
        password: OpenShift password for console login.
        secret_namespace: Namespace containing the S3 credentials secret.
        secret_name: Name of the secret containing S3 credentials.

    """

    username: str
    password: str
    secret_namespace: str
    secret_name: str


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
        cmd = ["htpasswd", "-B", "-b", htpasswd_path, username, password]
    else:
        cmd = ["htpasswd", "-c", "-B", "-b", htpasswd_path, username, password]
    exec_cmd(cmd, secrets=[password])


def create_htpasswd_secret(htpasswd_path, replace=False):
    """
    Create or update htpass-secret secret from file located on htpasswd_path.

    Args:
        htpasswd_path (str): Path to httpasswd file
        replace (bool): If secret already exists then this will replace it

    """
    kubeconfig = config.RUN["kubeconfig"]

    cmd = (
        f"oc create secret generic htpass-secret "
        f"--from-file=htpasswd={htpasswd_path} -n openshift-config "
        f"--kubeconfig {kubeconfig}"
    )
    if replace:
        secret_data = exec_cmd(f"{cmd} --dry-run=client -o yaml").stdout
        with NamedTemporaryFile(prefix="htpasswd_secret_") as secret_file:
            secret_file.write(secret_data)
            secret_file.flush()
            exec_cmd(f"oc apply --kubeconfig {kubeconfig} -f {secret_file.name}")
    else:
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
    rosa_depl = config.ENV_DATA["platform"].lower() in [
        constants.ROSA_HCP_PLATFORM,
        constants.ROSA_PLATFORM,
    ]
    if rosa_depl:
        idp_name = f"my_htpasswd-{get_random_str(size=3)}"
        cluster_name = config.ENV_DATA["cluster_name"]

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
            username = username + "".join(
                random.choice(string.ascii_letters + string.digits)
                for _ in range(random.randint(0, 10))
            )
        if not password:
            # Generate random password from letters, numbers and special
            # characters. Length is between 6 and 15 characters.
            password = "".join(
                random.choice(string.ascii_letters + string.digits + string.punctuation)
                for _ in range(random.randint(6, 15))
            )
        add_htpasswd_user(username, password, htpasswd_path)

        if rosa_depl:
            rosa_create_htpasswd_idp(htpasswd_path, cluster_name, idp_name=idp_name)
            for sample in TimeoutSampler(
                timeout=300,
                sleep=10,
                func=lambda: rosa_list_idps(cluster_name),
            ):
                if idp_name in sample.keys():
                    break
            log.info(
                "wait another minute for IDP propagate new credentials to management-console"
            )
            time.sleep(60)
        elif not _users:
            ocp_obj = ocp.OCP(
                kind=constants.SECRET, namespace=constants.OPENSHIFT_CONFIG_NAMESPACE
            )
            secret = ocp_obj.get(resource_name="htpass-secret", dont_raise=True) or None
            if secret:
                create_htpasswd_secret(htpasswd_path, replace=True)
            else:
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
        if rosa_depl:
            rosa_delete_htpasswd_idp(
                cluster_name=config.ENV_DATA["cluster_name"], idp_name=idp_name
            )
            return

        with open(htpasswd_path) as f:
            htpasswd = f.readlines()
        new_htpasswd = [line for line in htpasswd if not line.startswith(tuple(_users))]
        with open(htpasswd_path, "w+") as f:
            for line in new_htpasswd:
                f.write(line)
        create_htpasswd_secret(htpasswd_path, replace=True)

    request.addfinalizer(_finalizer)
    return _factory


@retry(CommandFailed, tries=5, delay=5, backoff=1)
def get_server_url():
    """
    Get server URL.

    Returns:
        str: Server URL

    """
    kubeconfig = config.RUN["kubeconfig"]
    cmd = f"oc whoami --show-server --kubeconfig={kubeconfig}"
    return exec_cmd(cmd, shell=True).stdout.decode().strip()


@retry(CommandFailed, tries=5, delay=5, backoff=1)
def login(server_url, user, password):
    """
    Login to the cluster using provided username and password.

    Args:
        server_url (str): Server URL
        user (str): Username
        password (str): Password

    """

    cmd = f"login {shlex.quote(server_url)} -u {shlex.quote(user)} -p {shlex.quote(password)}"
    OCP().exec_oc_cmd(cmd, skip_tls_verify=True, out_yaml_format=False)
    log.info(f"Logged in as {user}")


@catch_exceptions(CommandFailed)
def logout():
    """
    Logout from the cluster.

    """
    exec_cmd("oc logout")
    log.info("Logged out")


def create_noobaa_ui_clusterrole() -> OCS:
    """
    Create or update the noobaa-odf-ui ClusterRole for dev users.

    This ClusterRole grants minimal read permissions to NooBaa resources,
    namespaces, and secrets, allowing users to access the Object Browser UI
    without full admin rights.

    Uses 'oc apply' semantics to ensure the ClusterRole is always up-to-date.

    Returns:
        OCS: The ClusterRole resource.

    Raises:
        CommandFailed: If resource apply fails.

    """
    log.info("Applying noobaa-odf-ui ClusterRole")
    clusterrole_data = templating.load_yaml(constants.NOOBAA_ODF_UI_CLUSTERROLE_YAML)

    exec_cmd(f"oc apply -f {constants.NOOBAA_ODF_UI_CLUSTERROLE_YAML}")
    log.info("noobaa-odf-ui ClusterRole applied successfully")

    return OCS(**clusterrole_data)


def bind_user_to_noobaa_ui_role(username: str) -> OCS:
    """
    Bind a user to the noobaa-odf-ui ClusterRole.

    Args:
        username (str): The OpenShift username to bind.

    Returns:
        OCS: The created ClusterRoleBinding resource.

    Raises:
        CommandFailed: If resource creation fails.

    """
    log.info(f"Binding user {username} to noobaa-odf-ui ClusterRole")
    binding_data = templating.load_yaml(constants.NOOBAA_ODF_UI_CLUSTERROLEBINDING_YAML)

    binding_name = f"noobaa-odf-ui-binding-{username}"
    binding_data["metadata"]["name"] = binding_name
    binding_data["subjects"][0]["name"] = username

    create_resource(**binding_data)
    return OCS(**binding_data)


def delete_user_noobaa_ui_binding(username: str) -> None:
    """
    Delete the ClusterRoleBinding for a user.

    Args:
        username (str): The OpenShift username.

    """
    binding_name = f"noobaa-odf-ui-binding-{username}"
    ocp_obj = OCP(kind="ClusterRoleBinding")
    try:
        ocp_obj.delete(resource_name=binding_name)
        log.info(f"Deleted ClusterRoleBinding {binding_name}")
    except CommandFailed as e:
        log.warning(f"Failed to delete binding {binding_name}: {e}")
