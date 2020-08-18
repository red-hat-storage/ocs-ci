import os

from ocs_ci.ocs import constants
from ocs_ci.utility.utils import run_cmd

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
        create = ''
    else:
        create = ' -c'
    run_cmd(f"htpasswd{create} -B -b {htpasswd_path} {username} {password}")


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
            f" --dry-run -o yaml | oc replace -f - --kubeconfig {kubeconfig}"
        )
    else:
        replace = ''

    cmd = (
        f"oc create secret generic htpass-secret "
        f"--from-file=htpasswd=users.htpasswd -n openshift-config "
        f"--kubeconfig {kubeconfig}{replace}"
    )
    run_cmd(cmd)


def create_htpasswd_idp():
    """
    Create OAuth identity provider of HTPasswd type. It uses htpass-secret
    secret as a source for list of users.

    """
    cmd = f"oc apply -f {constants.HTPASSWD_IDP_YAML}"
