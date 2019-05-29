""" Create a namespace """
import os
import subprocess
import shlex
from utility import templating
from ocs.utils import create_oc_resource


def create_private_namespace(name, region='us-east-2'):
    """
    Create a namespace.

    Args:
        name (str): Namespace to be created
        region (str): AWS region.  Defaults to us-east-2

    Returns:
        False if unable to create the new namespace.  If True, a directory in
        /tmp with the namespace's name should contain the private.yaml file
        that created this namespace.

    """
    oc_cmd = "oc get namespace"
    process = subprocess.Popen(shlex.split(oc_cmd), stdout=subprocess.PIPE)
    output, error = process.communicate()
    if error:
        print(f"Error executing: {oc_cmd}")
        return False
    prev_ns = [x for x in output.decode().split('\n') if x.startswith(f"{name} ")]
    if prev_ns:
        print(f"{name} already exists")
        return False
    env_data = dict()
    env_data['cluster_namespace'] = name
    env_data['region'] = region
    local_dir = f"/tmp/{name}"
    if not os.path.exists(local_dir):
        os.makedirs(local_dir)
    templ_parm = templating.Templating()
    create_oc_resource('private.yaml', local_dir, templ_parm, env_data)
    return True
