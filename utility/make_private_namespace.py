""" Create a namespace """
import os
import subprocess
import shlex
from utility import templating
from ocs.utils import create_oc_resource

<<<<<<< HEAD

=======
>>>>>>> cd83d0b1ef5f3eebe869adca5f8da288df776e04
def make_private_namespace(name, region='us-east-2'):
    """
    Create a namespace.

    Args:
        name (str): namespace to be created
        region (str): aws region.  Defaults to us-east-2

    Returns:
        No return value.  The new namespace should exist, and a directory in
        /tmp with the namespace's name should contain the private.yaml file
        that created this namespace.

    """
    oc_cmd = "oc get namespace"
    process = subprocess.Popen(shlex.split(oc_cmd), stdout=subprocess.PIPE)
    output, error = process.communicate()
    if error:
        print(f"Error executing: {oc_cmd}")
        return
    prev_ns = [x for x in output.decode().split('\n') if x.startswith(f"{name} ")]
    if prev_ns:
        print(f"{name} already exists")
        return
    env_data = dict()
    env_data['platform'] = 'AWS'
    env_data['cluster_namespace'] = name
    env_data['region'] = region
    local_dir = f"/tmp/{name}"
    if not os.path.exists(local_dir):
        os.makedirs(local_dir)
    templ_parm = templating.Templating()
    create_oc_resource('private.yaml', local_dir, templ_parm, env_data)

<<<<<<< HEAD

=======
>>>>>>> cd83d0b1ef5f3eebe869adca5f8da288df776e04
if __name__ == "__main__":
    make_private_namespace('ocs-368')
