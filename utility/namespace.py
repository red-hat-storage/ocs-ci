""" Create a namespace """
import os
from utility import templating
from ocs.utils import create_oc_resource


def create_private_namespace(name, region='us-east-2'):
    """
    Create a namespace

    When a new namespace is created, a directory in /tmp with the namespace's
    name will be created containing the private.yaml file that created this
    namespace.

    Raises AlreadyExists exception if the namespace already exists.

    Args:
        name (str): Namespace to be created
        region (str): AWS region.  Defaults to us-east-2
    """
    env_data = dict()
    env_data['cluster_namespace'] = name
    env_data['region'] = region
    local_dir = f"/tmp/{name}"
    if not os.path.exists(local_dir):
        os.makedirs(local_dir)
    templ_parm = templating.Templating()
    create_oc_resource('private.yaml', local_dir, templ_parm,
                       template_data=env_data, template_dir="test-deployment")
