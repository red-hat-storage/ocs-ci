""" Create a namespace """
import os
from utility import templating
from ocs.utils import create_oc_resource


def create_namespace(name):
    """
    Create a namespace

    When a new namespace is created, a directory in /tmp with the namespace's
    name will be created containing the namespace.yaml file that created this
    namespace.

    Raises AlreadyExists exception if the namespace already exists.

    Args:
        name (str): Namespace to be created
    """
    env_data = dict()
    env_data['cluster_namespace'] = name
    local_dir = f"/tmp/{name}"
    if not os.path.exists(local_dir):
        os.makedirs(local_dir)
    templ_parm = templating.Templating()
    create_oc_resource(
        'namespace.yaml', local_dir, templ_parm, template_data=env_data,
        template_dir="test-deployment",
    )
