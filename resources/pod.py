"""
Pod related functionalities and context info

Each pod in the openshift cluster will have a corresponding pod object
"""
import logging
import tempfile
import yaml

from ocs.ocp import OCP
from ocs import defaults, kinds
from ocsci.config import ENV_DATA
from ocs.exceptions import CommandFailed

from resources.ocs import OCS

logger = logging.getLogger(__name__)


class Pod(OCS):
    """
    Handles per pod related context
    """

    def __init__(self, **kwargs):
        """
        Initializer function

        kwargs:
            Copy of ocs/defaults.py::<some pod> dictionary
        """
        self.pod_data = kwargs
        super(Pod, self).__init__(**kwargs)

        self.temp_yaml = tempfile.NamedTemporaryFile(
            mode='w+', prefix='POD_', delete=False
        )
        self._name = self.pod_data.get('metadata').get('name')
        self._labels = self.get_labels()
        self._roles = []
        self.ocp = OCP(
            api_version=defaults.API_VERSION, kind=kinds.POD,
            namespace=self.namespace
        )
        # TODO: get backend config !!

    @property
    def name(self):
        return self._name

    @property
    def namespace(self):
        return self._namespace

    @property
    def roles(self):
        return self._roles

    @property
    def labels(self):
        return self._labels

    def add_role(self, role):
        """
        Adds a new role for this pod

        Args:
            role (str): New role to be assigned for this pod
        """
        self._roles.append(role)

    def exec_cmd_on_pod(self, command):
        """
        Execute a command on a pod (e.g. oc rsh)

        Args:
            command (str): The command to execute on the given pod

        Returns:
            Munch Obj: This object represents a returned yaml file
        """
        rsh_cmd = f"rsh {self.name} "
        rsh_cmd += command
        return self.ocp.exec_oc_cmd(rsh_cmd)

    def get_labels(self):
        """
        Get labels from pod

        Raises:
            NotFoundError: If resource not found

        Returns:
            dict: All the openshift labels on a given pod
        """
        return self.pod_data.get('metadata').get('labels')

    def exec_ceph_cmd(self, ceph_cmd):
        """
        Execute a Ceph command on the Ceph tools pod

        Args:
            ceph_cmd (str): The Ceph command to execute on the Ceph tools pod

        Returns:
            dict: Ceph command output

        Raises:
            CommandFailed: In case the pod is not a toolbox pod
        """
        if 'rook-ceph-tools' not in self.labels.values():
            raise CommandFailed(
                "Ceph commands can be executed only on toolbox pod"
            )
        ceph_cmd += " --format json-pretty"
        out = self.exec_cmd_on_pod(ceph_cmd)

        # For some commands, like "ceph fs ls", the returned output is a list
        if isinstance(out, list):
            return [item for item in out if item]
        return out


# Helper functions for Pods

def get_all_pods(namespace=None):
    """
    Get all pods in a namespace.
    If namespace is None - get all pods

    Returns:
        list: List of Pod objects
    """
    ocp_pod_obj = OCP(kind=kinds.POD, namespace=namespace)
    pods = ocp_pod_obj.get()['items']
    pod_objs = [Pod(**yaml.safe_load(pod)) for pod in pods]
    return pod_objs


def get_ceph_tools_pod():
    """
    Get the Ceph tools pod

    Returns:
        Pod object: The Ceph tools pod object
    """
    ocp_pod_obj = OCP(
        kind=kinds.POD, namespace=ENV_DATA['cluster_namespace']
    )
    ct_pod = ocp_pod_obj.get(
        selector='app=rook-ceph-tools'
    )['items'][0]
    assert ct_pod, f"No Ceph tools pod found"
    ceph_pod = Pod(**ct_pod)
    return ceph_pod
