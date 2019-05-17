"""
General OCP object
"""
import os
import logging
import yaml

from munch import munchify
from utility.utils import TimeoutSampler
from utility.utils import run_cmd

log = logging.getLogger(__name__)


class OCP(object):
    """
    A basic OCP object to run basic 'oc' commands
    """

    def __init__(self, api_version='v1', kind='Service', namespace=None):
        """
        Initializer function

        Args:
            api_version (str): TBD
            kind (str): TBD
            namespace (str): The name of the namespace to use
        """
        self._api_version = api_version
        self._kind = kind
        self._namespace = namespace

    @property
    def api_version(self):
        return self._api_version

    @property
    def kind(self):
        return self._kind

    @property
    def namespace(self):
        return self._namespace

    def exec_oc_cmd(self, command):
        """
        Executing 'oc' command

        Args:
            command (str): The command to execute (e.g. create -f file.yaml)
                without the initial 'oc' at the beginning

        Returns:
            Munch Obj: this object represents a returned yaml file
        """
        oc_cmd = "oc "
        kubeconfig = os.getenv('KUBECONFIG')
        if self.namespace:
            oc_cmd += f"-n {self.namespace} "

        if kubeconfig:
            oc_cmd += f"--kubeconfig {kubeconfig} "

        oc_cmd += command
        out = run_cmd(cmd=oc_cmd)
        return munchify(yaml.safe_load(out))

    def get(self, resource_name='', out_yaml_format=True):
        """
        Get command - 'oc get <resource>'

        Args:
            resource_name (str): The resource name to fetch

        Example:
            get('my-pv1')

        Returns:
            Munch Obj: this object represents a returned yaml file
        """
        command = f"get {self.kind} {resource_name}"
        if out_yaml_format:
            command += " -o yaml"
        return self.exec_oc_cmd(command)

    def create(self, yaml_file, out_yaml_format=True):
        """
        Creates a new resource

        Args:
            yaml_file (str): Path to a yaml file to use in 'oc create -f
                file.yaml
            out_yaml_format (bool): Determines if the output should be
                formatted to a yaml like string

        Returns:
            Munch Obj: this object represents a returned yaml file
        """
        command = f"create -f {yaml_file}"
        if out_yaml_format:
            command += " -o yaml"

        return self.exec_oc_cmd(command)

    def delete(self, yaml_file, wait=True):
        """
        Deletes a resource

        Args:
            yaml_file (str): Path to a yaml file to use in 'oc delete -f
                file.yaml
            wait (bool): Determines if the delete command should wait to
                completion

        Returns:
            Munch Obj: this object represents a returned yaml file
        """
        command = f"delete -f {yaml_file}"
        if wait:
            command += " --wait=True"
        return self.exec_oc_cmd(command)

    def apply(self, yaml_file):
        """
        Applies configuration changes to a resource

        Args:
            yaml_file (str): Path to a yaml file to use in 'oc apply -f
                file.yaml

        Returns:
            Munch Obj: this object represents a returned yaml file
        """
        command = f"apply -f {yaml_file}"
        return self.exec_oc_cmd(command)

    def wait_for_resource_status(
        self, resource_name, condition, timeout=30, sleep=3
    ):
        """
        Wait for a resource to reach to a desired condition

        Args:
            resource_name (str): The name of the resource to wait
                for (e.g.my-pv1)
            condition (str): The desired state the resource should be at
                This is referring to: status.phase presented in the resource
                yaml file
                (e.g. status.phase == Available)
        """
        for sample in TimeoutSampler(
            timeout, sleep, self.get, resource_name
        ):
            if sample.status.phase == condition:
                return True
        return False
