"""
General OCP object
"""
import os
import logging
import yaml
import shlex
from munch import munchify

from ocs.exceptions import CommandFailed
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

    def get(self, resource_name='', out_yaml_format=True, selector=None):
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
        if selector is not None:
            command += f"--selector={selector}"
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

    def delete(self, yaml_file=None, resource_name='', wait=True):
        """
        Deletes a resource

        Args:
            yaml_file (str): Path to a yaml file to use in 'oc delete -f
                file.yaml
            resource_name (str): Name of the resource you want to delete
            wait (bool): Determines if the delete command should wait to
                completion

        Returns:
            Munch Obj: this object represents a returned yaml file

        Raises:
            CommandFailed: In case yaml_file and resource_name wasn't provided
        """
        if not (yaml_file or resource_name):
            raise CommandFailed(
                "At least one of resource_name or yaml_file have to "
                "be provided"
            )

        command = f"delete "
        if resource_name:
            command += f"{self.kind} {resource_name}"
        else:
            command += f"-f {yaml_file}"
        if wait:
            command += " --wait=true"
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

    def wait_for_resource(
        self, condition, resource_name='', selector=None, resource_count=0,
        to_delete=False, timeout=60, sleep=3
    ):
        """
        Wait for a resource to reach to a desired condition

        Args:
            condition (str): The desired state the resource should be at
                This is referring to: status.phase presented in the resource
                yaml file
                (e.g. status.phase == Available)
            resource_name (str): The name of the resource to wait
                for (e.g.my-pv1)
            selector (str): The resource selector to search with.
                Example: 'app=rook-ceph-mds'
            resource_count (int): How many resources expected to be
            to_delete (bool): Determines if wait_for_resource should wait for
                a resource to be deleted
            timeout (int): Time in seconds to wait
            sleep (int): Sampling time in seconds

        Returns:
            bool: True in case all resources reached desired condition,
                False otherwise

        """
        for sample in TimeoutSampler(
            timeout, sleep, self.get, resource_name, True, selector
        ):
            # Only 1 resource expected to be returned
            if resource_name:
                if sample.status.phase == condition:
                    return True
            # More than 1 resources returned
            elif sample.kind == 'List':
                in_condition = []
                sample = sample['items']
                for item in sample:
                    if item.status.phase == condition:
                        in_condition.append(item)
                    if resource_count:
                        if len(in_condition) == resource_count and (
                            len(sample) == len(in_condition)
                        ):
                            return True
                    elif len(sample) == len(in_condition):
                        return True
            if to_delete and not sample:
                return True

        return False


    # this method location is temporary, will be changed to ocs class
    def exec_ceph_cmd(self, ceph_cmd):
        """
        Execute a Ceph command on the Ceph tools pod

        Args:
            ceph_cmd (str): The Ceph command to execute on the Ceph tools pod

        Returns:
            str: Ceph command output in a Json format

        """
        pods_list = shlex.split(self.get(resource_name='pods'))
        ct_pod = [pod for pod in pods_list if "ceph-tools" in pod][0]
        assert ct_pod, f"No Ceph tools pod found"
        cmd = f"rsh {ct_pod} {ceph_cmd} --format pretty-json"
        return self.exec_oc_cmd(cmd)
