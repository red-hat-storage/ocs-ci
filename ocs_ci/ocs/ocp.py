"""
General OCP object
"""
import os
import logging
import time
import yaml
import shlex
import re

from ocs_ci.ocs.exceptions import CommandFailed, TimeoutExpiredError
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.utility.utils import run_cmd
from ocs_ci.ocs import defaults

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

    def exec_oc_cmd(self, command, out_yaml_format=True, secrets=None, **kwargs):
        """
        Executing 'oc' command

        Args:
            command (str): The command to execute (e.g. create -f file.yaml)
                without the initial 'oc' at the beginning

            out_yaml_format (bool): whether to return  yaml loaded python
                object or raw output

            secrets (list): A list of secrets to be masked with asterisks
                This kwarg is popped in order to not interfere with
                subprocess.run(**kwargs)

        Returns:
            dict: Dictionary represents a returned yaml file
        """
        oc_cmd = "oc "
        kubeconfig = os.getenv('KUBECONFIG')
        if self.namespace:
            oc_cmd += f"-n {self.namespace} "

        if kubeconfig:
            oc_cmd += f"--kubeconfig {kubeconfig} "

        oc_cmd += command
        out = run_cmd(cmd=oc_cmd, secrets=secrets, **kwargs)

        try:
            if out.startswith('hints = '):
                out = out[out.index('{'):]
        except ValueError:
            pass

        if out_yaml_format:
            return yaml.safe_load(out)
        return out

    def get(
        self, resource_name='', out_yaml_format=True, selector=None,
        all_namespaces=False
    ):
        """
        Get command - 'oc get <resource>'

        Args:
            resource_name (str): The resource name to fetch
            out_yaml_format (bool): Adding '-o yaml' to oc command
            selector (str): The label selector to look for
            all_namespaces (bool): Equal to oc get <resource> -A

        Example:
            get('my-pv1')

        Returns:
            dict: Dictionary represents a returned yaml file
        """
        command = f"get {self.kind} {resource_name}"
        if all_namespaces and not self.namespace:
            command += " -A"
        elif self.namespace:
            command += f" -n {self.namespace}"
        if selector is not None:
            command += f" --selector={selector}"
        if out_yaml_format:
            command += " -o yaml"
        return self.exec_oc_cmd(command)

    def describe(self, resource_name='', selector=None, all_namespaces=False):
        """
        Get command - 'oc describe <resource>'

        Args:
            resource_name (str): The resource name to fetch
            selector (str): The label selector to look for
            all_namespaces (bool): Equal to oc describe <resource> -A

        Example:
            describe('my-pv1')

        Returns:
            dict: Dictionary represents a returned yaml file
        """
        command = f"describe {self.kind} {resource_name}"
        if all_namespaces and not self.namespace:
            command += " -A"
        if selector is not None:
            command += f" --selector={selector}"
        return self.exec_oc_cmd(command, out_yaml_format=False)

    def create(self, yaml_file=None, resource_name='', out_yaml_format=True):
        """
        Creates a new resource

        Args:
            yaml_file (str): Path to a yaml file to use in 'oc create -f
                file.yaml
            resource_name (str): Name of the resource you want to create
            out_yaml_format (bool): Determines if the output should be
                formatted to a yaml like string

        Returns:
            dict: Dictionary represents a returned yaml file
        """
        if not (yaml_file or resource_name):
            raise CommandFailed(
                "At least one of resource_name or yaml_file have to "
                "be provided"
            )
        command = "create "
        if yaml_file:
            command += f"-f {yaml_file}"
        elif resource_name:
            # e.g "oc namespace my-project"
            command += f"{self.kind} {resource_name}"
        if out_yaml_format:
            command += " -o yaml"
        output = self.exec_oc_cmd(command)
        log.debug(f"{yaml.dump(output)}")
        return output

    def delete(self, yaml_file=None, resource_name='', wait=True, force=False):
        """
        Deletes a resource

        Args:
            yaml_file (str): Path to a yaml file to use in 'oc delete -f
                file.yaml
            resource_name (str): Name of the resource you want to delete
            wait (bool): Determines if the delete command should wait to
                completion
            force (bool): True for force deletion with --grace-period=0,
                False otherwise

        Returns:
            dict: Dictionary represents a returned yaml file

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
        if force:
            command += " --grace-period=0 --force"
        # oc default for wait is True
        if not wait:
            command += " --wait=false"
        return self.exec_oc_cmd(command)

    def apply(self, yaml_file):
        """
        Applies configuration changes to a resource

        Args:
            yaml_file (str): Path to a yaml file to use in 'oc apply -f
                file.yaml

        Returns:
            dict: Dictionary represents a returned yaml file
        """
        command = f"apply -f {yaml_file}"
        return self.exec_oc_cmd(command)

    def add_label(self, resource_name, label):
        """
        Adds a new label for this pod

        Args:
            resource_name (str): Name of the resource you want to label
            label (str): New label to be assigned for this pod
                E.g: "label=app='rook-ceph-mds'"
        """
        command = f"label {self.kind} {resource_name} {label}"
        status = self.exec_oc_cmd(command)
        return status

    def new_project(self, project_name):
        """
        Creates a new project

        Args:
            project_name (str): Name of the project to be created

        Returns:
            bool: True in case project creation succeeded, False otherwise
        """
        command = f"oc new-project {project_name}"
        if f'Now using project "{project_name}"' in run_cmd(f"{command}"):
            return True
        return False

    def login(self, user, password):
        """
        Logs user in

        Args:
            user (str): Name of user to be logged in
            password (str): Password of user to be logged in

        Returns:
            str: output of login command
        """
        command = f"oc login -u {user} -p {password}"
        status = run_cmd(command)
        return status

    def get_user_token(self):
        """
        Get user access token

        Returns:
            str: access token
        """
        command = 'whoami --show-token'
        token = self.exec_oc_cmd(command, out_yaml_format=False).rstrip()
        return token

    def wait_for_resource(
        self, condition, resource_name='', selector=None, resource_count=0,
        timeout=60, sleep=3
    ):
        """
        Wait for a resource to reach to a desired condition

        Args:
            condition (str): The desired state the resource that is sampled
                from 'oc get <kind> <resource_name>' command
            resource_name (str): The name of the resource to wait
                for (e.g.my-pv1)
            selector (str): The resource selector to search with.
                Example: 'app=rook-ceph-mds'
            resource_count (int): How many resources expected to be
            timeout (int): Time in seconds to wait
            sleep (int): Sampling time in seconds

        Returns:
            bool: True in case all resources reached desired condition,
                False otherwise

        """
        log.info(
            ("Waiting for a resource(s) of kind %s identified by name '%s'"
            " and selector %s to reach desired condition %s"),
            self._kind,
            resource_name,
            selector,
            condition)

        try:
            for sample in TimeoutSampler(
                timeout, sleep, self.get, resource_name, True, selector
            ):

                # Only 1 resource expected to be returned
                if resource_name:
                    status = self.get_resource_status(resource_name)
                    if status == condition:
                        return True
                    log.info(
                        "status of %s was %s, but we were waiting for %s",
                        resource_name,
                        status, condition)
                    actual_status = status
                # More than 1 resources returned
                elif sample.get('kind') == 'List':
                    in_condition = []
                    actual_status = []
                    sample = sample['items']
                    for item in sample:
                        try:
                            item_name = item.get('metadata').get('name')
                            status = self.get_resource_status(item_name)
                            actual_status.append(status)
                            if status  == condition:
                                in_condition.append(item)
                        except CommandFailed as ex:
                            log.info(
                                f"Failed to get status of resource: {item_name}, "
                                f"Error: {ex}"
                            )
                        if resource_count:
                            if len(in_condition) == resource_count and (
                                len(sample) == len(in_condition)
                            ):
                                return True
                        elif len(sample) == len(in_condition):
                            return True
                    log.info(
                        ("status of %s item(s) were %s, but we were waiting"
                        " for all of them to be %s"),
                        resource_name,
                        actual_status,
                        condition)
        except TimeoutExpiredError as ex:
            log.error("timeout expired: %s", ex)
            log.error(
                ("Wait for resource %s to reach desired condition %s failed,"
                " last actual status was %s"),
                resource_name,
                condition,
                actual_status)
            raise(ex)

        return False

    def wait_for_delete(self, resource_name='', timeout=60, sleep=3):
        """
        Wait for a resource to be deleted

        Args:
            resource_name (str): The name of the resource to wait
                for (e.g.my-pv1)
            timeout (int): Time in seconds to wait
            sleep (int): Sampling time in seconds

        Raises:
            CommandFailed: If failed to verify the resource deletion
            TimeoutError: If resource is not deleted within specified timeout

        Returns:
            bool: True in case resource deletion is successful

        """
        start_time = time.time()
        while True:
            try:
                self.get(resource_name=resource_name)
            except CommandFailed as ex:
                if "NotFound" in str(ex):
                    log.info(
                        f"{self.kind} {resource_name} got deleted successfully"
                    )
                    return True
                else:
                    raise ex

            if timeout < (time.time() - start_time):
                describe_out = self.describe(resource_name=resource_name)
                msg = (
                    f"Timeout when waiting for {resource_name} to delete. "
                    f"Describe output: {describe_out}"
                )
                raise TimeoutError(msg)
            time.sleep(sleep)

    def get_resource_status(self, resource_name):
        """
        Get the resource status based on:
        'oc get <resource_kind> <resource_name>' command

        Args:
            resource_name (str): The name of the resource to get its status

        Returns:
            str: The status returned by 'oc get' command not in the 'yaml'
                format
        """
        # Get the resource in str format
        resource = self.get(resource_name=resource_name, out_yaml_format=False)
        # get the list of titles
        titles = re.sub('\s{2,}', ',', resource)  # noqa: W605
        titles = titles.split(',')
        # Get the index of 'STATUS'
        status_index = titles.index('STATUS')
        resource = shlex.split(resource)
        # Get the values from the output including access modes in capital
        # letters
        resource_info = [
            i for i in resource if (
                not i.isupper() or i in ('RWO', 'RWX', 'ROX')
            )
        ]

        return resource_info[status_index]


def switch_to_project(project_name):
    """
    Switch to another project

    Args:
        project_name (str): Name of the project to be switched to

    Returns:
        bool: True on success, False otherwise
    """
    log.info(f'Switching to project {project_name}')
    cmd = f'oc project {project_name}'
    success_msgs = [
        f'Now using project "{project_name}"',
        f'Already on project "{project_name}"'
    ]
    ret = run_cmd(cmd)
    if any(msg in ret for msg in success_msgs):
        return True
    return False


def switch_to_default_rook_cluster_project():
    """
    Switch to default project

    Returns:
        bool: True on success, False otherwise
    """
    return switch_to_project(defaults.ROOK_CLUSTER_NAMESPACE)
