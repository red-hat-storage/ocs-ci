"""
General OCP object
"""
import logging
import os
import re
import shlex
import tempfile
import time
import yaml
import json
import copy

from ocs_ci.ocs.exceptions import (
    CommandFailed,
    NotSupportedFunctionError,
    NonUpgradedImagesFoundError,
    ResourceInUnexpectedState,
    ResourceNameNotSpecifiedException,
    TimeoutExpiredError,
)
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.utility.utils import run_cmd, update_container_with_mirrored_image
from ocs_ci.utility.templating import dump_data_to_temp_yaml, load_yaml
from ocs_ci.ocs import defaults, constants
from ocs_ci.framework import config


log = logging.getLogger(__name__)


class OCP(object):
    """
    A basic OCP object to run basic 'oc' commands
    """

    # If the resource has the phase in its metadata, set this _has_phase
    # class member to True in the child class.
    _has_phase = False

    def __init__(
        self, api_version='v1', kind='Service', namespace=None,
        resource_name='', selector=None,
    ):
        """
        Initializer function

        Args:
            api_version (str): TBD
            kind (str): TBD
            namespace (str): The name of the namespace to use
            resource_name (str): Resource name
            selector (str): The label selector to look for. It has higher
                priority than resource_name and is used instead of the name.
        """
        self._api_version = api_version
        self._kind = kind
        self._namespace = namespace
        self._resource_name = resource_name
        self._data = {}
        self.selector = selector

    @property
    def api_version(self):
        return self._api_version

    @property
    def kind(self):
        return self._kind

    @property
    def namespace(self):
        return self._namespace

    @property
    def resource_name(self):
        return self._resource_name

    @property
    def data(self):
        if self._data:
            return self._data
        self._data = self.get()
        return self._data

    def reload_data(self):
        """
        Reloading data of OCP object
        """
        self._data = self.get()

    def exec_oc_cmd(
        self, command, out_yaml_format=True, secrets=None, timeout=600,
        ignore_error=False, **kwargs
    ):
        """
        Executing 'oc' command

        Args:
            command (str): The command to execute (e.g. create -f file.yaml)
                without the initial 'oc' at the beginning
            out_yaml_format (bool): whether to return  yaml loaded python
                object or raw output
            secrets (list): A list of secrets to be masked with asterisks
                This kwarg is popped in order to not interfere with
                subprocess.run(``**kwargs``)
            timeout (int): timeout for the oc_cmd, defaults to 600 seconds
            ignore_error (bool): True if ignore non zero return code and do not
                raise the exception.

        Returns:
            dict: Dictionary represents a returned yaml file.
            str: If out_yaml_format is False.

        """
        oc_cmd = "oc "
        kubeconfig = os.getenv('KUBECONFIG')
        if self.namespace:
            oc_cmd += f"-n {self.namespace} "

        if kubeconfig:
            oc_cmd += f"--kubeconfig {kubeconfig} "

        oc_cmd += command
        out = run_cmd(
            cmd=oc_cmd, secrets=secrets, timeout=timeout,
            ignore_error=ignore_error, **kwargs
        )

        try:
            if out.startswith('hints = '):
                out = out[out.index('{'):]
        except ValueError:
            pass

        if out_yaml_format:
            return yaml.safe_load(out)
        return out

    def exec_oc_debug_cmd(self, node, cmd_list, timeout=300):
        """
        Function to execute "oc debug" command on OCP node

        Args:
            node (str): Node name where the command to be executed
            cmd_list (list): List of commands eg: ['cmd1', 'cmd2']
            timeout (int): timeout for the exec_oc_cmd, defaults to 600 seconds

        Returns:
            out (str): Returns output of the executed command/commands

        Raises:
            CommandFailed: When failure in command execution
        """
        # Appending one empty value in list for string manipulation
        create_cmd_list = copy.deepcopy(cmd_list)
        create_cmd_list.append(' ')
        err_msg = 'CMD FAILED'
        cmd = f" || echo '{err_msg}';".join(create_cmd_list)
        debug_cmd = f"debug nodes/{node} -- chroot /host /bin/bash -c \"{cmd}\""
        out = str(self.exec_oc_cmd(
            command=debug_cmd, out_yaml_format=False, timeout=timeout
        ))
        if err_msg in out:
            raise CommandFailed
        else:
            return out

    def get(
        self, resource_name='', out_yaml_format=True, selector=None,
        all_namespaces=False, retry=0, wait=3
    ):
        """
        Get command - 'oc get <resource>'

        Args:
            resource_name (str): The resource name to fetch
            out_yaml_format (bool): Adding '-o yaml' to oc command
            selector (str): The label selector to look for.
            all_namespaces (bool): Equal to oc get <resource> -A
            retry (int): Number of attempts to retry to get resource
            wait (int): Number of seconds to wait between attempts for retry

        Example:
            get('my-pv1')

        Returns:
            dict: Dictionary represents a returned yaml file
        """
        resource_name = resource_name if resource_name else self.resource_name
        selector = selector if selector else self.selector
        if selector:
            resource_name = ""
        command = f"get {self.kind} {resource_name}"
        if all_namespaces and not self.namespace:
            command += " -A"
        elif self.namespace:
            command += f" -n {self.namespace}"
        if selector is not None:
            command += f" --selector={selector}"
        if out_yaml_format:
            command += " -o yaml"
        retry += 1
        while retry:
            try:
                return self.exec_oc_cmd(command)
            except CommandFailed as ex:
                log.warning(
                    f"Failed to get resource: {resource_name} of kind: "
                    f"{self.kind}, selector: {selector}, Error: {ex}"
                )
                retry -= 1
                if not retry:
                    log.warning("Number of attempts to get resource reached!")
                    raise
                else:
                    log.info(
                        f"Number of attempts: {retry} to get resource: "
                        f"{resource_name}, selector: {selector}, remain! "
                        f"Trying again in {wait} sec."
                    )
                    time.sleep(wait if wait else 1)

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

        command = "delete "
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

    def patch(self, resource_name='', params=None, format_type=''):
        """
        Applies changes to resources

        Args:
            resource_name (str): Name of the resource
            params (str): Changes to be added to the resource
            type (str): Type of the operation

        Returns:
            bool: True in case if changes are applied. False otherwise

        """
        resource_name = resource_name or self.resource_name
        params = "\'" + f"{params}" + "\'"
        command = f"patch {self.kind} {resource_name} -n {self.namespace} -p {params}"
        if format_type:
            command += f" --type {format_type}"
        log.info(f"Command: {command}")
        result = self.exec_oc_cmd(command)
        if 'patched' in result:
            return True
        return False

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

    def delete_project(self, project_name):
        """
        Delete a project.  A project created by the new_project function does
        not have a corresponding yaml file so normal resource deletion calls
        do not work

        Args:
            project_name (str): Name of the project to be deleted

        Returns:
            bool: True in case project deletion succeeded.

        Raises:
            CommandFailed: When the project deletion does not succeed.

        """
        command = f"oc delete project {project_name}"
        if f' "{project_name}" deleted' in run_cmd(f"{command}"):
            return True
        raise CommandFailed(
            f"{project_name} was not deleted"
        )

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

    def login_as_sa(self):
        """
        Logs in as system:admin

        Returns:
            str: output of login command
        """
        kubeconfig = os.getenv('KUBECONFIG')
        command = "oc login -u system:admin "
        if kubeconfig:
            command += f"--kubeconfig {kubeconfig}"
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
        self, condition, resource_name='', column='STATUS', selector=None,
        resource_count=0, timeout=60, sleep=3,
        dont_allow_other_resources=False, error_condition=None,
    ):
        """
        Wait for a resource to reach to a desired condition

        Args:
            condition (str): The desired state the resource that is sampled
                from 'oc get <kind> <resource_name>' command
            resource_name (str): The name of the resource to wait
                for (e.g.my-pv1)
            column (str): The name of the column to compare with
            selector (str): The resource selector to search with.
                Example: 'app=rook-ceph-mds'
            resource_count (int): How many resources expected to be
            timeout (int): Time in seconds to wait
            sleep (int): Sampling time in seconds
            dont_allow_other_resources (bool): If True it will not allow other
                resources in different state. For example you are waiting for 2
                resources and there are currently 3 (2 in running state,
                1 in ContainerCreating) the function will continue to next
                iteration to wait for only 2 resources in running state and no
                other exists.
            error_condition (str): State of the resource that is sampled
                from 'oc get <kind> <resource_name>' command, which makes this
                method to fail immediately without waiting for a timeout. This
                is optional and makes sense only when there is a well defined
                unrecoverable state of the resource(s) which is not expected to
                be part of a workflow under test, and at the same time, the
                timeout itself is large.

        Returns:
            bool: True in case all resources reached desired condition,
                False otherwise

        """
        if condition == error_condition:
            # when this fails, this method is used in a wrong way
            raise ValueError(
                f"Condition '{condition}' we are waiting for must be different"
                f" from error condition '{error_condition}'"
                " which describes unexpected error state."
            )
        log.info((
            f"Waiting for a resource(s) of kind {self._kind}"
            f" identified by name '{resource_name}'"
            f" using selector {selector}"
            f" at column name {column}"
            f" to reach desired condition {condition}"))
        resource_name = resource_name if resource_name else self.resource_name
        selector = selector if selector else self.selector

        # actual status of the resource we are waiting for, setting it to None
        # now prevents UnboundLocalError raised when waiting timeouts
        actual_status = None

        try:
            for sample in TimeoutSampler(
                timeout, sleep, self.get, resource_name, True, selector
            ):

                # Only 1 resource expected to be returned
                if resource_name:
                    retry = int(timeout / sleep if sleep else timeout / 1)
                    status = self.get_resource(
                        resource_name, column, retry=retry, wait=sleep,
                    )
                    if status == condition:
                        log.info(
                            f"status of {resource_name} at {column}"
                            " reached condition!"
                        )
                        return True
                    log.info((
                        f"status of {resource_name} at column {column} was {status},"
                        f" but we were waiting for {condition}"))
                    actual_status = status
                    if error_condition is not None and status == error_condition:
                        raise ResourceInUnexpectedState(
                            f"Status of '{resource_name}' at column {column}"
                            f" is {status}."
                        )
                # More than 1 resources returned
                elif sample.get('kind') == 'List':
                    in_condition = []
                    in_condition_len = 0
                    actual_status = []
                    sample = sample['items']
                    sample_len = len(sample)
                    for item in sample:
                        try:
                            item_name = item.get('metadata').get('name')
                            status = self.get_resource(item_name, column)
                            actual_status.append(status)
                            if status == condition:
                                in_condition.append(item)
                                in_condition_len = len(in_condition)
                            if error_condition is not None and status == error_condition:
                                raise ResourceInUnexpectedState(
                                    f"Status of '{item_name}' "
                                    f" at column {column} is {status}."
                                )
                        except CommandFailed as ex:
                            log.info(
                                f"Failed to get status of resource: {item_name} at column {column}, "
                                f"Error: {ex}"
                            )
                        if resource_count:
                            if in_condition_len == resource_count:
                                log.info(
                                    f"{in_condition_len} resources already "
                                    f"reached condition!"
                                )
                                if (
                                    dont_allow_other_resources
                                    and sample_len != in_condition_len
                                ):
                                    log.info(
                                        f"There are {sample_len} resources in "
                                        f"total. Continue to waiting as "
                                        f"you don't allow other resources!"
                                    )
                                    continue
                                return True
                        elif len(sample) == len(in_condition):
                            return True
                    # preparing logging message with expected number of
                    # resource items we are waiting for
                    if resource_count > 0:
                        exp_num_str = f"all {resource_count}"
                    else:
                        exp_num_str = "all"
                    log.info((
                        f"status of {resource_name} at column {column} - item(s) were {actual_status},"
                        f" but we were waiting"
                        f" for {exp_num_str} of them to be {condition}"))
        except TimeoutExpiredError as ex:
            log.error(f"timeout expired: {ex}")
            log.error((
                f"Wait for {self._kind} resource {resource_name} at column {column}"
                f" to reach desired condition {condition} failed,"
                f" last actual status was {actual_status}"))
            # run `oc describe` on the resources we were waiting for to provide
            # evidence so that we can understand what was wrong
            output = self.describe(resource_name, selector=selector)
            log.warning(
                "Description of the resource(s) we were waiting for:\n%s",
                output
            )
            raise(ex)
        except ResourceInUnexpectedState:
            log.error(
                (
                    "Waiting for %s resource %s at column %s"
                    " to reach desired condition %s was aborted"
                    " because at least one is in unexpected %s state."
                ),
                self._kind,
                resource_name,
                column,
                condition,
                error_condition
            )
            output = self.describe(resource_name, selector=selector)
            log.warning(
                "Description of the resource(s) we were waiting for:\n%s",
                output
            )
            raise

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

    def get_resource(
        self, resource_name, column, retry=0, wait=3, selector=None
    ):
        """
        Get a column value for a resource based on:
        'oc get <resource_kind> <resource_name>' command

        Args:
            resource_name (str): The name of the resource to get its column value
            column (str): The name of the column to retrive
            retry (int): Number of attempts to retry to get resource
            wait (int): Number of seconds to wait beteween attempts for retry
            selector (str): The resource selector to search with.

        Returns:
            str: The output returned by 'oc get' command not in the 'yaml'
                format
        """
        resource_name = resource_name if resource_name else self.resource_name
        selector = selector if selector else self.selector
        # Get the resource in str format
        resource = self.get(
            resource_name=resource_name, out_yaml_format=False, retry=retry,
            wait=wait, selector=selector
        )
        # get the list of titles
        titles = re.sub(r'\s{2,}', ',', resource)  # noqa: W605
        titles = titles.split(',')
        # Get the index of column
        column_index = titles.index(column)
        resource = shlex.split(resource)
        # Get the values from the output including access modes in capital
        # letters
        resource_info = [
            i for i in resource if (
                not i.isupper() or i in ('RWO', 'RWX', 'ROX')
            )
        ]
        # WA, Failed to parse "oc get build" command
        # https://github.com/red-hat-storage/ocs-ci/issues/2312
        try:
            if self.data['items'][0]['kind'].lower() == 'build':
                return resource_info[column_index - 1]
        except Exception:
            pass

        return resource_info[column_index]

    def get_resource_status(self, resource_name):
        """
        Get the resource STATUS column based on:
        'oc get <resource_kind> <resource_name>' command

        Args:
            resource_name (str): The name of the resource to get its STATUS

        Returns:
            str: The status returned by 'oc get' command not in the 'yaml'
                format
        """

        return self.get_resource(resource_name, 'STATUS')

    def check_name_is_specified(self, resource_name=''):
        """
        Check if the name of the resource is specified in class level and
        if not raise the exception.

        Raises:
            ResourceNameNotSpecifiedException: in case the name is not
                specified.

        """
        resource_name = (
            resource_name if resource_name else self.resource_name
        )
        if not resource_name:
            raise ResourceNameNotSpecifiedException(
                "Resource name has to be specified in class!"
            )

    def check_function_supported(self, support_var):
        """
        Check if the resource supports the functionality based on the
        support_var.

        Args:
            support_var (bool): True if functionality is supported, False
                otherwise.

        Raises:
            NotSupportedFunctionError: If support_var == False

        """
        if not support_var:
            raise NotSupportedFunctionError(
                "Resource name doesn't support this functionality!"
            )

    def check_phase(self, phase):
        """
        Check phase of resource

        Args:
            phase (str): Phase of resource object

        Returns:
            bool: True if phase of object is the same as passed one, False
                otherwise.

        Raises:
            NotSupportedFunctionError: If resource doesn't have phase!
            ResourceNameNotSpecifiedException: in case the name is not
                specified.

        """
        self.check_function_supported(self._has_phase)
        self.check_name_is_specified()
        try:
            data = self.get()
        except CommandFailed:
            log.info(f"Cannot find resource object {self.resource_name}")
            return False
        try:
            current_phase = data['status']['phase']
            log.info(
                f"Resource {self.resource_name} is in phase: {current_phase}!"
            )
            return current_phase == phase
        except KeyError:
            log.info(
                f"Problem while reading phase status of resource "
                f"{self.resource_name}, data: {data}"
            )
        return False

    @retry(ResourceInUnexpectedState, tries=4, delay=5, backoff=1)
    def wait_for_phase(self, phase, timeout=300, sleep=5):
        """
        Wait till phase of resource is the same as required one passed in
        the phase parameter.

        Args:
            phase (str): Desired phase of resource object
            timeout (int): Timeout in seconds to wait for desired phase
            sleep (int): Time in seconds to sleep between attempts

        Raises:
            ResourceInUnexpectedState: In case the resource is not in expected
                phase.
            NotSupportedFunctionError: If resource doesn't have phase!
            ResourceNameNotSpecifiedException: in case the name is not
                specified.

        """
        self.check_function_supported(self._has_phase)
        self.check_name_is_specified()
        sampler = TimeoutSampler(
            timeout, sleep, self.check_phase, phase=phase
        )
        if not sampler.wait_for_func_status(True):
            raise ResourceInUnexpectedState(
                f"Resource: {self.resource_name} is not in expected phase: "
                f"{phase}"
            )

    def is_exist(self, resource_name="", selector=None):
        """
        Check if at least one of the resource exists.

        Args:
            resource_name (str): Name of the resource.
            selector (str): Selector of the resource.

        Raises:
            ResourceNameNotSpecifiedException: In case the name is not
                specified.

        Returns:
            bool: True if the resource exists False otherwise.

        """
        resource_name = resource_name or self.resource_name
        selector = selector or self.selector
        log.info(f"Check if resource: {resource_name} exists.")
        self.check_name_is_specified(resource_name)
        try:
            self.get(resource_name, selector=selector)
            log.info(f"Resource: {resource_name}, selector: {selector} found.")
            return True
        except CommandFailed:
            log.info(
                f"Resource: {resource_name}, selector: {selector} not found."
            )
            return False

    def get_logs(
        self,
        name,
        container_name=None,
        all_containers=False,
        secrets=None,
        timeout=None,
        ignore_error=False,
    ):
        """
        Execute ``oc logs`` command to fetch logs for a given k8s resource.

        Since the log is stored as a string in memory, this will be
        problematic when the log is large.

        Args:
            name (str): name of the resource to fetch logs from
            container_name (str): name of the container (optional)
            all_containers (bool): fetch logs from all containers of the
                resource
            secrets (list): A list of secrets to be masked with asterisks
            timeout (int): timeout for the oc_cmd
            ignore_error (bool): True if ignore non zero return code and do not
                raise the exception.

        Returns:
            str: container logs

        """
        log.info("fetching logs from %s/%s", self.kind, name)
        oc_cmd = f"logs {self.kind}/{name}"
        if container_name is not None:
            oc_cmd += f" --container='{container_name}'"
        if all_containers:
            oc_cmd += " --all-containers=true"
        output = self.exec_oc_cmd(
            oc_cmd,
            out_yaml_format=False,
            secrets=secrets,
            timeout=timeout,
            ignore_error=ignore_error
        )
        return output


def get_clustername():
    """
    Return the name (DNS short name) of the cluster

    Returns:
         str: the short DNS name of the cluster

    """

    ocp_cluster = OCP(
        namespace='openshift-console', kind='',
        resource_name='route')
    return ocp_cluster.get()['items'][0]['spec']['host'].split('.')[2]


def get_ocs_version():
    """
    Return the OCS Version

    Returns:
         str: The version of the OCS

    """

    ocp_cluster = OCP(
        namespace=config.ENV_DATA['cluster_namespace'],
        kind='', resource_name='csv')
    return ocp_cluster.get()['items'][0]['spec']['version']


def get_build():
    """
    Return the OCP Build Version

    Returns:
         str: The build version of the OCP

    """

    ocp_cluster = OCP(
        namespace=config.ENV_DATA['cluster_namespace'],
        kind='', resource_name='clusterversion')
    return ocp_cluster.get()['items'][0]['status']['desired']['version']


def get_ocp_channel():
    """
    Return the OCP Channel

    Returns:
         str: The channel of the OCP

    """

    ocp_cluster = OCP(
        namespace=config.ENV_DATA['cluster_namespace'],
        kind='', resource_name='clusterversion')
    return ocp_cluster.get()['items'][0]['spec']['channel']


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


def rsync(src, dst, node, dst_node=True, extra_params=""):
    """
    This function will rsync source folder to destination path.
    You can rsync local folder to the node or vice versa depends on
    dst_node parameter. By default the rsync is from local to the node.

    Args:
        src (str): Source path of folder to rsync.
        dst (str): Destination path where to rsync.
        node (str): Node to/from copy.
        dst_node (bool): True if the destination (dst) is the node, False
            when dst is the local folder.
        extra_params (str): "See: oc rsync --help for the extra params"

    """
    pod_name = f"rsync-{node.replace('.', '-')}"
    pod_data = load_yaml(constants.RSYNC_POD_YAML)
    pod_data['metadata']['name'] = pod_name
    pod_data['spec']['nodeName'] = node
    update_container_with_mirrored_image(pod_data)
    pod = OCP(kind='pod', namespace=constants.DEFAULT_NAMESPACE)
    src = src if dst_node else f"{pod_name}:/host{src}"
    dst = f"{pod_name}:/host{dst}" if dst_node else dst
    try:
        with tempfile.NamedTemporaryFile() as rsync_pod_yaml:
            dump_data_to_temp_yaml(pod_data, rsync_pod_yaml.name)
            pod.create(yaml_file=rsync_pod_yaml.name)
        pod.wait_for_resource(condition=constants.STATUS_RUNNING, timeout=120)
        rsync_cmd = f"rsync {extra_params} {src} {dst}"
        out = pod.exec_oc_cmd(rsync_cmd)
        log.info(f"Rsync out: {out}")
    finally:
        try:
            pod.delete(resource_name=pod_name)
        except CommandFailed:
            log.warning(f"Pod {pod_name} wasn't successfully deleted!")
            raise


def get_images(data, images=None):
    """
    Get the images from the ocp object like pod, CSV and so on.

    Args:
        data (dict): Yaml data from the object.
        images (dict): Dict where to put the images (doesn't have to be set!).

    Returns:
        dict: Images dict like: {'image_name': 'image.url.to:tag', ...}
    """
    if images is None:
        images = dict()
    data_type = type(data)
    if data_type == dict:
        # Check if we have those keys: 'name' and 'value' in the data dict.
        # If yes and the value ends with '_IMAGE' we found the image.
        if set(("name", "value")) <= data.keys() and (
            type(data["name"]) == str and data["name"].endswith("_IMAGE")
        ):
            image_name = data["name"].rstrip('_IMAGE').lower()
            image = data['value']
            images[image_name] = image
        else:
            for key, value in data.items():
                value_type = type(value)
                if value_type in (dict, list):
                    get_images(value, images)
                elif value_type == str and key == "image":
                    image_name = data.get('name')
                    if image_name:
                        images[image_name] = value
    elif data_type == list:
        for item in data:
            get_images(item, images)
    return images


def verify_images_upgraded(old_images, object_data):
    """
    Verify that all images in ocp object are upgraded.

    Args:
       old_images (set): Set with old images.
       object_data (dict): OCP object yaml data.

    Raises:
        NonUpgradedImagesFoundError: In case the images weren't upgraded.

    """
    current_images = get_images(object_data)
    not_upgraded_images = set(
        [image for image in current_images.values() if image in old_images]
    )
    name = object_data['metadata']['name']
    if not_upgraded_images:
        raise NonUpgradedImagesFoundError(
            f"Images: {not_upgraded_images} weren't upgraded in: {name}!"
        )
    log.info(
        f"All the images: {current_images} were successfully upgraded in: "
        f"{name}!"
    )


def confirm_cluster_operator_version(target_version, cluster_operator):
    """
    Check if cluster operator upgrade process is completed:

    Args:
        cluster_operator: (str): ClusterOperator name
        target_version (str): expected OCP client

    Returns:
        bool: True if success, False if failed

    """
    log.info(f"target_version: {target_version}")
    cur_version = get_cluster_operator_version(cluster_operator)
    log.info(f"current {cluster_operator} operator version is: {cur_version}")
    if cur_version == target_version or target_version.startswith(cur_version):
        log.info(f"{cluster_operator} cluster operator upgrade to build"
                 f" {target_version} completed")
        return True

    log.debug(f"{cluster_operator} upgrade not yet completed")
    return False


def upgrade_ocp(image_path, image):
    """
    upgrade OCP version

    Args:
        image (str): image to be installed
        image_path (str): path to image

    """
    ocp = OCP()
    ocp.exec_oc_cmd(
        f"adm upgrade --to-image={image_path}:{image} "
        f"--allow-explicit-upgrade --force "
    )
    log.info(f"Upgrading OCP to version: {image} ")


def get_current_oc_version():
    """
    Gets Current OCP client version

    Returns:
        str: current COP client version

    """
    ocp = OCP()
    oc_json = ocp.exec_oc_cmd('version -o json', out_yaml_format=False)
    log.debug(f"oc_json=: {oc_json}")
    oc_dict = json.loads(oc_json)
    log.debug(f"oc_dict=: {oc_dict}")

    return oc_dict.get("openshiftVersion")


def get_cluster_operator_version(cluster_operator_name):
    """
    Get image version of selected cluster operator

    Args:
        cluster_operator_name (str): ClusterOperator name

    Returns:
        str: cluster operator version: ClusterOperator image version

    """
    ocp = OCP(kind='ClusterOperator')
    operator_info = ocp.get(cluster_operator_name)
    log.debug(f"operator info: {operator_info}")
    operator_status = operator_info.get('status')
    version = operator_status.get('versions')[0]['version']
    version = version.rstrip('_openshift')

    return version


def get_all_cluster_operators():
    """
    Get all ClusterOperators names in OCP

    Returns:
        list: cluster-operator names

    """
    ocp = OCP(kind='ClusterOperator')
    operator_info = ocp.get("-o name", out_yaml_format=False, all_namespaces=True)
    operators_full_names = str(operator_info).split()
    operator_names = list()
    for name in operators_full_names:
        log.debug(f"original operator name: {name}")
        new_name = name.lstrip('clusteroperator.config.openshift.io').lstrip('/')
        log.info(f"fixed operator name: {new_name}")
        operator_names.append(new_name)

    log.info(f"ClusterOperators full list: {operator_names}")

    return operator_names


def verify_cluster_operator_status(cluster_operator):
    """
    Checks if cluster operator status is degraded or progressing,
    as sign that upgrade not yet completed

    Args:
        cluster_operator (str): OCP cluster operator name

    Returns:
        bool: True if cluster operator status is valid, False if cluster operator status
        is "degraded" or "progressing"

    """
    ocp = OCP(kind='clusteroperators')
    operator_data = ocp.get(
        resource_name=f'{cluster_operator} -o json', out_yaml_format=False
    )
    conditions = operator_data['status']['conditions']
    for condition in conditions:
        if condition['type'] == 'Degraded' and condition['status'] == 'True':
            log.info(f'{cluster_operator} status is Degraded')
            return False
        elif condition['type'] == 'Progressing' and condition['status'] == 'True':
            log.info(f'{cluster_operator} status is Progressing')
            return False
    log.info(f'{cluster_operator} status is valid')

    return True


def validate_cluster_version_status():
    """
    Verify OCP upgrade is completed, by checking 'oc get clusterversion'
    status

    Returns:
        bool: False in case that one of condition flags is invalid:
            Progressing (should be False), Failing(should be False)
            or Available (should be True)

    """
    ocp = OCP(kind="clusterversion")
    operator_data = ocp.get('-o json', out_yaml_format=False)
    conditions = operator_data['items'][0].get('status').get('conditions', [])
    for condition in conditions:
        if condition['type'] == 'Progressing' and condition['status'] == 'True':
            log.info('cluster version status is Progressing')
            return False
        elif condition['type'] == 'Failing' and condition['status'] == 'True':
            log.info('cluster version status is Failing')
            return False
        elif condition['type'] == 'Available' and condition['status'] != 'True':
            log.info('cluster status is not available')
            return False

    log.info('Cluster version validation - OK!')
    return True


def get_ocp_upgrade_channel():
    """
    Gets OCP upgrade channel

    Returns:
        str: OCP upgrade channel name

    """
    ocp = OCP(kind="clusterversion")
    log.info("Gathering Subscription Channel information")
    operator_version = ocp.get('-o json', out_yaml_format=False)
    log.debug(f"cluster version: {operator_version}")
    channel = operator_version['items'][0].get('spec').get('channel')
    log.info(f"Subscription Channel: {channel}")

    return channel


def patch_ocp_upgrade_channel(
    channel_variable=config.UPGRADE['ocp_channel']
):
    """
    Using 'oc patch clusterversion' if new OCP upgrade channel is
    different than current one

    Args:
        channel_variable (str): New OCP upgrade subscription channel

    """
    if get_ocp_upgrade_channel() != channel_variable:
        cmd = (
            f'patch clusterversions/version -p \'{{"spec":'
            f'{{"channel":"{channel_variable}"}}}}\' --type=merge'
        )
        ocp = OCP()
        log.info(f"Patching channel into {channel_variable}")
        ocp.exec_oc_cmd(cmd)

    else:
        log.info("No patch needed")


def verify_ocp_upgrade_channel(
    channel_variable=config.UPGRADE['ocp_channel']
):
    """
    When upgrade OCP version, verify that subscription channel is same
    as current one

    Args:
        channel_variable (str): New OCP upgrade subscription channel

    Returns:
        bool: True when OCP subscription channel is correct,
            and no patch needed

    """
    current_channel = get_ocp_upgrade_channel()
    if current_channel == channel_variable:
        log.info(f"Channel is {channel_variable}, no patch required")

        return True
    else:
        log.info(f"Current subscription channel is  {current_channel}")
        log.info(f"Required subscription channel is {channel_variable}")

        return False


def wait_for_cluster_connectivity(tries=200, delay=3):
    """
    Wait for the cluster to be reachable

    Args:
        tries (int): The number of retries
        delay (int): The delay in seconds between retries

    Returns:
        bool: True if cluster is reachable, False otherwise

    Raises:
        CommandFailed: In case the cluster is unreachable

    """
    service = OCP()
    log.info("Waiting for cluster connectivity")
    return retry(
        CommandFailed, tries=tries, delay=delay, backoff=1
    )(service.get)()
