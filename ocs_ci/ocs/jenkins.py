"""
Jenkins Class to run jenkins specific tests
"""
import logging
import re

from ocs_ci.ocs.exceptions import (
    CommandFailed, ResourceWrongStatusException, UnexpectedBehaviour
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility import templating
from ocs_ci.ocs.resources.ocs import OCS
from subprocess import CalledProcessError
from tests.helpers import create_pvc
from ocs_ci.ocs.resources.pod import get_pod_obj
from tests.helpers import wait_for_resource_state
from ocs_ci.ocs.utils import get_pod_name_by_pattern


log = logging.getLogger(__name__)


class Jenkins(object):
    """
    Workload operation using Jenkins
    """
    def __init__(self):
        """
        Initializer function

        """
        self.ocp = OCP(namespace=constants.JENKINS_NAMESPACE)
        self.build_completed = []

    def create_ocs_jenkins_template(self):
        """
        Create jenkins template

        """
        try:
            ocs_jenkins_template = templating.load_yaml(constants.JENKINS_TEMPLATE)
            self.ocs_jenkins_template = OCS(**ocs_jenkins_template)
            self.ocs_jenkins_template.create()
        except(CommandFailed, CalledProcessError) as cf:
            log.error('Failed to create jenkins template')
            raise cf

    def create_namespace(self):
        """
        create namespace for Jenkins
        """
        self.ocp.new_project(constants.JENKINS_NAMESPACE)

    def setup_jenkins_build_config(self):
        """
        Setup jenkins build config

        """
        try:
            jenkins_build_config = templating.load_yaml(constants.JENKINS_BUILDCONFIG_YAML)
            self.jenkins_build_config = OCS(**jenkins_build_config)
            self.jenkins_build_config.create()
        except(CommandFailed, CalledProcessError) as cf:
            log.error('Failed to create Jenkins build config')
            raise cf

    def wait_for_jenkins_deploy_status(self, status, timeout=None):
        """
        Wait for jenkins deploy pods status to reach running/completed

        Args:
            status (str): status to reach Running or Completed
            timeout (int): Time in seconds to wait

        """
        timeout = timeout if timeout else 600
        # Wait for jenkins-deploy pods to reached and running
        log.info(f"Waiting for jenkins-deploy pods to be reach {status} state")
        jenkins_deploy_pods = self.get_jenkins_deploy_pods()
        for jenkins_deploy_pod in jenkins_deploy_pods:
            try:
                wait_for_resource_state(
                    resource=jenkins_deploy_pod, state=status, timeout=timeout
                )
            except ResourceWrongStatusException:
                cmd = f'logs {jenkins_deploy_pod.name}'
                output = self.ocp.exec_oc_cmd(command=cmd, out_yaml_format=False)
                error_msg = f'{jenkins_deploy_pod.name} did not reach to {status} state after {timeout} sec\n{output}'
                log.error(error_msg)
                raise UnexpectedBehaviour(error_msg)

    def wait_for_build_status(self, status, timeout=None):
        """
        Wait for build status to reach running/completed

        Args:
            status (str): status to reach Running or Completed
            timeout (int): Time in seconds to wait

        """
        timeout = timeout if timeout else 600
        # Wait for jenkins-deploy pods to reached and running
        log.info(f"Waiting build to be reach {status} state")
        jenkins_builds = self.get_builds_obj()
        for jenkins_build in jenkins_builds:
            if jenkins_build.name not in self.build_completed:
                try:
                    wait_for_resource_state(
                        resource=jenkins_build, state=status, timeout=timeout
                    )
                    self.build_completed.append(jenkins_build.name)
                except ResourceWrongStatusException:
                    error_msg = f'{jenkins_build.name} did not reach to ' \
                                f'{status} state after {timeout} sec\n'
                    log.error(error_msg)
                    raise UnexpectedBehaviour(error_msg)

    def get_jenkins_deploy_pods(self):
        """
        Get all jenkins deploy pods

        Returns:
            List: jenkins deploy pod objects list

        """
        return [
            get_pod_obj(
                pod
            ) for pod in get_pod_name_by_pattern('deploy', namespace=constants.JENKINS_NAMESPACE)
        ]

    def get_builds_obj(self):
        """
        Get all jenkins builds

        Returns:
            List: jenkins deploy pod objects list

        """
        build_obj_list = []
        build_list = self.get_build_name_by_pattern(
            pattern=constants.JENKINS_BUILD, namespace=constants.JENKINS_NAMESPACE
        )
        for build_name in build_list:
            ocp_obj = OCP(api_version='v1', kind='Build', namespace=constants.JENKINS_NAMESPACE)
            ocp_dict = ocp_obj.get(resource_name=build_name)
            build_obj_list.append(OCS(**ocp_dict))
        return build_obj_list

    def create_jenkins_pvc(self):
        """
        create jenkins pvc

        """
        pvc_obj = create_pvc(
            pvc_name='dependencies', size='10Gi', sc_name='ocs-storagecluster-ceph-rbd',
            namespace=constants.JENKINS_NAMESPACE
        )
        return pvc_obj

    def create_app_jenkins(self):
        """
        create application jenkins

        """
        cmd = f'new-app --name=jenkins-ocs-rbd --template=jenkins-persistent-ocs'
        self.ocp.exec_oc_cmd(command=cmd, out_yaml_format=False)

    def start_build(self):
        """
        Start build on jenkins

        """
        log.info("Start build on jenkins")
        cmd = f"start-build {constants.JENKINS_BUILD}"
        self.ocp.exec_oc_cmd(command=cmd, out_yaml_format=False)

    def setup_jenkins(self):
        """
        Setup jenkins

        """
        self.create_ocs_jenkins_template()
        self.create_namespace()
        self.create_jenkins_pvc()
        self.create_app_jenkins()
        self.setup_jenkins_build_config()
        self.wait_for_jenkins_deploy_status(status=constants.STATUS_COMPLETED)

    def get_build_name_by_pattern(
        self,
        pattern='client',
        namespace=None,
        filter=None,
    ):
        """
        Get build name by pattern
        """
        ocp_obj = OCP(kind='Build', namespace=namespace)
        build_names = ocp_obj.exec_oc_cmd('get build -o name', out_yaml_format=False)
        build_names = build_names.split('\n')
        build_list = []
        for name in build_names:
            if filter is not None and re.search(filter, name):
                log.info(f'build name filtered {name}')
            elif re.search(pattern, name):
                (_, name) = name.split('/')
                log.info(f'pod name match found appending {name}')
                build_list.append(name)
        return build_list

    def cleanup(self):
        """
        Clean up

        """
        self.ocp.delete_project(constants.JENKINS_NAMESPACE)
        ocp_obj = OCP()
        cmd = "delete template.template.openshift.io/jenkins-persistent-ocs -n openshift"
        ocp_obj.exec_oc_cmd(command=cmd, out_yaml_format=False)
