"""
Jenkins Class to run jenkins specific tests
"""
import logging
from ocs_ci.ocs.exceptions import (
    CommandFailed, ResourceWrongStatusException, UnexpectedBehaviour
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility import templating
from ocs_ci.ocs.resources.ocs import OCS
from subprocess import CalledProcessError
from tests.helpers import create_pvc
from ocs_ci.utility.utils import run_cmd
from ocs_ci.ocs.resources.pod import get_pod_obj
from tests.helpers import wait_for_resource_state
from ocs_ci.ocs.utils import get_pod_name_by_pattern, get_build_name_by_pattern


log = logging.getLogger(__name__)


class Jenkins(object):
    """
    Workload operation using Jenkins
    """
    def __init__(self):
        """
        Initializer function

        """
        self.ocp = OCP()
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
            log.error('Failed during create ocs jenkins template')
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
            log.error('Failed during setup of Jenkins build config')
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
                output = run_cmd(f'oc logs {jenkins_deploy_pod.name} -n {constants.JENKINS_NAMESPACE}')
                error_msg = f'{jenkins_deploy_pod.name} did not reach to {status} state after {timeout} sec\n{output}'
                log.error(error_msg)
                raise UnexpectedBehaviour(error_msg)

    def wait_for_build_status(self, status, timeout=None):
        """
        Wait for jenkins deploy pods status to reach running/completed

        Args:
            status (str): status to reach Running or Completed
            timeout (int): Time in seconds to wait

        """
        timeout = timeout if timeout else 600
        # Wait for jenkins-deploy pods to reached and running
        log.info(f"Waiting build to be reach {status} state")
        jenkins_builds = self.get_builds_obj()
        for jenkins_build in jenkins_builds:
            try:
                wait_for_resource_state(
                    resource=jenkins_build, state=status, timeout=timeout
                )
            except ResourceWrongStatusException:
                error_msg = f'{jenkins_build.name} did not reach to {status} state after {timeout} sec\n'
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
        build_list = get_build_name_by_pattern('jax-rs', namespace=constants.JENKINS_NAMESPACE)
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
        cmd = f"oc new-app -n {constants.JENKINS_NAMESPACE} --name=jenkins-ocs-rbd --template=jenkins-persistent-ocs"
        run_cmd(cmd)

    def start_build(self):
        """
        Start build on jenkins

        """
        log.info("Start build on jenkins")
        cmd = f"oc -n {constants.JENKINS_NAMESPACE} start-build {constants.JENKINS_BUILD}"
        run_cmd(cmd)

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
        self.start_build()
        self.wait_for_build_status(status='Complete')

    def cleanup(self):
        """
        Clean up

        """
        log.info("Deleting jenkins project and template")
        run_cmd(f'oc delete project {constants.JENKINS_NAMESPACE}')
        run_cmd('oc delete template.template.openshift.io/jenkins-persistent-ocs -n openshift')
