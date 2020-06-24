"""
Jenkins Class to run jenkins specific tests
"""
import logging
import re
import collections

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

    Args:
        projects (iterable): project names
        num_of_builds (int): number of builds per project
    """
    def __init__(self, projects=('myjenkins-1'), num_of_builds=1):
        """
        Initializer function
        """
        if not isinstance(projects, collections.Iterable):
            raise ValueError('pojects arg must be an iterable')
        if not isinstance(num_of_builds, int):
            raise ValueError('num_of_builds arg must be an integer')

        self.num_of_builds = num_of_builds
        self.projects = projects
        self.build_completed = []

    @property
    def project_names(self):
        return self.projects

    @project_names.setter
    def project_names(self, projects):
        if not isinstance(projects, collections.Iterable):
            raise ValueError('pojects arg must be an iterable')
        self.projects = projects

    @property
    def number_builds_per_project(self):
        return self.num_of_builds

    @number_builds_per_project.setter
    def number_builds_per_project(self, num_of_builds):
        if not isinstance(num_of_builds, int):
            raise ValueError('num_of_builds arg must be an integer')
        self.num_of_builds = num_of_builds

    def create_ocs_jenkins_template(self):
        """
        Create jenkins template

        """
        try:
            log.info('***********create jenkins template****************')
            ocs_jenkins_template = templating.load_yaml(
                constants.JENKINS_TEMPLATE
            )
            ocs_jenkins_template_obj = OCS(**ocs_jenkins_template)
            ocs_jenkins_template_obj.create()
        except(CommandFailed, CalledProcessError) as cf:
            log.error('Failed to create jenkins template')
            raise cf

    def create_jenkins_build_config(self):
        """
        create jenkins build config

        """
        for project in self.projects:
            try:
                log.info(f'create build config on {project}')
                jenkins_build_config = templating.load_yaml(
                    constants.JENKINS_BUILDCONFIG_YAML
                )
                jenkins_build_config['metadata']['namespace'] = project
                jenkins_build_config_obj = OCS(**jenkins_build_config)
                jenkins_build_config_obj.create()
            except(CommandFailed, CalledProcessError) as cf:
                log.error('Failed to create Jenkins build config')
                raise cf

    def wait_for_jenkins_deploy_status(self, status, timeout=600):
        """
        Wait for jenkins deploy pods status to reach running/completed

        Args:
            status (str): status to reach Running or Completed
            timeout (int): Time in seconds to wait

        """
        log.info(f"Waiting for jenkins-deploy pods to be reach {status} state")
        for project in self.projects:
            jenkins_deploy_pods = self.get_jenkins_deploy_pods(namespace=project)
            for jenkins_deploy_pod in jenkins_deploy_pods:
                try:
                    wait_for_resource_state(
                        resource=jenkins_deploy_pod, state=status, timeout=timeout
                    )
                except ResourceWrongStatusException:
                    cmd = f'logs {jenkins_deploy_pod.name}'
                    ocp_obj = OCP(namespace=project)
                    output = ocp_obj.exec_oc_cmd(command=cmd, out_yaml_format=False)
                    error_msg = (
                        f'{jenkins_deploy_pod.name} did not reach to '
                        f'{status} state after {timeout} sec\n{output}'
                    )
                    log.error(error_msg)
                    raise UnexpectedBehaviour(error_msg)

    def wait_for_build_status(self, status, timeout=900):
        """
        Wait for build status to reach running/completed

        Args:
            status (str): status to reach Running or Completed
            timeout (int): Time in seconds to wait

        """
        log.info(f"Waiting for the build to reach {status} state")
        for project in self.projects:
            jenkins_builds = self.get_builds_obj(namespace=project)
            for jenkins_build in jenkins_builds:
                if (jenkins_build.name, project) not in self.build_completed:
                    try:
                        wait_for_resource_state(
                            resource=jenkins_build, state=status, timeout=timeout
                        )
                        self.build_completed.append((jenkins_build.name, project))
                    except ResourceWrongStatusException:
                        ocp_obj = OCP(namespace=project, kind='build')
                        output = ocp_obj.describe(resource_name=jenkins_build.name)
                        error_msg = (
                            f'{jenkins_build.name} did not reach to '
                            f'{status} state after {timeout} sec\n'
                            f'oc describe output of {jenkins_build.name} \n:{output}'
                        )
                        log.error(error_msg)
                        raise UnexpectedBehaviour(error_msg)

    def get_jenkins_deploy_pods(self, namespace):
        """
        Get all jenkins deploy pods

        Args:
            namespace (str): get pods in namespace

        Returns:
            pod_objs (list): jenkins deploy pod objects list

        """
        pod_objs = []
        pod_names = get_pod_name_by_pattern('deploy', namespace=namespace)
        for pod_name in pod_names:
            pod_objs.append(get_pod_obj(pod_name, namespace=namespace))
        return pod_objs

    def get_builds_obj(self, namespace):
        """
        Get all jenkins builds

        Returns:
            List: jenkins deploy pod objects list

        """
        build_obj_list = []
        build_list = self.get_build_name_by_pattern(
            pattern=constants.JENKINS_BUILD, namespace=namespace
        )
        for build_name in build_list:
            ocp_obj = OCP(
                api_version='v1', kind='Build', namespace=namespace
            )
            ocp_dict = ocp_obj.get(resource_name=build_name)
            build_obj_list.append(OCS(**ocp_dict))
        return build_obj_list

    def create_jenkins_pvc(self):
        """
        create jenkins pvc

        """
        pvc_objs = []
        for project in self.projects:
            pvc_obj = create_pvc(
                pvc_name='dependencies', size='10Gi',
                sc_name=constants.DEFAULT_STORAGECLASS_RBD,
                namespace=project
            )
            pvc_objs.append(pvc_obj)
        return pvc_obj

    def create_app_jenkins(self):
        """
        create application jenkins

        """
        for project in self.projects:
            ocp_obj = OCP(namespace=project)
            ocp_obj.new_project(project)
            cmd = 'new-app --name=jenkins-ocs-rbd --template=jenkins-persistent-ocs'
            ocp_obj.exec_oc_cmd(command=cmd, out_yaml_format=False)

    def start_build(self):
        """
        Start build on jenkins

        """
        for project in self.projects:
            for build_num in range(1, self.num_of_builds + 1):
                log.info(f"Start Jenkins build on {project} project, build number:{build_num} ")
                cmd = f"start-build {constants.JENKINS_BUILD}"
                build = OCP(namespace=project)
                build.exec_oc_cmd(command=cmd, out_yaml_format=False)

    def get_build_name_by_pattern(
        self, pattern='client', namespace=None, filter=None
    ):
        """
        Get build name by pattern

        Returns:
            list: build name

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
                log.info(f'build name match found appending {name}')
                build_list.append(name)
        return build_list

    def cleanup(self):
        """
        Clean up

        """
        for project in self.projects:
            log.info(f"Delete Jenkins project: {project}")
            ocp_obj = OCP(namespace=project)
            ocp_obj.delete_project(project)

        log.info("Delete Jenkins Template")
        ocp_obj = OCP()
        cmd = "delete template.template.openshift.io/jenkins-persistent-ocs -n openshift"
        ocp_obj.exec_oc_cmd(command=cmd, out_yaml_format=False)
