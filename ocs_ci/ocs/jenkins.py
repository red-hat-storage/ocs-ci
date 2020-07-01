"""
Jenkins Class to run jenkins specific tests
"""
import logging
import re
import time

from prettytable import PrettyTable
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
    def __init__(self, num_of_projects=1, num_of_builds=1):
        """
        Initializer function
        """
        if not isinstance(num_of_projects, int):
            raise ValueError('num_of_projects arg must be an integer')
        if not isinstance(num_of_builds, int):
            raise ValueError('num_of_builds arg must be an integer')

        self.num_of_builds = num_of_builds
        self.num_of_projects = num_of_projects
        self.build_completed = []
        self.create_project_names()
        self.default_sc = None

    @property
    def number_projects(self):
        return self.num_of_projects

    @number_projects.setter
    def number_projects(self, num_of_projects):
        if not isinstance(num_of_projects, int):
            raise ValueError('pojects arg must be an integer')
        self.num_of_projects = num_of_projects
        self.create_project_names()

    @property
    def number_builds_per_project(self):
        return self.num_of_builds

    @number_builds_per_project.setter
    def number_builds_per_project(self, num_of_builds):
        if not isinstance(num_of_builds, int):
            raise ValueError('num_of_builds arg must be an integer')
        self.num_of_builds = num_of_builds

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
                    output_log = ocp_obj.exec_oc_cmd(command=cmd, out_yaml_format=False)
                    cmd = f'describe {jenkins_deploy_pod.name}'
                    output_describe = ocp_obj.exec_oc_cmd(command=cmd, out_yaml_format=False)
                    error_msg = (
                        f'{jenkins_deploy_pod.name} did not reach to '
                        f'{status} state after {timeout} sec'
                        f'\n output log {jenkins_deploy_pod.name}:\n{output_log}'
                        f'\n output describe {jenkins_deploy_pod.name}:\n{output_describe}'
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
                        self.get_builds_logs()
                        raise UnexpectedBehaviour(error_msg)

    def get_jenkins_deploy_pods(self, namespace):
        """
        Get all jenkins deploy pods

        Args:
            namespace (str): get pods in namespace

        Returns:
            pod_objs (list): jenkins deploy pod objects list

        """
        return [
            get_pod_obj(
                pod, namespace=namespace
            ) for pod in get_pod_name_by_pattern('deploy', namespace=namespace)
        ]

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

        Returns:
            List: pvc_objs
        """
        pvc_objs = []
        for project in self.projects:
            log.info(f'create jenkins pvc on project {project}')
            pvc_obj = create_pvc(
                pvc_name='dependencies', size='10Gi',
                sc_name=constants.DEFAULT_STORAGECLASS_RBD,
                namespace=project
            )
            pvc_objs.append(pvc_obj)
        return pvc_objs

    def create_app_jenkins(self):
        """
        create application jenkins

        """
        for project in self.projects:
            log.info(f'create app jenkins on project {project}')
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

    def create_project_names(self):
        """
        Create project names

        """
        self.projects = []
        for project_id in range(1, self.num_of_projects + 1):
            self.projects.append('myjenkins-' + str(project_id))

    def create_ocs_jenkins_template(self):
        """

        Create OCS Jenkins Template
        """
        log.info("Create Jenkins Template, jenkins-persistent-ocs")
        ocp_obj = OCP(namespace='openshift', kind='template')
        tmp_dict = ocp_obj.get(
            resource_name='jenkins-persistent', out_yaml_format=True
        )
        tmp_dict['labels']['app'] = 'jenkins-persistent-ocs'
        tmp_dict['labels']['template'] = 'jenkins-persistent-ocs-template'
        tmp_dict['metadata']['name'] = 'jenkins-persistent-ocs'
        tmp_dict['objects'][1]['metadata']['annotations'] = {
            'volume.beta.kubernetes.io/storage-class': 'ocs-storagecluster-ceph-rbd'
        }
        tmp_dict['objects'][2]['spec']['template']['spec']['containers'][0]['env'].append(
            {'name': 'JAVA_OPTS', 'value': '${JAVA_OPTS}'})
        tmp_dict['parameters'][4]['value'] = '10Gi'
        tmp_dict['parameters'].append({
            'description': "Override jenkins options to speed up slave spawning",
            'displayName': 'Override jenkins options to speed up slave spawning',
            'name': 'JAVA_OPTS',
            'value': "-Dhudson.slaves.NodeProvisioner.initialDelay=0 "
                     "-Dhudson.slaves.NodeProvisioner.MARGIN=50 -Dhudson."
                     "slaves.NodeProvisioner.MARGIN0=0.85"
        })
        ocs_jenkins_template_obj = OCS(**tmp_dict)
        ocs_jenkins_template_obj.create()

    def get_builds_logs(self):
        """

        Get builds logs and print them on table
        """
        log.info('Print builds results')
        build_table = PrettyTable()
        build_table.field_names = ["Project", "Build", "Duration"]
        for build in self.build_completed:
            ocp_obj = OCP(namespace=build[1], kind='build')
            output_build = ocp_obj.describe(resource_name=build[0])
            list_build = output_build.split()
            index = list_build.index('Duration:')
            build_table.add_row([build[1], build[0], list_build[index + 1]])
        log.info(f'\n{build_table}\n')

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
        # Wait for the resources to delete
        # https://github.com/red-hat-storage/ocs-ci/issues/2417
        time.sleep(120)
