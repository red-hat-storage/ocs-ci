"""
Jenkins Class to run jenkins specific tests
"""
import logging
import re
import time

from collections import OrderedDict
from prettytable import PrettyTable

from ocs_ci.ocs.exceptions import (
    CommandFailed,
    ResourceWrongStatusException,
    UnexpectedBehaviour,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.constants import JENKINS_BUILD_COMPLETE
from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility import templating
from ocs_ci.ocs.resources.ocs import OCS
from subprocess import CalledProcessError
from ocs_ci.ocs.resources.pod import get_pod_obj
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.utility import utils
from ocs_ci.utility.spreadsheet.spreadsheet_api import GoogleSpreadSheetAPI
from ocs_ci.ocs.node import get_nodes, get_app_pod_running_nodes, get_worker_nodes
from ocs_ci.framework import config
from ocs_ci.helpers.helpers import (
    wait_for_resource_state,
    create_pvc,
    storagecluster_independent_check,
)

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
            raise ValueError("num_of_projects arg must be an integer")
        if not isinstance(num_of_builds, int):
            raise ValueError("num_of_builds arg must be an integer")
        self.ocp_version = utils.get_ocp_version()
        self.num_of_builds = num_of_builds
        self.num_of_projects = num_of_projects
        self.build_completed = OrderedDict()
        self.create_project_names()

    @property
    def number_projects(self):
        return self.num_of_projects

    @number_projects.setter
    def number_projects(self, num_of_projects):
        if not isinstance(num_of_projects, int):
            raise ValueError("pojects arg must be an integer")
        self.num_of_projects = num_of_projects
        self.create_project_names()

    @property
    def number_builds_per_project(self):
        return self.num_of_builds

    @number_builds_per_project.setter
    def number_builds_per_project(self, num_of_builds):
        if not isinstance(num_of_builds, int):
            raise ValueError("num_of_builds arg must be an integer")
        self.num_of_builds = num_of_builds

    def create_jenkins_build_config(self):
        """
        create jenkins build config

        """
        for project in self.projects:
            try:
                log.info(f"create build config on {project}")
                jenkins_build_config = templating.load_yaml(
                    constants.JENKINS_BUILDCONFIG_YAML
                )
                jenkins_build_config["metadata"]["namespace"] = project
                jenkins_build_config["spec"]["strategy"]["jenkinsPipelineStrategy"][
                    "jenkinsfile"
                ] = jenkins_build_config["spec"]["strategy"]["jenkinsPipelineStrategy"][
                    "jenkinsfile"
                ].replace(
                    "latest", self.ocp_version
                )
                jenkins_build_config_obj = OCS(**jenkins_build_config)
                jenkins_build_config_obj.create()
            except (CommandFailed, CalledProcessError) as cf:
                log.error("Failed to create Jenkins build config")
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
                    cmd = f"logs {jenkins_deploy_pod.name}"
                    ocp_obj = OCP(namespace=project)
                    output_log = ocp_obj.exec_oc_cmd(command=cmd, out_yaml_format=False)
                    cmd = f"describe pod {jenkins_deploy_pod.name}"
                    output_describe = ocp_obj.exec_oc_cmd(
                        command=cmd, out_yaml_format=False
                    )
                    error_msg = (
                        f"{jenkins_deploy_pod.name} did not reach to "
                        f"{status} state after {timeout} sec"
                        f"\n output log {jenkins_deploy_pod.name}:\n{output_log}"
                        f"\n output  describe {jenkins_deploy_pod.name}:\n{output_describe}"
                    )
                    log.error(error_msg)
                    raise UnexpectedBehaviour(error_msg)

    def wait_for_build_to_complete(self, timeout=1200):
        """
        Wait for build status to reach complete state

        Args:
            timeout (int): Time  in seconds to wait

        """
        log.info(f"Waiting for the build to reach {JENKINS_BUILD_COMPLETE} state")
        for project in self.projects:
            jenkins_builds = self.get_builds_sorted_by_number(project=project)
            for jenkins_build in jenkins_builds:
                if (jenkins_build.name, project) not in self.build_completed:
                    try:
                        wait_for_resource_state(
                            resource=jenkins_build,
                            state=JENKINS_BUILD_COMPLETE,
                            timeout=timeout,
                        )
                        self.get_build_duration_time(
                            namespace=project, build_name=jenkins_build.name
                        )
                    except ResourceWrongStatusException:
                        ocp_obj = OCP(namespace=project, kind="build")
                        output = ocp_obj.describe(resource_name=jenkins_build.name)
                        error_msg = (
                            f"{jenkins_build.name} did not reach to "
                            f"{JENKINS_BUILD_COMPLETE} state after {timeout} sec\n"
                            f"oc describe output of {jenkins_build.name} \n:{output}"
                        )
                        log.error(error_msg)
                        self.print_completed_builds_results()
                        raise UnexpectedBehaviour(error_msg)

    def get_builds_sorted_by_number(self, project):
        """
        Get builds per project and sort builds by build name number

        Args:
            project (str): project name

        Returns:
            List: List of Jenkins build OCS obj

        """
        jenkins_builds_unsorted = self.get_builds_obj(namespace=project)
        jenkins_builds_sorted = [0] * self.num_of_builds
        for build in jenkins_builds_unsorted:
            build_num = int(re.sub("[^0-9]", "", build.name))
            jenkins_builds_sorted[build_num - 1] = build
        return jenkins_builds_sorted

    def get_jenkins_deploy_pods(self, namespace):
        """
        Get all jenkins deploy pods

        Args:
            namespace (str): get pods in namespace

        Returns:
            pod_objs (list): jenkins deploy pod objects list

        """
        return [
            get_pod_obj(pod, namespace=namespace)
            for pod in get_pod_name_by_pattern("deploy", namespace=namespace)
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
            ocp_obj = OCP(api_version="v1", kind="Build", namespace=namespace)
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
        sc_name = (
            constants.DEFAULT_EXTERNAL_MODE_STORAGECLASS_RBD
            if storagecluster_independent_check()
            and config.ENV_DATA["platform"].lower()
            not in constants.HCI_PC_OR_MS_PLATFORM
            else constants.DEFAULT_STORAGECLASS_RBD
        )
        for project in self.projects:
            log.info(f"create jenkins pvc on project {project}")
            pvc_obj = create_pvc(
                pvc_name="dependencies",
                size="10Gi",
                sc_name=sc_name,
                namespace=project,
            )
            pvc_objs.append(pvc_obj)
        return pvc_objs

    def create_app_jenkins(self):
        """
        create application jenkins

        """
        for project in self.projects:
            log.info(f"create app jenkins on project {project}")
            ocp_obj = OCP(namespace=project)
            ocp_obj.new_project(project)
            cmd = "new-app --name=jenkins-ocs-rbd --template=jenkins-persistent-ocs"
            ocp_obj.exec_oc_cmd(command=cmd, out_yaml_format=False)

    def start_build(self):
        """
        Start build on jenkins

        """
        for project in self.projects:
            for build_num in range(1, self.num_of_builds + 1):
                log.info(
                    f"Start Jenkins build on {project} project, build number:{build_num} "
                )
                cmd = f"start-build {constants.JENKINS_BUILD}"
                build = OCP(namespace=project)
                build.exec_oc_cmd(command=cmd, out_yaml_format=False)

    def get_build_name_by_pattern(self, pattern="client", namespace=None, filter=None):
        """
        Get build name by pattern

        Returns:
            list: build name

        """
        ocp_obj = OCP(kind="Build", namespace=namespace)
        build_names = ocp_obj.exec_oc_cmd("get build -o name", out_yaml_format=False)
        build_names = build_names.split("\n")
        build_list = []
        for name in build_names:
            if filter is not None and re.search(filter, name):
                log.info(f"build name filtered {name}")
            elif re.search(pattern, name):
                (_, name) = name.split("/")
                log.info(f"build name match found appending {name}")
                build_list.append(name)
        return build_list

    def create_project_names(self):
        """
        Create project names

        """
        self.projects = []
        for project_id in range(1, self.num_of_projects + 1):
            self.projects.append("myjenkins-" + str(project_id))

    def create_ocs_jenkins_template(self):
        """

        Create OCS Jenkins Template
        """
        log.info("Create Jenkins Template, jenkins-persistent-ocs")
        ocp_obj = OCP(namespace="openshift", kind="template")
        tmp_dict = ocp_obj.get(resource_name="jenkins-persistent", out_yaml_format=True)
        tmp_dict["labels"]["app"] = "jenkins-persistent-ocs"
        tmp_dict["labels"]["template"] = "jenkins-persistent-ocs-template"
        tmp_dict["metadata"]["name"] = "jenkins-persistent-ocs"
        # Find Kind: 'PersistentVolumeClaim' position in the objects list, differs in OCP 4.5 and OCP 4.6.
        sc_name = (
            constants.DEFAULT_EXTERNAL_MODE_STORAGECLASS_RBD
            if storagecluster_independent_check()
            else constants.DEFAULT_STORAGECLASS_RBD
        )
        for i in range(len(tmp_dict["objects"])):
            if tmp_dict["objects"][i]["kind"] == constants.PVC:
                tmp_dict["objects"][i]["metadata"]["annotations"] = {
                    "volume.beta.kubernetes.io/storage-class": sc_name
                }

        tmp_dict["parameters"][4]["value"] = "10Gi"
        tmp_dict["parameters"].append(
            {
                "description": "Override jenkins options to speed up slave spawning",
                "displayName": "Override jenkins options to speed up slave spawning",
                "name": "JAVA_OPTS",
                "value": "-Dhudson.slaves.NodeProvisioner.initialDelay=0 "
                "-Dhudson.slaves.NodeProvisioner.MARGIN=50 -Dhudson."
                "slaves.NodeProvisioner.MARGIN0=0.85",
            }
        )
        ocs_jenkins_template_obj = OCS(**tmp_dict)
        ocs_jenkins_template_obj.create()

    def get_build_duration_time(self, namespace, build_name):
        """
        get build duration time

        Args:
            namespace (str): get build in namespace
            build_name (str): the name of the jenkins build
        """
        ocp_obj = OCP(namespace=namespace, kind="build")
        output_build = ocp_obj.describe(resource_name=build_name)
        list_build = output_build.split()
        index = list_build.index("Duration:")
        self.build_completed[(build_name, namespace)] = list_build[index + 1]

    def print_completed_builds_results(self):
        """

        Get builds logs and print them on table
        """
        log.info("Print builds results")
        build_table = PrettyTable()
        build_table.field_names = ["Project", "Build", "Duration"]
        for build, time_build in self.build_completed.items():
            build_table.add_row([build[1], build[0], time_build])
        log.info(f"\n{build_table}\n")

    def export_builds_results_to_googlesheet(
        self, sheet_name="E2E Workloads", sheet_index=3
    ):
        """
        Collect builds results, output to google spreadsheet

        Args:
            sheet_name (str): Name of the sheet
            sheet_index (int): Index of sheet

        """
        # Collect data and export to Google doc spreadsheet
        log.info("Exporting Jenkins data to google spreadsheet")
        g_sheet = GoogleSpreadSheetAPI(sheet_name=sheet_name, sheet_index=sheet_index)
        for build, time_build in reversed(self.build_completed.items()):
            g_sheet.insert_row([build[1], build[0], time_build], 2)
        g_sheet.insert_row(["Project", "Build", "Duration"], 2)
        # Capturing versions(OCP, OCS and Ceph) and test run name
        g_sheet.insert_row(
            [
                f"ocp_version:{utils.get_cluster_version()}",
                f"ocs_build_number:{utils.get_ocs_build_number()}",
                f"ceph_version:{utils.get_ceph_version()}",
                f"test_run_name:{utils.get_testrun_name()}",
            ],
            2,
        )

    def get_node_name_where_jenkins_pod_not_hosted(
        self, node_type=constants.WORKER_MACHINE, num_of_nodes=1
    ):
        """
        get nodes

        Args:
            node_type (str): The node type  (e.g. worker, master)
            num_of_nodes (int): The number of nodes to be returned

        Returns:
            list: List of compute node names
        """
        if node_type == constants.MASTER_MACHINE:
            nodes_drain = [
                node.name
                for node in get_nodes(node_type=node_type, num_of_nodes=num_of_nodes)
            ]
        elif node_type == constants.WORKER_MACHINE:
            pod_objs = []
            for project in self.projects:
                pod_names = get_pod_name_by_pattern(
                    pattern="jenkins", namespace=project
                )
                pod_obj = [
                    get_pod_obj(name=pod_name, namespace=project)
                    for pod_name in pod_names
                ]
                pod_objs += pod_obj
            nodes_app_name = set(get_app_pod_running_nodes(pod_objs))
            nodes_worker_name = set(get_worker_nodes())
            nodes_drain = nodes_worker_name - nodes_app_name
        else:
            raise ValueError("The node type is worker or master")
        return list(nodes_drain)[:num_of_nodes]

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
        cmd = (
            "delete template.template.openshift.io/jenkins-persistent-ocs -n openshift"
        )
        ocp_obj.exec_oc_cmd(command=cmd, out_yaml_format=False)
        # Wait for the resources to delete
        # https://github.com/red-hat-storage/ocs-ci/issues/2417
        time.sleep(120)
