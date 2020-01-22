"""
This file contains the testcases for openshift-logging
"""

import logging

import pytest

import random
from ocs_ci.ocs.resources.csv import CSV
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.framework import config
from ocs_ci.ocs.ocp import OCP
from tests import helpers, disruption_helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pod import get_all_pods, get_pod_obj
from ocs_ci.utility.retry import retry
from ocs_ci.framework.testlib import E2ETest, workloads, tier4, ignore_leftovers
from ocs_ci.utility import deployment_openshift_logging as ocp_logging_obj
from ocs_ci.utility.uninstall_openshift_logging import uninstall_cluster_logging
from ocs_ci.utility import templating

logger = logging.getLogger(__name__)


@pytest.fixture()
def test_fixture(request):
    """
    Setup and teardown
    * The setup will deploy openshift-logging in the cluster
    * The teardown will uninstall cluster-logging from the cluster
    """

    def finalizer():
        teardown()

    request.addfinalizer(finalizer)

    # Deploys elastic-search operator on the project openshift-operators-redhat
    ocp_logging_obj.create_namespace(yaml_file=constants.EO_NAMESPACE_YAML)
    assert ocp_logging_obj.create_elasticsearch_operator_group(
        yaml_file=constants.EO_OG_YAML,
        resource_name='openshift-operators-redhat'
    )
    assert ocp_logging_obj.set_rbac(
        yaml_file=constants.EO_RBAC_YAML, resource_name='prometheus-k8s'
    )
    logging_version = config.ENV_DATA['logging_version']
    subscription_yaml = templating.load_yaml(constants.EO_SUB_YAML)
    subscription_yaml['spec']['channel'] = logging_version
    helpers.create_resource(**subscription_yaml)
    assert ocp_logging_obj.get_elasticsearch_subscription()

    # Deploys cluster-logging operator on the project openshift-loggingno nee
    ocp_logging_obj.create_namespace(yaml_file=constants.CL_NAMESPACE_YAML)
    assert ocp_logging_obj.create_clusterlogging_operator_group(
        yaml_file=constants.CL_OG_YAML
    )
    cl_subscription = templating.load_yaml(constants.CL_SUB_YAML)
    cl_subscription['spec']['channel'] = logging_version
    helpers.create_resource(**cl_subscription)
    assert ocp_logging_obj.get_clusterlogging_subscription()
    cluster_logging_operator = OCP(
        kind=constants.POD, namespace=constants.OPENSHIFT_LOGGING_NAMESPACE
    )
    logger.info(f"The cluster-logging-operator {cluster_logging_operator.get()}")

    create_instance()


@retry(CommandFailed, tries=10, delay=10, backoff=3)
def create_instance():
    """
    The function is used to create instance for
    cluster-logging
    """

    # Create instance
    assert ocp_logging_obj.create_instance_in_clusterlogging()

    # Check the health of the cluster-logging
    assert ocp_logging_obj.check_health_of_clusterlogging()

    csv_obj = CSV(namespace=constants.OPENSHIFT_LOGGING_NAMESPACE)

    # Get the CSV installed
    get_csv = csv_obj.get(out_yaml_format=True)
    logger.info(f'The installed CSV is {get_csv}')


def teardown():
    """
    The teardown will uninstall the openshift-logging from the cluster
    """
    uninstall_cluster_logging()


@pytest.mark.usefixtures(
    test_fixture.__name__
)
@ignore_leftovers
class Test_openshift_logging_on_ocs(E2ETest):
    """
    The class contains tests to verify openshift-logging backed by OCS.
    """
    @pytest.fixture()
    def create_pvc_and_deploymentconfig_pod(self, request, pvc_factory):
        """
        """
        def finalizer():
            helpers.delete_deploymentconfig_pods(pod_obj)

        request.addfinalizer(finalizer)

        # Create pvc
        pvc_obj = pvc_factory()

        # Create service_account to get privilege for deployment pods
        sa_name = helpers.create_serviceaccount(pvc_obj.project.namespace)

        helpers.add_scc_policy(sa_name=sa_name.name, namespace=pvc_obj.project.namespace)

        pod_obj = helpers.create_pod(
            interface_type=constants.CEPHBLOCKPOOL,
            pvc_name=pvc_obj.name,
            namespace=pvc_obj.project.namespace,
            sa_name=sa_name.name,
            dc_deployment=True
        )
        helpers.wait_for_resource_state(resource=pod_obj, state=constants.STATUS_RUNNING)
        return pod_obj, pvc_obj

    @retry(ModuleNotFoundError, tries=10, delay=200, backoff=3)
    def validate_project_exists(self, pvc_obj):
        """
        This function checks whether the new project exists in the
        EFK stack
        """
        pod_list = get_all_pods(namespace='openshift-logging')
        elasticsearch_pod = [
            pod.name for pod in pod_list if pod.name.startswith('elasticsearch')
        ]
        elasticsearch_pod_obj = get_pod_obj(
            name=elasticsearch_pod[1], namespace='openshift-logging'
        )
        project_index = elasticsearch_pod_obj.exec_cmd_on_pod(
            command='indices', out_yaml_format=False
        )
        project = pvc_obj.project.namespace

        if project in project_index:
            logger.info(f'The project {project} exists in the EFK stack')
            for item in project_index.split("\n"):
                if project in item:
                    logger.info(item.strip())
                    if 'green' in item.strip():
                        logger.info(f'The project {project} is Healthy')
                    else:
                        logger.info(f'The project {project} is unhealthy')
        else:
            raise ModuleNotFoundError

    def get_elasticsearch_pod_obj(self):
        """
        This function returns the Elasticsearch pod obj
        """
        pod_list = get_all_pods(namespace=constants.OPENSHIFT_LOGGING_NAMESPACE)
        elasticsearch_pod = [
            pod.name for pod in pod_list if pod.name.startswith('elasticsearch')
        ]
        elasticsearch_pod = random.choice(elasticsearch_pod)
        elasticsearch_pod_obj = get_pod_obj(
            name=elasticsearch_pod, namespace=constants.OPENSHIFT_LOGGING_NAMESPACE
        )
        return elasticsearch_pod_obj

    @pytest.mark.polarion_id("OCS-657")
    @workloads
    def test_create_new_project_to_verify_logging(self, create_pvc_and_deploymentconfig_pod):
        """
        This function creates new project to verify logging in EFK stack
        1. Creates new project
        2. Creates PVC
        3. Creates Deployment pod in the new_project and run-io on the app pod
        4. Logs into the EFK stack and check for new_project
        5. And checks for the file_count in the new_project in EFK stack
        """

        pod_obj, pvc_obj = create_pvc_and_deploymentconfig_pod

        # Running IO on the app_pod
        pod_obj.run_io(storage_type='fs', size=6000)

        self.validate_project_exists(pvc_obj)

    @pytest.mark.polarion_id("OCS-650")
    @tier4
    @retry(ModuleNotFoundError, 6, 300, 3)
    def test_respin_osd_pods_to_verify_logging(self, create_pvc_and_deploymentconfig_pod):
        """
        This function creates projects before and after respin of osd
        and verify project existence in EFK stack.
        1. Creates new project with PVC and app-pods
        2. Respins osd
        3. Logs into the EFK stack and checks for the health of cluster-logging
        4. Logs into the EFK stack and checks project existence
        5. Checks for the shards of the project in the EFK stack
        6. Creates new project and checks the existence again
        """

        # Create 1st project and app_pod
        dc_pod_obj, dc_pvc_obj = create_pvc_and_deploymentconfig_pod

        project1 = dc_pvc_obj.project.namespace

        # Delete the OSD pod
        disruption = disruption_helpers.Disruptions()
        disruption.set_resource(resource='osd')
        disruption.delete_resource()

        # Check the health of the cluster-logging
        assert ocp_logging_obj.check_health_of_clusterlogging()

        # Check for the 1st project created in EFK stack before the respin
        self.validate_project_exists(dc_pvc_obj)

        # Check the files in the project
        elasticsearch_pod_obj = self.get_elasticsearch_pod_obj()

        project1_filecount = elasticsearch_pod_obj.exec_cmd_on_pod(
            command=f'es_util --query=project.{project1}.*/_count'
        )
        if project1_filecount['_shards']['successful'] != 0:
            logger.info(f'The files in the project 1 {project1_filecount}')
        else:
            raise FileNotFoundError

        # Create another app_pod in new project
        pod_obj, pvc_obj = create_pvc_and_deploymentconfig_pod

        project2 = pvc_obj.project.namespace

        # Check the 2nd project exists in the EFK stack
        self.validate_project_exists(pvc_obj)

        project2_filecount = elasticsearch_pod_obj.exec_cmd_on_pod(
            command=f'es_util --query=project.{project2}.*/_count', out_yaml_format=True
        )
        logger.info(f'The files in the project2 {project2_filecount}')

        if project2_filecount['_shards']['successful'] != 0:
            logger.info(f'The files in the project 2 {project2_filecount}')
        else:
            raise FileNotFoundError
