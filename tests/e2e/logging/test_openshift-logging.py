"""
This file contains the testcases for openshift-logging
"""

import logging

import pytest

import random


from tests import helpers, disruption_helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pod import get_all_pods, get_pod_obj
from ocs_ci.utility.retry import retry
from ocs_ci.framework.testlib import E2ETest, workloads, tier1, ignore_leftovers
from ocs_ci.utility import deployment_openshift_logging as ocp_logging_obj


logger = logging.getLogger(__name__)


@pytest.fixture()
def setup_fixture(install_logging):
    """
    Installs openshift-logging
    """
    logger.info("Testcases execution post deployment of openshift-logging")


@pytest.mark.usefixtures(
    setup_fixture.__name__
)
@ignore_leftovers
class Testopenshiftloggingonocs(E2ETest):
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
                    assert 'green' in item.strip(), f"Project {project} is Unhealthy"
        else:
            raise ModuleNotFoundError

    def get_elasticsearch_pod_obj(self):
        """
        This function returns the Elasticsearch pod obj
        """
        pod_list = get_all_pods(namespace=constants.OPENSHIFT_LOGGING_NAMESPACE)
        elasticsearch_pod = [
            pod for pod in pod_list if pod.name.startswith('elasticsearch')
        ]
        elasticsearch_pod_obj = random.choice(elasticsearch_pod)
        return elasticsearch_pod_obj

    @pytest.mark.polarion_id("OCS-657")
    @tier1
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
    @workloads
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
        assert project1_filecount['_shards']['successful'] != 0, (
            f"No files found in project {project1}"
        )
        logger.info(f'Total number of files in project 1 {project1_filecount}')

        # Create another app_pod in new project
        pod_obj, pvc_obj = create_pvc_and_deploymentconfig_pod

        project2 = pvc_obj.project.namespace

        # Check the 2nd project exists in the EFK stack
        self.validate_project_exists(pvc_obj)

        project2_filecount = elasticsearch_pod_obj.exec_cmd_on_pod(
            command=f'es_util --query=project.{project2}.*/_count', out_yaml_format=True
        )
        assert project2_filecount['_shards']['successful'] != 0, (
            f"No files found in project {project2}"
        )
        logger.info(f'Total number of files in the project 2 {project2_filecount}')

    @pytest.mark.polarion_id("OCS-651")
    @workloads
    def test_respin_elasticsearch_pod(self, create_pvc_and_deploymentconfig_pod):
        """
        Test to verify respin of elasticsearch pod has no functional impact
        on logging backed by OCS.
        """

        elasticsearch_pod_obj = self.get_elasticsearch_pod_obj()

        # Respin the elastic-search pod
        elasticsearch_pod_obj.delete(force=True)

        # Checks the health of logging cluster after a respin
        assert ocp_logging_obj.check_health_of_clusterlogging()

        # Checks .operations index
        es_pod_obj = self.get_elasticsearch_pod_obj()

        operations_index = es_pod_obj.exec_cmd_on_pod(
            command='es_util --query=.operations.*/_search?pretty', out_yaml_format=True
        )
        assert operations_index['_shards']['failed'] == 0, (
            "Unable to access the logs of .operations from ES pods"
        )

        # Creates new-project and app-pod and checks the logs are retained
        pod_obj, pvc_obj = create_pvc_and_deploymentconfig_pod

        self.validate_project_exists(pvc_obj)
