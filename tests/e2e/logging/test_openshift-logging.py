"""
This file contains the testcases for openshift-logging
"""

import pytest
import logging

from tests import helpers
from ocs_ci.ocs.resources.pod import get_all_pods, get_pod_obj
from ocs_ci.ocs import constants
from ocs_ci.utility import deployment_openshift_logging as ocp_logging_obj
from ocs_ci.utility.uninstall_openshift_logging import uninstall_cluster_logging
from ocs_ci.framework.testlib import E2ETest, tier1
from ocs_ci.utility.retry import retry

logger = logging.getLogger(__name__)


@pytest.fixture(scope='class')
def test_fixture(request):
    """
    Setup and teardown
    * The setup will deploy openshift-logging in the cluster
    * The teardown will uninstall cluster-logging from the cluster
    """

    def finalizer():
        teardown(cbp_obj, sc_obj)

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
    assert ocp_logging_obj.create_elasticsearch_subscription(constants.EO_SUB_YAML)

    # Deploys cluster-logging operator on the project openshift-logging
    ocp_logging_obj.create_namespace(yaml_file=constants.CL_NAMESPACE_YAML)
    assert ocp_logging_obj.create_clusterlogging_operator_group(
        yaml_file=constants.CL_OG_YAML
    )
    assert ocp_logging_obj.create_clusterlogging_subscription(
        yaml_file=constants.CL_SUB_YAML
    )

    # Creates storage class
    cbp_obj = helpers.create_ceph_block_pool()
    sc_obj = helpers.create_storage_class(
        interface_type=constants.CEPHBLOCKPOOL,
        interface_name=cbp_obj.name,
        secret_name=constants.DEFAULT_SECRET,
        reclaim_policy="Delete"
    )
    assert ocp_logging_obj.create_instance_in_clusterlogging(sc_name=sc_obj.name)

    # Check the health of the cluster-logging
    assert ocp_logging_obj.check_health_of_clusterlogging()


def teardown(cbp_obj, sc_obj):
    """
    The teardown will uninstall the openshift-logging from the cluster
    """
    cbp_obj.delete()
    sc_obj.delete()
    uninstall_cluster_logging()


@pytest.mark.usefixtures(
    test_fixture.__name__
)
class Test_openshift_logging_on_ocs(E2ETest):
    """
    The class contains tests to verify openshift-logging backed by OCS.
    """
    @pytest.fixture()
    def create_pvc_and_deploymentconfig_pod(self, request, pvc_factory):
        """
        """
        def finalizer():
            helpers.delete_deploymentconfig(pod_obj)

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

    @pytest.mark.polarion_id("OCS-657")
    @tier1
    @retry(ModuleNotFoundError, 6, 300, 3)
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
        pod_obj.run_io(storage_type='fs', size=8000)

        # Searching for new_project in EFK stack
        pod_list = get_all_pods(namespace='openshift-logging')
        elasticsearch_pod = [
            pod.name for pod in pod_list if pod.name.startswith('elasticsearch')
        ]
        elasticsearch_pod_obj = get_pod_obj(
            name=elasticsearch_pod[1], namespace='openshift-logging'
        )
        projects = elasticsearch_pod_obj.exec_cmd_on_pod(
            command='indices | grep project', out_yaml_format=True
        )
        logger.info(projects)
        if pvc_obj.project.namespace in projects:
            logger.info("The new project exists in the EFK stack")
        else:
            raise ModuleNotFoundError
