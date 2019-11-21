import os
import logging

from kubernetes import config
from openshift.dynamic import DynamicClient, exceptions

from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.utility.utils import run_cmd
import ocs_ci.ocs.defaults as default

log = logging.getLogger(__name__)


class OCP(object):
    """
    Class which contains various utility functions for interacting
    with OpenShift

    """
    def __init__(self):
        config.load_kube_config(config_file=os.getenv('KUBECONFIG'))
        k8s_client = config.new_client_from_config()
        dyn_client = DynamicClient(k8s_client)

        self.v1_service_list = dyn_client.resources.get(
            api_version='v1', kind='ServiceList'
        )
        self.v1_projects = dyn_client.resources.get(
            api_version='project.openshift.io/v1', kind='Project'
        )
        self.pods = dyn_client.resources.get(
            api_version=default.API_VERSION, kind='Pod'
        )
        self.deployments = dyn_client.resources.get(
            api_version=default.API_VERSION, kind='Deployment'
        )
        self.services = dyn_client.resources.get(
            api_version=default.API_VERSION, kind='Service'
        )

    @staticmethod
    def call_api(method, **kw):
        """
        This function makes generic REST calls

        Args:
            method(str): one of the GET, CREATE, PATCH, POST, DELETE
            **kw: Based on context of the call kw will be populated by caller

        Returns:
            ResourceInstance object
        """
        # Get the resource type on which we want to operate
        resource = kw.pop('resource')

        if method == "GET":
            return resource.get(**kw)
        elif method == "CREATE":
            return resource.create(**kw)
        elif method == "PATCH":
            return resource.patch(**kw)
        elif method == "DELETE":
            return resource.delete(**kw)
        elif method == "POST":
            return resource.post(**kw)

    def get_pods(self, **kw):
        """
        Get pods in specific namespace or across oc cluster.

        Args:
            **kw: ex: namespace=rook-ceph, label_selector='x==y'

        Returns:
            list: of pods names, if no namespace provided then this function
                returns all pods across openshift cluster.
        """
        resource = self.pods

        try:
            pod_data = resource.get(**kw)
            log.info(pod_data)
        except exceptions.NotFoundError:
            log.error("Failed to get pods: resource not found.")
            raise
        except Exception:
            log.error("Unexpected error.")
            raise

        return [item.metadata.name for item in pod_data.items]

    def get_labels(self, pod_name, pod_namespace):
        """
        Get labels from specific pod

        Args:
            pod_name (str): Name of pod in oc cluster
            pod_namespace (str): pod namespace in which the pod lives

        Raises:
            NotFoundError: If resource not found

        Returns:
            dict: All the openshift labels on a given pod
        """

        resource = self.pods.status

        try:
            pod_meta = resource.get(
                name=pod_name,
                namespace=pod_namespace,
            )
        except exceptions.NotFoundError:
            log.error("Failed to get pods: resource not found.")
            raise
        except Exception:
            log.error("Unexpected error")
            raise

        data = pod_meta['metadata']['labels']
        pod_labels = {k: v for k, v in data.items()}

        return pod_labels

    @staticmethod
    def set_kubeconfig(kubeconfig_path):
        """
        Export environment variable KUBECONFIG for future calls of OC commands
        or other API calls

        Args:
            kubeconfig_path (str): path to kubeconfig file to be exported

        Returns:
            boolean: True if successfully connected to cluster, False otherwise
        """
        # Test cluster access
        log.info("Testing access to cluster with %s", kubeconfig_path)
        if not os.path.isfile(kubeconfig_path):
            log.warning(
                "The kubeconfig file %s doesn't exist!", kubeconfig_path
            )
            return False
        os.environ['KUBECONFIG'] = kubeconfig_path
        try:
            run_cmd("oc cluster-info")
        except CommandFailed as ex:
            log.error("Cluster is not ready to use: %s", ex)
            return False
        log.info("Access to cluster is OK!")
        return True

    def get_projects(self):
        """
        Gets all the projects in the cluster

        Returns:
            list: List of projects

        """
        ret = self.v1_projects.get()
        return [each.metadata.name for each in ret.items]

    def get_services(self):
        """
        Gets all the services in the cluster

        Returns:
            dict: defaultdict of services, key represents the namespace
                  and value represents the services

        """
        ret = self.v1_service_list.get()
        services = {
            each.metadata.namespace: each.metadata.name for each in
            ret.items
        }

        return services

    def get_services_in_namespace(self, namespace):
        """
        Gets the services in a namespace

        Returns:
            list: list of services in a namespace

        """
        ret = self.v1_service_list.get(namespace=namespace)
        return [each.metadata.name for each in ret.items]

    def create_project(self, project):
        """
        Creates new project

        Args:
            project (str): project name

        Returns:
            bool: True if successful otherwise False

        """
        _rc = False
        body = {
            'kind': 'Project',
            'apiVersion': 'project.openshift.io/v1',
            'metadata': {'name': project},
        }
        try:
            self.v1_projects.create(body)
            _rc = True
        except exceptions.ConflictError:
            log.info("project %s might already exists", project)
        except Exception as err:
            log.error("Error while creating project %s: %s", project, err)
            raise Exception(err)

        return _rc
