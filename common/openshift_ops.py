import logging
from collections import defaultdict
from kubernetes import config
from openshift.dynamic import DynamicClient

log = logging.getLogger(__name__)


class OCP(object):
    """Class which contains various utility functions for interacting
    with OpenShift

    """
    def __init__(self):

        k8s_client = config.new_client_from_config()
        dyn_client = DynamicClient(k8s_client)

        self.v1_service_list = dyn_client.resources.get(
            api_version='v1', kind='ServiceList'
            )
        self.v1_projects = dyn_client.resources.get(
            api_version='project.openshift.io/v1', kind='Project'
            )

    def get_projects(self):
        """Gets all the projects in the cluster.

        Returns:
            list: List of projects
            NoneType: on failure

        """
        try:
            ret = self.v1_projects.get()
            return [each.metadata.name for each in ret.items]
        except Exception as err:
            log.error("Unable to list the projects. Error: %s", err)
            return None

    def get_services(self):
        """Gets all the services in the cluster.

        Returns:
            dict: defaultdict of services, key represents the namespace
                  and value represents the services
            NoneType: on failure

        """
        services = defaultdict(list)
        try:
            ret = self.v1_service_list.get()
            "{services[each.metadata.namespace].append(each.metadata.name)"
            "for each in ret.items}"
            return services
        except Exception as err:
            log.error(
                "Unable to fetch the services in cluster. Error: %s", err
            )
            return None

    def get_services_in_namespace(self, namespace):
        """Gets the services in a namespace

        Returns:
            list: list of services in a namespace
            NoneType: on failure

        """
        try:
            ret = self.v1_service_list.get(namespace=namespace)
            return [each.metadata.name for each in ret.items]
        except Exception as err:
            log.error(
                "Unable to get services in namespace %s. "
                "Error: %s", namespace, err
            )
            return None

    def create_project(self, project):
        """creates new project.

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

        return _rc
