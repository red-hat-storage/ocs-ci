"""
A module for implementing specific api client for interacting with openshift or
kubernetes cluster

APIClientBase is an abstract base class which imposes a contract on
methods to be implemented in derived classes which are specific to api client
"""


import logging
from abc import ABCMeta, abstractmethod

logger = logging.getLogger(__name__)


def get_api_client(client_name):
    """
    Get instance of corresponding api-client object with given name

    Args:
        client_name (str): name of the api client to be instantiated

    Returns:
        api client object
    """
    res = filter(
        lambda x: x.__name__ == client_name,
        APIClientBase.__subclasses__()
    )

    try:
        cls = next(res)
        return cls()
    except StopIteration:
        logger.error(f'Could not find api-client {client_name}')
        raise


class APIClientBase(metaclass=ABCMeta):
    """
    Abstract base class for all api-client classes

    This is an abstract base class and api-client specific classes
    should implement all the methods for interacting with openshift cluster
    """

    @property
    @abstractmethod
    def name(self):
        """Concrete class will have respective api-client name"""
        pass

    @abstractmethod
    def api_get(self):
        raise NotImplementedError("api_get method is not implemented")

    @abstractmethod
    def api_post(self):
        raise NotImplementedError("api_post method is not implemented")

    @abstractmethod
    def api_delete(self):
        raise NotImplementedError("api_delete method is not implemented")

    @abstractmethod
    def api_patch(self):
        raise NotImplementedError("api_patch method is not implemented")

    @abstractmethod
    def api_create(self):
        raise NotImplementedError("api_create method is not implemented")

    @abstractmethod
    def get_pods(self, **kwargs):
        """
        Because of discrepancies in IO format of each client api
        leaving this to be implemented by specific client

        Args:
            **kwargs: ex: namespace='namespace',openshift namespace from
                which we need pods

        Raises:
            NotImplementedError: if client has not implemented this function.

        Returns:
            pod_names (list): A list of pod names
        """

        raise NotImplementedError("get_pods method is not implemented")

    @abstractmethod
    def get_labels(self, pod_name, pod_namespace):
        """
        Get the openshift labels on a given pod

        Args:
            pod_name(str): Name of pod for which labels to be fetched
            pod_namespace(str): namespace of pod where this pod lives

        Raises:
            NotImplementedError: if function not implemented by client

        Returns:
           dict: Labels associated with pod
            """

        raise NotImplementedError("get_labels method is not implemented")

    @abstractmethod
    def create_service(self, **kw):
        """
        Create an openshift service

        Args:
            **kw: ex body="content", namespace='ocnamespace'

        Returns:
            dict: Response from api server

            Note: Returns could be tricky because it varies from client to
            client. If its oc-cli then extra work needs to be done in the
            specific implementation.
        """

        raise NotImplementedError("get_labels method is not implemented")


# All openshift REST client specific imports here
from oc import openshift_ops
from openshift.dynamic import exceptions


class OCRESTClient(APIClientBase):
    """ All activities using openshift REST client"""

    def __init__(self):
        """TODO: get REST client instance from ctx which is shared globally"""
        self.rest_client = openshift_ops.OCP()

    @property
    def name(self):
        return "OCRESTClient"

    def get_pods(self, **kwargs):
        """
        Get pods in specific namespace or across oc cluster

        Args:
            **kwargs: ex: namespace=rook-ceph, label_selector='x==y'

        Returns:
            list: of pods names,if no namespace provided then this function
                returns all pods across openshift cluster
        """

        resource = self.rest_client.v1_pods

        try:
            kwargs.update({'resource': resource})
            pod_data = self.api_get(**kwargs)
        except exceptions.NotFoundError:
            logger.error("Failed to get pods: resource not found")
            raise
        except Exception:
            logger.error("Unexpected error")
            raise

        return [item.metadata.name for item in pod_data.items]

    def get_labels(self, pod_name, pod_namespace):
        """
        Get labels from a specific pod

        Args:
            pod_name (str): Name of the pod
            pod_namespace (str): namespace where this pod lives

        Raises:
            NotFoundError: If resource not found

        Returns:
            dict: All the labels on a pod
        """

        resource = self.rest_client.v1_pods.status
        try:
            pod_meta = self.api_get(
                resource=resource,
                name=pod_name,
                namespace=pod_namespace,
            )
        except exceptions.NotFoundError:
            logger.error("Failed to get pods: resource not found")
            raise
        except Exception:
            logger.error("Unexpected error")
            raise

        data = pod_meta['metadata']['labels']
        pod_labels = {k: v for k, v in data.items()}
        return pod_labels

    def create_service(self, **kw):
        """
        Args:
            kw: ex: body={body} for the request which has service spec

        Returns:
            ResourceInstance
        """
        if 'body' not in kw:
            logger.error("create must have body ")

        resource = self.rest_client.v1_services
        kw.update({'resource': resource})
        return self.api_create(**kw)

    def api_get(self, **kw):
        return self.rest_client.call_api("GET", **kw)

    def api_post(self, **kw):
        pass

    def api_delete(self, **kw):
        pass

    def api_patch(self, **kw):
        pass

    def api_create(self, **kw):
        return self.rest_client.call_api("CREATE", **kw)


class OCCLIClient(APIClientBase):
    """
    All activities using oc-cli.

    This implements all functionalities like create, patch, delete using
    oc commands.
    """

    @property
    def name(self):
        return "OCCLIClient"


class KubeClient(APIClientBase):
    """
    All activities using upstream kubernetes python client
    """

    @property
    def name(self):
        return "KubeClient"
