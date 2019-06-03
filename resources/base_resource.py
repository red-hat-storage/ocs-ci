"""
General OCP object
"""
import logging
from ocs.ocp import OCP

log = logging.getLogger(__name__)


class BaseOCSClass(object):
    """
    A basic OCP object to run basic 'oc' commands
    """

    def __init__(self, api_version='v1', kind='Service', namespace=None):
        """
        Initializer function

        Args:
            api_version (str): TBD
            kind (str): TBD
            namespace (str): The name of the namespace to use
        """
        self._api_version = api_version
        self._kind = kind
        self._namespace = namespace
        self.ocp = OCP(
            api_version=self.api_version, kind=self.kind,
            namespace=self.namespace
        )

    @property
    def api_version(self):
        return self._api_version

    @property
    def kind(self):
        return self._kind

    @property
    def namespace(self):
        return self._namespace
