"""
General OCP object
"""
import logging
import yaml
import tempfile
from ocs.ocp import OCP
from ocs import defaults
from utility import utils

log = logging.getLogger(__name__)


class BaseOCSClass(object):
    """
    Base OCSClass inherited by StorageClass, CephFilesystem, secret, PVC, etc
    """

    def __init__(
        self, api_version=defaults.API_VERSION,
        kind='Service', namespace=None
    ):
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
        self.temp_yaml = tempfile.NamedTemporaryFile(
            mode='w+', prefix=self._kind, delete=False
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

    def apply(self, **data):
        with open(self.temp_yaml.name, 'w') as yaml_file:
            yaml.dump(data, yaml_file)
        assert self.ocp.apply(yaml_file=self.temp_yaml.name), (
            f"Failed to apply changes {data}"
        )

    def delete_temp_yaml_file(self):
        utils.delete_file(self.temp_yaml.name)
