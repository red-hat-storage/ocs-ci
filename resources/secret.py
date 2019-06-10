"""
General Secret object
"""
import os
import logging
import base64
import tempfile

from utility.templating import dump_to_temp_yaml
from resources.ocs import OCS
from ocs import ocp, constants

log = logging.getLogger(__name__)


class Secret(OCS):
    """
    A basic secret kind resource
    """

    def __init__(
        self, api_version='v1', kind='secret', namespace=None,
        interface='cephfs'
    ):
        """
        Initializer function

        Args:
            api_version (str): TBD
            kind (str): TBD
            namespace (str): The name of the namespace to use
        """
        super(Secret, self).__init__(api_version, kind, namespace)
        self.interface = interface
        self.secret_data = {
            'base64_encoded_admin_password': get_admin_key()
        }
        self.temp_yaml = tempfile.NamedTemporaryFile(
            mode='w+', prefix='SECRET_', delete=False
        )
    # def get(self, resource_name, selector=''):
    #     return self.ocp.get(resource_name=resource_name, selector=selector)

    def create(self):
        log.info(f"Creating secret for {self.interface}")
        template = os.path.join(
            constants.TEMPLATES_DIR, f"csi-{self.interface}-secret.yaml"
        )
        dump_to_temp_yaml(template, self.temp_yaml.name, **self.secret_data)
        assert self.ocp.create(yaml_file=self.temp_yaml.name)

    def delete(self):
        # TODO: implement the functionality
        pass

    def apply(self):
        # TODO: implement the functionality
        pass


def get_admin_key():
    """
    Fetches admin key secret from ceph
    """
    out = ocp.exec_ceph_cmd('ceph auth get-key client.admin')
    base64_output = base64.b64encode(out['key'].encode()).decode()
    return base64_output
