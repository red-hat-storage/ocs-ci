"""
General Secret object
"""
import os
import logging
import ocs.defaults as default
import base64


from templating import dump_to_temp_yaml
from resources.base_resource import BaseOCSClass
from ocs import ocp

log = logging.getLogger(__name__)


class Secret(BaseOCSClass):
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

    # def get(self, resource_name, selector=''):
    #     return self.ocp.get(resource_name=resource_name, selector=selector)

    def create(self):
        log.info(f"Creating secret for {self.interface}")
        template = os.path.join(
            default.TEMPLATES_DIR, f"csi-{self.interface}-secret.yaml"
        )
        dump_to_temp_yaml(template, default.TEMP_YAML, **self.secret_data)
        assert self.ocp.create(yaml_file=default.TEMP_YAML)

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
