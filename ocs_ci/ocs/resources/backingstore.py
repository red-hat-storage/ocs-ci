import logging

from ocs_ci.ocs.ocp import OCP
from ocs_ci.framework import config

log = logging.getLogger(__name__)


class BackingStore():
    """
    A class that represents BackingStore objects

    """
    def __init__(self, name, uls_name=None, secret_name=None):
        self.name = name
        self.uls_name = uls_name
        self.secret_name = secret_name

    def delete(self):
        log.info(f'Cleaning up backingstore {self.name}')

        OCP(
            namespace=config.ENV_DATA['cluster_namespace']
        ).exec_oc_cmd(
            command=f'delete backingstore {self.name}',
            out_yaml_format=False
        )

        log.info(
            f"Verifying whether backingstore {self.name} exists after deletion"
        )
        # Todo: implement deletion assertion
