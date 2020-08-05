from ocs_ci.ocs.ocp import OCP
import logging
from ocs_ci.framework import config

log = logging.getLogger(__name__)



class BucketClass():
    """
    A class that represents BucketClass objects

    """
    def __init__(self, name, backingstores, placement):
        self.name = name
        self.backingstores = backingstores
        self.placement = placement

    def delete(self):
        log.info(f'Cleaning up bucket class {self.name}')

        OCP(
            namespace=config.ENV_DATA['cluster_namespace']
        ).exec_oc_cmd(
            command=f'delete bucketclass {self.name}',
            out_yaml_format=False
        )

        log.info(
            f"Verifying whether bucket class {self.name} exists after deletion"
        )
        # Todo: implement deletion assertion
