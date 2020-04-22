import logging
from abc import ABC, abstractmethod

from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility import templating
from tests.helpers import create_unique_resource_name, create_resource

logger = logging.getLogger(name=__file__)


class MCGBucket(ABC):
    """
    Base abstract class for MCG buckets
    """
    mcg, name = (None,) * 2

    def __init__(self, mcg, name, *args, **kwargs):
        """
        Constructor of an MCG bucket
        """
        self.mcg = mcg
        self.name = name
        logger.info(f"Creating bucket: {self.name}")

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        if type(other) == str:
            return self.name == other
        elif type(other) == MCGBucket:
            return self.name == other.name

    def delete(self):
        """
        Super method that first logs the bucket deletion and then calls
        the appropriate implementation
        """
        logger.info(f"Deleting bucket: {self.name}")
        self.internal_delete()

    @abstractmethod
    def internal_delete(self):
        pass


class S3Bucket(MCGBucket):
    """
    Implementation of an MCG bucket using the S3 API
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mcg.s3_resource.create_bucket(Bucket=self.name)

    def internal_delete(self):
        """
        Deletes the bucket using the S3 API
        """
        self.mcg.s3_resource.Bucket(self.name).object_versions.delete()
        self.mcg.s3_resource.Bucket(self.name).delete()

    @property
    def status(self):
        """
        Returns the OBC mode as shown in the NB UI and retrieved via RPC

        Returns:
            str: The bucket's mode

        """
        return self.mcg.get_bucket_info(self.name).get('mode')

    def verify_health(self):
        """
        Verifies that the bucket is healthy by checking its mode

        Returns:
            bool: True if the bucket is healthy, False otherwise

        """
        return self.status == constants.HEALTHY_OB


class OCBucket(MCGBucket):
    """
    Implementation of an MCG bucket using the OC CLI
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        obc_data = templating.load_yaml(constants.MCG_OBC_YAML)
        if self.name is None:
            self.name = create_unique_resource_name('oc', 'obc')
        obc_data['metadata']['name'] = self.name
        obc_data['spec']['bucketName'] = self.name
        obc_data['spec']['storageClassName'] = self.mcg.namespace + '.noobaa.io'
        obc_data['metadata']['namespace'] = self.mcg.namespace
        if 'bucketclass' in kwargs:
            obc_data['spec']['additionalConfig']['bucketclass'] = kwargs['bucketclass']
        create_resource(**obc_data)

    def internal_delete(self):
        """
        Deletes the bucket using the OC CLI
        """
        OCP(kind='obc', namespace=self.mcg.namespace).delete(resource_name=self.name)

    @property
    def status(self):
        """
        Returns the OBC's phase

        Returns:
            str: OBC phase

        """
        return OCP(kind='obc', namespace=self.mcg.namespace).get(
            resource_name=self.name
        )['status']['phase']

    def verify_health(self):
        """
        Verifies that the bucket is healthy by checking its phase

        Returns:
            bool: True if the bucket is healthy, False otherwise

        """
        return self.status == constants.HEALTHY_OBC


class CLIBucket(MCGBucket):
    """
    Implementation of an MCG bucket using the NooBaa CLI
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mcg.exec_mcg_cmd(f'obc create --exact {self.name}')

    def internal_delete(self):
        """
        Deletes the bucket using the NooBaa CLI
        """
        self.mcg.exec_mcg_cmd(f'obc delete {self.name}')

    @property
    def status(self):
        """
        Returns the OBC status as printed by the NB CLI

        Returns:
            str: OBC status

        """
        return run_mcg_cmd(f'obc status {self.name}')

    def verify_health(self):
        """
        Verifies that the bucket is healthy using the CLI

        Returns:
            bool: True if the bucket is healthy, False otherwise

        """
        return (
            constants.HEALTHY_OB_CLI_MODE in self.status
            and constants.HEALTHY_OBC_CLI_PHASE in self.status
        )
