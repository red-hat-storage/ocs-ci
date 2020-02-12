import logging
from abc import ABC, abstractmethod

from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility import templating
from ocs_ci.utility.utils import run_mcg_cmd
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

    @property
    def phase(self):
        """
        Returns phase of bucket claim

        Returns:
            str: OBC phase
        """
        return OCP(kind='obc', namespace=self.mcg.namespace).get(
            resource_name=self.name
        ).get('status').get('phase')

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


class CLIBucket(MCGBucket):
    """
    Implementation of an MCG bucket using the NooBaa CLI
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        run_mcg_cmd(f'obc create --exact {self.name}')

    def internal_delete(self):
        """
        Deletes the bucket using the NooBaa CLI
        """
        run_mcg_cmd(f'obc delete {self.name}')
