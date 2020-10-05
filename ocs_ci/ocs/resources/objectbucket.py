import base64
import logging
from abc import ABC, abstractmethod

import boto3
from tests.helpers import create_resource, create_unique_resource_name, storagecluster_independent_check

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed, TimeoutExpiredError
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.bucket_utils import retrieve_verification_mode
from ocs_ci.ocs.utils import oc_get_all_obc_names
from ocs_ci.utility import templating
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(name=__file__)


class OBC(object):
    """
    Wrapper class for Object Bucket Claim credentials
    """

    (
        s3_resource, s3_endpoint, obc_name,
        ob_name, bucket_name, obc_account,
        access_key_id, access_key, namespace
    ) = (None,) * 9

    def __init__(self, obc_name):
        """
        Initializer function

        Args:
            obc_name (str): Name of the Object Bucket Claim
        """
        self.obc_name = obc_name
        self.namespace = config.ENV_DATA['cluster_namespace']
        obc_resource = OCP(namespace=self.namespace, kind='ObjectBucketClaim', resource_name=self.obc_name).get()
        self.ob_name = obc_resource.get('spec').get('ObjectBucketName')
        self.bucket_name = obc_resource.get('spec').get('bucketName')
        ob_obj = OCP(namespace=self.namespace, kind='ObjectBucket', resource_name=self.ob_name).get()
        self.obc_account = ob_obj.get('spec').get('additionalState').get('account')
        secret_obc_obj = OCP(kind='secret', namespace=self.namespace, resource_name=self.obc_name).get()

        obc_configmap = OCP(namespace=self.namespace, kind='ConfigMap', resource_name=self.obc_name).get()
        obc_configmap_data = obc_configmap.get('data')

        obc_provisioner = obc_resource.get('metadata').get('labels').get('bucket-provisioner')

        self.region = obc_configmap_data.get('BUCKET_REGION')

        self.access_key_id = base64.b64decode(
            secret_obc_obj.get('data').get('AWS_ACCESS_KEY_ID')
        ).decode('utf-8')
        self.access_key = base64.b64decode(
            secret_obc_obj.get('data').get('AWS_SECRET_ACCESS_KEY')
        ).decode('utf-8')

        if 'noobaa' in obc_provisioner:
            get_noobaa = OCP(kind='noobaa', namespace=self.namespace).get()
            self.s3_internal_endpoint = (
                get_noobaa.get('items')[0].get('status').get('services')
                .get('serviceS3').get('internalDNS')[0]
            )
            self.s3_external_endpoint = (
                get_noobaa.get('items')[0].get('status').get('services')
                .get('serviceS3').get('externalDNS')[0]
            )
            self.s3_resource = boto3.resource(
                's3', verify=retrieve_verification_mode(),
                endpoint_url=self.s3_external_endpoint,
                aws_access_key_id=self.access_key_id,
                aws_secret_access_key=self.access_key
            )
            self.s3_client = self.s3_resource.meta.client

        elif 'rook' in obc_provisioner:
            # TODO: implement network forwarding to access the internal address
            self.s3_internal_endpoint = (
                'http://' + obc_configmap_data.get('BUCKET_HOST') + ':'
                + obc_configmap_data.get('BUCKET_PORT')
            )


class ObjectBucket(ABC):
    """
    Base abstract class for MCG buckets

    """
    mcg, name = (None,) * 2

    def __init__(self, name, mcg=None, rgw=None, *args, **kwargs):
        """
        Constructor of an MCG bucket

        """
        self.name = name
        self.mcg = mcg
        self.rgw = rgw
        self.namespace = config.ENV_DATA['cluster_namespace']
        logger.info(f"Creating bucket: {self.name}")

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        if type(other) == str:
            return self.name == other
        elif type(other) == ObjectBucket:
            return self.name == other.name

    def delete(self, verify=True):
        """
        Super method that first logs the bucket deletion and then calls
        the appropriate implementation

        """
        logger.info(f"Deleting bucket: {self.name}")
        try:
            self.internal_delete()
        except CommandFailed as e:
            if 'not found' in str(e):
                logger.warning(f'{self.name} was not found, or already deleted.')
                return True
            else:
                raise e
        if verify:
            return self.verify_deletion()
        else:
            return True

    @property
    def status(self):
        """
        A method that first logs the bucket's status and then calls
        the appropriate implementation

        """
        status_var = self.internal_status
        logger.info(f"{self.name} status is {status_var}")
        return status_var

    def verify_deletion(self, timeout=60, interval=5):
        """
        Super method used for logging the deletion verification
        process and then calls the appropriate implementatation

        """
        logger.info(f"Verifying deletion of {self.name}")
        try:
            for del_check in TimeoutSampler(timeout, interval, self.internal_verify_deletion):
                if del_check:
                    logger.info(f'{self.name} was deleted successfuly')
                    return True
                else:
                    logger.info(f'{self.name} still exists. Retrying...')
        except TimeoutExpiredError:
            logger.error(
                f'{self.name} was not deleted within {timeout} seconds.'
            )
            assert False, f'{self.name} was not deleted within {timeout} seconds.'

    def verify_health(self, timeout=60, interval=5):
        """
        Health verification function that tries to verify
        the a bucket's health by using its appropriate internal_verify_health
        function until a given time limit is reached

        Args:
            timeout (int): Timeout for the check, in seconds
            interval (int): Interval to wait between checks, in seconds

        Returns:
            (bool): True if the bucket is healthy, False otherwise

        """
        logger.info(f'Waiting for {self.name} to be healthy')
        try:
            for health_check in TimeoutSampler(timeout, interval, self.internal_verify_health):
                if health_check:
                    logger.info(f'{self.name} is healthy')
                    return True
                else:
                    logger.info(f'{self.name} is unhealthy. Rechecking.')
        except TimeoutExpiredError:
            logger.error(
                f'{self.name} did not reach a healthy state within {timeout} seconds.'
            )
            assert False, f'{self.name} did not reach a healthy state within {timeout} seconds.'

    """
    The following methods are abstract, internal methods.
    The reason for the "internal" naming scheme/design is in order to allow each inheriting class
    to implement its appropriate methods using the necessary APIs and logics, while still
    sharing a common entry point.
    For example - the way to check MCGS3Bucket's status is by using RPC, while MCGCLIBucket's status
    has to be checked via the MCG CLI tool. However, we would like both to output the status in a
    consistent manner, without having to log/print the status each time.
    Thus, the internal_status methods only return the status message, and the general status method
    logs it and returns it further if needed.
    """
    @abstractmethod
    def internal_delete(self):
        """
        Abstract internal deletion method

        """
        raise NotImplementedError()

    @abstractmethod
    def internal_status(self):
        """
        Abstract status method

        """
        raise NotImplementedError()

    @abstractmethod
    def internal_verify_health(self):
        """
        Abstract health verification method

        """
        raise NotImplementedError()

    @abstractmethod
    def internal_verify_deletion(self):
        """
        Abstract deletion verification method

        """
        raise NotImplementedError()


class MCGCLIBucket(ObjectBucket):
    """
    Implementation of an MCG bucket using the NooBaa CLI
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        bc = f" --bucketclass={kwargs['bucketclass']}" if 'bucketclass' in kwargs else ''
        self.mcg.exec_mcg_cmd(f'obc create --exact {self.name}{bc}')

    def internal_delete(self):
        """
        Deletes the bucket using the NooBaa CLI
        """
        self.mcg.exec_mcg_cmd(f'obc delete {self.name}')

    @property
    def internal_status(self):
        """
        Returns the OBC status as printed by the NB CLI

        Returns:
            str: OBC status

        """
        return self.mcg.exec_mcg_cmd(f'obc status {self.name}')

    def internal_verify_health(self):
        """
        Verifies that the bucket is healthy using the CLI

        Returns:
            bool: True if the bucket is healthy, False otherwise

        """
        return (
            all(
                healthy_mark in self.status.stdout.replace(' ', '') for healthy_mark
                in [constants.HEALTHY_OB_CLI_MODE, constants.HEALTHY_OBC_CLI_PHASE]
            )
        )

    def internal_verify_deletion(self):
        return self.name not in self.mcg.cli_get_all_bucket_names()


class MCGS3Bucket(ObjectBucket):
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
    def internal_status(self):
        """
        Returns the OBC mode as shown in the NB UI and retrieved via RPC

        Returns:
            str: The bucket's mode

        """
        return self.mcg.get_bucket_info(self.name).get('mode')

    def internal_verify_health(self):
        """
        Verifies that the bucket is healthy by checking its mode

        Returns:
            bool: True if the bucket is healthy, False otherwise

        """
        return self.status == constants.HEALTHY_OB

    def internal_verify_deletion(self):
        return self.name not in self.mcg.s3_get_all_bucket_names()


class OCBucket(ObjectBucket):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def internal_delete(self, verify=True):
        """
        Deletes the bucket using the OC CLI
        """
        OCP(kind='obc', namespace=self.namespace).delete(resource_name=self.name)

    @property
    def internal_status(self):
        """
        Returns the OBC's phase

        Returns:
            str: OBC phase

        """
        return OCP(
            kind='obc', namespace=self.namespace, resource_name=self.name
        ).get()['status']['phase']

    def internal_verify_health(self):
        """
        Verifies that the bucket is healthy by checking its phase

        Returns:
            bool: True if the bucket is healthy, False otherwise

        """
        return self.status == constants.HEALTHY_OBC

    def internal_verify_deletion(self):
        return self.name not in oc_get_all_obc_names()


class MCGOCBucket(OCBucket):
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
        obc_data['spec']['storageClassName'] = self.namespace + '.noobaa.io'
        obc_data['metadata']['namespace'] = self.namespace
        if 'bucketclass' in kwargs:
            obc_data.setdefault('spec', {}).setdefault('additionalConfig', {}).setdefault(
                'bucketclass', kwargs['bucketclass']
            )
        create_resource(**obc_data)


class RGWOCBucket(OCBucket):
    """
    Implementation of an RGW bucket using the S3 API
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        obc_data = templating.load_yaml(constants.MCG_OBC_YAML)
        if self.name is None:
            self.name = create_unique_resource_name('oc', 'obc')
        obc_data['metadata']['name'] = self.name
        obc_data['spec']['bucketName'] = self.name
        if storagecluster_independent_check():
            obc_data['spec']['storageClassName'] = constants.DEFAULT_EXTERNAL_MODE_STORAGECLASS_RGW
        else:
            obc_data['spec']['storageClassName'] = constants.DEFAULT_STORAGECLASS_RGW
        obc_data['metadata']['namespace'] = self.namespace
        create_resource(**obc_data)


class MCGNamespaceBucket(ObjectBucket):
    """
    Implementation of an MCG bucket using the S3 API
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.read_ns_resources = kwargs.get('read_ns_resources')
        self.write_ns_resource = kwargs.get('write_ns_resource')
        self.mcg.send_rpc_query('bucket_api', 'create_bucket', {
            'name': self.name,
            'namespace': {
                'write_resource': self.write_ns_resource,
                'read_resources': self.read_ns_resources
            }
        })

    def internal_delete(self):
        """
        Deletes the bucket using the S3 API
        """
        self.mcg.send_rpc_query('bucket_api', 'delete_bucket', {'name': self.name})

    @property
    def internal_status(self):
        """
        Returns the OBC mode as shown in the NB UI and retrieved via RPC

        Returns:
            str: The bucket's mode

        """
        return self.mcg.get_bucket_info(self.name).get('mode')

    def internal_verify_health(self):
        """
        Verifies that the bucket is healthy by checking its mode

        Returns:
            bool: True if the bucket is healthy, False otherwise

        """
        # Retrieve the NooBaa system information
        system_state = self.mcg.read_system()

        # Retrieve the correct namespace bucket info
        match_buckets = [
            ns_bucket for ns_bucket in system_state
            .get('buckets') if ns_bucket.get('name') == self.name
        ]
        if not match_buckets:
            return False
        ns_properties = match_buckets[0].get('namespace')
        actual_read_resources = ns_properties.get('read_resources')
        actual_write_resource = ns_properties.get('write_resource')
        return actual_read_resources == self.read_ns_resources and actual_write_resource == self.write_ns_resource

    def internal_verify_deletion(self):
        # Retrieve the NooBaa system information
        system_state = self.mcg.read_system()

        # Retrieve the correct namespace bucket info
        match_buckets = [
            ns_bucket for ns_bucket in system_state
            .get('buckets') if ns_bucket.get('name') == self.name
        ]
        return len(match_buckets) == 0


BUCKET_MAP = {
    's3': MCGS3Bucket,
    'oc': MCGOCBucket,
    'cli': MCGCLIBucket,
    'rgw-oc': RGWOCBucket,
    'mcg-namespace': MCGNamespaceBucket
}
