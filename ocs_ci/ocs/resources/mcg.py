import base64
import json
import logging
import shlex
from time import sleep

import boto3
import requests
from botocore.client import ClientError

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed, TimeoutExpiredError
from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility import templating
from ocs_ci.utility.utils import run_mcg_cmd, TimeoutSampler, run_cmd
from tests.helpers import create_unique_resource_name, create_resource


logger = logging.getLogger(name=__file__)


class MCG(object):
    """
    Wrapper class for the Multi Cloud Gateway's S3 service
    """

    (
        s3_resource, s3_endpoint, ocp_resource,
        mgmt_endpoint, region, access_key_id, access_key,
        namespace, noobaa_user, noobaa_password, noobaa_token
    ) = (None,) * 11

    def __init__(self):
        """
        Constructor for the MCG class
        """
        self.namespace = config.ENV_DATA['cluster_namespace']
        ocp_obj = OCP(kind='noobaa', namespace=self.namespace)
        results = ocp_obj.get()
        try:
            self.s3_endpoint = (
                results.get('items')[0].get('status').get('services')
                .get('serviceS3').get('externalDNS')[1]
            )
            self.mgmt_endpoint = (
                results.get('items')[0].get('status').get('services')
                .get('serviceMgmt').get('externalDNS')[1]
            ) + '/rpc'
        except IndexError:
            self.s3_endpoint = (
                results.get('items')[0].get('status').get('services')
                .get('serviceS3').get('externalDNS')[0]
            )
            self.mgmt_endpoint = (
                results.get('items')[0].get('status').get('services')
                .get('serviceMgmt').get('externalDNS')[0]
            ) + '/rpc'
        self.region = self.s3_endpoint.split('.')[1]
        creds_secret_name = (
            results.get('items')[0].get('status').get('accounts')
            .get('admin').get('secretRef').get('name')
        )
        secret_ocp_obj = OCP(kind='secret', namespace=self.namespace)
        creds_secret_obj = secret_ocp_obj.get(creds_secret_name)

        self.access_key_id = base64.b64decode(
            creds_secret_obj.get('data').get('AWS_ACCESS_KEY_ID')
        ).decode('utf-8')
        self.access_key = base64.b64decode(
            creds_secret_obj.get('data').get('AWS_SECRET_ACCESS_KEY')
        ).decode('utf-8')

        self.noobaa_user = base64.b64decode(
            creds_secret_obj.get('data').get('email')
        ).decode('utf-8')
        self.noobaa_password = base64.b64decode(
            creds_secret_obj.get('data').get('password')
        ).decode('utf-8')

        self.noobaa_token = self.send_rpc_query(
            'auth_api', 'create_auth', params={
                'role': 'admin',
                'system': 'noobaa',
                'email': self.noobaa_user,
                'password': self.noobaa_password
            }
        ).json().get('reply').get('token')

        self.cred_req_obj = self.request_aws_credentials()

        secret_ocp_obj = OCP(kind='secret', namespace=self.namespace)
        cred_req_secret_dict = secret_ocp_obj.get(self.cred_req_obj.name)

        self.aws_access_key_id = base64.b64decode(
            cred_req_secret_dict.get('data').get('aws_access_key_id')
        ).decode('utf-8')

        self.aws_access_key = base64.b64decode(
            cred_req_secret_dict.get('data').get('aws_secret_access_key')
        ).decode('utf-8')

        self._ocp_resource = ocp_obj

        self.s3_resource = boto3.resource(
            's3', verify=False, endpoint_url=self.s3_endpoint,
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.access_key
        )

        self.aws_s3_resource = boto3.resource(
            's3', verify=False, endpoint_url="https://s3.amazonaws.com",
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_access_key
        )

    def s3_list_all_bucket_names(self):
        """
        Returns:
            list: A list of all bucket names

        """
        return [bucket.name for bucket in self.s3_resource.buckets.all()]

    def oc_list_all_bucket_names(self):
        """
        Returns:
            list: A list of all bucket names

        """
        all_obcs_in_namespace = OCP(namespace=self.namespace, kind='obc').get().get('items')
        return [bucket.get('spec').get('bucketName')
                for bucket
                in all_obcs_in_namespace]

    def cli_list_all_bucket_names(self):
        """
        Returns:
            list: A list of all bucket names

        """
        obc_lst = run_mcg_cmd('obc list').split('\n')[1:-1]
        # TODO assert the bucket passed the Pending state
        return [row.split()[1] for row in obc_lst]

    def s3_list_all_objects_in_bucket(self, bucketname):
        """
        Returns:
            list: A list of all bucket objects
        """
        return [obj for obj in self.s3_resource.Bucket(bucketname).objects.all()]

    def s3_get_all_buckets(self):
        """
        Returns:
            list: A list of all s3.Bucket objects

        """
        return [bucket for bucket in self.s3_resource.buckets.all()]

    def s3_verify_bucket_exists(self, bucketname):
        """
        Verifies whether a bucket with the given bucketname exists
        Args:
            bucketname : The bucket name to be verified

        Returns:
              bool: True if bucket exists, False otherwise

        """
        try:
            self.s3_resource.meta.client.head_bucket(Bucket=bucketname)
            logger.info(f"{bucketname} exists")
            return True
        except ClientError:
            logger.info(f"{bucketname} does not exist")
            return False

    def verify_s3_object_integrity(self, original_object_path, result_object_path, awscli_pod):
        """
        Verifies checksum between orignial object and result object on an awscli pod

        Args:
            original_object_path (str): The Object that is uploaded to the s3 bucket
            result_object_path (str):  The Object that is downloaded from the s3 bucket
            awscli_pod (pod): A pod running the AWSCLI tools

        Returns:
              bool: True if checksum matches, False otherwise

        """
        md5sum = shlex.split(awscli_pod.exec_cmd_on_pod(command=f'md5sum {original_object_path} {result_object_path}'))
        if md5sum[0] == md5sum[2]:
            logger.info(f'Passed: MD5 comparison for {original_object_path} and {result_object_path}')
            return True
        else:
            logger.error(f'Failed: MD5 comparison of {original_object_path} and {result_object_path} - '
                         f'{md5sum[0]} ≠ {md5sum[2]}')
            return False

    def oc_verify_bucket_exists(self, bucketname):
        """
        Verifies whether a bucket with the given bucketname exists
        Args:
            bucketname : The bucket name to be verified

        Returns:
              bool: True if bucket exists, False otherwise

        """
        try:
            OCP(namespace=self.namespace, kind='obc').get(bucketname)
            logger.info(f"{bucketname} exists")
            return True
        except CommandFailed as e:
            if 'NotFound' in repr(e):
                logger.info(f"{bucketname} does not exist")
                return False
            raise

    def cli_verify_bucket_exists(self, bucketname):
        """
        Verifies whether a bucket with the given bucketname exists
        Args:
            bucketname : The bucket name to be verified

        Returns:
              bool: True if bucket exists, False otherwise

        """
        return bucketname in self.cli_list_all_bucket_names()

    def send_rpc_query(self, api, method, params):
        """
        Templates and sends an RPC query to the MCG mgmt endpoint

        Args:
            api: The name of the API to use
            method: The method to use inside the API
            params: A dictionary containing the command payload

        Returns:
            The server's response

        """
        payload = {
            'api': api,
            'method': method,
            'params': params,
            'auth_token': self.noobaa_token
        }
        return requests.post(url=self.mgmt_endpoint, data=json.dumps(payload), verify=False)

    def check_data_reduction(self, bucketname):
        """
        Checks whether the data reduction on the MCG server works properly
        Args:
            bucketname: An example bucket name that contains compressed/deduped data

        Returns:
            bool: True if the data reduction mechanics work, False otherwise

        """

        def _retrieve_reduction_data():
            payload = {
                "api": "bucket_api",
                "method": "read_bucket",
                "params": {"name": bucketname},
                "auth_token": self.noobaa_token
            }
            request_str = json.dumps(payload)
            resp = requests.post(url=self.mgmt_endpoint, data=request_str, verify=False)
            bucket_data = resp.json().get('reply').get('data').get('size')

            payload = {
                "api": "bucket_api",
                "method": "read_bucket",
                "params": {"name": bucketname},
                "auth_token": self.noobaa_token
            }
            request_str = json.dumps(payload)
            resp = requests.post(url=self.mgmt_endpoint, data=request_str, verify=False)
            bucket_data_reduced = resp.json().get('reply').get('data').get('size_reduced')

            logger.info(
                'Overall bytes stored: ' + str(bucket_data) + '. Amount reduced: ' + str(bucket_data_reduced)
            )

            return bucket_data, bucket_data_reduced

        try:
            for total_size, total_reduced in TimeoutSampler(120, 5, _retrieve_reduction_data):
                if total_size - total_reduced > 80000000:
                    logger.info(
                        'Data reduced:' + str(total_size - total_reduced)
                    )
                    return True
                else:
                    logger.info(
                        f'Data reduction is not yet sufficient - '
                        f'Total size: {total_size}, Reduced: {total_reduced}.'
                        f'Retrying in 5 seconds...'
                    )
        except TimeoutExpiredError:
            logger.error(
                'Not enough data reduction. Something is wrong.'
            )
            return False

    def request_aws_credentials(self):
        awscreds_data = templating.load_yaml(constants.MCG_AWS_CREDS_YAML)
        req_name = create_unique_resource_name('awscredreq', 'credentialsrequests')
        awscreds_data['metadata']['name'] = req_name
        awscreds_data['metadata']['namespace'] = self.namespace
        awscreds_data['spec']['secretRef']['name'] = req_name
        awscreds_data['spec']['secretRef']['namespace'] = self.namespace
        creds_request = create_resource(**awscreds_data)

        def _retrieve_credreq_provision_state():
            return run_cmd(
                f"oc get credentialsrequests {creds_request.name} -o jsonpath={{.status.provisioned}}"
            )

        try:
            for provisioned_state in TimeoutSampler(30, 5, _retrieve_credreq_provision_state):
                if provisioned_state == "true":
                    sleep(10)
                    logger.info(f'Secret created successfully.')
                    break
                else:
                    logger.info(
                        f'Credentials were not yet provisioned or could not be found. '
                        f'Retrying...'
                    )
        except TimeoutExpiredError:
            logger.error(
                'Failed to create credentials'
            )

        return creds_request

    def create_new_aws_connection(self, conn_name=None):
        if conn_name is None:
            conn_name = create_unique_resource_name('awsconnection', 'mcgconn')

        params = {
            "auth_method": "AWS_V4",
            "endpoint": "https://s3.amazonaws.com",
            "endpoint_type": "AWS",
            "identity": self.aws_access_key_id,
            "name": conn_name,
            "secret": self.aws_access_key
        }

        try:
            for resp in TimeoutSampler(
                30, 3, self.send_rpc_query, 'account_api', 'add_external_connection', params
            ):
                if 'error' not in resp.text:
                    logger.info(f'Connection {conn_name} created successfully')
                    break
                else:
                    logger.info('AWS IAM did not yet propagate')
        except TimeoutExpiredError:
            logger.error(f'Could not create connection {conn_name}')
            return False

    def create_new_backingstore_bucket(self, backingstore_info):
        if backingstore_info.get('name') is None:
            backingstore_info['name'] = create_unique_resource_name('backingstorebucket', 'awsbucket')
        if backingstore_info.get('region') is None:
            backingstore_info['region'] = self.region

        self.aws_s3_resource.create_bucket(
            Bucket=backingstore_info['name'],
            CreateBucketConfiguration={
                'LocationConstraint': backingstore_info['region']
            }
        )

    def create_aws_backingstore_secret(self, name):
        bs_secret_data = templating.load_yaml(constants.MCG_BACKINGSTORE_SECRET_YAML)
        bs_secret_data['metadata']['name'] += f'-{name}'
        bs_secret_data['metadata']['namespace'] = self.namespace
        bs_secret_data['data']['AWS_ACCESS_KEY_ID'] = base64.urlsafe_b64encode(
            self.aws_access_key_id.encode('UTF-8')
        ).decode('ascii')
        bs_secret_data['data']['AWS_SECRET_ACCESS_KEY'] = base64.urlsafe_b64encode(
            self.aws_access_key.encode('UTF-8')
        ).decode('ascii')
        return create_resource(**bs_secret_data)

    def oc_create_aws_backingstore(self, name, targetbucket, secretname, region):
        bs_data = templating.load_yaml(constants.MCG_BACKINGSTORE_YAML)
        bs_data['metadata']['name'] += f'-{name}'
        bs_data['metadata']['namespace'] = self.namespace
        bs_data['spec']['awsS3']['secret']['name'] = secretname
        bs_data['spec']['awsS3']['targetBucket'] = targetbucket
        bs_data['spec']['awsS3']['region'] = region
        return create_resource(**bs_data)

    def oc_create_bucketclass(self, name, backingstores, placement):
        bc_data = templating.load_yaml(constants.MCG_BUCKETCLASS_YAML)
        bc_data['metadata']['name'] = name
        bc_data['metadata']['namespace'] = self.namespace
        tiers = bc_data['spec']['placementPolicy']['tiers'][0]
        tiers['backingStores'] = backingstores
        tiers['placement'] = placement
        return create_resource(**bc_data)

    def toggle_bucket_readwrite(self, bucketname, block=True):
        if block:
            bucket_policy = {
                "Version": "2012-10-17",
                "Id": "DenyReadWrite",
                "Statement": [
                    {
                        "Effect": "Deny",
                        "Principal": {
                            "AWS": "*"
                        },
                        "Action": [
                            "s3:GetObject",
                            "s3:PutObject",
                            "s3:ListBucket"
                        ],
                        "Resource": [
                            f"arn:aws:s3:::{bucketname}/*",
                            f"arn:aws:s3:::{bucketname}"
                        ]
                    }
                ]
            }
            bucket_policy = json.dumps(bucket_policy)
            self.aws_s3_resource.meta.client.put_bucket_policy(
                Bucket=bucketname, Policy=bucket_policy
            )
        else:
            self.aws_s3_resource.meta.client.delete_bucket_policy(
                Bucket=bucketname
            )

    def check_if_mirroring_is_done(self, bucket_name):
        def _check_mirroring():
            results = []
            obj_list = self.send_rpc_query('object_api', 'list_objects', params={
                'bucket': bucket_name
            }).json().get('reply').get('objects')

            for written_object in obj_list:
                object_chunks = self.send_rpc_query('object_api', 'read_object_mapping', params={
                    'bucket': bucket_name,
                    'key': written_object.get('key'),
                    'obj_id': written_object.get('obj_id')
                }).json().get('reply').get('chunks')

                for object_chunk in object_chunks:
                    mirror_blocks = object_chunk.get('frags')[0].get('blocks')
                    mirror_nodes = [mirror_blocks[i].get('block_md').get('node') for i in range(len(mirror_blocks))]
                    if 2 <= len(mirror_blocks) == len(set(mirror_nodes)):
                        results.append(True)
                    else:
                        results.append(False)

            return all(results)

        # Pause
        try:
            for mirroring_is_complete in TimeoutSampler(120, 5, _check_mirroring):
                if mirroring_is_complete:
                    logger.info(
                        'All objects mirrored successfully.'
                    )
                    return True
                else:
                    logger.info(
                        'Mirroring process is not yet done, or has malfunctioned.\nRetrying...'
                    )
        except TimeoutExpiredError:
            logger.error(
                'The mirroring process did not complete in the given time frame.'
            )
            assert False

    def check_backingstore_state(self, backingstore_name, desired_state):
        def _check_state():
            sysinfo = self.send_rpc_query('system_api', 'read_system', params={}).json()['reply']
            for pool in sysinfo.get('pools'):
                if pool.get('name') == backingstore_name:
                    if pool.get('mode') == desired_state:
                        return True
            return False

        # Pause
        try:
            for reached_state in TimeoutSampler(180, 10, _check_state):
                if reached_state:
                    logger.info(
                        f'BackingStore {backingstore_name} reached state {desired_state}.'
                    )
                    return True
                else:
                    logger.info(
                        'BackingStore did not reach desired state yet.\nRetrying...'
                    )
        except TimeoutExpiredError:
            logger.error(
                'The mirroring process did not complete in the given time frame.'
            )
            assert False
