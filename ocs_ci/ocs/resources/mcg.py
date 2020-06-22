import base64
import json
import logging
import os
import shlex
from time import sleep

import boto3
import requests
from botocore.client import ClientError

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed, CredReqSecretNotFound, TimeoutExpiredError
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.resources.pod import cal_md5sum
from ocs_ci.ocs.resources.pod import get_pods_having_label, Pod
from ocs_ci.utility import templating
from ocs_ci.utility.utils import TimeoutSampler, exec_cmd
from tests.helpers import (
    create_unique_resource_name, create_resource,
    calc_local_file_md5_sum, retrieve_default_ingress_crt
)
import subprocess
import stat

logger = logging.getLogger(name=__file__)


class MCG(object):
    """
    Wrapper class for the Multi Cloud Gateway's S3 service
    """

    (
        s3_resource, s3_endpoint, s3_internal_endpoint, ocp_resource,
        mgmt_endpoint, region, access_key_id, access_key,
        namespace, noobaa_user, noobaa_password, noobaa_token
    ) = (None,) * 12

    def __init__(self):
        """
        Constructor for the MCG class
        """
        self.namespace = config.ENV_DATA['cluster_namespace']
        self.operator_pod = Pod(
            **get_pods_having_label(
                constants.NOOBAA_OPERATOR_POD_LABEL, self.namespace
            )[0]
        )
        self.core_pod = Pod(
            **get_pods_having_label(constants.NOOBAA_CORE_POD_LABEL, self.namespace)[0]
        )

        self.retrieve_noobaa_cli_binary()

        """
        The certificate will be copied on each mcg_obj instantiation since
        the process is so light and quick, that the time required for the redundant
        copy is neglible in comparison to the time a hash comparison will take.
        """
        retrieve_default_ingress_crt()

        get_noobaa = OCP(kind='noobaa', namespace=self.namespace).get()

        self.s3_endpoint = (
            get_noobaa.get('items')[0].get('status').get('services')
            .get('serviceS3').get('externalDNS')[0]
        )
        self.s3_internal_endpoint = (
            get_noobaa.get('items')[0].get('status').get('services')
            .get('serviceS3').get('internalDNS')[0]
        )
        self.mgmt_endpoint = (
            get_noobaa.get('items')[0].get('status').get('services')
            .get('serviceMgmt').get('externalDNS')[0]
        ) + '/rpc'
        self.region = config.ENV_DATA['region']

        creds_secret_name = (
            get_noobaa.get('items')[0].get('status').get('accounts')
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

        self.s3_resource = boto3.resource(
            's3', verify=constants.DEFAULT_INGRESS_CRT_LOCAL_PATH,
            endpoint_url=self.s3_endpoint,
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.access_key
        )

        self.s3_client = self.s3_resource.meta.client

        if config.ENV_DATA['platform'].lower() == 'aws':
            (
                self.cred_req_obj,
                self.aws_access_key_id,
                self.aws_access_key
            ) = self.request_aws_credentials()

            self.aws_s3_resource = boto3.resource(
                's3', endpoint_url="https://s3.amazonaws.com",
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_access_key
            )
            logger.info('Checking whether RGW pod is not present on AWS platform')
            pods = pod.get_pods_having_label(label=constants.RGW_APP_LABEL, namespace=self.namespace)
            assert len(pods) == 0, 'RGW pod should not exist on AWS platform'

        elif (
            config.ENV_DATA.get('platform') == constants.VSPHERE_PLATFORM
        ) or (
            config.ENV_DATA.get('platform') == constants.BAREMETAL_PLATFORM
        ):
            logger.info(f"Checking for RGW pod on {config.ENV_DATA.get('platform')} platform")
            rgw_pod = OCP(kind=constants.POD, namespace=self.namespace)
            assert rgw_pod.wait_for_resource(
                condition=constants.STATUS_RUNNING,
                selector=constants.RGW_APP_LABEL,
                resource_count=1,
                timeout=60
            )

    def s3_get_all_bucket_names(self):
        """
        Returns:
            set: A set of all bucket names

        """
        return {bucket.name for bucket in self.s3_resource.buckets.all()}

    def read_system(self):
        """
        Returns:
            dict: A dictionary with information about MCG resources

        """
        return self.send_rpc_query(
            'system_api',
            'read_system',
            params={}
        ).json()['reply']

    def get_bucket_info(self, bucket_name):
        """
        Args:
            bucket_name (str): Name of searched bucket

        Returns:
            dict: Information about the bucket

        """
        logger.info(f'Requesting information about bucket {bucket_name}')
        for bucket in self.read_system().get('buckets'):
            if bucket['name'] == bucket_name:
                logger.debug(bucket)
                return bucket
        logger.warning(f'Bucket {bucket_name} was not found')
        return None

    def oc_get_all_bucket_names(self):
        """
        Returns:
            set: A set of all bucket names

        """
        all_obcs_in_namespace = OCP(namespace=self.namespace, kind='obc').get().get('items')
        return {bucket.get('spec').get('bucketName')
                for bucket
                in all_obcs_in_namespace}

    def cli_get_all_bucket_names(self):
        """
        Returns:
            set: A set of all bucket names

        """
        obc_lst = self.exec_mcg_cmd('obc list').stdout.split('\n')[1:-1]
        # TODO assert the bucket passed the Pending state
        return {row.split()[1] for row in obc_lst}

    def s3_list_all_objects_in_bucket(self, bucketname):
        """
        Returns:
            list: A list of all bucket objects
        """
        return {obj for obj in self.s3_resource.Bucket(bucketname).objects.all()}

    def s3_get_all_buckets(self):
        """
        Returns:
            list: A list of all s3.Bucket objects

        """
        return {bucket for bucket in self.s3_resource.buckets.all()}

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
        return bucketname in self.cli_get_all_bucket_names()

    def send_rpc_query(self, api, method, params=None):
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
        return requests.post(
            url=self.mgmt_endpoint,
            data=json.dumps(payload),
            verify=constants.DEFAULT_INGRESS_CRT_LOCAL_PATH
        )

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
            resp = requests.post(
                url=self.mgmt_endpoint,
                data=request_str,
                verify=constants.DEFAULT_INGRESS_CRT_LOCAL_PATH
            )
            bucket_data = resp.json().get('reply').get('data').get('size')

            payload = {
                "api": "bucket_api",
                "method": "read_bucket",
                "params": {"name": bucketname},
                "auth_token": self.noobaa_token
            }
            request_str = json.dumps(payload)
            resp = requests.post(
                url=self.mgmt_endpoint,
                data=request_str,
                verify=constants.DEFAULT_INGRESS_CRT_LOCAL_PATH
            )
            bucket_data_reduced = resp.json().get('reply').get('data').get('size_reduced')

            logger.info(
                'Overall bytes stored: ' + str(bucket_data) + '. Reduced size: ' + str(bucket_data_reduced)
            )

            return bucket_data, bucket_data_reduced

        try:
            for total_size, total_reduced in TimeoutSampler(140, 5, _retrieve_reduction_data):
                if total_size - total_reduced > 100 * 1024 * 1024:
                    logger.info(
                        'Data reduced:' + str(total_size - total_reduced)
                    )
                    return True
                else:
                    logger.info(
                        'Data reduction is not yet sufficient. '
                        'Retrying in 5 seconds...'
                    )
        except TimeoutExpiredError:
            logger.error(
                'Not enough data reduction. Something is wrong.'
            )
            assert False

    def request_aws_credentials(self):
        """
        Uses a CredentialsRequest CR to create an AWS IAM that allows the program
        to interact with S3

        Returns:
            OCS: The CredentialsRequest resource
        """
        awscreds_data = templating.load_yaml(constants.MCG_AWS_CREDS_YAML)
        req_name = create_unique_resource_name('awscredreq', 'credentialsrequests')
        awscreds_data['metadata']['name'] = req_name
        awscreds_data['metadata']['namespace'] = self.namespace
        awscreds_data['spec']['secretRef']['name'] = req_name
        awscreds_data['spec']['secretRef']['namespace'] = self.namespace

        creds_request = create_resource(**awscreds_data)
        sleep(5)

        secret_ocp_obj = OCP(kind='secret', namespace=self.namespace)
        try:
            cred_req_secret_dict = secret_ocp_obj.get(resource_name=creds_request.name, retry=5)
        except CommandFailed:
            logger.error(
                'Failed to retrieve credentials request secret'
            )
            raise CredReqSecretNotFound(
                'Please make sure that the cluster used is an AWS cluster, '
                'or that the `platform` var in your config is correct.'
            )

        aws_access_key_id = base64.b64decode(
            cred_req_secret_dict.get('data').get('aws_access_key_id')
        ).decode('utf-8')

        aws_access_key = base64.b64decode(
            cred_req_secret_dict.get('data').get('aws_secret_access_key')
        ).decode('utf-8')

        def _check_aws_credentials():
            try:
                s3_res = boto3.resource(
                    's3', endpoint_url="https://s3.amazonaws.com",
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_access_key
                )
                test_bucket = s3_res.create_bucket(
                    Bucket=create_unique_resource_name('cred-verify', 's3-bucket')
                )
                test_bucket.delete()
                return True

            except ClientError:
                logger.info('Credentials are still not active. Retrying...')
                return False

        try:
            for api_test_result in TimeoutSampler(40, 5, _check_aws_credentials):
                if api_test_result:
                    logger.info('AWS credentials created successfully.')
                    break

        except TimeoutExpiredError:
            logger.error(
                'Failed to create credentials'
            )
            assert False

        return creds_request, aws_access_key_id, aws_access_key

    def create_new_aws_connection(self, conn_name=None):
        """
        Creates a new NooBaa connection to an AWS backend

        Args:
            conn_name: The connection name to be used

        Returns:
            bool: False if the connection creation failed

        """
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
                    return True
                else:
                    logger.info('AWS IAM did not yet propagate')
        except TimeoutExpiredError:
            logger.error(f'Could not create connection {conn_name}')
            assert False

    def create_new_backingstore_aws_bucket(self, backingstore_info):
        """
        Creates an S3 target bucket for NooBaa to use as a backing store

        Args:
            backingstore_info: A tuple containing the BS information
            to be used in its creation.

        """
        if backingstore_info.get('name') is None:
            backingstore_info['name'] = create_unique_resource_name('backingstorebucket', 'awsbucket')

        if backingstore_info.get('region') is None:
            self.aws_s3_resource.create_bucket(Bucket=backingstore_info['name'])
        else:
            self.aws_s3_resource.create_bucket(
                Bucket=backingstore_info['name'],
                CreateBucketConfiguration={
                    'LocationConstraint': backingstore_info['region']
                }
            )

    def create_aws_backingstore_secret(self, name):
        """
        Creates a secret for NooBaa's backingstore
        Args:
            name: The name to be given to the secret

        Returns:
            OCS: The secret resource

        """
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
        """
        Creates a new NooBaa backing store
        Args:
            name: The name to be given to the backing store
            targetbucket: The S3 target bucket to connect to
            secretname: The secret to use for authentication
            region: The target bucket's region

        Returns:
            OCS: The backingstore resource

        """
        bs_data = templating.load_yaml(constants.MCG_BACKINGSTORE_YAML)
        bs_data['metadata']['name'] += f'-{name}'
        bs_data['metadata']['namespace'] = self.namespace
        bs_data['spec']['awsS3']['secret']['name'] = secretname
        bs_data['spec']['awsS3']['targetBucket'] = targetbucket
        bs_data['spec']['awsS3']['region'] = region
        return create_resource(**bs_data)

    def oc_create_bucketclass(self, name, backingstores, placement):
        """
        Creates a new NooBaa bucket class
        Args:
            name: The name to be given to the bucket class
            backingstores: The backing stores to use as part of the policy
            placement: The placement policy to be used - Mirror | Spread

        Returns:
            OCS: The bucket class resource

        """
        bc_data = templating.load_yaml(constants.MCG_BUCKETCLASS_YAML)
        bc_data['metadata']['name'] = name
        bc_data['metadata']['namespace'] = self.namespace
        tiers = bc_data['spec']['placementPolicy']['tiers'][0]
        tiers['backingStores'] = backingstores
        tiers['placement'] = placement
        return create_resource(**bc_data)

    def toggle_aws_bucket_readwrite(self, bucketname, block=True):
        """
        Toggles a bucket's IO using a bucket policy

        Args:
            bucketname: The name of the bucket that should be manipulated
            block: Whether to block RW or un-block. True | False

        """
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
        """
        Check whether all object chunks in a bucket
        are mirrored across all backing stores.

        Args:
            bucket_name: The name of the bucket that should be checked

        Returns:
            bool: Whether mirroring finished successfully

        """

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
                    mirror_nodes = [
                        mirror_blocks[i].get('block_md').get('node')
                        for i in range(len(mirror_blocks))
                    ]
                    if 2 <= len(mirror_blocks) == len(set(mirror_nodes)):
                        results.append(True)
                    else:
                        results.append(False)

            return all(results)

        try:
            for mirroring_is_complete in TimeoutSampler(140, 5, _check_mirroring):
                if mirroring_is_complete:
                    logger.info(
                        'All objects mirrored successfully.'
                    )
                    return True
                else:
                    logger.info(
                        'Waiting for the mirroring process to finish...'
                    )
        except TimeoutExpiredError:
            logger.error(
                'The mirroring process did not complete within the time limit.'
            )
            assert False

    def check_backingstore_state(
        self,
        backingstore_name,
        desired_state,
        timeout=600
    ):
        """
        Checks whether the backing store reached a specific state
        Args:
            backingstore_name (str): Name of the backing store to be checked
            desired_state (str): The desired state of the backing store
            timeout (int): Number of seconds for timeout which will be used
            in the checks used in this function.

        Returns:
            bool: Whether the backing store has reached the desired state

        """

        def _check_state():
            sysinfo = self.read_system()
            for pool in sysinfo.get('pools'):
                if pool.get('name') == backingstore_name:
                    current_state = pool.get('mode')
                    logger.info(
                        f'Current state of backingstore {backingstore_name} '
                        f'is {current_state}'
                    )
                    if current_state == desired_state:
                        return True
            return False

        try:
            for reached_state in TimeoutSampler(timeout, 10, _check_state):
                if reached_state:
                    logger.info(
                        f'BackingStore {backingstore_name} reached state '
                        f'{desired_state}.'
                    )
                    return True
                else:
                    logger.info(
                        f'Waiting for BackingStore {backingstore_name} to '
                        f'reach state {desired_state}...'
                    )
        except TimeoutExpiredError:
            logger.error(
                f'The BackingStore did not reach the desired state '
                f'{desired_state} within the time limit.'
            )
            assert False

    def exec_mcg_cmd(self, cmd, namespace=None, **kwargs):
        """
        Executes an MCG CLI command through the noobaa-operator pod's CLI binary

        Args:
            cmd (str): The command to run
            namespace (str): The namespace to run the command in

        Returns:
            str: stdout of the command

        """

        kubeconfig = os.getenv('KUBECONFIG')
        if kubeconfig:
            kubeconfig = f"--kubeconfig {kubeconfig} "

        namespace = f'-n {namespace}' if namespace else f'-n {self.namespace}'
        result = exec_cmd(
            f'{constants.NOOBAA_OPERATOR_LOCAL_CLI_PATH} {cmd} {namespace}',
            **kwargs
        )
        result.stdout = result.stdout.decode()
        result.stderr = result.stderr.decode()
        return result

    def retrieve_noobaa_cli_binary(self):
        """
        Copy the NooBaa CLI binary from the operator pod
        if it wasn't found locally, or if the hashes between
        the two don't match.

        """
        def _compare_cli_hashes():
            """
            Verify that the remote and local CLI binaries are the same
            in order to make sure the local bin is up to date

            Returns:
                bool: Whether the local and remote hashes are identical

            """
            remote_cli_bin_md5 = cal_md5sum(
                self.operator_pod,
                constants.NOOBAA_OPERATOR_POD_CLI_PATH
            )
            local_cli_bin_md5 = calc_local_file_md5_sum(
                constants.NOOBAA_OPERATOR_LOCAL_CLI_PATH
            )
            return remote_cli_bin_md5 == local_cli_bin_md5

        if (
            not os.path.isfile(constants.NOOBAA_OPERATOR_LOCAL_CLI_PATH)
            or not _compare_cli_hashes()
        ):
            cmd = (
                f"oc exec -n {self.namespace} {self.operator_pod.name}"
                f" -- cat {constants.NOOBAA_OPERATOR_POD_CLI_PATH}"
                f"> {constants.NOOBAA_OPERATOR_LOCAL_CLI_PATH}"
            )
            subprocess.run(cmd, shell=True)
            # Add an executable bit in order to allow usage of the binary
            current_file_permissions = os.stat(constants.NOOBAA_OPERATOR_LOCAL_CLI_PATH)
            os.chmod(
                constants.NOOBAA_OPERATOR_LOCAL_CLI_PATH,
                current_file_permissions.st_mode | stat.S_IEXEC
            )
            # Make sure the binary was copied properly and has the correct permissions
            assert os.path.isfile(constants.NOOBAA_OPERATOR_LOCAL_CLI_PATH), (
                f'MCG CLI file not found at {constants.NOOBAA_OPERATOR_LOCAL_CLI_PATH}'
            )
            assert os.access(constants.NOOBAA_OPERATOR_LOCAL_CLI_PATH, os.X_OK), (
                "The MCG CLI binary does not have execution permissions"
            )
            assert _compare_cli_hashes(), (
                "Binary hash doesn't match the one on the operator pod"
            )

    @property
    def status(self):
        """
        Verify noobaa status output is clean without any errors

        Returns:
            bool: return False if any of the non optional components of noobaa is not available

        """
        # Get noobaa status
        status = self.exec_mcg_cmd('status').stderr
        for line in status.split('\n'):
            if 'Not Found' in line and 'Optional' not in line:
                logger.error(f"Error in noobaa status output- {line}")
                return False
        logger.info("Verified: noobaa status does not contain any error.")
        return True
