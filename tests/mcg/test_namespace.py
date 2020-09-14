import logging
import pytest
from ocs_ci.framework.testlib import aws_platform_required, ManageTest, tier1
from ocs_ci.ocs.bucket_utils import (
    sync_object_directory,
    verify_s3_object_integrity
)
from ocs_ci.framework import config
from ocs_ci.ocs import constants
logger = logging.getLogger(__name__)


class TestNamespace(ManageTest):
    """
    Test creation of a namespace resource
    """

    MCG_NS_RESULT_DIR = '/result'
    MCG_NS_ORIGINAL_DIR = '/original'

    # Test is skipped for other platforms due to
    # https://github.com/red-hat-storage/ocs-ci/issues/2774
    @aws_platform_required
    @pytest.mark.polarion_id("OCS-2255")
    @tier1
    def test_namespace_resource_creation(self, ns_resource_factory):
        """
        Test namespace resource creation using the MCG RPC.
        """
        # Create the namespace resource and verify health
        ns_resource_factory()

    # Test is skipped for other platforms due to
    # https://github.com/red-hat-storage/ocs-ci/issues/2774
    @aws_platform_required
    @pytest.mark.polarion_id("OCS-2256")
    @tier1
    def test_namespace_bucket_creation(self, ns_resource_factory, bucket_factory):
        """
        Test namespace bucket creation using the MCG RPC.
        """
        # Create the namespace resource and verify health
        ns_resource_name = ns_resource_factory()[1]

        # Create the namespace bucket on top of the namespace resource
        bucket_factory(amount=1, interface='mcg-namespace', write_ns_resource=ns_resource_name, read_ns_resources=[
            ns_resource_name])

    # Test is skipped for other platforms due to
    # https://github.com/red-hat-storage/ocs-ci/issues/2774
    @aws_platform_required
    @pytest.mark.polarion_id("OCS-2257")
    @tier1
    def test_write_to_aws_read_from_ns(self, mcg_obj, cld_mgr, awscli_pod, ns_resource_factory, bucket_factory):
        """
        Test Write to AWS and read from ns bucket using MCG RPC.
        """
        # Create the namespace resource and verify health
        result = ns_resource_factory()
        target_bucket_name = result[0]
        ns_resource_name = result[1]

        # Create the namespace bucket on top of the namespace resource
        rand_ns_bucket = bucket_factory(amount=1, interface='mcg-namespace', write_ns_resource=ns_resource_name,
                                        read_ns_resources=[ns_resource_name])[0].name

        s3_creds = {'access_key_id': cld_mgr.aws_client.access_key, 'access_key': cld_mgr.aws_client.secret_key,
                    'endpoint': constants.MCG_NS_AWS_ENDPOINT, 'region': config.ENV_DATA['region']}
        # Upload files directly to AWS
        self.write_files_to_pod_and_upload(mcg_obj, awscli_pod,
                                           bucket_to_write=target_bucket_name, amount=3, s3_creds=s3_creds)
        # Read files from ns bucket
        self.download_files(mcg_obj, awscli_pod, bucket_to_read=rand_ns_bucket)

        # Compare between uploaded files and downloaded files
        self.compare_dirs(awscli_pod, amount=3)

    # Test is skipped for other platforms due to
    # https://github.com/red-hat-storage/ocs-ci/issues/2774
    @aws_platform_required
    @pytest.mark.polarion_id("OCS-2258")
    @tier1
    def test_write_to_ns_read_from_aws(self, mcg_obj, cld_mgr, awscli_pod, ns_resource_factory, bucket_factory):
        """
        Test Write to ns bucket using MCG RPC and read directly from AWS.
        """

        # Create the namespace resource and verify health
        result = ns_resource_factory()
        target_bucket_name = result[0]
        ns_resource_name = result[1]

        # Create the namespace bucket on top of the namespace resource
        rand_ns_bucket = bucket_factory(amount=1, interface='mcg-namespace', write_ns_resource=ns_resource_name,
                                        read_ns_resources=[ns_resource_name])[0].name

        s3_creds = {'access_key_id': cld_mgr.aws_client.access_key, 'access_key': cld_mgr.aws_client.secret_key,
                    'endpoint': constants.MCG_NS_AWS_ENDPOINT, 'region': config.ENV_DATA['region']}
        # Upload files to NS bucket
        self.write_files_to_pod_and_upload(mcg_obj, awscli_pod,
                                           bucket_to_write=rand_ns_bucket, amount=3)
        # Read files directly from AWS
        self.download_files(mcg_obj, awscli_pod, bucket_to_read=target_bucket_name, s3_creds=s3_creds)

        # Compare between uploaded files and downloaded files
        self.compare_dirs(awscli_pod, amount=3)

    def write_files_to_pod_and_upload(self, mcg_obj, awscli_pod, bucket_to_write, amount=1, s3_creds=None):
        """
        Upload files to bucket (NS or uls)
        """
        awscli_pod.exec_cmd_on_pod(command=f'mkdir {self.MCG_NS_ORIGINAL_DIR}')
        full_object_path = f"s3://{bucket_to_write}"

        for i in range(amount):
            file_name = f"testfile{i}"
            awscli_pod.exec_cmd_on_pod(
                f"dd if=/dev/urandom of={self.MCG_NS_ORIGINAL_DIR}/{file_name}.txt bs=1M count=1 status=none")
        if s3_creds:
            # Write data directly to target bucket from original dir
            sync_object_directory(awscli_pod, self.MCG_NS_ORIGINAL_DIR,
                                  full_object_path, signed_request_creds=s3_creds)
        else:
            # Write data directly to NS bucket from original dir
            sync_object_directory(awscli_pod, self.MCG_NS_ORIGINAL_DIR, full_object_path, mcg_obj)

    def download_files(self, mcg_obj, awscli_pod, bucket_to_read, s3_creds=None):
        """
        Download files from bucket (NS or uls)
        """
        awscli_pod.exec_cmd_on_pod(command=f'mkdir {self.MCG_NS_RESULT_DIR}')
        ns_bucket_path = f"s3://{bucket_to_read}"

        if s3_creds:
            # Read data directly from target bucket (uls) to result dir
            sync_object_directory(awscli_pod, ns_bucket_path, self.MCG_NS_RESULT_DIR,
                                  signed_request_creds=s3_creds)
        else:
            # Read data from NS bucket to result dir
            sync_object_directory(awscli_pod, ns_bucket_path, self.MCG_NS_RESULT_DIR, mcg_obj)

    def compare_dirs(self, awscli_pod, amount=1):
        # Checksum is compared between original and result object
        for i in range(amount):
            file_name = f"testfile{i}.txt"
            assert verify_s3_object_integrity(
                original_object_path=f'{self.MCG_NS_ORIGINAL_DIR}/{file_name}',
                result_object_path=f'{self.MCG_NS_RESULT_DIR}/{file_name}', awscli_pod=awscli_pod
            ), 'Checksum comparision between original and result object failed'
