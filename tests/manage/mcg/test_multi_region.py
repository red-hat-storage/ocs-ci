import logging

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    tier1, tier4, tier4a,
    bugzilla, skipif_ocs_version
)
from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import skipif_aws_creds_are_missing
from ocs_ci.ocs.constants import BS_AUTH_FAILED, BS_OPTIMAL
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.ocs.bucket_utils import (
    retrieve_test_objects_to_pod, sync_object_directory,
    verify_s3_object_integrity
)

logger = logging.getLogger(__name__)


@skipif_aws_creds_are_missing
class TestMultiRegion:
    """
    Test the multi region functionality
    """

    @tier1
    @pytest.mark.polarion_id("OCS-1599")
    def test_multiregion_bucket_creation(self, mcg_obj, multiregion_mirror_setup):
        """
        Test bucket creation using the S3 SDK
        """

        mirrored_bucket_name = multiregion_mirror_setup[0].name
        system_bucket, mirror_tier_name, mirror_attached_pools = (None,) * 3

        # Make sure that the bucket is up and running
        try:
            for resp in TimeoutSampler(
                30, 3, mcg_obj.s3_get_all_bucket_names
            ):
                if mirrored_bucket_name in resp:
                    break
                else:
                    logger.info(f'Did not yet find mirrored bucket {mirrored_bucket_name}')
        except TimeoutExpiredError:
            logger.error(f'Could not find bucket {mirrored_bucket_name}')
            assert False

        # Retrieve the NooBaa system information
        system_state = mcg_obj.send_rpc_query('system_api', 'read_system').json().get('reply')

        # Retrieve the correct bucket's tier name
        for bucket in system_state.get('buckets'):
            if bucket.get('name') == mirrored_bucket_name:
                mirror_tier_name = bucket.get('tiering').get('tiers')[0].get('tier')
                break

        # Retrieved the pools attached to the tier
        for tier in system_state.get('tiers'):
            if tier.get('name') == mirror_tier_name:
                mirror_attached_pools = tier.get('attached_pools')
                break

        assert len(mirror_attached_pools) == 2, "Multiregion bucket did not have two backingstores attached"

    @tier4
    @tier4a
    @bugzilla('1827317')
    @skipif_ocs_version("==4.4")
    @pytest.mark.polarion_id("OCS-1784")
    def test_multiregion_mirror(self, cld_mgr, mcg_obj, awscli_pod, multiregion_mirror_setup):
        """
        Test multi-region bucket creation using the S3 SDK
        """

        bucket, backingstores = multiregion_mirror_setup
        backingstore1 = backingstores[0]
        backingstore2 = backingstores[1]

        bucket_name = bucket.name
        aws_client = cld_mgr.aws_client

        # Download test objects from the public bucket
        downloaded_objs = retrieve_test_objects_to_pod(awscli_pod, '/aws/original/')

        logger.info('Uploading all pod objects to MCG bucket')
        local_testobjs_dir_path = '/aws/original'
        local_temp_path = '/aws/temp'
        mcg_bucket_path = f's3://{bucket_name}'

        sync_object_directory(awscli_pod, 's3://' + constants.TEST_FILES_BUCKET, local_testobjs_dir_path)

        # Upload test objects to the NooBucket
        sync_object_directory(awscli_pod, local_testobjs_dir_path, mcg_bucket_path, mcg_obj)

        mcg_obj.check_if_mirroring_is_done(bucket_name)

        # Bring bucket A down
        aws_client.toggle_aws_bucket_readwrite(backingstore1.uls_name)
        mcg_obj.check_backingstore_state('backing-store-' + backingstore1.name, BS_AUTH_FAILED)

        # Verify integrity of B
        # Retrieve all objects from MCG bucket to result dir in Pod
        sync_object_directory(awscli_pod, mcg_bucket_path, local_temp_path, mcg_obj)

        # Checksum is compared between original and result object
        for obj in downloaded_objs:
            assert verify_s3_object_integrity(
                original_object_path=f'{local_testobjs_dir_path}/{obj}',
                result_object_path=f'{local_temp_path}/{obj}', awscli_pod=awscli_pod
            ), 'Checksum comparision between original and result object failed'

        # Clean up the temp dir
        awscli_pod.exec_cmd_on_pod(command=f'sh -c \"rm -rf {local_temp_path}/*\"')

        # Bring B down, bring A up
        logger.info('Blocking bucket B')
        aws_client.toggle_aws_bucket_readwrite(backingstore2.uls_name)
        logger.info('Freeing bucket A')
        aws_client.toggle_aws_bucket_readwrite(backingstore1.uls_name, block=False)
        mcg_obj.check_backingstore_state('backing-store-' + backingstore1.name, BS_OPTIMAL)
        mcg_obj.check_backingstore_state('backing-store-' + backingstore2.name, BS_AUTH_FAILED)

        # Verify integrity of A
        # Retrieve all objects from MCG bucket to result dir in Pod
        sync_object_directory(awscli_pod, mcg_bucket_path, local_temp_path, mcg_obj)

        # Checksum is compared between original and result object
        for obj in downloaded_objs:
            assert verify_s3_object_integrity(
                original_object_path=f'{local_testobjs_dir_path}/{obj}',
                result_object_path=f'{local_temp_path}/{obj}', awscli_pod=awscli_pod
            ), 'Checksum comparision between original and result object failed'
        # Bring B up
        aws_client.toggle_aws_bucket_readwrite(backingstore2.uls_name, block=False)
        mcg_obj.check_backingstore_state('backing-store-' + backingstore2.name, BS_OPTIMAL)
