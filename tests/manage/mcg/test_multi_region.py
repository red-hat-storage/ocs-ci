import logging

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    tier1, aws_platform_required,
    filter_insecure_request_warning, tier4, tier4a
)

from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.utility.utils import TimeoutSampler
from tests.manage.mcg.helpers import retrieve_test_objects_to_pod, sync_object_directory

logger = logging.getLogger(__name__)


@filter_insecure_request_warning
@aws_platform_required
class TestMultiRegion:
    """
    Test the multi region functionality
    """

    @tier1
    @pytest.mark.parametrize(
        argnames="backingstore_amount,policy",
        argvalues=[
            pytest.param(
                *[3, 'Mirror'],
                marks=[pytest.mark.polarion_id("OCS-1599")]
            ),
            pytest.param(
                *[3, 'Spread'],
                marks=[pytest.mark.polarion_id("OCS-2019")]
            ),
        ]
    )
    def test_multiregion_bucket_creation(self, mcg_obj, multiregion_setup_factory, backingstore_amount, policy):
        """
        Test bucket creation using the S3 SDK
        """

        multiregion_bucket_setup = multiregion_setup_factory(backingstore_amount, policy)
        bucket_name = multiregion_bucket_setup[0]
        system_bucket, tier_name, attached_pools = (None,) * 3

        # Make sure that the bucket is up and running
        try:
            for resp in TimeoutSampler(
                30, 3, mcg_obj.s3_get_all_bucket_names
            ):
                if bucket_name in resp:
                    break
                else:
                    logger.info(f'Did not yet find {policy}ed bucket {bucket_name}')
        except TimeoutExpiredError:
            logger.error(f'Could not find bucket {bucket_name}')
            assert False

        # Retrieve the NooBaa system information
        system_state = mcg_obj.send_rpc_query('system_api', 'read_system').json().get('reply')

        # Retrieve the correct bucket's tier name
        for bucket in system_state.get('buckets'):
            if bucket.get('name') == bucket_name:
                tier_name = bucket.get('tiering').get('tiers')[0].get('tier')
                break

        # Retrieved the pools attached to the tier
        for tier in system_state.get('tiers'):
            if tier.get('name') == tier_name:
                attached_pools = tier.get('attached_pools')
                break

        assert len(attached_pools) == backingstore_amount, (
            "Multiregion bucket did not have required backingstores attached"
        )

    @tier4
    @tier4a
    @pytest.mark.polarion_id("OCS-1784")
    def test_multiregion_mirror(self, mcg_obj, awscli_pod, multiregion_setup_factory):
        """
        Test multi-region bucket creation using the S3 SDK
        """

        bucket_name, backingstores = multiregion_setup_factory(3, 'Mirror')

        # Download test objects from the public bucket
        downloaded_objs = retrieve_test_objects_to_pod(awscli_pod, '/aws/original/')

        logger.info(f'Uploading all pod objects to MCG bucket')
        local_testobjs_dir_path = '/aws/original'
        local_temp_path = '/aws/temp'
        mcg_bucket_path = f's3://{bucket_name}'

        # Upload test objects to the NooBucket
        sync_object_directory(awscli_pod, local_testobjs_dir_path, mcg_bucket_path, mcg_obj)

        mcg_obj.check_if_mirroring_is_done(bucket_name)

        for i in range(3):
            # (i+ 1 or 2) % 3 equals to cells (1,2), (0,2), (0,1)
            # Bring buckets down
            toggled_buckets = [backingstores[((i + 1) % 3)]['name'], backingstores[((i + 2) % 3)]['name']]
            logger.info(f'Blocking buckets {toggled_buckets}')
            mcg_obj.toggle_aws_bucket_readwrite(toggled_buckets)
            # Verify integrity of i bucketstore
            # Retrieve all objects from MCG bucket to result dir in Pod
            sync_object_directory(awscli_pod, mcg_bucket_path, local_temp_path, mcg_obj)
            # Checksum is compared between original and result object
            for obj in downloaded_objs:
                assert mcg_obj.verify_s3_object_integrity(
                    original_object_path=f'{local_testobjs_dir_path}/{obj}',
                    result_object_path=f'{local_temp_path}/{obj}', awscli_pod=awscli_pod
                ), 'Checksum comparision between original and result object failed'
            # Clean up the temp dir and bring bucket back up
            awscli_pod.exec_cmd_on_pod(command=f'sh -c \"rm -rf {local_temp_path}/*\"')
            logger.info(f'Freeing buckets {toggled_buckets}')
            mcg_obj.toggle_aws_bucket_readwrite(toggled_buckets, block=False)
