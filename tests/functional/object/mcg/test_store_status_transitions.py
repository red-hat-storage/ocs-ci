import logging

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    mcg,
    red_squad,
    runs_on_provider,
    skipif_aws_creds_are_missing,
    skipif_disconnected_cluster,
    tier4,
)
from ocs_ci.framework.testlib import MCGTest
from ocs_ci.ocs import constants
from ocs_ci.ocs.bucket_utils import assert_store_phase_and_mode

logger = logging.getLogger(__name__)


@mcg
@red_squad
@tier4
@runs_on_provider
@skipif_disconnected_cluster
class TestStoreStatusTransitions(MCGTest):
    """
    Tests for backingstore and namespacestore status transitions when
    the underlying storage is disrupted.

    Validates that stores correctly transition from Ready to Rejected
    when access to the underlying bucket is denied or the bucket is
    deleted, and recover when the disruption is resolved.

    Covers the fix for noobaa-core PR #9709 (AWS SDK v3 error property
    casing bug in block_store_s3.js).
    """

    @skipif_aws_creds_are_missing
    @pytest.mark.parametrize(
        "store_type",
        [
            pytest.param("backingstore", marks=pytest.mark.polarion_id("OCS-8041")),
            pytest.param(
                "namespacestore",
                marks=[
                    pytest.mark.polarion_id("OCS-8042"),
                    pytest.mark.skip(
                        reason="namespace_monitor health check does not "
                        "detect bucket policy denials (DFBUGS-8507)"
                    ),
                ],
            ),
        ],
    )
    def test_store_status_on_access_denied(
        self,
        mcg_obj,
        cld_mgr,
        bucket_factory,
        store_type,
    ):
        """
        Verify that a store transitions to Rejected/AUTH_FAILED when
        access to its underlying bucket is denied via a bucket policy,
        and recovers to Ready/OPTIMAL when access is restored.

        Steps:
            1. Create an AWS-backed store via bucket_factory
            2. Verify the store is in Ready phase and OPTIMAL mode
            3. Apply a Deny bucket policy to block access
            4. Verify the store transitions to Rejected/AUTH_FAILED
            5. Remove the Deny bucket policy to restore access
            6. Verify the store recovers to Ready/OPTIMAL
        """
        store_kind = (
            constants.BACKINGSTORE
            if store_type == "backingstore"
            else constants.NAMESPACESTORE
        )

        # 1. Create an AWS-backed store via bucket_factory
        if store_type == "backingstore":
            bucketclass_dict = {
                "interface": "CLI",
                "backingstore_dict": {"aws": [(1, constants.AWS_REGION)]},
            }
        else:
            bucketclass_dict = {
                "interface": "CLI",
                "namespace_policy_dict": {
                    "type": "Single",
                    "namespacestore_dict": {"aws": [(1, constants.AWS_REGION)]},
                },
            }
        bucket = bucket_factory(1, "OC", bucketclass=bucketclass_dict)[0]
        bc = bucket.bucketclass
        store = (
            bc.backingstores[0]
            if store_type == "backingstore"
            else bc.namespacestores[0]
        )
        aws_client = cld_mgr.aws_client
        logger.info(
            f"Created {store_type} {store.name} backed by "
            f"AWS bucket {store.uls_name}"
        )

        # 2. Verify the store is in Ready phase and OPTIMAL mode
        assert_store_phase_and_mode(
            store.name,
            store_kind,
            constants.STATUS_READY,
            constants.BS_OPTIMAL,
            mcg_obj,
        )

        # 3. Apply a Deny bucket policy to block access
        logger.info(f"Blocking access to underlying bucket {store.uls_name}")
        aws_client.toggle_aws_bucket_readwrite(store.uls_name)
        try:
            # 4. Verify the store transitions to Rejected/AUTH_FAILED
            # namespace_monitor polls every 3 min (NAMESPACE_MONITOR_DELAY)
            disruption_timeout = 300 if store_type == "namespacestore" else 180
            assert_store_phase_and_mode(
                store.name,
                store_kind,
                constants.STATUS_REJECTED,
                constants.BS_AUTH_FAILED,
                mcg_obj,
                timeout=disruption_timeout,
            )
        finally:
            # 5. Remove the Deny bucket policy to restore access
            logger.info(f"Restoring access to underlying bucket {store.uls_name}")
            aws_client.toggle_aws_bucket_readwrite(store.uls_name, block=False)

        # 6. Verify the store recovers to Ready/OPTIMAL
        # namespace_monitor polls less frequently than nodes_monitor
        recovery_timeout = 300 if store_type == "namespacestore" else 180
        assert_store_phase_and_mode(
            store.name,
            store_kind,
            constants.STATUS_READY,
            constants.BS_OPTIMAL,
            mcg_obj,
            timeout=recovery_timeout,
        )

    @pytest.mark.parametrize(
        "store_type",
        [
            pytest.param("backingstore", marks=pytest.mark.polarion_id("OCS-8043")),
            pytest.param("namespacestore", marks=pytest.mark.polarion_id("OCS-8044")),
        ],
    )
    def test_store_status_on_underlying_bucket_deletion(
        self,
        mcg_obj,
        bucket_factory,
        store_type,
    ):
        """
        Verify that a store transitions to Rejected/STORAGE_NOT_EXIST
        when its underlying bucket is deleted, and recovers to
        Ready/OPTIMAL when the bucket is recreated.

        Steps:
            1. Create a store backed by a self-ref MCG bucket
            2. Verify the store is in Ready phase and OPTIMAL mode
            3. Delete the underlying MCG bucket
            4. Verify the store transitions to Rejected/STORAGE_NOT_EXIST
            5. Recreate the underlying MCG bucket
            6. Verify the store recovers to Ready/OPTIMAL
        """
        store_kind = (
            constants.BACKINGSTORE
            if store_type == "backingstore"
            else constants.NAMESPACESTORE
        )

        # 1. Create a store backed by a self-ref MCG bucket
        if store_type == "backingstore":
            bucketclass_dict = {
                "interface": "CLI",
                "backingstore_dict": {"self-ref-mcg": [(1, None)]},
            }
        else:
            bucketclass_dict = {
                "interface": "CLI",
                "namespace_policy_dict": {
                    "type": "Single",
                    "namespacestore_dict": {"self-ref-mcg": [(1, None)]},
                },
            }
        bucket = bucket_factory(1, "OC", bucketclass=bucketclass_dict)[0]
        bc = bucket.bucketclass
        store = (
            bc.backingstores[0]
            if store_type == "backingstore"
            else bc.namespacestores[0]
        )
        uls_name = store.uls_name
        logger.info(
            f"Created {store_type} {store.name} backed by "
            f"self-ref MCG bucket {uls_name}"
        )

        # 2. Verify the store is in Ready phase and OPTIMAL mode
        assert_store_phase_and_mode(
            store.name,
            store_kind,
            constants.STATUS_READY,
            constants.BS_OPTIMAL,
            mcg_obj,
        )

        # 3. Delete the underlying MCG bucket
        logger.info(f"Deleting underlying MCG bucket: {uls_name}")
        mcg_obj.s3_resource.Bucket(uls_name).objects.all().delete()
        mcg_obj.s3_resource.Bucket(uls_name).delete()
        try:
            # 4. Verify the store transitions to Rejected/STORAGE_NOT_EXIST
            # namespace_monitor polls every 3 min (NAMESPACE_MONITOR_DELAY)
            disruption_timeout = 300 if store_type == "namespacestore" else 180
            assert_store_phase_and_mode(
                store.name,
                store_kind,
                constants.STATUS_REJECTED,
                constants.BS_STORAGE_NOT_EXIST,
                mcg_obj,
                timeout=disruption_timeout,
            )
        finally:
            # 5. Recreate the underlying MCG bucket
            logger.info(f"Recreating underlying MCG bucket: {uls_name}")
            mcg_obj.s3_resource.create_bucket(Bucket=uls_name)

        # 6. Verify the store recovers to Ready/OPTIMAL
        # namespace_monitor polls less frequently than nodes_monitor
        recovery_timeout = 300 if store_type == "namespacestore" else 180
        assert_store_phase_and_mode(
            store.name,
            store_kind,
            constants.STATUS_READY,
            constants.BS_OPTIMAL,
            mcg_obj,
            timeout=recovery_timeout,
        )
