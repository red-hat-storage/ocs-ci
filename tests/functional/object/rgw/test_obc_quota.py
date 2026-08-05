import logging
import pytest
import time

from ocs_ci.ocs import constants
from ocs_ci.utility.prometheus import PrometheusAPI
from ocs_ci.ocs.resources.objectbucket import OBC
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.bucket_utils import (
    copy_random_individual_objects,
    rm_object_recursive,
    write_random_test_objects_to_bucket,
)
from ocs_ci.ocs.exceptions import CommandFailed, TimeoutExpiredError
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.framework.pytest_customization.marks import (
    tier1,
    tier2,
    skipif_ocs_version,
    skipif_mcg_only,
    red_squad,
    rgw,
    runs_on_provider,
    polarion_id,
)

logger = logging.getLogger(__name__)


@rgw
@red_squad
@runs_on_provider
@skipif_ocs_version("<4.10")
@skipif_mcg_only
class TestOBCQuota:
    """
    Test OBC Quota feature
    """

    @tier1
    @pytest.mark.parametrize(
        argnames="amount,interface,quota",
        argvalues=[
            pytest.param(
                *[1, "RGW-OC", {"maxObjects": "1", "maxSize": "50M"}],
                marks=[
                    pytest.mark.polarion_id("OCS-3904"),
                ],
            ),
        ],
    )
    def test_rgw_obc_quota(
        self,
        awscli_pod_session,
        rgw_bucket_factory,
        test_directory_setup,
        mcg_obj_session,
        amount,
        interface,
        quota,
    ):
        """
        Test OBC quota feature
            * Create OBC with maxObjects quota set
            * Write objects exceeding maxObjects, verify QuotaExceeded
            * Patch maxObjects higher, verify additional writes succeed
            * Decrease maxObjects below current usage, verify writes are
              blocked (any CommandFailed accepted — RGW may return errors
              other than QuotaExceeded when quota is reduced below usage)
        """
        bucket_name = rgw_bucket_factory(amount, interface, quota=quota)[0].name
        obc_obj = OBC(bucket_name)
        full_bucket_path = f"s3://{bucket_name}"
        amount = int(quota["maxObjects"]) + 1
        test_dir = test_directory_setup.result_dir
        err_msg = "(QuotaExceeded)"
        try:
            copy_random_individual_objects(
                awscli_pod_session,
                pattern="object-",
                file_dir=test_dir,
                target=full_bucket_path,
                amount=amount,
                s3_obj=obc_obj,
                ignore_error=False,
            )
        except CommandFailed as e:
            logger.info(f"Quota {quota} blocked writes as expected: {e}")
        else:
            assert (
                False
            ), "Quota didnt work!! Since more than maximum number of objects were written to the bucket!"

        # Patch the OBC to change the quota
        new_quota = 4
        new_quota_str = '{"spec": {"additionalConfig":{"maxObjects": "4"}}}'
        cmd = f"patch obc {bucket_name} -p '{new_quota_str}' -n openshift-storage --type=merge"
        OCP().exec_oc_cmd(cmd)
        logger.info(f"Patched new quota to obc {bucket_name}")

        # wait for few seconds to make sure the quota really gets applied
        time.sleep(20)

        # check if the new quota applied works
        amount = new_quota - int(quota["maxObjects"])
        try:
            copy_random_individual_objects(
                awscli_pod_session,
                pattern="new-object-",
                file_dir=test_dir,
                target=full_bucket_path,
                amount=amount,
                s3_obj=obc_obj,
                ignore_error=False,
            )
        except CommandFailed as e:
            if err_msg in str(e):
                assert False, f"New quota {new_quota_str} didn't get applied!!"
            else:
                logger.error(f"Copy objects to bucket failed unexpectedly: {e}")
        else:
            logger.info(f"New quota {new_quota_str} got applied!!")

        # Decrease maxObjects below current usage and verify writes are blocked
        decreased_quota = 2
        decreased_quota_str = (
            f'{{"spec": {{"additionalConfig":{{"maxObjects": "{decreased_quota}"}}}}}}'
        )
        cmd = f"patch obc {bucket_name} -p '{decreased_quota_str}' -n openshift-storage --type=merge"
        OCP().exec_oc_cmd(cmd)
        logger.info(
            f"Decreased maxObjects to {decreased_quota} (below current usage) on obc {bucket_name}"
        )
        time.sleep(20)

        try:
            copy_random_individual_objects(
                awscli_pod_session,
                pattern="decreased-object-",
                file_dir=test_dir,
                target=full_bucket_path,
                amount=1,
                s3_obj=obc_obj,
                ignore_error=False,
            )
        except CommandFailed as e:
            logger.info(
                f"Decreased maxObjects quota to {decreased_quota} blocked writes "
                f"as expected: {e}"
            )
        else:
            assert False, (
                f"Decreased maxObjects to {decreased_quota} below current usage "
                "but writes still succeeded!!"
            )

    @tier1
    @pytest.mark.parametrize(
        argnames="amount,interface,quota",
        argvalues=[
            pytest.param(
                *[1, "RGW-OC", {"maxSize": "5M", "maxObjects": "100"}],
                marks=[
                    pytest.mark.polarion_id("OCS-8081"),
                ],
            ),
        ],
    )
    def test_rgw_obc_size_quota(
        self,
        awscli_pod_session,
        rgw_bucket_factory,
        test_directory_setup,
        mcg_obj_session,
        amount,
        interface,
        quota,
    ):
        """
        Test RGW OBC maxSize quota enforcement
            * Create OBC with a size quota (maxSize) set
            * Write objects until the size quota is exceeded (QuotaExceeded)
            * Increase the maxSize quota via oc patch
            * Verify that additional writes succeed under the new limit
        """
        bucket_name = rgw_bucket_factory(amount, interface, quota=quota)[0].name
        obc_obj = OBC(bucket_name)
        full_bucket_path = f"s3://{bucket_name}"
        test_dir = test_directory_setup.result_dir
        err_msg = "(QuotaExceeded)"

        # Upload 1MB objects one-by-one until maxSize (5MB) is exceeded
        upload_amount = 10
        try:
            copy_random_individual_objects(
                awscli_pod_session,
                pattern="object-",
                file_dir=test_dir,
                target=full_bucket_path,
                amount=upload_amount,
                s3_obj=obc_obj,
                ignore_error=False,
            )
        except CommandFailed as e:
            logger.info(f"Size quota {quota} blocked writes as expected: {e}")
        else:
            assert (
                False
            ), "Size quota didn't work!! All objects were written without exceeding maxSize!"

        # Patch the OBC to increase maxSize
        new_max_size = "20M"
        new_quota_str = (
            f'{{"spec": {{"additionalConfig":{{"maxSize": "{new_max_size}"}}}}}}'
        )
        cmd = f"patch obc {bucket_name} -p '{new_quota_str}' -n openshift-storage --type=merge"
        OCP().exec_oc_cmd(cmd)
        logger.info(f"Patched maxSize quota to {new_max_size} on obc {bucket_name}")

        # Wait for the new quota to propagate to RGW
        time.sleep(20)

        # Verify writes succeed under the new limit
        post_patch_amount = 5
        try:
            copy_random_individual_objects(
                awscli_pod_session,
                pattern="new-object-",
                file_dir=test_dir,
                target=full_bucket_path,
                amount=post_patch_amount,
                s3_obj=obc_obj,
                ignore_error=False,
            )
        except CommandFailed as e:
            if err_msg in str(e):
                assert False, f"New maxSize quota {new_max_size} didn't get applied!!"
            else:
                logger.error(f"Copy objects to bucket failed unexpectedly: {e}")
                raise
        else:
            logger.info(f"New maxSize quota {new_max_size} got applied!!")

    @tier2
    @pytest.mark.parametrize(
        argnames="amount,interface,quota",
        argvalues=[
            pytest.param(
                *[1, "RGW-OC", {"maxObjects": "5", "maxSize": "5M"}],
                marks=[
                    pytest.mark.polarion_id("OCS-8083"),
                ],
            ),
        ],
    )
    def test_rgw_obc_combined_quota(
        self,
        awscli_pod_session,
        rgw_bucket_factory,
        test_directory_setup,
        mcg_obj_session,
        amount,
        interface,
        quota,
    ):
        """
        Test RGW OBC combined maxObjects and maxSize quota enforcement
            * Create OBC with both maxObjects and maxSize set
            * Write objects until both quotas are hit
            * Patch only maxSize higher, verify writes still blocked by maxObjects
            * Patch only maxObjects higher, verify writes still blocked by maxSize
            * Patch both quotas higher, verify writes succeed
        """
        bucket_name = rgw_bucket_factory(amount, interface, quota=quota)[0].name
        obc_obj = OBC(bucket_name)
        full_bucket_path = f"s3://{bucket_name}"
        test_dir = test_directory_setup.result_dir
        err_msg = "(QuotaExceeded)"

        # Write objects to hit both limits (5 x 1MB = 5MB and 5 objects)
        max_objects = int(quota["maxObjects"])
        upload_amount = max_objects + 1
        try:
            copy_random_individual_objects(
                awscli_pod_session,
                pattern="object-",
                file_dir=test_dir,
                target=full_bucket_path,
                amount=upload_amount,
                s3_obj=obc_obj,
                ignore_error=False,
            )
        except CommandFailed as e:
            logger.info(f"Both quotas blocked writes as expected: {e}")
        else:
            assert False, "Combined quota didn't work!! All objects were written!!"

        # Patch only maxSize higher — writes should still be blocked by maxObjects
        patch_str = '{"spec": {"additionalConfig":{"maxSize": "20M"}}}'
        cmd = f"patch obc {bucket_name} -p '{patch_str}' -n openshift-storage --type=merge"
        OCP().exec_oc_cmd(cmd)
        logger.info(f"Patched only maxSize to 20M on obc {bucket_name}")

        def check_write_blocked_by_max_objects():
            try:
                copy_random_individual_objects(
                    awscli_pod_session,
                    pattern="size-patched-",
                    file_dir=test_dir,
                    target=full_bucket_path,
                    amount=1,
                    s3_obj=obc_obj,
                    ignore_error=False,
                )
                return False
            except CommandFailed:
                return True

        try:
            for blocked in TimeoutSampler(
                timeout=120, sleep=20, func=check_write_blocked_by_max_objects
            ):
                if blocked:
                    logger.info(
                        "Write blocked by maxObjects as expected after "
                        "patching only maxSize"
                    )
                    break
                logger.info(
                    "Write succeeded — RGW hasn't re-enforced "
                    "maxObjects yet, retrying..."
                )
        except TimeoutExpiredError:
            assert False, (
                "Write never got blocked by maxObjects after patching "
                "only maxSize — waited 120s"
            )

        # Patch only maxObjects higher (reset maxSize back to original) —
        # writes should still be blocked by maxSize
        patch_str = (
            '{"spec": {"additionalConfig":{"maxObjects": "20", "maxSize": "5M"}}}'
        )
        cmd = f"patch obc {bucket_name} -p '{patch_str}' -n openshift-storage --type=merge"
        OCP().exec_oc_cmd(cmd)
        logger.info(
            f"Patched maxObjects to 20 and maxSize back to 5M on obc {bucket_name}"
        )

        def check_write_blocked_by_max_size():
            try:
                copy_random_individual_objects(
                    awscli_pod_session,
                    pattern="obj-patched-",
                    file_dir=test_dir,
                    target=full_bucket_path,
                    amount=1,
                    s3_obj=obc_obj,
                    ignore_error=False,
                )
                return False
            except CommandFailed:
                return True

        try:
            for blocked in TimeoutSampler(
                timeout=120, sleep=20, func=check_write_blocked_by_max_size
            ):
                if blocked:
                    logger.info(
                        "Write blocked by maxSize as expected after "
                        "patching only maxObjects"
                    )
                    break
                logger.info(
                    "Write succeeded — RGW hasn't re-enforced "
                    "maxSize yet, retrying..."
                )
        except TimeoutExpiredError:
            assert False, (
                "Write never got blocked by maxSize after patching "
                "only maxObjects — waited 120s"
            )

        # Patch both quotas higher — writes should succeed
        patch_str = (
            '{"spec": {"additionalConfig":{"maxObjects": "20", "maxSize": "20M"}}}'
        )
        cmd = f"patch obc {bucket_name} -p '{patch_str}' -n openshift-storage --type=merge"
        OCP().exec_oc_cmd(cmd)
        logger.info(f"Patched both quotas higher on obc {bucket_name}")
        time.sleep(20)

        try:
            copy_random_individual_objects(
                awscli_pod_session,
                pattern="both-patched-",
                file_dir=test_dir,
                target=full_bucket_path,
                amount=3,
                s3_obj=obc_obj,
                ignore_error=False,
            )
        except CommandFailed as e:
            if err_msg in str(e):
                assert False, "Both quotas were increased but writes still blocked!!"
            else:
                logger.error(f"Copying objects to bucket failed unexpectedly: {e}")
                raise
        else:
            logger.info("Writes succeeded after patching both quotas!!")

    @tier2
    @pytest.mark.polarion_id("OCS-8084")
    def test_rgw_obc_multi_bucket_quota(
        self,
        awscli_pod_session,
        rgw_bucket_factory,
        test_directory_setup,
        mcg_obj_session,
    ):
        """
        Test RGW OBC quota with multiple buckets having different quota configs
            * Create 4 RGW buckets: no quota, maxSize only, maxObjects only, both
            * Write objects to exceed quotas on the 3 quota-limited buckets
            * Verify no-quota bucket remains writable
            * Remove objects from quota-limited buckets
            * Verify all 4 buckets are writable again
        """
        interface = "RGW-OC"
        test_dir = test_directory_setup.result_dir

        # Create 4 buckets with different quota configurations
        bucket_no_quota = rgw_bucket_factory(1, interface)[0].name
        bucket_max_size = rgw_bucket_factory(1, interface, quota={"maxSize": "5M"})[
            0
        ].name
        bucket_max_objects = rgw_bucket_factory(
            1, interface, quota={"maxObjects": "5"}
        )[0].name
        bucket_both = rgw_bucket_factory(
            1, interface, quota={"maxObjects": "5", "maxSize": "5M"}
        )[0].name

        obc_no_quota = OBC(bucket_no_quota)
        obc_max_size = OBC(bucket_max_size)
        obc_max_objects = OBC(bucket_max_objects)
        obc_both = OBC(bucket_both)

        logger.info(
            f"Created 4 buckets: no_quota={bucket_no_quota}, "
            f"max_size={bucket_max_size}, max_objects={bucket_max_objects}, "
            f"both={bucket_both}"
        )

        # Exceed maxSize quota (upload 10 x 1MB objects, expect failure around 6th)
        try:
            copy_random_individual_objects(
                awscli_pod_session,
                pattern="size-obj-",
                file_dir=test_dir,
                target=f"s3://{bucket_max_size}",
                amount=10,
                s3_obj=obc_max_size,
                ignore_error=False,
            )
        except CommandFailed as e:
            logger.info(
                f"maxSize quota blocked writes on {bucket_max_size} "
                f"as expected: {e}"
            )
        else:
            assert False, f"maxSize quota not enforced on {bucket_max_size}!!"

        # Exceed maxObjects quota (upload 6 objects, expect failure at 6th)
        try:
            copy_random_individual_objects(
                awscli_pod_session,
                pattern="obj-count-",
                file_dir=test_dir,
                target=f"s3://{bucket_max_objects}",
                amount=6,
                s3_obj=obc_max_objects,
                ignore_error=False,
            )
        except CommandFailed as e:
            logger.info(
                f"maxObjects quota blocked writes on {bucket_max_objects} "
                f"as expected: {e}"
            )
        else:
            assert False, f"maxObjects quota not enforced on {bucket_max_objects}!!"

        # Exceed combined quota (upload 6 objects, expect failure)
        try:
            copy_random_individual_objects(
                awscli_pod_session,
                pattern="both-obj-",
                file_dir=test_dir,
                target=f"s3://{bucket_both}",
                amount=6,
                s3_obj=obc_both,
                ignore_error=False,
            )
        except CommandFailed as e:
            logger.info(
                f"Combined quota blocked writes on {bucket_both} " f"as expected: {e}"
            )
        else:
            assert False, f"Combined quota not enforced on {bucket_both}!!"

        # Verify no-quota bucket is still writable
        try:
            copy_random_individual_objects(
                awscli_pod_session,
                pattern="no-quota-obj-",
                file_dir=test_dir,
                target=f"s3://{bucket_no_quota}",
                amount=3,
                s3_obj=obc_no_quota,
                ignore_error=False,
            )
        except CommandFailed:
            assert (
                False
            ), f"No-quota bucket {bucket_no_quota} should be writable but failed!!"
        else:
            logger.info(f"No-quota bucket {bucket_no_quota} is writable as expected!!")

        # Remove all objects from quota-limited buckets
        rm_object_recursive(awscli_pod_session, bucket_max_size, obc_max_size)
        logger.info(f"Removed all objects from {bucket_max_size}")
        rm_object_recursive(awscli_pod_session, bucket_max_objects, obc_max_objects)
        logger.info(f"Removed all objects from {bucket_max_objects}")
        rm_object_recursive(awscli_pod_session, bucket_both, obc_both)
        logger.info(f"Removed all objects from {bucket_both}")

        # Wait for RGW to recalculate quotas after object removal
        time.sleep(20)

        # Verify all 4 buckets are writable again
        for name, obc_obj, label in [
            (bucket_no_quota, obc_no_quota, "no-quota"),
            (bucket_max_size, obc_max_size, "max-size"),
            (bucket_max_objects, obc_max_objects, "max-objects"),
            (bucket_both, obc_both, "both"),
        ]:
            try:
                copy_random_individual_objects(
                    awscli_pod_session,
                    pattern=f"post-rm-{label}-",
                    file_dir=test_dir,
                    target=f"s3://{name}",
                    amount=1,
                    s3_obj=obc_obj,
                    ignore_error=False,
                )
            except CommandFailed:
                assert False, (
                    f"Bucket {name} ({label}) should be writable after "
                    "removing objects but writes failed!!"
                )
            else:
                logger.info(
                    f"Bucket {name} ({label}) is writable after removing objects!!"
                )

    @tier2
    @polarion_id("OCS-6178")
    @pytest.mark.parametrize(
        argnames="amount,interface,quota",
        argvalues=[
            pytest.param(
                *[1, "RGW-OC", {"maxObjects": "10"}],
            ),
        ],
    )
    def test_obc_quota_full_alert(
        self,
        rgw_bucket_factory,
        rgw_obj_session,
        awscli_pod_session,
        test_directory_setup,
        threading_lock,
        amount,
        interface,
        quota,
    ):
        """
        Test OBC object count quota Prometheus alert at 80% threshold
            * Create OBC with an object count quota (maxObjects=10) set
            * Write objects to ~90% of maxObjects capacity
            * Wait for ObcQuotaObjectsAlert (80% warning) to fire and verify
            * Verify alert description matches expected format
        """

        # create the bucket
        bucket_name = rgw_bucket_factory(amount, interface, quota=quota)[0].name
        logger.info(f"created rgw bucket {bucket_name} with quota {quota}")

        # fill the bucket with about 90% of maxObjects
        number_of_objects = int(quota["maxObjects"])
        write_random_test_objects_to_bucket(
            awscli_pod_session,
            bucket_name,
            test_directory_setup.origin_dir,
            amount=(number_of_objects * 90) // 100,
            mcg_obj=OBC(bucket_name),
        )
        logger.info(f"Filled bucket {bucket_name} with 90% maxObjects capacity")

        # wait for obc full alert to occur and verify
        prometheus = PrometheusAPI(threading_lock=threading_lock)
        alerts = [
            alert
            for alert in prometheus.wait_for_alert(
                name=constants.ALERT_OBC_QUOTA_OBJECTS_ALERT,
                state="firing",
                timeout=600,
            )
            if alert.get("labels").get("objectbucketclaim") == bucket_name
        ]

        assert len(alerts) > 0, (
            f"Alert {constants.ALERT_OBC_QUOTA_OBJECTS_ALERT} doesn't seem to occur "
            f"despite the bucket being 90% full"
        )

        alert_desc = (
            f"ObjectBucketClaim {bucket_name} has crossed 80% "
            f"of the size limit set by the quota(objects)"
        )
        for alert in alerts:
            assert alert_desc in alert.get("annotations").get(
                "description"
            ), f"Alert {constants.ALERT_OBC_QUOTA_OBJECTS_ALERT} doesn't seem have expected format"
        logger.info(f"Verified the alert {constants.ALERT_OBC_QUOTA_OBJECTS_ALERT}")

    @tier2
    @pytest.mark.parametrize(
        argnames="amount,interface,quota",
        argvalues=[
            pytest.param(
                *[1, "RGW-OC", {"maxSize": "10M"}],
                marks=[
                    pytest.mark.polarion_id("OCS-8082"),
                ],
            ),
        ],
    )
    def test_obc_quota_size_alert(
        self,
        rgw_bucket_factory,
        rgw_obj_session,
        awscli_pod_session,
        test_directory_setup,
        threading_lock,
        amount,
        interface,
        quota,
    ):
        """
        Test OBC size quota Prometheus alerts at 80% and 100% thresholds
            * Create OBC with a size quota (maxSize=10M) set
            * Write data to ~90% of maxSize capacity
            * Wait for ObcQuotaBytesAlert (80% warning) to fire and verify
            * Reduce maxSize below current usage to push ratio above 100%
            * Wait for ObcQuotaBytesExhausedAlert (100% critical) to fire
            * Verify alert description matches expected format
        """

        bucket_name = rgw_bucket_factory(amount, interface, quota=quota)[0].name
        logger.info(f"created rgw bucket {bucket_name} with quota {quota}")

        # Fill the bucket with ~90% of maxSize (9 x 1MB objects for 10MB quota)
        max_size_mb = int(quota["maxSize"].rstrip("M"))
        fill_amount = (max_size_mb * 90) // 100
        write_random_test_objects_to_bucket(
            awscli_pod_session,
            bucket_name,
            test_directory_setup.origin_dir,
            amount=fill_amount,
            mcg_obj=OBC(bucket_name),
        )
        logger.info(f"Filled bucket {bucket_name} with ~90% maxSize capacity")

        # Wait for ObcQuotaBytesAlert to fire and verify
        prometheus = PrometheusAPI(threading_lock=threading_lock)
        alerts = [
            alert
            for alert in prometheus.wait_for_alert(
                name=constants.ALERT_OBC_QUOTA_BYTES_ALERT,
                state="firing",
                timeout=600,
            )
            if alert.get("labels").get("objectbucketclaim") == bucket_name
        ]

        assert len(alerts) > 0, (
            f"Alert {constants.ALERT_OBC_QUOTA_BYTES_ALERT} doesn't seem to occur "
            f"despite the bucket being ~90% full by size"
        )

        alert_desc = (
            f"ObjectBucketClaim {bucket_name} has crossed 80% "
            f"of the size limit set by the quota(bytes)"
        )
        for alert in alerts:
            assert alert_desc in alert.get("annotations").get(
                "description"
            ), f"Alert {constants.ALERT_OBC_QUOTA_BYTES_ALERT} doesn't seem have expected format"
        logger.info(f"Verified the alert {constants.ALERT_OBC_QUOTA_BYTES_ALERT}")

        # Phase 2: Reduce quota below current usage to trigger exhaustion alert.
        # RGW enforces maxSize strictly, so writes can't push past 100%.
        # Instead, patch maxSize down so current usage exceeds the new limit.
        reduced_max_size = fill_amount // 2
        patch_str = (
            f'{{"spec": {{"additionalConfig":{{"maxSize": "{reduced_max_size}M"}}}}}}'
        )
        cmd = (
            f"patch obc {bucket_name} -p '{patch_str}' "
            f"-n openshift-storage --type=merge"
        )
        OCP().exec_oc_cmd(cmd)
        logger.info(
            f"Reduced maxSize from {max_size_mb}M to {reduced_max_size}M "
            f"on bucket {bucket_name} to trigger exhaustion"
        )

        exhausted_alerts = [
            alert
            for alert in prometheus.wait_for_alert(
                name=constants.ALERT_OBC_QUOTA_BYTES_EXHAUSED_ALERT,
                state="firing",
                timeout=600,
            )
            if alert.get("labels").get("objectbucketclaim") == bucket_name
        ]

        assert len(exhausted_alerts) > 0, (
            f"Alert {constants.ALERT_OBC_QUOTA_BYTES_EXHAUSED_ALERT} didn't fire "
            f"despite the bucket exceeding 100% of maxSize"
        )

        exhausted_desc = (
            f"ObjectBucketClaim {bucket_name} has crossed the limit "
            f"set by the quota(bytes) and will be read-only now"
        )
        for alert in exhausted_alerts:
            assert exhausted_desc in alert.get("annotations").get("description"), (
                f"Alert {constants.ALERT_OBC_QUOTA_BYTES_EXHAUSED_ALERT} "
                f"doesn't have expected format"
            )
        logger.info(
            f"Verified the alert {constants.ALERT_OBC_QUOTA_BYTES_EXHAUSED_ALERT}"
        )
