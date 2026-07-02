import logging

import botocore.exceptions as boto3exception
import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    mcg,
    red_squad,
    runs_on_provider,
    tier2,
)
from ocs_ci.ocs.bucket_utils import (
    get_bucket_tagging,
    get_noobaa_bucket_tagging_metric_results,
    verify_bucket_tagging_matches_labels,
    verify_noobaa_bucket_tagging_metric,
)
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.objectbucket import OBC

logger = logging.getLogger(__name__)

NOOBAA_BUCKET_TAGGING_METRIC = "NooBaa_bucket_tagging"
OBC_LABEL_KEY = "test-label"
OBC_LABEL_VALUE = "verified"
# NooBaa stats aggregator refreshes bucket metrics about every 5 minutes.
NOOBAA_BUCKET_TAGGING_METRIC_TIMEOUT = 360
NOOBAA_BUCKET_TAGGING_METRIC_SLEEP = 15


@tier2
@mcg
@red_squad
@runs_on_provider
class TestOBCLabelBucketTagging:
    """
    Verify OBC labels are propagated as S3 bucket tags and reflected in
    NooBaa bucket tagging metrics.
    """

    def test_obc_labels_sync_to_bucket_tags(self, bucket_factory, threading_lock):
        """
        1. Create a new OBC
        2. Verify the bucket has no tags before labeling the OBC
        3. Verify NooBaa_bucket_tagging metric is absent before label update
        4. Add labels to the OBC
        5. Verify OBC metadata contains the labels
        6. Verify the bucket has matching S3 tags
        7. Verify NooBaa_bucket_tagging metric reflects the labels
        """
        logger.test_step("Create OBC and verify initial state")
        obc_name = bucket_factory(amount=1, interface="OC")[0].name
        obc_obj = OBC(obc_name)
        bucket_name = obc_obj.bucket_name
        expected_labels = {OBC_LABEL_KEY: OBC_LABEL_VALUE}
        logger.info(f"Created OBC '{obc_name}' with S3 bucket '{bucket_name}'")

        logger.test_step("Verify bucket has no tags before labeling OBC")
        with pytest.raises(boto3exception.ClientError) as exc:
            get_bucket_tagging(obc_obj.s3_client, bucket_name)
        logger.assertion(
            f"Bucket tagging error: expected='NoSuchTagSet', "
            f"actual='{exc.value.response['Error']['Code']}'"
        )
        assert (
            exc.value.response["Error"]["Code"] == "NoSuchTagSet"
        ), f"Expected NoSuchTagSet before labeling, got {exc.value.response}"

        logger.test_step(
            "Verify NooBaa_bucket_tagging metric is absent before label update"
        )
        metric_before = get_noobaa_bucket_tagging_metric_results(
            NOOBAA_BUCKET_TAGGING_METRIC,
            bucket_name,
            threading_lock,
        )
        logger.assertion(
            f"NooBaa_bucket_tagging metric before labeling: "
            f"results_count={len(metric_before)}, expected=0"
        )
        assert not metric_before, (
            f"Expected no {NOOBAA_BUCKET_TAGGING_METRIC} results before labeling, "
            f"got {metric_before}"
        )
        logger.info(
            f"Confirmed no {NOOBAA_BUCKET_TAGGING_METRIC} results for bucket '{bucket_name}'"
        )

        logger.test_step("Add labels to OBC")
        obc_ocp = OCP(
            kind="obc",
            namespace=config.ENV_DATA["cluster_namespace"],
            resource_name=obc_name,
        )
        label = f"{OBC_LABEL_KEY}={OBC_LABEL_VALUE}"
        label_added = obc_ocp.add_label(resource_name=obc_name, label=label)
        logger.info(f"Added label '{label}' to OBC '{obc_name}'")
        logger.assertion(f"Label addition: success={label_added}")
        assert label_added, f"Failed to add label {label} to OBC {obc_name}"

        logger.test_step("Verify OBC metadata contains the labels")
        obc_labels = (
            obc_ocp.get(resource_name=obc_name).get("metadata", {}).get("labels", {})
        )
        logger.assertion(
            f"OBC labels check: expected={expected_labels}, "
            f"actual={obc_labels}, "
            f"match={obc_labels.get(OBC_LABEL_KEY) == OBC_LABEL_VALUE}"
        )
        assert (
            obc_labels.get(OBC_LABEL_KEY) == OBC_LABEL_VALUE
        ), f"OBC {obc_name} labels {obc_labels} missing {expected_labels}"

        logger.test_step("Verify bucket S3 tags match OBC labels")
        bucket_tags = verify_bucket_tagging_matches_labels(
            obc_obj.s3_client,
            bucket_name,
            expected_labels,
        )
        logger.assertion(
            f"S3 tags verification: expected_subset={expected_labels}, actual={bucket_tags} "
        )
        assert all(
            bucket_tags.get(key) == value for key, value in expected_labels.items()
        ), f"Bucket tags {bucket_tags} missing expected labels {expected_labels}"

        logger.test_step(
            "Verify NooBaa_bucket_tagging metric reflects OBC labels "
            f"(timeout: {NOOBAA_BUCKET_TAGGING_METRIC_TIMEOUT}s)"
        )
        metric_after = verify_noobaa_bucket_tagging_metric(
            NOOBAA_BUCKET_TAGGING_METRIC,
            bucket_name,
            expected_labels,
            threading_lock,
            timeout=NOOBAA_BUCKET_TAGGING_METRIC_TIMEOUT,
            sleep=NOOBAA_BUCKET_TAGGING_METRIC_SLEEP,
        )
        logger.info(
            f"{NOOBAA_BUCKET_TAGGING_METRIC} after label update: {metric_after} "
            f"(before: {metric_before})"
        )
