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
    tag_set_to_dict,
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
        obc_name = bucket_factory(amount=1, interface="OC")[0].name
        obc_obj = OBC(obc_name)
        bucket_name = obc_obj.bucket_name
        expected_labels = {OBC_LABEL_KEY: OBC_LABEL_VALUE}
        logger.info(f"Created OBC {obc_name} with S3 bucket {bucket_name}")

        # Bucket should have no tags before OBC labels are applied
        with pytest.raises(boto3exception.ClientError) as exc:
            get_bucket_tagging(obc_obj.s3_client, bucket_name)
        assert (
            exc.value.response["Error"]["Code"] == "NoSuchTagSet"
        ), f"Expected NoSuchTagSet before labeling, got {exc.value.response}"

        metric_before = get_noobaa_bucket_tagging_metric_results(
            NOOBAA_BUCKET_TAGGING_METRIC,
            bucket_name,
            threading_lock,
        )
        assert not metric_before, (
            f"Expected no {NOOBAA_BUCKET_TAGGING_METRIC} results before labeling, "
            f"got {metric_before}"
        )
        logger.info(
            f"No {NOOBAA_BUCKET_TAGGING_METRIC} results for bucket {bucket_name} "
            "before label update"
        )

        # Add labels to the OBC
        obc_ocp = OCP(
            kind="obc",
            namespace=config.ENV_DATA["cluster_namespace"],
            resource_name=obc_name,
        )
        label = f"{OBC_LABEL_KEY}={OBC_LABEL_VALUE}"
        assert obc_ocp.add_label(
            resource_name=obc_name, label=label
        ), f"Failed to add label {label} to OBC {obc_name}"

        # Verify OBC metadata contains the label
        obc_labels = (
            obc_ocp.get(resource_name=obc_name).get("metadata", {}).get("labels", {})
        )
        assert (
            obc_labels.get(OBC_LABEL_KEY) == OBC_LABEL_VALUE
        ), f"OBC {obc_name} labels {obc_labels} missing {expected_labels}"

        # Step 6: verify bucket S3 tags match OBC labels
        bucket_tags = verify_bucket_tagging_matches_labels(
            obc_obj.s3_client,
            bucket_name,
            expected_labels,
        )
        tag_set = get_bucket_tagging(obc_obj.s3_client, bucket_name)
        assert tag_set_to_dict(tag_set) == bucket_tags

        # Verify NooBaa_bucket_tagging metric reflects OBC labels (gauge updated
        # by the stats aggregator, which runs on a ~5 minute cycle).
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
