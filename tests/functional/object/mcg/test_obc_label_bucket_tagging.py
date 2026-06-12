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
    get_noobaa_bucket_metric_value,
    tag_set_to_dict,
    verify_bucket_tagging_matches_labels,
)
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.objectbucket import OBC
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)

NOOBAA_BUCKET_TAGGING_METRIC = "NooBaa_bucket_tagging"
OBC_LABEL_KEY = "test-label"
OBC_LABEL_VALUE = "verified"


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
        3. Record NooBaa_bucket_tagging metric before label update
        4. Add labels to the OBC
        5. Verify OBC metadata contains the labels
        6. Verify the bucket has matching S3 tags
        7. Verify NooBaa_bucket_tagging metric increased after label update
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

        metric_before = get_noobaa_bucket_metric_value(
            NOOBAA_BUCKET_TAGGING_METRIC,
            bucket_name,
            threading_lock,
        )
        logger.info(
            f"{NOOBAA_BUCKET_TAGGING_METRIC} before label update: {metric_before}"
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

        # Verify NooBaa_bucket_tagging metric increased
        metric_after = None
        for sample in TimeoutSampler(
            timeout=180,
            sleep=10,
            func=get_noobaa_bucket_metric_value,
            metric_name=NOOBAA_BUCKET_TAGGING_METRIC,
            bucket_name=bucket_name,
            threading_lock=threading_lock,
        ):
            if sample is not None and (metric_before is None or sample > metric_before):
                metric_after = sample
                break
        if metric_after is None:
            raise TimeoutExpiredError(
                f"{NOOBAA_BUCKET_TAGGING_METRIC} did not increase after labeling. "
                f"before={metric_before}, after={metric_after}"
            )
        logger.info(
            f"{NOOBAA_BUCKET_TAGGING_METRIC} after label update: {metric_after} "
            f"(before: {metric_before})"
        )
