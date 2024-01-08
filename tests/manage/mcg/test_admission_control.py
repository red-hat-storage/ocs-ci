import logging
import pytest
import re
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility import templating

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import red_squad, mcg
from ocs_ci.framework.testlib import (
    MCGTest,
    skipif_ocs_version,
    ignore_leftovers,
    tier3,
    polarion_id,
    bugzilla,
)
from ocs_ci.helpers.helpers import create_resource, create_unique_resource_name
from ocs_ci.ocs import constants

logger = logging.getLogger(__name__)


@mcg
@red_squad
@skipif_ocs_version("<4.10")
# We have to ignore leftovers since environment_checker runs before the
# bucket_factory_session teardown, due to it being session-scoped.
# Thus, it falsely recognizes leftovers that are deleted in a later stage
@ignore_leftovers
@bugzilla("1981732")
class TestAdmissionWebhooks(MCGTest):
    @pytest.mark.parametrize(
        argnames="spec_dict,err_msg",
        argvalues=[
            pytest.param(
                *[
                    {
                        "type": "aws-s3",
                        "awsS3": {
                            "targetBucket": "nonexistent-bucket",
                            "secret": {"name": ""},
                        },
                    },
                    "please provide a valid ARN or secret name",
                ],
                marks=[tier3, polarion_id("OCS-2780")],
            ),
            pytest.param(
                *[
                    {
                        "type": "invalid-type",
                        "awsS3": {
                            "targetBucket": "nonexistent-bucket",
                            "secret": {"name": "secret"},
                        },
                    },
                    "please provide a valid Backingstore type",
                ],
                marks=[tier3, polarion_id("OCS-2781")],
            ),
            pytest.param(
                *[
                    {
                        "type": "s3-compatible",
                        "s3Compatible": {
                            "endpoint": "https://s3.amazonaws.com",
                            "secret": {"name": "secret", "namespace": "default"},
                            "signatureVersion": "v3",
                            "targetBucket": "nonexistent-bucket",
                        },
                    },
                    "Invalid S3 compatible Backingstore signature version",
                ],
                marks=[tier3, polarion_id("OCS-2782")],
            ),
            pytest.param(
                *[
                    {
                        "type": "pv-pool",
                        "pvPool": {
                            "numVolumes": 1,
                            "resources": {"requests": {"storage": "14Gi"}},
                        },
                    },
                    "minimum volume size is 16Gi",
                ],
                marks=[tier3, polarion_id("OCS-2777")],
            ),
            pytest.param(
                *[
                    {
                        "type": "pv-pool",
                        "pvPool": {
                            "numVolumes": 21,
                            "resources": {"requests": {"storage": "18Gi"}},
                        },
                    },
                    "Unsupported volume count",
                ],
                marks=[tier3, polarion_id("OCS-2778")],
            ),
            pytest.param(
                *[
                    {
                        "type": "pv-pool",
                        "pvPool": {
                            "numVolumes": 1,
                            "resources": {"requests": {"storage": "18Gi"}},
                        },
                    },
                    "Unsupported BackingStore name length",
                ],
                marks=[tier3, polarion_id("OCS-2779")],
            ),
        ],
        ids=[
            "Empty secret name",
            "Invalid type",
            "Invalid S3 compatible version signature",
            "Exceedingly small PVPool vol size",
            "Exceedingly high PVPool vol amount",
            "Exceedingly long PVPool name",
        ],
    )
    def test_backingstore_creation_webhook(self, spec_dict, err_msg):
        """
        Test the MCG admission control webhooks for backingstore creation
        """
        if spec_dict["type"] == "pv-pool":
            bs_data = templating.load_yaml(constants.PV_BACKINGSTORE_YAML)
        else:
            bs_data = templating.load_yaml(constants.MCG_BACKINGSTORE_YAML)
        bs_data["metadata"]["name"] = create_unique_resource_name(
            "backingstore", "invalid"
        )
        if spec_dict["type"] == "pv-pool" and "name length" in err_msg:
            bs_data["metadata"]["name"] += bs_data["metadata"]["name"]
        bs_data["metadata"]["namespace"] = config.ENV_DATA["cluster_namespace"]
        bs_data["spec"] = spec_dict
        try:
            created_bs = create_resource(**bs_data)
        except CommandFailed as e:
            if err_msg in e.args[0]:
                logger.info("Backingstore creation failed with an expected error")
            else:
                raise
        else:
            created_bs.delete()
            assert False, "Backingstore creation succeeded unexpectedly"

    @pytest.mark.parametrize(
        argnames="spec_dict,err_msg",
        argvalues=[
            pytest.param(
                *[
                    {
                        "type": "aws-s3",
                        "awsS3": {
                            "targetBucket": "nonexistent-bucket",
                            "secret": {"name": ""},
                        },
                    },
                    "please provide.*secret name",
                ],
                marks=[tier3, polarion_id("OCS-2786")],
            ),
            pytest.param(
                *[
                    {
                        "type": "invalid-type",
                        "awsS3": {
                            "targetBucket": "nonexistent-bucket",
                            "secret": {"name": "secret"},
                        },
                    },
                    "please provide a valid Namespacestore type",
                ],
                marks=[tier3, polarion_id("OCS-2787")],
            ),
            pytest.param(
                *[
                    {
                        "type": "nsfs",
                        "nsfs": {
                            "pvcName": "",
                            "subPath": "",
                        },
                    },
                    "PvcName must not be empty",
                ],
                marks=[tier3, polarion_id("OCS-2788")],
            ),
            pytest.param(
                *[
                    {
                        "type": "nsfs",
                        "nsfs": {
                            "pvcName": "pvc",
                            "subPath": "/path/",
                        },
                    },
                    "must be a relative path",
                ],
                marks=[tier3, polarion_id("OCS-2783")],
            ),
            pytest.param(
                *[
                    {
                        "type": "nsfs",
                        "nsfs": {
                            "pvcName": "pvc",
                            "subPath": "../path/",
                        },
                    },
                    "must not contain '..'",
                ],
                marks=[tier3, polarion_id("OCS-2784")],
            ),
            pytest.param(
                *[
                    {
                        "type": "nsfs",
                        "nsfs": {
                            "pvcName": "pvc",
                            "subPath": "path/",
                        },
                    },
                    "must be no more than 63 characters",
                ],
                marks=[tier3, polarion_id("OCS-2785")],
            ),
        ],
        ids=[
            "Empty secret name",
            "Invalid type",
            "Empty NSFS PVC name",
            "SubPath is not relative",
            "SubPath contains ..",
            "Exceedingly long mount path",
        ],
    )
    def test_namespacestore_creation_webhook(self, spec_dict, err_msg):
        """
        Test the MCG admission control webhooks for Namespacestore creation
        """
        store_data = templating.load_yaml(constants.MCG_NAMESPACESTORE_YAML)
        store_data["metadata"]["name"] = create_unique_resource_name(
            "namespacestore", "invalid"
        )
        if "63 characters" in err_msg:
            store_data["metadata"]["name"] += store_data["metadata"]["name"]
        store_data["metadata"]["namespace"] = config.ENV_DATA["cluster_namespace"]
        store_data["spec"] = spec_dict
        try:
            created_bs = create_resource(**store_data)
        except CommandFailed as e:
            if re.search(err_msg, e.args[0]):
                logger.info("Namespacestore creation failed with an expected error")
            else:
                raise
        else:
            created_bs.delete()
            assert False, "Namespacestore creation succeeded unexpectedly"

    @pytest.mark.parametrize(
        argnames="bucketclass_dict",
        argvalues=[
            pytest.param(
                {
                    "interface": "OC",
                    "backingstore_dict": {"aws": [(1, "eu-central-1")]},
                },
                marks=[tier3, polarion_id("OCS-2795")],
            ),
            pytest.param(
                {
                    "interface": "OC",
                    "namespace_policy_dict": {
                        "type": "Single",
                        "namespacestore_dict": {"aws": [(1, "eu-central-1")]},
                    },
                },
                marks=[tier3, polarion_id("OCS-2796")],
            ),
        ],
        ids=["AWS Backingstore", "AWS Namespacestore"],
    )
    def test_deletion_of_store_with_attached_buckets(
        self, bucket_factory_session, bucketclass_dict
    ):
        """
        Verify that store deletion fails when there are buckets attached to it
        """
        try:
            if "backingstore_dict" in bucketclass_dict:
                bucket_factory_session(1, bucketclass=bucketclass_dict)[
                    0
                ].bucketclass.backingstores[0].delete(retry=False)
            else:
                bucket_factory_session(1, bucketclass=bucketclass_dict)[
                    0
                ].bucketclass.namespacestores[0].delete(retry=False)
        except CommandFailed as e:
            if all(
                err in e.args[0] for err in ["cannot complete", 'in "IN_USE" state']
            ):
                logger.info("Store deletion failed with an expected error")
            else:
                raise
        else:
            assert False, "Store deletion succeeded unexpectedly"

    @pytest.mark.parametrize(
        argnames="bucketclass_dict",
        argvalues=[
            pytest.param(
                {
                    "interface": "OC",
                    "backingstore_dict": {"aws": [(1, "eu-central-1")]},
                },
                marks=[tier3, polarion_id("OCS-2797")],
            ),
            pytest.param(
                {
                    "interface": "OC",
                    "namespace_policy_dict": {
                        "type": "Single",
                        "namespacestore_dict": {"aws": [(1, "eu-central-1")]},
                    },
                },
                marks=[tier3, polarion_id("OCS-2791")],
            ),
        ],
        ids=["AWS Backingstore", "AWS Namespacestore"],
    )
    def test_store_target_bucket_change(self, bucket_factory_session, bucketclass_dict):
        """
        Verify that store target bucket changing is blocked
        """
        if "backingstore_dict" in bucketclass_dict:
            store_name = (
                bucket_factory_session(1, bucketclass=bucketclass_dict)[0]
                .bucketclass.backingstores[0]
                .name
            )
            kind = "backingstore"
        else:
            store_name = (
                bucket_factory_session(1, bucketclass=bucketclass_dict)[0]
                .bucketclass.namespacestores[0]
                .name
            )
            kind = "namespacestore"
        try:
            OCP(
                kind=kind,
                namespace=config.ENV_DATA["cluster_namespace"],
                resource_name=store_name,
            ).patch(
                params='{"spec":{"awsS3":{"targetBucket": "other-bucket"}}}',
                format_type="merge",
            )

        except CommandFailed as e:
            if f"changing a {kind} target bucket is unsupported" in e.args[0].lower():
                logger.info("Store patch failed with an expected error")
            else:
                raise
        else:
            assert False, "Store patch succeeded unexpectedly"

    @polarion_id("OCS-2792")
    def test_pvpool_downscaling(self, backingstore_factory_session):
        """
        Verify that PVPool downscaling is blocked
        """
        pv_backingstore = backingstore_factory_session(
            "oc",
            {"pv": [(2, constants.MIN_PV_BACKINGSTORE_SIZE_IN_GB, None)]},
        )[0]
        try:
            OCP(
                kind="backingstore",
                namespace=config.ENV_DATA["cluster_namespace"],
                resource_name=pv_backingstore.name,
            ).patch(params='{"spec":{"pvPool":{"numVolumes":1}}}', format_type="merge")

        except CommandFailed as e:
            if (
                "Scaling down the number of nodes is not currently supported"
                in e.args[0]
            ):
                logger.info("Store patch failed with an expected error")
            else:
                raise
        else:
            assert False, "Store patch succeeded unexpectedly"

    @pytest.mark.parametrize(
        argnames="spec_dict,err_msg",
        argvalues=[
            pytest.param(
                {"quota": {"maxObjects": "-1"}},
                "invalid maxObjects value",
                marks=[tier3, polarion_id("OCS-2793")],
            ),
            pytest.param(
                {"quota": {"maxSize": "900M"}},
                "invalid obcMaxSizeValue value",
                marks=[tier3, polarion_id("OCS-2794")],
            ),
            pytest.param(
                {"quota": {"maxSize": "1024Pi"}},
                "invalid obcMaxSizeValue value",
                marks=[tier3, polarion_id("OCS-2789")],
            ),
            pytest.param(
                {
                    "placementPolicy": {
                        "tiers": [
                            {"backingStores": ["nonexistent-bs"]},
                            {"backingStores": ["nonexistent-bs"]},
                            {"backingStores": ["nonexistent-bs"]},
                        ]
                    }
                },
                "unsupported number of tiers",
                marks=[tier3, polarion_id("OCS-2790")],
            ),
        ],
        ids=[
            "Negative amount of max object quota",
            "Exceedingly small maxSize quota",
            "Exceedinly large maxSize quota",
            "Exceedingly high amount of tiers",
        ],
    )
    def test_bucketclass_creation(self, spec_dict, err_msg):
        """
        Test that bucketclass creation fails when given invalid parameters

        """
        bc_data = templating.load_yaml(constants.MCG_BUCKETCLASS_YAML)
        bc_data["metadata"]["name"] = create_unique_resource_name(
            "bucketclass", "invalid"
        )
        bc_data["metadata"]["namespace"] = config.ENV_DATA["cluster_namespace"]
        bc_data["spec"] = spec_dict
        try:
            created_bc = create_resource(**bc_data)
        except CommandFailed as e:
            if err_msg in e.args[0]:
                logger.info("Bucketclass creation failed with an expected error")
            else:
                raise
        else:
            created_bc.delete()
            assert False, "Bucketclass creation succeeded unexpectedly"
