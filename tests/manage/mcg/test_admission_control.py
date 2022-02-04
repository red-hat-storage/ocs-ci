import logging
import pytest
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.utility import templating

from ocs_ci.framework import config
from ocs_ci.framework.testlib import MCGTest, skipif_ocs_version, tier3
from ocs_ci.helpers.helpers import create_resource, create_unique_resource_name
from ocs_ci.ocs import constants

logger = logging.getLogger(__name__)


@skipif_ocs_version("<4.10")
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
                    "please provide secret name",
                ],
                marks=[tier3],
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
                marks=[tier3],
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
                marks=[tier3],
            ),
            pytest.param(
                *[
                    {
                        "type": "pv-pool",
                        "pvPool": {
                            "numVolumes": 1,
                            "resources": {"requests": {"storage": "15Gi"}},
                        },
                    },
                    "minimum volume size is 16Gi",
                ],
                marks=[tier3],
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
                marks=[tier3],
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
                marks=[tier3],
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
                logger.info("Backingstore creation failed with expected error")
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
                    "please provide secret name",
                ],
                marks=[tier3],
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
                marks=[tier3],
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
                marks=[tier3],
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
                marks=[tier3],
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
                marks=[tier3],
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
                marks=[tier3],
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
        bs_data = templating.load_yaml(constants.MCG_NAMESPACESTORE_YAML)
        bs_data["metadata"]["name"] = create_unique_resource_name(
            "namespacestore", "invalid"
        )
        if "63 characters" in err_msg:
            bs_data["metadata"]["name"] += bs_data["metadata"]["name"]
        bs_data["metadata"]["namespace"] = config.ENV_DATA["cluster_namespace"]
        bs_data["spec"] = spec_dict
        try:
            created_bs = create_resource(**bs_data)
        except CommandFailed as e:
            if err_msg in e.args[0]:
                logger.info("Namespacestore creation failed with expected error")
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
                marks=[tier3],
            ),
            pytest.param(
                {
                    "interface": "OC",
                    "namespace_policy_dict": {
                        "type": "Single",
                        "namespacestore_dict": {"aws": [(1, "eu-central-1")]},
                    },
                },
                marks=[tier3],
            ),
        ],
        ids=["AWS Backingstore", "AWS Namespacestore"],
    )
    def test_deletion_of_store_with_attached_buckets(
        self, bucket_factory, bucketclass_dict
    ):
        """
        Test that store deletion fails when there are buckets attached to it
        """
        try:
            if "backingstore_dict" in bucketclass_dict:
                bucket_factory(1, bucketclass=bucketclass_dict)[
                    0
                ].bucketclass.backingstores[0].delete(retry=False)
            else:
                bucket_factory(1, bucketclass=bucketclass_dict)[
                    0
                ].bucketclass.namespacestores[0].delete(retry=False)
        except CommandFailed as e:
            if all(
                err in e.args[0] for err in ["cannot complete", 'in "IN_USE" state']
            ):
                logger.info("Store deletion failed with expected error")
            else:
                raise
        else:
            assert False, "Store deletion succeeded unexpectedly"
