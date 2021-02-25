"""
Tests for Namespace resources and buckets by using OpenShift CRDs only.
These tests are valid only for OCS version lesser than 4.6 because in later
versions are for Namespace bucket creation used CRDs instead of NooBaa RPC calls.
"""
import logging
import pytest

from ocs_ci.framework.testlib import (
    MCGTest,
    on_prem_platform_required,
    skipif_ocs_version,
    tier1,
    tier2,
    tier3,
    tier4,
    tier4a,
)
from ocs_ci.ocs.bucket_utils import sync_object_directory, verify_s3_object_integrity
from ocs_ci.framework.pytest_customization.marks import skipif_aws_creds_are_missing
from ocs_ci.ocs import constants
from ocs_ci.ocs.cluster import CephCluster
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.resources import pod
from ocs_ci.framework.pytest_customization.marks import skipif_openshift_dedicated

logger = logging.getLogger(__name__)


@skipif_openshift_dedicated
@skipif_aws_creds_are_missing
@skipif_ocs_version("<4.7")
class TestNamespace(MCGTest):
    """
    Test creation of a namespace resources and buckets via OpenShift CRDs.
    """

    MCG_NS_RESULT_DIR = "/result"
    MCG_NS_ORIGINAL_DIR = "/original"
    # TODO: fix this when https://github.com/red-hat-storage/ocs-ci/issues/3338
    # is resolved
    DEFAULT_REGION = "us-east-2"

    @tier1
    @pytest.mark.parametrize(
        argnames="nss_tup",
        argvalues=[
            pytest.param(("oc", {"aws": [(1, "eu-central-1")]})),
            pytest.param(("oc", {"azure": [(1, None)]})),
            pytest.param(("oc", {"rgw": [(1, None)]}), marks=on_prem_platform_required),
        ],
        # A test ID list for describing the parametrized tests
        # <CLOUD_PROVIDER>-<METHOD>-<AMOUNT-OF-BACKINGSTORES>
        ids=[
            "AWS-OC-1",
            "AZURE-OC-1",
            "RGW-OC-1",
        ],
    )
    @pytest.mark.polarion_id("OCS-2255")
    def test_namespace_store_creation_crd(self, namespace_store_factory, nss_tup):
        """
        Test namespace store creation using the MCG CRDs.
        """
        # Create the namespace store and verify health
        namespace_store_factory(*nss_tup)

    @pytest.mark.parametrize(
        argnames=["bucketclass_dict"],
        argvalues=[
            pytest.param(
                {
                    "interface": "OC",
                    "namespace_policy_dict": {
                        "type": "Single",
                        "namespacestore_dict": {"aws": [(1, None)]},
                    },
                },
                marks=[tier1, pytest.mark.polarion_id("OCS-2256")],
            ),
            pytest.param(
                {
                    "interface": "OC",
                    "namespace_policy_dict": {
                        "type": "Single",
                        "namespacestore_dict": {"azure": [(1, None)]},
                    },
                },
                marks=[tier1, pytest.mark.polarion_id("OCS-2409")],
            ),
            pytest.param(
                {
                    "interface": "OC",
                    "namespace_policy_dict": {
                        "type": "Single",
                        "namespacestore_dict": {"rgw": [(1, None)]},
                    },
                },
                marks=[
                    tier1,
                    on_prem_platform_required,
                    pytest.mark.polarion_id("OCS-2407"),
                ],
            ),
            pytest.param(
                {
                    "interface": "OC",
                    "namespace_policy_dict": {
                        "type": "Multi",
                        "namespacestore_dict": {
                            "aws": [(1, "eu-central-1")],
                            "azure": [(1, None)],
                        },
                    },
                },
                marks=[tier2, pytest.mark.polarion_id("OCS-2416")],
            ),
            pytest.param(
                {
                    "interface": "OC",
                    "namespace_policy_dict": {
                        "type": "Multi",
                        "namespacestore_dict": {
                            "aws": [(2, "eu-central-1")],
                        },
                    },
                },
                marks=[tier2, pytest.mark.polarion_id("OCS-2418")],
            ),
            pytest.param(
                {
                    "interface": "OC",
                    "namespace_policy_dict": {
                        "type": "Multi",
                        "namespacestore_dict": {
                            "azure": [(2, None)],
                        },
                    },
                },
                marks=[tier2, pytest.mark.polarion_id("OCS-2419")],
            ),
            pytest.param(
                {
                    "interface": "OC",
                    "namespace_policy_dict": {
                        "type": "Multi",
                        "namespacestore_dict": {
                            "rgw": [(2, None)],
                        },
                    },
                },
                marks=[
                    tier2,
                    on_prem_platform_required,
                    pytest.mark.polarion_id("OCS-2417"),
                ],
            ),
        ],
        ids=[
            "AWS-OC-Single",
            "Azure-OC-Single",
            "RGW-OC-Single",
            "AWS+Azure-OC-Multi",
            "AWS+AWS-OC-Multi",
            "AZURE+AZURE-OC-Multi",
            "RGW+RGW-OC-Multi",
        ],
    )
    def test_namespace_bucket_creation_crd(self, bucket_factory, bucketclass_dict):
        """
        Test namespace bucket creation using the MCG CRDs.
        """

        # Create the namespace bucket on top of the namespace resource
        bucket_factory(
            amount=1,
            interface=bucketclass_dict["interface"],
            bucketclass=bucketclass_dict,
        )

    @tier1
    @pytest.mark.parametrize(
        argnames=["bucketclass_dict"],
        argvalues=[
            pytest.param(
                {
                    "interface": "OC",
                    "namespace_policy_dict": {
                        "type": "Single",
                        "namespacestore_dict": {"aws": [(1, "eu-central-1")]},
                    },
                },
                marks=pytest.mark.polarion_id("OCS-2257"),
            ),
        ],
    )
    def test_write_to_aws_read_from_nsb_crd(
        self,
        mcg_obj,
        cld_mgr,
        awscli_pod,
        bucket_factory,
        bucketclass_dict,
    ):
        """
        Test writing to AWS and reading from an ns bucket
        """

        ns_bucket = bucket_factory(
            amount=1,
            interface=bucketclass_dict["interface"],
            bucketclass=bucketclass_dict,
        )[0]

        s3_creds = {
            "access_key_id": cld_mgr.aws_client.access_key,
            "access_key": cld_mgr.aws_client.secret_key,
            "endpoint": constants.MCG_NS_AWS_ENDPOINT,
            "region": self.DEFAULT_REGION,
        }
        aws_target_bucket = ns_bucket.bucketclass.namespacestores[0].uls_name

        # Upload files directly to AWS
        self.write_files_to_pod_and_upload(
            mcg_obj,
            awscli_pod,
            bucket_to_write=aws_target_bucket,
            amount=3,
            s3_creds=s3_creds,
        )
        # Read files from ns bucket
        self.download_files(mcg_obj, awscli_pod, bucket_to_read=ns_bucket.name)

        # Compare between uploaded files and downloaded files
        assert self.compare_dirs(awscli_pod, amount=3)

    @tier1
    @pytest.mark.parametrize(
        argnames=["bucketclass_dict"],
        argvalues=[
            pytest.param(
                {
                    "interface": "OC",
                    "namespace_policy_dict": {
                        "type": "Single",
                        "namespacestore_dict": {"aws": [(1, "eu-central-1")]},
                    },
                },
                marks=pytest.mark.polarion_id("OCS-2258"),
            ),
        ],
    )
    def test_write_to_ns_read_from_aws_crd(
        self,
        mcg_obj,
        cld_mgr,
        awscli_pod,
        bucket_factory,
        bucketclass_dict,
    ):
        """
        Test Write to ns bucket using MCG RPC and read directly from AWS.
        """

        ns_bucket = bucket_factory(
            amount=1,
            interface=bucketclass_dict["interface"],
            bucketclass=bucketclass_dict,
        )[0]

        s3_creds = {
            "access_key_id": cld_mgr.aws_client.access_key,
            "access_key": cld_mgr.aws_client.secret_key,
            "endpoint": constants.MCG_NS_AWS_ENDPOINT,
            "region": self.DEFAULT_REGION,
        }
        aws_target_bucket = ns_bucket.bucketclass.namespacestores[0].uls_name

        # Upload files to NS bucket
        self.write_files_to_pod_and_upload(
            mcg_obj, awscli_pod, bucket_to_write=ns_bucket.name, amount=3
        )
        # Read files directly from AWS
        self.download_files(
            mcg_obj, awscli_pod, bucket_to_read=aws_target_bucket, s3_creds=s3_creds
        )

        # Compare between uploaded files and downloaded files
        assert self.compare_dirs(awscli_pod, amount=3)

    @tier1
    @pytest.mark.polarion_id("OCS-2258")
    def test_distribution_of_objects_in_ns_bucket_crd(
        self,
        mcg_obj,
        cld_mgr,
        awscli_pod,
        bucket_factory,
        namespace_store_factory,
    ):
        """
        Test that uploaded objects into resources were correctly uploaded even
        when some file is the same and downloaded after that.
        """
        logger.info("Create the namespace resources and verify health")
        nss_tup = ("oc", {"rgw": [(1, "eu-central-1")]})
        ns_store1 = namespace_store_factory(*nss_tup)
        nss_tup = ("oc", {"aws": [(1, "eu-central-1")]})
        ns_store2 = namespace_store_factory(*nss_tup)

        logger.info("Upload files directly to first target bucket")
        rgw_creds = {
            "access_key_id": cld_mgr.rgw_client.access_key,
            "access_key": cld_mgr.rgw_client.secret_key,
            "endpoint": cld_mgr.rgw_client.endpoint,
        }
        self.write_files_to_pod_and_upload(
            mcg_obj,
            awscli_pod,
            bucket_to_write=ns_store1.uls_name,
            amount=4,
            s3_creds=rgw_creds,
        )

        logger.info("Create the namespace bucket on top of the namespace resource")
        bucketclass_dict = (
            {
                "interface": "OC",
                "namespace_policy_dict": {
                    "type": "Multi",
                    "namespacestores": [ns_store1, ns_store2],
                },
            },
        )

        ns_bucket = bucket_factory(
            amount=1,
            interface=bucketclass_dict["interface"],
            bucketclass=bucketclass_dict,
        )[0]

        logger.info("Rewrite 3 files and upload them directly to second target bucket")
        aws_creds = {
            "access_key_id": cld_mgr.aws_client.access_key,
            "access_key": cld_mgr.aws_client.secret_key,
            "endpoint": constants.MCG_NS_AWS_ENDPOINT,
            "region": self.DEFAULT_REGION,
        }
        self.write_files_to_pod_and_upload(
            mcg_obj,
            awscli_pod,
            bucket_to_write=ns_store2.uls_name,
            amount=3,
            s3_creds=aws_creds,
        )

        logger.info("Read files from ns bucket")
        self.download_files(mcg_obj, awscli_pod, bucket_to_read=rand_ns_bucket)

        logger.info("Compare between uploaded files and downloaded files")
        assert self.compare_dirs(awscli_pod, amount=4)

    def write_files_to_pod_and_upload(
        self, mcg_obj, awscli_pod, bucket_to_write, amount=1, s3_creds=None
    ):
        """
        Upload files to bucket (NS or uls)
        """
        awscli_pod.exec_cmd_on_pod(command=f"mkdir -p {self.MCG_NS_ORIGINAL_DIR}")
        full_object_path = f"s3://{bucket_to_write}"

        for i in range(amount):
            file_name = f"testfile{i}"
            awscli_pod.exec_cmd_on_pod(
                f"dd if=/dev/urandom of={self.MCG_NS_ORIGINAL_DIR}/{file_name}.txt bs=1M count=1 status=none"
            )
        if s3_creds:
            # Write data directly to target bucket from original dir
            sync_object_directory(
                awscli_pod,
                self.MCG_NS_ORIGINAL_DIR,
                full_object_path,
                signed_request_creds=s3_creds,
            )
        else:
            # Write data directly to NS bucket from original dir
            sync_object_directory(
                awscli_pod, self.MCG_NS_ORIGINAL_DIR, full_object_path, mcg_obj
            )

    def download_files(self, mcg_obj, awscli_pod, bucket_to_read, s3_creds=None):
        """
        Download files from bucket (NS or uls)
        """
        awscli_pod.exec_cmd_on_pod(command=f"mkdir {self.MCG_NS_RESULT_DIR}")
        ns_bucket_path = f"s3://{bucket_to_read}"

        if s3_creds:
            # Read data directly from target bucket (uls) to result dir
            sync_object_directory(
                awscli_pod,
                ns_bucket_path,
                self.MCG_NS_RESULT_DIR,
                signed_request_creds=s3_creds,
            )
        else:
            # Read data from NS bucket to result dir
            sync_object_directory(
                awscli_pod, ns_bucket_path, self.MCG_NS_RESULT_DIR, mcg_obj
            )

    def compare_dirs(self, awscli_pod, amount=1):
        # Checksum is compared between original and result object
        result = True
        for i in range(amount):
            file_name = f"testfile{i}.txt"
            original_object_path = f"{self.MCG_NS_ORIGINAL_DIR}/{file_name}"
            result_object_path = f"{self.MCG_NS_RESULT_DIR}/{file_name}"
            if not verify_s3_object_integrity(
                original_object_path=original_object_path,
                result_object_path=result_object_path,
                awscli_pod=awscli_pod,
            ):
                logger.warning(
                    f"Checksum comparision between original object "
                    f"{original_object_path} and result object "
                    f"{result_object_path} failed"
                )
                result = False
        return result
