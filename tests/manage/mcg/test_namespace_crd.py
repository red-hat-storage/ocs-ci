"""
Tests for Namespace resources and buckets by using OpenShift CRDs only.
These tests are valid only for OCS version 4.7 and above because in later
versions are for Namespace bucket creation used CRDs instead of NooBaa RPC calls.
"""
import logging
from time import sleep
import uuid

import boto3
import pytest
from botocore import UNSIGNED
from botocore.config import Config
import botocore.exceptions as boto3exception

from ocs_ci.framework.pytest_customization import marks
from ocs_ci.framework.testlib import (
    MCGTest,
    on_prem_platform_required,
    skipif_ocs_version,
    skipif_disconnected_cluster,
    tier1,
    tier2,
    tier4c,
)
from ocs_ci.ocs.bucket_utils import (
    sync_object_directory,
    verify_s3_object_integrity,
    check_cached_objects_by_name,
    s3_delete_object,
    retrieve_verification_mode,
    wait_for_cache,
)
from ocs_ci.framework.pytest_customization.marks import (
    skipif_aws_creds_are_missing,
    red_squad,
    mcg,
)
from ocs_ci.ocs import constants, bucket_utils
from ocs_ci.ocs.cluster import CephCluster
from ocs_ci.ocs.exceptions import CommandFailed, UnexpectedBehaviour
from ocs_ci.ocs.resources import pod
from ocs_ci.framework.pytest_customization.marks import skipif_managed_service
from ocs_ci.ocs.resources.bucket_policy import HttpResponseParser

logger = logging.getLogger(__name__)


@red_squad
@mcg
@skipif_managed_service
@skipif_aws_creds_are_missing
@skipif_disconnected_cluster
@skipif_ocs_version("<4.7")
class TestNamespace(MCGTest):
    """
    Test creation of a namespace resources and buckets via OpenShift CRDs.
    """

    # TODO: fix this when https://github.com/red-hat-storage/ocs-ci/issues/3338
    # is resolved
    DEFAULT_REGION = "us-east-2"

    @tier1
    @pytest.mark.parametrize(
        argnames="nss_tup",
        argvalues=[
            pytest.param(("oc", {"aws": [(1, DEFAULT_REGION)]})),
            pytest.param(("oc", {"azure": [(1, None)]})),
            pytest.param(
                ("oc", {"rgw": [(1, None)]}),
                marks=on_prem_platform_required,
            ),
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
                            "aws": [(1, DEFAULT_REGION)],
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
                            "aws": [(2, DEFAULT_REGION)],
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
                        "namespacestore_dict": {"aws": [(1, DEFAULT_REGION)]},
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
        awscli_pod_session,
        bucket_factory,
        test_directory_setup,
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

        original_folder = test_directory_setup.origin_dir
        result_folder = test_directory_setup.result_dir

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
            awscli_pod_session,
            bucket_to_write=aws_target_bucket,
            original_dir=original_folder,
            amount=3,
            s3_creds=s3_creds,
        )
        # Read files from ns bucket
        self.download_files(
            mcg_obj,
            awscli_pod_session,
            bucket_to_read=ns_bucket.name,
            download_dir=result_folder,
        )

        # Compare between uploaded files and downloaded files
        assert self.compare_dirs(
            awscli_pod_session,
            origin=original_folder,
            destination=result_folder,
            amount=3,
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
                        "namespacestore_dict": {"aws": [(1, DEFAULT_REGION)]},
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
        awscli_pod_session,
        bucket_factory,
        test_directory_setup,
        bucketclass_dict,
    ):
        """
        Test Write to ns bucket using OpenShift CRDs and read directly from AWS.
        """

        ns_bucket = bucket_factory(
            amount=1,
            interface=bucketclass_dict["interface"],
            bucketclass=bucketclass_dict,
        )[0]

        original_folder = test_directory_setup.origin_dir
        result_folder = test_directory_setup.result_dir

        s3_creds = {
            "access_key_id": cld_mgr.aws_client.access_key,
            "access_key": cld_mgr.aws_client.secret_key,
            "endpoint": constants.MCG_NS_AWS_ENDPOINT,
            "region": self.DEFAULT_REGION,
        }
        aws_target_bucket = ns_bucket.bucketclass.namespacestores[0].uls_name

        # Upload files to NS bucket
        self.write_files_to_pod_and_upload(
            mcg_obj,
            awscli_pod_session,
            bucket_to_write=ns_bucket.name,
            original_dir=original_folder,
            amount=3,
        )
        # Read files directly from AWS
        self.download_files(
            mcg_obj,
            awscli_pod_session,
            bucket_to_read=aws_target_bucket,
            download_dir=result_folder,
            s3_creds=s3_creds,
        )

        # Compare between uploaded files and downloaded files
        assert self.compare_dirs(
            awscli_pod_session,
            origin=original_folder,
            destination=result_folder,
            amount=3,
        )

    @tier1
    @pytest.mark.polarion_id("OCS-2258")
    @on_prem_platform_required
    def test_distribution_of_objects_in_ns_bucket_crd(
        self,
        mcg_obj,
        cld_mgr,
        awscli_pod_session,
        bucket_factory,
        namespace_store_factory,
        test_directory_setup,
    ):
        """
        Test that uploaded objects into resources were correctly uploaded even
        when some file is the same and downloaded after that.
        """
        logger.info("Create the namespace resources and verify health")
        nss_tup = ("oc", {"rgw": [(1, None)]})
        ns_store1 = namespace_store_factory(*nss_tup)[0]
        nss_tup = ("oc", {"aws": [(1, self.DEFAULT_REGION)]})
        ns_store2 = namespace_store_factory(*nss_tup)[0]

        logger.info("Upload files directly to first target bucket")
        rgw_creds = {
            "access_key_id": cld_mgr.rgw_client.access_key,
            "access_key": cld_mgr.rgw_client.secret_key,
            "endpoint": cld_mgr.rgw_client.endpoint,
        }

        original_folder = test_directory_setup.origin_dir
        result_folder = test_directory_setup.result_dir

        self.write_files_to_pod_and_upload(
            mcg_obj,
            awscli_pod_session,
            bucket_to_write=ns_store1.uls_name,
            original_dir=original_folder,
            amount=4,
            s3_creds=rgw_creds,
        )

        logger.info("Create the namespace bucket on top of the namespace resource")
        bucketclass_dict = {
            "interface": "OC",
            "namespace_policy_dict": {
                "type": "Multi",
                "namespacestores": [ns_store1, ns_store2],
            },
        }

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
            awscli_pod_session,
            bucket_to_write=ns_store2.uls_name,
            original_dir=original_folder,
            amount=3,
            s3_creds=aws_creds,
        )

        logger.info("Read files from ns bucket")
        self.download_files(
            mcg_obj,
            awscli_pod_session,
            download_dir=result_folder,
            bucket_to_read=ns_bucket.name,
        )

        logger.info("Compare between uploaded files and downloaded files")
        assert self.compare_dirs(
            awscli_pod_session,
            origin=original_folder,
            destination=result_folder,
            amount=4,
        )

    @tier1
    @pytest.mark.parametrize(
        argnames=["bucketclass_dict"],
        argvalues=[
            pytest.param(
                {
                    "interface": "OC",
                    "namespace_policy_dict": {
                        "type": "Cache",
                        "ttl": 3600,
                        "namespacestore_dict": {
                            "aws": [(1, "eu-central-1")],
                        },
                    },
                    "placement_policy": {
                        "tiers": [
                            {"backingStores": [constants.DEFAULT_NOOBAA_BACKINGSTORE]}
                        ]
                    },
                }
            ),
        ],
        ids=[
            "AWS-OC-Cache",
        ],
    )
    def test_read_non_cached_object(
        self,
        bucket_factory,
        mcg_obj,
        cld_mgr,
        awscli_pod_session,
        test_directory_setup,
        bucketclass_dict,
    ):
        """
        Test reading an object that is not present in a cache bucket.
        """

        # Create the cached namespace bucket on top of the namespace resource
        bucket_obj = bucket_factory(bucketclass=bucketclass_dict)[0]
        s3_creds = {
            "access_key_id": cld_mgr.aws_client.access_key,
            "access_key": cld_mgr.aws_client.secret_key,
            "endpoint": constants.MCG_NS_AWS_ENDPOINT,
            "region": self.DEFAULT_REGION,
        }

        original_folder = test_directory_setup.origin_dir
        result_folder = test_directory_setup.result_dir

        aws_target_bucket = bucket_obj.bucketclass.namespacestores[0].uls_name

        # Upload files directly to AWS
        writen_objs_names = self.write_files_to_pod_and_upload(
            mcg_obj,
            awscli_pod_session,
            bucket_to_write=aws_target_bucket,
            original_dir=original_folder,
            amount=3,
            s3_creds=s3_creds,
        )
        if not check_cached_objects_by_name(mcg_obj, bucket_obj.name):
            raise UnexpectedBehaviour(
                "Objects were found in the cache of an empty bucket"
            )
        # Read files from ns bucket
        self.download_files(
            mcg_obj,
            awscli_pod_session,
            download_dir=result_folder,
            bucket_to_read=bucket_obj.name,
        )
        wait_for_cache(mcg_obj, bucket_obj.name, writen_objs_names)

    @tier1
    @pytest.mark.parametrize(
        argnames=["bucketclass_dict"],
        argvalues=[
            pytest.param(
                {
                    "interface": "OC",
                    "namespace_policy_dict": {
                        "type": "Cache",
                        "ttl": 300000,
                        "namespacestore_dict": {
                            "aws": [(1, "eu-central-1")],
                        },
                    },
                    "placement_policy": {
                        "tiers": [
                            {"backingStores": [constants.DEFAULT_NOOBAA_BACKINGSTORE]}
                        ]
                    },
                }
            ),
        ],
        ids=[
            "AWS-OC-Cache",
        ],
    )
    def test_read_cached_object(
        self,
        bucket_factory,
        mcg_obj,
        cld_mgr,
        awscli_pod_session,
        test_directory_setup,
        bucketclass_dict,
    ):
        """
        Test reading an object that is present in a cache bucket.
        """

        # Create the cached namespace bucket on top of the namespace resource
        bucket_obj = bucket_factory(bucketclass=bucketclass_dict)[0]
        s3_creds = {
            "access_key_id": cld_mgr.aws_client.access_key,
            "access_key": cld_mgr.aws_client.secret_key,
            "endpoint": constants.MCG_NS_AWS_ENDPOINT,
            "region": self.DEFAULT_REGION,
        }

        original_folder = test_directory_setup.origin_dir
        result_folder = test_directory_setup.result_dir

        aws_target_bucket = bucket_obj.bucketclass.namespacestores[0].uls_name
        # Upload files to NS bucket
        writen_objs_names = self.write_files_to_pod_and_upload(
            mcg_obj,
            awscli_pod_session,
            bucket_to_write=bucket_obj.name,
            original_dir=original_folder,
            amount=1,
        )
        wait_for_cache(mcg_obj, bucket_obj.name, writen_objs_names)

        # Upload files directly to AWS
        self.write_files_to_pod_and_upload(
            mcg_obj,
            awscli_pod_session,
            bucket_to_write=aws_target_bucket,
            original_dir=original_folder,
            amount=1,
            s3_creds=s3_creds,
        )
        # Read files from ns bucket
        self.download_files(
            mcg_obj,
            awscli_pod_session,
            bucket_to_read=bucket_obj.name,
            download_dir=result_folder,
        )

        # Compare dirs should return false since we expect the cached object to return
        # instead of the new object currently present in the original dir
        if self.compare_dirs(
            awscli_pod_session, origin=original_folder, destination=result_folder
        ):
            raise UnexpectedBehaviour("Cached object was not downloaded")

    @tier1
    @pytest.mark.parametrize(
        argnames=["bucketclass_dict"],
        argvalues=[
            pytest.param(
                {
                    "interface": "OC",
                    "namespace_policy_dict": {
                        "type": "Cache",
                        "ttl": 10000,
                        "namespacestore_dict": {
                            "aws": [(1, "eu-central-1")],
                        },
                    },
                    "placement_policy": {
                        "tiers": [
                            {"backingStores": [constants.DEFAULT_NOOBAA_BACKINGSTORE]}
                        ]
                    },
                }
            ),
        ],
        ids=[
            "AWS-OC-Cache",
        ],
    )
    def test_read_stale_object(
        self,
        bucket_factory,
        mcg_obj,
        cld_mgr,
        awscli_pod_session,
        test_directory_setup,
        bucketclass_dict,
    ):
        """
        Test reading a stale object from a cache bucket.
        """

        # Create the cached namespace bucket on top of the namespace resource
        bucket_obj = bucket_factory(bucketclass=bucketclass_dict)[0]
        original_folder = test_directory_setup.origin_dir
        result_folder = test_directory_setup.result_dir
        s3_creds = {
            "access_key_id": cld_mgr.aws_client.access_key,
            "access_key": cld_mgr.aws_client.secret_key,
            "endpoint": constants.MCG_NS_AWS_ENDPOINT,
            "region": self.DEFAULT_REGION,
        }
        aws_target_bucket = bucket_obj.bucketclass.namespacestores[0].uls_name
        # Upload files to NS bucket
        writen_objs_names = self.write_files_to_pod_and_upload(
            mcg_obj,
            awscli_pod_session,
            bucket_to_write=bucket_obj.name,
            original_dir=original_folder,
            amount=1,
        )
        wait_for_cache(mcg_obj, bucket_obj.name, writen_objs_names)

        awscli_pod_session.exec_cmd_on_pod(
            f"mv {original_folder}/testfile0.txt {original_folder}/testfile1.txt"
        )
        # Upload files directly to AWS
        self.write_files_to_pod_and_upload(
            mcg_obj,
            awscli_pod_session,
            bucket_to_write=aws_target_bucket,
            original_dir=original_folder,
            amount=1,
            s3_creds=s3_creds,
        )
        awscli_pod_session.exec_cmd_on_pod(
            f"mv {original_folder}/testfile1.txt {original_folder}/testfile0.txt"
        )
        # using sleep and not TimeoutSampler because we need to wait throughout the whole ttl
        sleep(bucketclass_dict["namespace_policy_dict"]["ttl"] / 1000)

        # Read files from ns bucket
        self.download_files(
            mcg_obj,
            awscli_pod_session,
            bucket_to_read=bucket_obj.name,
            download_dir=result_folder,
        )

        if self.compare_dirs(
            awscli_pod_session, origin=original_folder, destination=result_folder
        ):
            raise UnexpectedBehaviour(
                "Updated file was not fetched after ttl was exceeded"
            )

    @tier1
    @pytest.mark.parametrize(
        argnames=["bucketclass_dict"],
        argvalues=[
            pytest.param(
                {
                    "interface": "OC",
                    "namespace_policy_dict": {
                        "type": "Cache",
                        "ttl": 10000,
                        "namespacestore_dict": {
                            "aws": [(1, "eu-central-1")],
                        },
                    },
                    "placement_policy": {
                        "tiers": [
                            {"backingStores": [constants.DEFAULT_NOOBAA_BACKINGSTORE]}
                        ]
                    },
                }
            ),
        ],
        ids=[
            "AWS-OC-Cache",
        ],
    )
    def test_write_object_to_cache(
        self,
        bucket_factory,
        mcg_obj,
        cld_mgr,
        awscli_pod_session,
        test_directory_setup,
        bucketclass_dict,
    ):
        """
        Test writing an object to a cache bucket.
        """

        # Create the cached namespace bucket on top of the namespace resource
        bucket_obj = bucket_factory(bucketclass=bucketclass_dict)[0]
        original_folder = test_directory_setup.origin_dir
        # Upload files to NS bucket
        writen_objs_names = self.write_files_to_pod_and_upload(
            mcg_obj,
            awscli_pod_session,
            bucket_to_write=bucket_obj.name,
            original_dir=original_folder,
            amount=1,
        )
        wait_for_cache(mcg_obj, bucket_obj.name, writen_objs_names)

    @tier1
    @pytest.mark.parametrize(
        argnames=["bucketclass_dict"],
        argvalues=[
            pytest.param(
                {
                    "interface": "OC",
                    "namespace_policy_dict": {
                        "type": "Cache",
                        "ttl": 1000,
                        "namespacestore_dict": {
                            "aws": [(1, "eu-central-1")],
                        },
                    },
                    "placement_policy": {
                        "tiers": [
                            {"backingStores": [constants.DEFAULT_NOOBAA_BACKINGSTORE]}
                        ]
                    },
                }
            ),
        ],
        ids=[
            "AWS-OC-Cache",
        ],
    )
    def test_list_cached_objects(
        self,
        bucket_factory,
        mcg_obj,
        cld_mgr,
        awscli_pod_session,
        test_directory_setup,
        bucketclass_dict,
    ):
        """
        Test the ability to list the object stored in a cache bucket.
        """

        # Create the cached namespace bucket on top of the namespace resource
        bucket_obj = bucket_factory(bucketclass=bucketclass_dict)[0]
        original_folder = test_directory_setup.origin_dir
        # Upload files to NS bucket
        writen_objs_names = self.write_files_to_pod_and_upload(
            mcg_obj,
            awscli_pod_session,
            bucket_to_write=bucket_obj.name,
            original_dir=original_folder,
            amount=3,
        )
        wait_for_cache(mcg_obj, bucket_obj.name, writen_objs_names)

    @tier1
    @pytest.mark.parametrize(
        argnames=["bucketclass_dict"],
        argvalues=[
            pytest.param(
                {
                    "interface": "OC",
                    "namespace_policy_dict": {
                        "type": "Cache",
                        "ttl": 1000,
                        "namespacestore_dict": {
                            "aws": [(1, "eu-central-1")],
                        },
                    },
                    "placement_policy": {
                        "tiers": [
                            {"backingStores": [constants.DEFAULT_NOOBAA_BACKINGSTORE]}
                        ]
                    },
                }
            ),
        ],
        ids=[
            "AWS-OC-Cache",
        ],
    )
    def test_delete_cached_object(
        self,
        bucket_factory,
        mcg_obj,
        cld_mgr,
        awscli_pod_session,
        test_directory_setup,
        bucketclass_dict,
    ):
        """
        Test the deletion of an object that is present in the cache of a cache bucket.
        """

        # Create the cached namespace bucket on top of the namespace resource
        bucket_obj = bucket_factory(bucketclass=bucketclass_dict)[0]

        original_folder = test_directory_setup.origin_dir

        # Upload files to NS bucket
        writen_objs_names = self.write_files_to_pod_and_upload(
            mcg_obj,
            awscli_pod_session,
            bucket_to_write=bucket_obj.name,
            original_dir=original_folder,
            amount=1,
        )
        wait_for_cache(mcg_obj, bucket_obj.name, writen_objs_names)

        # Delete the object from mcg interface
        s3_delete_object(mcg_obj, bucket_obj.name, writen_objs_names[0])
        sleep(1)
        if not check_cached_objects_by_name(mcg_obj, bucket_obj.name):
            raise UnexpectedBehaviour("Object was not deleted from cache properly")

        # Check deletion in the cloud provider
        aws_target_bucket = bucket_obj.bucketclass.namespacestores[0].uls_name
        aws_obj_list = list(
            cld_mgr.aws_client.client.Bucket(aws_target_bucket).objects.all()
        )
        if writen_objs_names[0] in aws_obj_list:
            raise UnexpectedBehaviour("Object was not deleted from cache properly")

    @pytest.mark.polarion_id("OCS-2290")
    @tier2
    @on_prem_platform_required
    def test_create_ns_bucket_from_utilized_resources_crd(
        self,
        mcg_obj,
        cld_mgr,
        awscli_pod_session,
        namespace_store_factory,
        bucket_factory,
        rgw_deployments,
        test_directory_setup,
    ):
        """
        Test Write to 2 resources, create bucket from them and read from the NS bucket.
        """
        logger.info("Create the namespace resources and verify health")
        nss_tup = ("oc", {"rgw": [(1, None)]})
        ns_store1 = namespace_store_factory(*nss_tup)[0]
        nss_tup = ("oc", {"aws": [(1, self.DEFAULT_REGION)]})
        ns_store2 = namespace_store_factory(*nss_tup)[0]
        logger.info("Upload files directly to cloud target buckets")
        rgw_creds = {
            "access_key_id": cld_mgr.rgw_client.access_key,
            "access_key": cld_mgr.rgw_client.secret_key,
            "endpoint": cld_mgr.rgw_client.endpoint,
        }
        aws_creds = {
            "access_key_id": cld_mgr.aws_client.access_key,
            "access_key": cld_mgr.aws_client.secret_key,
            "endpoint": constants.MCG_NS_AWS_ENDPOINT,
            "region": self.DEFAULT_REGION,
        }

        original_folder = test_directory_setup.origin_dir
        result_folder = test_directory_setup.result_dir

        self.write_files_to_pod_and_upload(
            mcg_obj,
            awscli_pod_session,
            bucket_to_write=ns_store1.uls_name,
            original_dir=original_folder,
            amount=3,
            s3_creds=rgw_creds,
        )
        self.write_files_to_pod_and_upload(
            mcg_obj,
            awscli_pod_session,
            bucket_to_write=ns_store2.uls_name,
            original_dir=original_folder,
            amount=3,
            s3_creds=aws_creds,
        )
        logger.info("Create the namespace bucket on top of the namespace resource")
        bucketclass_dict = {
            "interface": "OC",
            "namespace_policy_dict": {
                "type": "Multi",
                "namespacestores": [ns_store1, ns_store2],
            },
        }
        ns_bucket = bucket_factory(
            amount=1,
            interface=bucketclass_dict["interface"],
            bucketclass=bucketclass_dict,
        )[0].name
        logger.info("Read files from ns bucket")
        self.download_files(
            mcg_obj,
            awscli_pod_session,
            bucket_to_read=ns_bucket,
            download_dir=result_folder,
        )
        logger.info("Compare between uploaded files and downloaded files")
        assert self.compare_dirs(
            awscli_pod_session,
            origin=original_folder,
            destination=result_folder,
            amount=3,
        )

    @tier4c
    @pytest.mark.parametrize(
        argnames=["mcg_pod"],
        argvalues=[
            pytest.param(*["noobaa-db"], marks=pytest.mark.polarion_id("OCS-2291")),
            pytest.param(*["noobaa-core"], marks=pytest.mark.polarion_id("OCS-2319")),
            pytest.param(
                *["noobaa-operator"], marks=pytest.mark.polarion_id("OCS-2320")
            ),
        ],
    )
    def test_respin_mcg_pod_and_check_data_integrity_crd(
        self,
        mcg_obj,
        cld_mgr,
        awscli_pod_session,
        namespace_store_factory,
        bucket_factory,
        test_directory_setup,
        mcg_pod,
    ):
        """
        Test Write to ns bucket using CRDs and read directly from AWS.
        Respin one of mcg pods when data are uploaded.
        """

        logger.info("Create the namespace resources and verify health")
        nss_tup = ("oc", {"aws": [(1, self.DEFAULT_REGION)]})
        ns_store = namespace_store_factory(*nss_tup)[0]

        logger.info("Create the namespace bucket on top of the namespace stores")
        bucketclass_dict = {
            "interface": "OC",
            "namespace_policy_dict": {
                "type": "Single",
                "namespacestores": [ns_store],
            },
        }
        logger.info("Create the namespace bucket on top of the namespace resource")
        ns_bucket = bucket_factory(
            amount=1,
            interface=bucketclass_dict["interface"],
            bucketclass=bucketclass_dict,
        )[0].name
        s3_creds = {
            "access_key_id": cld_mgr.aws_client.access_key,
            "access_key": cld_mgr.aws_client.secret_key,
            "endpoint": constants.MCG_NS_AWS_ENDPOINT,
            "region": self.DEFAULT_REGION,
        }
        original_folder = test_directory_setup.origin_dir
        result_folder = test_directory_setup.result_dir
        logger.info("Upload files to NS bucket")
        self.write_files_to_pod_and_upload(
            mcg_obj,
            awscli_pod_session,
            bucket_to_write=ns_bucket,
            original_dir=original_folder,
            amount=3,
        )

        logger.info(f"Respin mcg resource {mcg_pod}")
        noobaa_pods = pod.get_noobaa_pods()
        pod_obj = [pod for pod in noobaa_pods if pod.name.startswith(mcg_pod)][0]
        pod_obj.delete(force=True)
        logger.info("Wait for noobaa pods to come up")
        assert pod_obj.ocp.wait_for_resource(
            condition="Running",
            selector="app=noobaa",
            resource_count=len(noobaa_pods),
            timeout=1000,
        )
        logger.info("Wait for noobaa health to be OK")
        ceph_cluster_obj = CephCluster()
        ceph_cluster_obj.wait_for_noobaa_health_ok()

        logger.info("Read files directly from AWS")
        self.download_files(
            mcg_obj,
            awscli_pod_session,
            bucket_to_read=ns_store.uls_name,
            download_dir=result_folder,
            s3_creds=s3_creds,
        )

        logger.info("Compare between uploaded files and downloaded files")
        assert self.compare_dirs(
            awscli_pod_session,
            origin=original_folder,
            destination=result_folder,
            amount=3,
        )

    @pytest.mark.polarion_id("OCS-2293")
    @tier2
    def test_namespace_bucket_creation_with_many_resources_crd(
        self, namespace_store_factory, bucket_factory
    ):
        """
        Test namespace bucket creation using the CRD.
        Use 100+ read resources.
        """
        logger.info("Create namespace resources and verify health")
        nss_tup = ("oc", {"aws": [(100, self.DEFAULT_REGION)]})
        ns_resources = namespace_store_factory(*nss_tup)

        logger.info("Create the namespace bucket with many namespace resources")
        bucketclass_dict = {
            "interface": "OC",
            "namespace_policy_dict": {
                "type": "Multi",
                "namespacestores": ns_resources,
            },
        }
        logger.info("Create the namespace bucket on top of the namespace resource")
        bucket_factory(
            amount=1,
            interface=bucketclass_dict["interface"],
            bucketclass=bucketclass_dict,
        )

    @pytest.mark.polarion_id("OCS-2325")
    @tier2
    def test_block_read_resource_in_namespace_bucket_crd(
        self,
        mcg_obj,
        awscli_pod_session,
        namespace_store_factory,
        bucket_factory,
        cld_mgr,
        test_directory_setup,
    ):
        """
        Test blocking namespace resource in namespace bucket.
        Check data availability.
        """
        aws_client = cld_mgr.aws_client
        s3_creds = {
            "access_key_id": cld_mgr.aws_client.access_key,
            "access_key": cld_mgr.aws_client.secret_key,
            "endpoint": constants.MCG_NS_AWS_ENDPOINT,
            "region": self.DEFAULT_REGION,
        }

        logger.info("Create namespace resources and verify health")
        nss_tup = ("oc", {"aws": [(1, self.DEFAULT_REGION)]})
        ns_store1 = namespace_store_factory(*nss_tup)[0]
        ns_store2 = namespace_store_factory(*nss_tup)[0]

        original_folder = test_directory_setup.origin_dir
        result_folder = test_directory_setup.result_dir

        logger.info("Upload files to NS resources")
        self.write_files_to_pod_and_upload(
            mcg_obj,
            awscli_pod_session,
            bucket_to_write=ns_store1.uls_name,
            original_dir=original_folder,
            amount=3,
            s3_creds=s3_creds,
        )
        self.write_files_to_pod_and_upload(
            mcg_obj,
            awscli_pod_session,
            bucket_to_write=ns_store2.uls_name,
            original_dir=original_folder,
            amount=2,
            s3_creds=s3_creds,
        )

        logger.info("Create the namespace bucket")
        bucketclass_dict = {
            "interface": "OC",
            "namespace_policy_dict": {
                "type": "Multi",
                "namespacestores": [ns_store1, ns_store2],
            },
        }

        ns_bucket = bucket_factory(
            amount=1,
            interface=bucketclass_dict["interface"],
            bucketclass=bucketclass_dict,
        )[0]

        logger.info("Bring ns_store1 down")
        aws_client.toggle_aws_bucket_readwrite(ns_store1.uls_name)

        logger.info("Read files directly from AWS")
        try:
            self.download_files(
                mcg_obj,
                awscli_pod_session,
                bucket_to_read=ns_bucket,
                download_dir=result_folder,
            )
        except CommandFailed:
            logger.info("Attempt to read files failed as expected")
            logger.info("Bring ns_store1 up")
            aws_client.toggle_aws_bucket_readwrite(ns_store1.uls_name, block=False)
        else:
            logger.info("Bring ns_store1 up")
            aws_client.toggle_aws_bucket_readwrite(ns_store1.uls_name, block=False)
            msg = (
                "It should not be possible to download from Namespace bucket "
                "in current state according to "
                "https://bugzilla.redhat.com/show_bug.cgi?id=1887417#c2"
            )
            logger.error(msg)
            assert False, msg

    @pytest.mark.polarion_id("OCS-2504")
    @tier2
    @marks.bugzilla("1927367")
    def test_ns_bucket_unsigned_access(
        self, mcg_obj, bucket_factory, namespace_store_factory
    ):
        """
        Test anonymous(unsigned) access of S3 operations are denied on Namespace bucket.
        """
        sample_data = "Sample string content to write to a S3 object"
        object_key = "ObjKey-" + str(uuid.uuid4().hex)

        # Create the namespace bucket
        nss_tup = ("oc", {"aws": [(1, self.DEFAULT_REGION)]})
        ns_store = namespace_store_factory(*nss_tup)[0]
        bucketclass_dict = {
            "interface": "OC",
            "namespace_policy_dict": {
                "type": "Single",
                "namespacestores": [ns_store],
            },
        }
        ns_bucket = bucket_factory(
            amount=1,
            interface=bucketclass_dict["interface"],
            bucketclass=bucketclass_dict,
        )[0].name

        # Put and Get object operations done with s3 credentials
        logger.info(f"Put and Get object operations on {ns_bucket}")
        assert bucket_utils.s3_put_object(
            s3_obj=mcg_obj,
            bucketname=ns_bucket,
            object_key=object_key,
            data=sample_data,
        ), "Failed: PutObject"
        assert bucket_utils.s3_get_object(
            s3_obj=mcg_obj, bucketname=ns_bucket, object_key=object_key
        ), "Failed: GetObject"

        # Boto3 client with signing disabled
        anon_s3_client = boto3.client(
            "s3",
            verify=retrieve_verification_mode(),
            endpoint_url=mcg_obj.s3_endpoint,
            config=Config(signature_version=UNSIGNED),
        )

        logger.info(
            f"Verifying anonymous access is blocked on namespace bucket: {ns_bucket}"
        )
        try:
            anon_s3_client.get_object(Bucket=ns_bucket, Key=object_key)
        except boto3exception.ClientError as e:
            response = HttpResponseParser(e.response)
            assert (
                response.error["Code"] == "AccessDenied"
            ), f"Invalid error code:{response.error['Code']}"
            assert (
                response.status_code == 403
            ), f"Invalid status code:{response.status_code}"
            assert (
                response.error["Message"] == "Access Denied"
            ), f"Invalid error message:{response.error['Message']}"
        else:
            assert (
                False
            ), "GetObject operation has been granted access, when it should have been blocked"

    def write_files_to_pod_and_upload(
        self,
        mcg_obj,
        awscli_pod,
        bucket_to_write,
        original_dir,
        amount=1,
        s3_creds=None,
    ):
        """
        Upload files to bucket (NS or uls)
        """
        full_object_path = f"s3://{bucket_to_write}"
        object_list = []

        for i in range(amount):
            file_name = f"testfile{i}.txt"
            object_list.append(file_name)
            awscli_pod.exec_cmd_on_pod(
                f"dd if=/dev/urandom of={original_dir}/{file_name} bs=1M count=1 status=none"
            )
        if s3_creds:
            # Write data directly to target bucket from original dir
            sync_object_directory(
                awscli_pod,
                original_dir,
                full_object_path,
                signed_request_creds=s3_creds,
            )
        else:
            # Write data directly to NS bucket from original dir
            sync_object_directory(awscli_pod, original_dir, full_object_path, mcg_obj)
        return object_list

    def download_files(
        self, mcg_obj, awscli_pod, bucket_to_read, download_dir, s3_creds=None
    ):
        """
        Download files from bucket (NS or uls)
        """
        ns_bucket_path = f"s3://{bucket_to_read}"

        if s3_creds:
            # Read data directly from target bucket (uls) to result dir
            sync_object_directory(
                awscli_pod,
                ns_bucket_path,
                download_dir,
                signed_request_creds=s3_creds,
            )
        else:
            # Read data from NS bucket to result dir
            sync_object_directory(awscli_pod, ns_bucket_path, download_dir, mcg_obj)

    def compare_dirs(self, awscli_pod, origin, destination, amount=1):
        # Checksum is compared between original and result object
        result = True
        for i in range(amount):
            file_name = f"testfile{i}.txt"
            original_object_path = f"{origin}/{file_name}"
            result_object_path = f"{destination}/{file_name}"
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
