import logging
import pytest
from ocs_ci.framework.testlib import (
    MCGTest,
    tier1,
    tier2,
    tier3,
    tier4,
    tier4a,
    acceptance,
)
from ocs_ci.ocs.bucket_utils import sync_object_directory, verify_s3_object_integrity
from ocs_ci.framework.pytest_customization.marks import skipif_aws_creds_are_missing
from ocs_ci.ocs import constants
from ocs_ci.ocs.cluster import CephCluster
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.resources import pod

logger = logging.getLogger(__name__)


@skipif_aws_creds_are_missing
class TestNamespace(MCGTest):
    """
    Test creation of a namespace resource
    """

    MCG_NS_RESULT_DIR = "/result"
    MCG_NS_ORIGINAL_DIR = "/original"
    # TODO: fix this when https://github.com/red-hat-storage/ocs-ci/issues/3338
    # is resolved
    DEFAULT_REGION = "us-east-2"

    @pytest.mark.polarion_id("OCS-2255")
    @tier1
    def test_namespace_resource_creation(self, ns_resource_factory):
        """
        Test namespace resource creation using the MCG RPC.
        """
        # Create the namespace resource and verify health
        ns_resource_factory()

    @tier1
    @acceptance
    @pytest.mark.parametrize(
        argnames=["platform"],
        argvalues=[
            pytest.param(
                constants.AWS_PLATFORM, marks=pytest.mark.polarion_id("OCS-2256")
            ),
            pytest.param(
                constants.AZURE_PLATFORM, marks=pytest.mark.polarion_id("OCS-2409")
            ),
        ],
    )
    def test_namespace_bucket_creation(
        self, ns_resource_factory, bucket_factory, platform
    ):
        """
        Test namespace bucket creation using the MCG RPC.
        """
        # Create the namespace resource and verify health
        ns_resource_name = ns_resource_factory(platform=platform)[1]

        # Create the namespace bucket on top of the namespace resource
        bucket_factory(
            amount=1,
            interface="mcg-namespace",
            write_ns_resource=ns_resource_name,
            read_ns_resources=[ns_resource_name],
        )

    @pytest.mark.polarion_id("OCS-2407")
    @tier1
    def test_namespace_bucket_creation_with_rgw(
        self, ns_resource_factory, bucket_factory, rgw_deployments
    ):
        """
        Test namespace bucket creation using the MCG RPC.

        """
        # Create the namespace resource and verify health
        ns_resource_name = ns_resource_factory(platform=constants.RGW_PLATFORM)[1]

        # Create the namespace bucket on top of the namespace resource
        bucket_factory(
            amount=1,
            interface="mcg-namespace",
            write_ns_resource=ns_resource_name,
            read_ns_resources=[ns_resource_name],
        )

    @pytest.mark.polarion_id("OCS-2257")
    @tier1
    def test_write_to_aws_read_from_ns(
        self, mcg_obj, cld_mgr, awscli_pod, ns_resource_factory, bucket_factory
    ):
        """
        Test Write to AWS and read from ns bucket using MCG RPC.
        """
        # Create the namespace resource and verify health
        result = ns_resource_factory()
        target_bucket_name = result[0]
        ns_resource_name = result[1]

        # Create the namespace bucket on top of the namespace resource
        rand_ns_bucket = bucket_factory(
            amount=1,
            interface="mcg-namespace",
            write_ns_resource=ns_resource_name,
            read_ns_resources=[ns_resource_name],
        )[0].name

        s3_creds = {
            "access_key_id": cld_mgr.aws_client.access_key,
            "access_key": cld_mgr.aws_client.secret_key,
            "endpoint": constants.MCG_NS_AWS_ENDPOINT,
            "region": self.DEFAULT_REGION,
        }
        # Upload files directly to AWS
        self.write_files_to_pod_and_upload(
            mcg_obj,
            awscli_pod,
            bucket_to_write=target_bucket_name,
            amount=3,
            s3_creds=s3_creds,
        )
        # Read files from ns bucket
        self.download_files(mcg_obj, awscli_pod, bucket_to_read=rand_ns_bucket)

        # Compare between uploaded files and downloaded files
        assert self.compare_dirs(awscli_pod, amount=3)

    @pytest.mark.polarion_id("OCS-2258")
    @tier1
    def test_write_to_ns_read_from_aws(
        self, mcg_obj, cld_mgr, awscli_pod, ns_resource_factory, bucket_factory
    ):
        """
        Test Write to ns bucket using MCG RPC and read directly from AWS.
        """

        # Create the namespace resource and verify health
        result = ns_resource_factory()
        target_bucket_name = result[0]
        ns_resource_name = result[1]

        # Create the namespace bucket on top of the namespace resource
        rand_ns_bucket = bucket_factory(
            amount=1,
            interface="mcg-namespace",
            write_ns_resource=ns_resource_name,
            read_ns_resources=[ns_resource_name],
        )[0].name

        s3_creds = {
            "access_key_id": cld_mgr.aws_client.access_key,
            "access_key": cld_mgr.aws_client.secret_key,
            "endpoint": constants.MCG_NS_AWS_ENDPOINT,
            "region": self.DEFAULT_REGION,
        }
        # Upload files to NS bucket
        self.write_files_to_pod_and_upload(
            mcg_obj, awscli_pod, bucket_to_write=rand_ns_bucket, amount=3
        )
        # Read files directly from AWS
        self.download_files(
            mcg_obj, awscli_pod, bucket_to_read=target_bucket_name, s3_creds=s3_creds
        )

        # Compare between uploaded files and downloaded files
        assert self.compare_dirs(awscli_pod, amount=3)

    @pytest.mark.polarion_id("OCS-2292")
    @tier2
    def test_distribution_of_objects_in_ns_bucket(
        self,
        mcg_obj,
        cld_mgr,
        awscli_pod,
        ns_resource_factory,
        bucket_factory,
        rgw_deployments,
    ):
        """
        Test that uploaded objects into resources were correctly uploaded even
        when some file is the same and downloaded after that.

        """
        logger.info("Create the namespace resources and verify health")
        target_bucket1, resource1 = ns_resource_factory(platform=constants.RGW_PLATFORM)
        target_bucket2, resource2 = ns_resource_factory(platform=constants.AWS_PLATFORM)

        logger.info("Upload files directly to first target bucket")
        rgw_creds = {
            "access_key_id": cld_mgr.rgw_client.access_key,
            "access_key": cld_mgr.rgw_client.secret_key,
            "endpoint": cld_mgr.rgw_client.endpoint,
        }
        self.write_files_to_pod_and_upload(
            mcg_obj,
            awscli_pod,
            bucket_to_write=target_bucket1,
            amount=4,
            s3_creds=rgw_creds,
        )

        logger.info("Create the namespace bucket on top of the namespace resource")
        rand_ns_bucket = bucket_factory(
            amount=1,
            interface="mcg-namespace",
            write_ns_resource=resource1,
            read_ns_resources=[resource1, resource2],
        )[0].name

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
            bucket_to_write=target_bucket2,
            amount=3,
            s3_creds=aws_creds,
        )

        logger.info("Read files from ns bucket")
        self.download_files(mcg_obj, awscli_pod, bucket_to_read=rand_ns_bucket)

        logger.info("Compare between uploaded files and downloaded files")
        assert self.compare_dirs(awscli_pod, amount=4)

    @pytest.mark.polarion_id("OCS-2290")
    @tier2
    def test_create_ns_bucket_from_utilized_resources(
        self,
        mcg_obj,
        cld_mgr,
        awscli_pod,
        ns_resource_factory,
        bucket_factory,
        rgw_deployments,
    ):
        """
        Test Write to 2 resources, create bucket from them and read from the NS bucket.

        """
        logger.info("Create the namespace resources and verify health")
        target_bucket1, resource1 = ns_resource_factory(platform=constants.RGW_PLATFORM)
        target_bucket2, resource2 = ns_resource_factory(platform=constants.AWS_PLATFORM)

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
        self.write_files_to_pod_and_upload(
            mcg_obj,
            awscli_pod,
            bucket_to_write=target_bucket1,
            amount=3,
            s3_creds=rgw_creds,
        )
        self.write_files_to_pod_and_upload(
            mcg_obj,
            awscli_pod,
            bucket_to_write=target_bucket2,
            amount=3,
            s3_creds=aws_creds,
        )

        logger.info("Create the namespace bucket on top of the namespace resource")
        rand_ns_bucket = bucket_factory(
            amount=1,
            interface="mcg-namespace",
            write_ns_resource=resource1,
            read_ns_resources=[resource1, resource2],
        )[0].name

        logger.info("Read files from ns bucket")
        self.download_files(mcg_obj, awscli_pod, bucket_to_read=rand_ns_bucket)

        logger.info("Compare between uploaded files and downloaded files")
        assert self.compare_dirs(awscli_pod, amount=3)

    @tier2
    @pytest.mark.parametrize(
        argnames=["platform1", "platform2"],
        argvalues=[
            pytest.param(
                *[constants.AWS_PLATFORM, constants.AZURE_PLATFORM],
                marks=pytest.mark.polarion_id("OCS-2416"),
            ),
            pytest.param(
                *[constants.AWS_PLATFORM, constants.AWS_PLATFORM],
                marks=pytest.mark.polarion_id("OCS-2418"),
            ),
            pytest.param(
                *[constants.AZURE_PLATFORM, constants.AZURE_PLATFORM],
                marks=pytest.mark.polarion_id("OCS-2419"),
            ),
        ],
    )
    def test_resource_combinations(
        self, ns_resource_factory, bucket_factory, platform1, platform2
    ):
        """
        Test namespace bucket creation using the MCG RPC. Use 2 resources.

        """
        # Create the namespace resources and verify health
        ns_resource_name1 = ns_resource_factory(platform=platform1)[1]
        ns_resource_name2 = ns_resource_factory(platform=platform2)[1]

        # Create the namespace bucket on top of the namespace resource
        bucket_factory(
            amount=1,
            interface="mcg-namespace",
            write_ns_resource=ns_resource_name1,
            read_ns_resources=[ns_resource_name1, ns_resource_name2],
        )

    @pytest.mark.polarion_id("OCS-2417")
    @tier2
    def test_resource_combinations_with_rgw(
        self, ns_resource_factory, rgw_deployments, bucket_factory
    ):
        """
        Test namespace bucket creation using the MCG RPC. Use 2 resources.

        """
        # Create the namespace resource and verify health
        ns_resource_name1 = ns_resource_factory(platform=constants.RGW_PLATFORM)[1]
        ns_resource_name2 = ns_resource_factory(platform=constants.RGW_PLATFORM)[1]

        # Create the namespace bucket on top of the namespace resource
        bucket_factory(
            amount=1,
            interface="mcg-namespace",
            write_ns_resource=ns_resource_name1,
            read_ns_resources=[ns_resource_name1, ns_resource_name2],
        )

    @pytest.mark.polarion_id("OCS-2280")
    @pytest.mark.bugzilla("1900760")
    @tier3
    def test_create_resource_with_invalid_target_bucket(
        self, mcg_obj, mcg_connection_factory
    ):
        """
        Test that a proper error message is reported when invalid target
        bucket is provided during namespace resource creation.

        """
        connection_name = mcg_connection_factory()
        for target_bucket in ("", " ", "/*-#$%@^"):
            response = mcg_obj.send_rpc_query(
                "pool_api",
                "create_namespace_resource",
                {
                    "name": "invalid_resource",
                    "connection": connection_name,
                    "target_bucket": target_bucket,
                },
            )
            assert "error" in response.json()

    @pytest.mark.polarion_id("OCS-2282")
    @tier3
    def test_delete_resource_used_in_ns_bucket(
        self, mcg_obj, cld_mgr, awscli_pod, ns_resource_factory, bucket_factory
    ):
        """
        Test that a proper error message is reported when invalid target
        bucket is provided during namespace resource creation.

        """
        # Create the namespace resources and verify health
        _, resource1 = ns_resource_factory()
        _, resource2 = ns_resource_factory()

        # Create the namespace bucket on top of the namespace resource
        bucket_factory(
            amount=1,
            interface="mcg-namespace",
            write_ns_resource=resource1,
            read_ns_resources=[resource1, resource2],
        )
        response = mcg_obj.send_rpc_query(
            "pool_api", "delete_namespace_resource", {"name": resource2}
        )
        assert "error" in response.json()

    @pytest.mark.polarion_id("OCS-2282")
    @tier3
    def test_delete_nonexistent_resource(self, mcg_obj):
        """
        Test that a proper error message is reported when nonexistent resource
        is deleted.

        """
        response = mcg_obj.send_rpc_query(
            "pool_api", "delete_namespace_resource", {"name": "notexisting_resource"}
        )
        assert "error" in response.json()

    @tier4
    @tier4a
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
    def test_respin_mcg_pod_and_check_data_integrity(
        self, mcg_obj, cld_mgr, awscli_pod, ns_resource_factory, bucket_factory, mcg_pod
    ):
        """
        Test Write to ns bucket using MCG RPC and read directly from AWS.
        Respin one of mcg pods when data are uploaded.

        """

        logger.info("Create the namespace resource and verify health")
        resource = ns_resource_factory()
        target_bucket_name = resource[0]
        ns_resource_name = resource[1]
        s3_creds = {
            "access_key_id": cld_mgr.aws_client.access_key,
            "access_key": cld_mgr.aws_client.secret_key,
            "endpoint": constants.MCG_NS_AWS_ENDPOINT,
            "region": self.DEFAULT_REGION,
        }

        logger.info("Create the namespace bucket on top of the namespace resource")
        rand_ns_bucket = bucket_factory(
            amount=1,
            interface="mcg-namespace",
            write_ns_resource=ns_resource_name,
            read_ns_resources=[ns_resource_name],
        )[0].name

        logger.info("Upload files to NS bucket")
        self.write_files_to_pod_and_upload(
            mcg_obj, awscli_pod, bucket_to_write=rand_ns_bucket, amount=3
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
            mcg_obj, awscli_pod, bucket_to_read=target_bucket_name, s3_creds=s3_creds
        )

        logger.info("Compare between uploaded files and downloaded files")
        assert self.compare_dirs(awscli_pod, amount=3)

    @pytest.mark.polarion_id("OCS-2293")
    @tier4
    @tier4a
    def test_namespace_bucket_creation_with_many_resources(
        self, ns_resource_factory, bucket_factory
    ):
        """
        Test namespace bucket creation using the MCG RPC.
        Use 100+ read resources.

        """
        logger.info("Create namespace resources and verify health")
        ns_resources = [ns_resource_factory()[1] for _ in range(0, 100)]

        logger.info("Create the namespace bucket with many namespace resources")
        bucket_factory(
            amount=1,
            interface="mcg-namespace",
            write_ns_resource=ns_resources[0],
            read_ns_resources=ns_resources,
        )

    @pytest.mark.polarion_id("OCS-2325")
    @tier4
    @tier4a
    def test_block_read_resource_in_namespace_bucket(
        self, mcg_obj, awscli_pod, ns_resource_factory, bucket_factory, cld_mgr
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
        resource1 = ns_resource_factory()
        resource2 = ns_resource_factory()

        logger.info("Upload files to NS resources")
        self.write_files_to_pod_and_upload(
            mcg_obj,
            awscli_pod,
            bucket_to_write=resource1[0],
            amount=3,
            s3_creds=s3_creds,
        )
        self.write_files_to_pod_and_upload(
            mcg_obj,
            awscli_pod,
            bucket_to_write=resource2[0],
            amount=2,
            s3_creds=s3_creds,
        )

        logger.info("Create the namespace bucket")
        rand_ns_bucket = bucket_factory(
            amount=1,
            interface="mcg-namespace",
            write_ns_resource=resource2[1],
            read_ns_resources=[resource1[1], resource2[1]],
        )[0].name

        logger.info("Bring resource1 down")
        aws_client.toggle_aws_bucket_readwrite(resource1[0])

        logger.info("Read files directly from AWS")
        try:
            self.download_files(mcg_obj, awscli_pod, bucket_to_read=rand_ns_bucket)
        except CommandFailed:
            logger.info("Attempt to read files failed as expected")
            logger.info("Bring resource1 up")
            aws_client.toggle_aws_bucket_readwrite(resource1[0], block=False)
        else:
            logger.info("Bring resource1 up")
            aws_client.toggle_aws_bucket_readwrite(resource1[0], block=False)
            msg = (
                "It should not be possible to download from Namespace bucket "
                "in current state according to "
                "https://bugzilla.redhat.com/show_bug.cgi?id=1887417#c2"
            )
            logger.error(msg)
            assert False, msg

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
