import logging
import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    red_squad,
    rgw,
    runs_on_provider,
    skipif_ec_pools_disabled,
    tier2,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.resources.objectbucket import OBC
from ocs_ci.ocs.bucket_utils import write_random_test_objects_to_bucket
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)


@rgw
@red_squad
@runs_on_provider
@skipif_ec_pools_disabled
class TestRGWECConfig:
    """
    Test RGW EC pool configuration and data operations
    """

    @tier2
    def test_rgw_ec_pool_configuration(
        self,
        rgw_bucket_factory,
        awscli_pod,
        test_directory_setup,
    ):
        """
        Test to validate RGW EC pool configuration and data operations

        Steps:
        1. Validate CephObjectStore exists and is in Ready phase
        2. Validate RGW data pool is deployed with defined k+m value
        3. Validate RGW index pool is deployed with 3 copy replication
        4. Write 1 GB of data into RGW bucket
        5. Validate RGW data pool consumption
        """

        namespace = config.ENV_DATA["cluster_namespace"]
        objectstore_name = constants.CEPHOBJECTSTORE_NAME

        # Step 1: Validate object storage is created for EC pool
        logger.info("Validating CephObjectStore exists and is Ready")

        cos_obj = OCP(
            kind=constants.CEPHOBJECTSTORE,
            namespace=namespace,
        )

        try:
            cos_data = cos_obj.get(resource_name=objectstore_name)
            logger.info(f"CephObjectStore '{objectstore_name}' found")
        except CommandFailed:
            pytest.fail(
                f"CephObjectStore '{objectstore_name}' not found. "
                "Ensure EC pool deployment is configured."
            )

        # Validate CephObjectStore status is Ready
        phase = cos_data.get("status", {}).get("phase")
        assert phase == "Ready", (
            f"CephObjectStore '{objectstore_name}' is not in Ready phase. "
            f"Current phase: {phase}"
        )
        logger.info(f"CephObjectStore '{objectstore_name}' is in Ready phase")

        # Step 2: Validate RGW data pool is deployed with defined k+m value
        logger.info("Validating RGW data pool EC configuration (k+m values)")

        expected_k = config.DEPLOYMENT.get("ec_data_chunks", 2)
        expected_m = config.DEPLOYMENT.get("ec_coding_chunks", 1)
        expected_fd = config.DEPLOYMENT.get("ec_failure_domain", "host")

        ec_config = cos_data["spec"]["dataPool"].get("erasureCoded", {})

        actual_k = ec_config.get("dataChunks")
        actual_m = ec_config.get("codingChunks")
        actual_fd = cos_data["spec"]["dataPool"].get("failureDomain")

        assert (
            actual_k == expected_k
        ), f"Data chunks mismatch: expected {expected_k}, got {actual_k}"
        assert (
            actual_m == expected_m
        ), f"Coding chunks mismatch: expected {expected_m}, got {actual_m}"
        assert (
            actual_fd == expected_fd
        ), f"Failure domain mismatch: expected {expected_fd}, got {actual_fd}"

        logger.info(
            f"RGW data pool is correctly configured with EC k={actual_k}, m={actual_m}"
        )

        # Step 3: Validate RGW index pool is deployed with 3 copy replication
        logger.info("Validating RGW index pool replication")

        metadata_pool_spec = cos_data["spec"]["metadataPool"]
        replicated_config = metadata_pool_spec.get("replicated", {})
        replica_size = replicated_config.get("size")

        expected_replica_size = 3
        assert replica_size == expected_replica_size, (
            f"Metadata pool replication mismatch: expected {expected_replica_size}, "
            f"got {replica_size}"
        )

        logger.info(
            f"RGW metadata pool is correctly configured with {replica_size} replicas"
        )

        # Verify pool configuration at Ceph level
        logger.info("Verifying pool configuration at Ceph level")
        ct_pod = pod.get_ceph_tools_pod()

        # Get RGW pool names from ceph osd pool ls
        pool_list_output = ct_pod.exec_ceph_cmd(
            ceph_cmd="ceph osd pool ls detail", format="json"
        )

        # Find RGW data and index pools for this CephObjectStore
        rgw_data_pool = None
        rgw_index_pool = None

        for pool_info in pool_list_output:
            pool_name = pool_info["pool_name"]
            if objectstore_name.replace("-", ".") in pool_name.replace("-", "."):
                if "buckets.data" in pool_name:
                    rgw_data_pool = pool_info
                elif "buckets.index" in pool_name:
                    rgw_index_pool = pool_info

        # Verify we found the required pools
        assert (
            rgw_data_pool is not None
        ), f"RGW data pool (*.buckets.data) not found for CephObjectStore '{objectstore_name}'"
        assert (
            rgw_index_pool is not None
        ), f"RGW index pool (*.buckets.index) not found for CephObjectStore '{objectstore_name}'"

        data_pool_name = rgw_data_pool["pool_name"]
        index_pool_name = rgw_index_pool["pool_name"]

        logger.info(f"RGW data pool: {data_pool_name}")
        logger.info(f"RGW index pool: {index_pool_name}")

        # Verify data pool is erasure coded
        data_pool_type = rgw_data_pool.get("type")
        assert (
            data_pool_type == 3
        ), f"Data pool '{data_pool_name}' is not erasure coded (type={data_pool_type}, expected 3)"
        logger.info(f"Data pool '{data_pool_name}' is erasure coded as expected")

        # Verify index pool is replicated
        index_pool_type = rgw_index_pool.get("type")
        assert (
            index_pool_type == 1
        ), f"Index pool '{index_pool_name}' is not replicated (type={index_pool_type}, expected 1)"

        # Check index pool replica size
        index_pool_size = rgw_index_pool.get("size", 0)
        logger.info(
            f"Index pool '{index_pool_name}' has size (replica count): {index_pool_size}"
        )

        assert index_pool_size == expected_replica_size, (
            f"Index pool '{index_pool_name}' replica size mismatch: "
            f"expected {expected_replica_size}, got {index_pool_size}"
        )
        logger.info(
            f"Index pool '{index_pool_name}' is correctly configured with {index_pool_size} replicas"
        )

        # Step 4: Write 1 GB of data into RGW bucket
        logger.info("Writing 1 GB of data into RGW bucket")

        # Get initial pool size and object count before writing data
        logger.info("Capturing initial pool statistics before writing data")
        rados_df_output = ct_pod.exec_ceph_cmd(
            ceph_cmd=f"rados df -p {data_pool_name}", format="json"
        )
        initial_size_bytes = 0
        initial_num_objects = 0
        if rados_df_output and "pools" in rados_df_output:
            initial_size_bytes = rados_df_output["pools"][0].get("size_bytes", 0)
            initial_num_objects = rados_df_output["pools"][0].get("num_objects", 0)
            logger.info(
                f"Pool '{data_pool_name}' initial statistics: "
                f"Size: {initial_size_bytes / (1024 * 1024):.2f} MB, "
                f"Objects: {initial_num_objects}"
            )

        # Create bucket using EC storage class
        bucket_name = rgw_bucket_factory(1, "RGW-OC")[0].name
        logger.info(f"Created bucket: {bucket_name}")

        object_size = "100M"
        num_objects = 10

        # Write random test objects
        obc_obj = OBC(bucket_name)

        write_random_test_objects_to_bucket(
            io_pod=awscli_pod,
            bucket_to_write=bucket_name,
            file_dir=test_directory_setup.origin_dir,
            amount=num_objects,
            pattern="rgw-ec-test-",
            bs=object_size,
            mcg_obj=obc_obj,
        )

        logger.info(
            f"Successfully wrote {num_objects} objects to bucket '{bucket_name}'"
        )

        # Step 5: Validate RGW data pool consumption
        logger.info("Validating RGW data pool consumption")

        # Expected size range (accounting for EC overhead and metadata)
        # We wrote 1 GB (1000 MB) of random data, which doesn't compress
        # With EC k+m, the actual stored data depends on the configuration:
        # For k=4, m=2: overhead = (k+m)/k = 6/4 = 1.5x → expect ~1500 MB
        # For k=2, m=1: overhead = (k+m)/k = 3/2 = 1.5x → expect ~1500 MB
        # Setting minimum to 700 MB (allowing 10% variance below the 1 GB written)
        min_expected_mb = 700  # At least 700 MB
        max_expected_mb = 2000  # At most 2 GB (accounting for EC overhead and metadata)

        logger.info(
            f"Waiting for pool '{data_pool_name}' statistics to reflect the written data..."
        )

        def check_pool_size_increase():
            """
            Check if pool size has increased by the expected amount.
            Returns True if size increase is within expected range.
            """
            rados_df_output = ct_pod.exec_ceph_cmd(
                ceph_cmd=f"rados df -p {data_pool_name}", format="json"
            )

            if rados_df_output and "pools" in rados_df_output:
                pool_stats = rados_df_output["pools"][0]
                current_size_bytes = pool_stats.get("size_bytes", 0)
                size_increase_bytes = current_size_bytes - initial_size_bytes
                size_increase_mb = size_increase_bytes / (1024 * 1024)

                logger.info(
                    f"Current pool size: {current_size_bytes / (1024 * 1024):.2f} MB, "
                    f"Increase: {size_increase_mb:.2f} MB"
                )

                # Check if size increase is at least the minimum expected
                if size_increase_mb >= min_expected_mb:
                    return True

            return False

        sample = TimeoutSampler(
            timeout=300,
            sleep=10,
            func=check_pool_size_increase,
        )

        # Wait for the pool size to increase
        sample.wait_for_func_value(value=True)

        # Get final statistics for validation and logging
        rados_df_output = ct_pod.exec_ceph_cmd(
            ceph_cmd=f"rados df -p {data_pool_name}", format="json"
        )

        if rados_df_output and "pools" in rados_df_output:
            pool_stats = rados_df_output["pools"][0]
            final_size_bytes = pool_stats.get("size_bytes", 0)
            num_objects_pool = pool_stats.get("num_objects", 0)

            # Calculate the size and object count increases
            size_increase_bytes = final_size_bytes - initial_size_bytes
            size_increase_mb = size_increase_bytes / (1024 * 1024)
            size_increase_gb = size_increase_mb / 1024
            object_count_increase = num_objects_pool - initial_num_objects

            logger.info(
                f"Pool '{data_pool_name}' final statistics:\n"
                f"  - Initial size: {initial_size_bytes / (1024 * 1024):.2f} MB\n"
                f"  - Final size: {final_size_bytes / (1024 * 1024):.2f} MB\n"
                f"  - Size increase: {size_increase_gb:.2f} GB ({size_increase_mb:.2f} MB)\n"
                f"  - Initial objects: {initial_num_objects}\n"
                f"  - Final objects: {num_objects_pool}\n"
                f"  - Object count increase: {object_count_increase}"
            )

            # Verify that objects were written
            assert object_count_increase > 0, (
                f"Pool '{data_pool_name}' object count did not increase. "
                f"Expected ~{num_objects} new objects, but count only increased by {object_count_increase}"
            )

            logger.info(
                f"Pool '{data_pool_name}' object count increased by {object_count_increase} "
                f"(wrote {num_objects} objects to bucket)"
            )

            # Verify size increase is within expected range
            assert min_expected_mb <= size_increase_mb <= max_expected_mb, (
                f"Pool '{data_pool_name}' size increase {size_increase_mb:.2f} MB is outside "
                f"expected range [{min_expected_mb}, {max_expected_mb}] MB"
            )

            logger.info(
                f"Pool '{data_pool_name}' consumption increase {size_increase_mb:.2f} MB is within "
                f"expected range [{min_expected_mb}, {max_expected_mb}] MB"
            )

        logger.info("All validation steps completed successfully!")
