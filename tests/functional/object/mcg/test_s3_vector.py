import logging
import math
import random
import string

from botocore.exceptions import ClientError, ParamValidationError

from ocs_ci.framework.pytest_customization.marks import (
    tier2,
    red_squad,
    post_upgrade,
    mcg,
    jira,
)
from ocs_ci.framework.testlib import MCGTest
from ocs_ci.framework import config
from ocs_ci.ocs import vector_utils
from ocs_ci.ocs.resources.objectbucket import OBC
from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility.utils import TimeoutSampler


logger = logging.getLogger(__name__)


@mcg
@red_squad
class TestS3Vector(MCGTest):
    """
    Test S3 Vector functionality against noobaa
    """

    def _create_index(
        self,
        s3vectors_client,
        index_name,
        dimension,
        vector_bucket_name,
        data_type="float32",
        distance_metric="cosine",
    ):
        """
        Create a vector index, tolerating the known NooBaa VECTOR_INDEX_ALREADY_OWNED_BY_YOU
        error that occurs when the index is created successfully but the response erroneously
        indicates a duplicate.

        Args:
            s3vectors_client: boto3 s3vectors client
            index_name (str): Name of the index to create
            dimension (int): Dimensionality of the index
            vector_bucket_name (str): Name of the vector bucket
            data_type (str): Vector data type (default: float32)
            distance_metric (str): Distance metric to use (default: cosine)
        """
        try:
            vector_utils.create_index(
                s3vectors_client,
                index_name=index_name,
                data_type=data_type,
                dimension=dimension,
                distance_metric=distance_metric,
                vectorBucketName=vector_bucket_name,
            )
            logger.info(f"✓ Created {dimension}-dimensional index: {index_name}")
        except ClientError as e:
            if "VECTOR_INDEX_ALREADY_OWNED_BY_YOU" in str(e):
                logger.warning(
                    f"Index {index_name} already exists (NooBaa bug), continuing..."
                )
            else:
                raise

    @tier2
    @post_upgrade
    def test_s3_vector_operations(self, vector_bucket_factory, mcg_obj):
        """
        Test complete S3 vector workflow:
        1. Create a vector bucket (auto-creates nsfs namespacestore and vectorPolicy bucketclass)
        2. Validate vector bucket exists
        3. Create 3-dimensional vector index
        4. List indices from vector bucket
        5. Create at least 100 vectors in vector index
        6. List vectors from vector index
        7. Perform similarity search query
        8. Delete all vector entries from vector index
        9. Delete vector indices
        10. Delete vector bucket

        Note: vector_bucket_factory automatically creates:
        - Filesystem-backed namespacestore (nsfs with 50Gi PVC)
        - Bucketclass with vectorPolicy (resource: nsfs, vectorDBType: lance)
        """
        # Generate unique index name
        random_suffix = "".join(
            random.choices(string.ascii_lowercase + string.digits, k=8)
        )
        index_name = f"test-vector-index-{random_suffix}"
        dimension = 3
        num_vectors = 100

        # Step 1: Create a vector bucket using factory
        # Factory auto-creates: nsfs namespacestore + vectorPolicy bucketclass
        logger.info(
            "Creating vector bucket (auto-creates nsfs + vectorPolicy bucketclass)"
        )
        vector_bucket_name = vector_bucket_factory(amount=1)[0].name
        logger.info(f"✓ Vector bucket created: {vector_bucket_name}")

        # Step 2: Validate vector bucket exists and create s3vectors client with OBC creds
        logger.info("Validating vector bucket exists and creating s3vectors client")
        obc_obj = OBC(vector_bucket_name)
        s3vectors_client = vector_utils.create_s3vectors_client(mcg_obj, obc_obj)

        bucket_info = vector_utils.get_vector_bucket(
            s3vectors_client, vector_bucket_name
        )
        assert (
            bucket_info["ResponseMetadata"]["HTTPStatusCode"] == 200
        ), f"Vector bucket {vector_bucket_name} validation failed"
        logger.info(f"✓ Vector bucket {vector_bucket_name} validated")

        # Step 3: Create 3-dimensional vector index
        logger.info(f"Creating {dimension}-dimensional vector index")
        self._create_index(s3vectors_client, index_name, dimension, vector_bucket_name)

        # Step 4: List indices and validate
        logger.info("Listing vector indices")
        indices_response = vector_utils.list_indexes(
            s3vectors_client, vectorBucketName=vector_bucket_name
        )
        index_names = [idx["indexName"] for idx in indices_response.get("indexes", [])]

        assert (
            index_name in index_names
        ), f"Index {index_name} not found in indices list: {index_names}"
        logger.info(f"✓ Found {len(index_names)} indices")

        # Step 5: Create 100 vectors
        logger.info(f"Creating {num_vectors} vectors in batches")
        vectors = vector_utils.generate_test_vectors_with_metadata(
            dimension=dimension, num_vectors=num_vectors
        )

        # Store vectors using put_vectors
        vector_utils.put_vectors(
            s3vectors_client,
            vectors,
            vectorBucketName=vector_bucket_name,
            indexName=index_name,
        )
        logger.info(f"✓ Created {num_vectors} vectors")

        # Step 6: List vectors
        logger.info("Listing vectors from index")
        vectors_response = vector_utils.list_vectors(
            s3vectors_client,
            vectorBucketName=vector_bucket_name,
            indexName=index_name,
        )
        listed_vectors = vectors_response.get("vectors", [])

        assert len(listed_vectors) >= num_vectors, (
            f"Vector count mismatch. Listed: {len(listed_vectors)}, "
            f"Expected: {num_vectors}"
        )
        logger.info(f"✓ Listed {len(listed_vectors)} vectors")

        # Step 7: Perform similarity search
        logger.info("Performing vector similarity search")
        query_vector = [random.uniform(0.0, 2.0) for _ in range(dimension)]
        top_k = 5

        search_results = vector_utils.query_vectors(
            s3vectors_client,
            query_vector=query_vector,
            top_k=top_k,
            vectorBucketName=vector_bucket_name,
            indexName=index_name,
            returnDistance=True,
        )

        results = search_results.get("vectors", [])
        assert len(results) > 0, "Vector search returned no results"
        assert (
            len(results) <= top_k
        ), f"Search returned more results than requested: {len(results)} > {top_k}"

        logger.info(f"✓ Query returned {len(results)} results")
        for idx, result in enumerate(results[:3], 1):
            logger.info(
                f"  Top {idx}: Key={result.get('key')}, "
                f"distance={result.get('distance', 'N/A')}"
            )

        # Step 8: Delete all vectors
        logger.info("Deleting all vectors from index")
        deleted_count = vector_utils.delete_all_vectors(
            s3vectors_client, vectorBucketName=vector_bucket_name, indexName=index_name
        )

        assert deleted_count >= num_vectors, (
            f"Not all vectors were deleted. Deleted: {deleted_count}, "
            f"Expected: {num_vectors}"
        )
        logger.info(f"✓ Deleted {deleted_count} vectors")

        # Verify vectors are deleted (with retry as deletion is asynchronous)
        logger.info("Verifying all vectors are deleted from index")
        for sample in TimeoutSampler(
            timeout=60,
            sleep=5,
            func=lambda: vector_utils.list_vectors(
                s3vectors_client,
                vectorBucketName=vector_bucket_name,
                indexName=index_name,
            ),
        ):
            remaining = sample.get("vectors", [])
            if len(remaining) == 0:
                logger.info("✓ All vectors successfully deleted from index")
                break
            else:
                logger.info(
                    f"Waiting for vectors to be deleted... {len(remaining)} still remaining"
                )

        # Step 9: Delete vector index
        logger.info("Deleting vector index")
        vector_utils.delete_index(
            s3vectors_client, vectorBucketName=vector_bucket_name, indexName=index_name
        )
        logger.info(f"✓ Index '{index_name}' deleted")

        # Verify index is deleted
        indices_after_delete = vector_utils.list_indexes(
            s3vectors_client, vectorBucketName=vector_bucket_name
        )
        remaining_indices = [
            idx["indexName"] for idx in indices_after_delete.get("indexes", [])
        ]
        assert (
            index_name not in remaining_indices
        ), f"Index {index_name} still exists after deletion"

        # Step 10: Delete vector bucket
        logger.info("Deleting vector bucket")
        vector_utils.delete_vector_bucket(s3vectors_client, vector_bucket_name)
        logger.info(f"✓ Vector bucket '{vector_bucket_name}' deleted")

        # Verify bucket is deleted
        buckets_after_delete = vector_utils.list_vector_buckets(s3vectors_client)
        remaining_buckets = [
            bucket["vectorBucketName"]
            for bucket in buckets_after_delete.get("vectorBuckets", [])
        ]
        assert (
            vector_bucket_name not in remaining_buckets
        ), f"Vector bucket {vector_bucket_name} still exists after deletion"
        logger.info("✓ Verified vector bucket deletion")

        logger.info("✅ All S3 vector operations completed successfully!")

    @tier2
    def test_vector_bucket_naming_validation(self, mcg_obj, vector_bucket_factory):
        """
        Test vector bucket name validation and constraints:
            1. Create vector bucket with name between 3-63 characters (valid range)
            2. Create vector bucket with exactly 3 characters (minimum valid)
            3. Create vector bucket with exactly 63 characters (maximum valid)
            4. List vector buckets and verify all created buckets
            5. Get operation on vector buckets
            6. Try to create duplicate bucket with same name (should fail)
            7. Try invalid names: special characters, camelCase, uppercase
            8. Try name with 64+ characters (should fail)
            9. Try name with 2 characters (should fail)
        """
        # Create a base vector bucket to get NSR name for API operations
        logger.info("Creating base vector bucket to obtain NSR configuration")
        base_bucket = vector_bucket_factory(amount=1)[0]

        bc_ocp = OCP(
            kind="bucketclass",
            resource_name=base_bucket.bucketclass.name,
            namespace=config.ENV_DATA["cluster_namespace"],
        )
        bc_data = bc_ocp.get()
        nsr_name = bc_data["spec"]["vectorPolicy"]["resource"]
        logger.info(f"Using namespace resource: {nsr_name}")

        obc_obj = OBC(base_bucket.name)

        # Create s3vectors client with NSR header
        s3vectors_client = vector_utils.create_s3vectors_client(
            mcg_obj, obc_obj, nsr_name=nsr_name
        )

        created_buckets = []
        valid_bucket_names = []

        try:
            # Step 1 & 2 & 3: Create buckets with valid names
            logger.info("Creating vector buckets with valid name lengths")

            # Bucket with name between 3-63 characters (e.g., 10 characters)
            bucket_10_chars = "vec-" + "".join(
                random.choices(string.ascii_lowercase + string.digits, k=6)
            )
            logger.info(f"Creating bucket with 10 characters: {bucket_10_chars}")
            vector_utils.create_vector_bucket(s3vectors_client, bucket_10_chars)
            created_buckets.append(bucket_10_chars)
            valid_bucket_names.append(bucket_10_chars)
            logger.info(f"✓ Bucket with 10 characters created: {bucket_10_chars}")

            # Bucket with exactly 3 characters (minimum)
            bucket_3_chars = "".join(
                random.choices(string.ascii_lowercase + string.digits, k=3)
            )
            logger.info(f"Creating bucket with 3 characters: {bucket_3_chars}")
            vector_utils.create_vector_bucket(s3vectors_client, bucket_3_chars)
            created_buckets.append(bucket_3_chars)
            valid_bucket_names.append(bucket_3_chars)
            logger.info(f"✓ Bucket with 3 characters created: {bucket_3_chars}")

            # Bucket with exactly 63 characters (maximum)
            bucket_63_chars = "vec-" + "".join(
                random.choices(string.ascii_lowercase + string.digits, k=59)
            )
            logger.info(f"Creating bucket with 63 characters: {bucket_63_chars}")
            vector_utils.create_vector_bucket(s3vectors_client, bucket_63_chars)
            created_buckets.append(bucket_63_chars)
            valid_bucket_names.append(bucket_63_chars)
            logger.info(f"✓ Bucket with 63 characters created: {bucket_63_chars}")

            # Step 4: List vector buckets
            logger.info("Listing all vector buckets")
            buckets_list = vector_utils.list_vector_buckets(s3vectors_client)
            bucket_names_from_list = [
                b["vectorBucketName"] for b in buckets_list.get("vectorBuckets", [])
            ]
            logger.info(f"Found {len(bucket_names_from_list)} vector buckets")

            for bucket_name in valid_bucket_names:
                assert (
                    bucket_name in bucket_names_from_list
                ), f"Bucket {bucket_name} not found in list"
            logger.info("✓ All created buckets found in list")

            # Step 5: Get operation on vector bucket
            logger.info("Performing get operation on a randomly selected vector bucket")
            random_bucket = random.choice(valid_bucket_names)
            bucket_info = vector_utils.get_vector_bucket(
                s3vectors_client, random_bucket
            )
            assert (
                bucket_info.get("ResponseMetadata", {}).get("HTTPStatusCode") == 200
            ), f"Get operation failed for {random_bucket}: {bucket_info}"
            logger.info(f"✓ Get operation successful for {random_bucket}")

            # Step 6: Create duplicate bucket with same name
            logger.info("Attempting to create duplicate bucket")
            duplicate_name = valid_bucket_names[0]
            try:
                vector_utils.create_vector_bucket(s3vectors_client, duplicate_name)
                assert (
                    False
                ), f"Duplicate bucket creation should have failed for {duplicate_name}"
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code", "")
                assert error_code in [
                    "BUCKET_ALREADY_OWNED_BY_YOU",
                    "InternalFailure",
                ], f"Unexpected error for duplicate bucket: {error_code}"
                logger.info(
                    f"✓ Duplicate bucket creation correctly rejected: {error_code}"
                )

            # Step 7: Try invalid names
            logger.info("Testing invalid bucket names")
            invalid_names = [
                ("special-chars-@#$", "special characters"),
                ("CamelCaseName", "camelCase"),
                ("UPPERCASENAME", "uppercase"),
                ("bucket_with_underscore", "underscore"),
            ]

            for invalid_name, reason in invalid_names:
                logger.info(
                    f"Attempting to create bucket with {reason}: {invalid_name}"
                )
                try:
                    vector_utils.create_vector_bucket(s3vectors_client, invalid_name)
                    assert (
                        False
                    ), f"Bucket with {reason} should have failed: {invalid_name}"
                except ClientError as e:
                    error_code = e.response.get("Error", {}).get("Code", "")
                    logger.info(
                        f"✓ Bucket with {reason} correctly rejected: {error_code}"
                    )

            # Step 8 & 9: Try invalid bucket name lengths
            logger.info("Testing bucket names with invalid lengths")
            invalid_lengths = [
                (
                    "vec-"
                    + "".join(
                        random.choices(string.ascii_lowercase + string.digits, k=61)
                    ),
                    "64+ characters",
                ),
                ("ab", "2 characters"),
            ]

            for invalid_name, reason in invalid_lengths:
                logger.info(
                    f"Attempting to create bucket with {reason}: {invalid_name}"
                )
                try:
                    vector_utils.create_vector_bucket(s3vectors_client, invalid_name)
                    assert (
                        False
                    ), f"Bucket with {reason} should have failed: {invalid_name}"
                except (ClientError, ParamValidationError) as e:
                    if isinstance(e, ClientError):
                        error_code = e.response.get("Error", {}).get("Code", "")
                        logger.info(
                            f"✓ Bucket with {reason} correctly rejected: {error_code}"
                        )
                    else:
                        logger.info(
                            f"✓ Bucket with {reason} correctly rejected by client-side validation"
                        )

            logger.info(
                "✅ Vector bucket naming validation test completed successfully!"
            )

        finally:
            # Cleanup: Delete all created vector buckets
            logger.info("Cleaning up created vector buckets")
            for bucket_name in created_buckets:
                try:
                    vector_utils.delete_vector_bucket(s3vectors_client, bucket_name)
                    logger.info(f"✓ Deleted bucket: {bucket_name}")
                except Exception as e:
                    logger.warning(f"Failed to delete bucket {bucket_name}: {e}")

    @tier2
    def test_vector_index_dimension_validation(self, vector_bucket_factory, mcg_obj):
        """
        Test vector index dimension validation and constraints:
            1. Create vector bucket
            2. Create 3-dimensional and 4096-dimensional vector indices (min and max valid)
            3. Create 10 more indices with different dimensions between 3-4096
            4. List vector indices in the bucket
            5. Get specific index info from all indices
            6. Delete a specific index from the list
            7. Try to create index with invalid dimensions (4097, -1, 0)
            8. Try to create index with duplicate name
        """
        # Step 1: Create vector bucket
        logger.info("Creating vector bucket for index dimension tests")
        vector_bucket_name = vector_bucket_factory(amount=1)[0].name
        logger.info(f"✓ Vector bucket created: {vector_bucket_name}")

        obc_obj = OBC(vector_bucket_name)
        s3vectors_client = vector_utils.create_s3vectors_client(mcg_obj, obc_obj)

        valid_index_names = []

        # Step 2: Create indices with minimum (3) and maximum (4096) dimensions
        logger.info("Creating indices with min (3) and max (4096) dimensions")

        # 3-dimensional index (minimum)
        index_3d = f"index-3d-{random.randint(1000, 9999)}"
        self._create_index(s3vectors_client, index_3d, 3, vector_bucket_name)
        valid_index_names.append(index_3d)

        # 4096-dimensional index (maximum)
        index_4096d = f"index-4096d-{random.randint(1000, 9999)}"
        self._create_index(s3vectors_client, index_4096d, 4096, vector_bucket_name)
        valid_index_names.append(index_4096d)

        # Step 3: Create 10 more indices with different dimensions between 3-4096
        logger.info("Creating 10 indices with varying dimensions (3-4096)")
        # Generate 10 random unique dimensions between 3-4096
        random_dimensions = random.sample(range(3, 4097), 10)
        logger.info(f"Random dimensions selected: {sorted(random_dimensions)}")

        for dim in random_dimensions:
            index_name = f"index-{dim}d-{random.randint(1000, 9999)}"
            self._create_index(s3vectors_client, index_name, dim, vector_bucket_name)
            valid_index_names.append(index_name)

        logger.info(f"✓ Created total of {len(valid_index_names)} indices")

        # Step 4: List vector indices
        logger.info("Listing all vector indices in bucket")
        indices_response = vector_utils.list_indexes(
            s3vectors_client, vectorBucketName=vector_bucket_name
        )
        listed_indices = indices_response.get("indexes", [])
        listed_index_names = [idx["indexName"] for idx in listed_indices]

        logger.info(f"Found {len(listed_index_names)} indices in bucket")

        for index_name in valid_index_names:
            assert (
                index_name in listed_index_names
            ), f"Index {index_name} not found in list"
        logger.info("✓ All created indices found in list")

        # Step 5: Get specific index info
        logger.info("Getting detailed info for each index")
        for index_name in valid_index_names:
            index_info = vector_utils.get_index(
                s3vectors_client,
                vectorBucketName=vector_bucket_name,
                indexName=index_name,
            )
            # Verify successful retrieval by checking HTTP status code
            assert (
                index_info.get("ResponseMetadata", {}).get("HTTPStatusCode") == 200
            ), f"Get operation failed for {index_name}: {index_info}"
            index_details = index_info.get("index", {})
            logger.info(
                f"✓ Retrieved info for {index_name}: dimension={index_details.get('dimension')}, "
                f"dataType={index_details.get('dataType')}, distanceMetric={index_details.get('distanceMetric')}"
            )

        # Step 6: Delete a specific index
        logger.info("Deleting a specific index from the list")
        index_to_delete = random.choice(valid_index_names)
        logger.info(f"Randomly selected index to delete: {index_to_delete}")
        vector_utils.delete_index(
            s3vectors_client,
            vectorBucketName=vector_bucket_name,
            indexName=index_to_delete,
        )
        logger.info(f"✓ Deleted index: {index_to_delete}")

        # Verify deletion
        indices_after_delete = vector_utils.list_indexes(
            s3vectors_client, vectorBucketName=vector_bucket_name
        )
        remaining_index_names = [
            idx["indexName"] for idx in indices_after_delete.get("indexes", [])
        ]
        assert (
            index_to_delete not in remaining_index_names
        ), f"Index {index_to_delete} still exists after deletion"
        logger.info("✓ Verified index deletion")

        # Step 7: Try to create indices with invalid dimensions
        logger.info("Testing index creation with invalid dimensions")
        invalid_dimensions = [
            (4097, "above-maximum (4097)"),
            (-1, "negative (-1)"),
            (0, "zero (0)"),
        ]

        for dim, description in invalid_dimensions:
            index_name_invalid = f"index-{description}-{random.randint(1000, 9999)}"
            logger.info(
                f"Attempting to create index with {description} dimension ({dim})"
            )
            try:
                vector_utils.create_index(
                    s3vectors_client,
                    index_name=index_name_invalid,
                    data_type="float32",
                    dimension=dim,
                    distance_metric="cosine",
                    vectorBucketName=vector_bucket_name,
                )
                assert (
                    False
                ), f"Index with {description} dimension ({dim}) should have failed"
            except (ClientError, ParamValidationError, ValueError) as e:
                logger.info(
                    f"✓ Index with {description} dimension correctly rejected: {type(e).__name__}"
                )

        # Step 8: Try to create index with duplicate name
        logger.info("Testing index creation with duplicate name")
        duplicate_index_name = valid_index_names[0]
        try:
            vector_utils.create_index(
                s3vectors_client,
                index_name=duplicate_index_name,
                data_type="float32",
                dimension=100,
                distance_metric="cosine",
                vectorBucketName=vector_bucket_name,
            )
            assert (
                False
            ), f"Duplicate index creation should have failed for {duplicate_index_name}"
        except ClientError as e:
            error_message = str(e)
            # Could be ALREADY_OWNED or other duplicate error
            assert (
                "VECTOR_INDEX_ALREADY_OWNED_BY_YOU" in error_message
                or "already exists" in error_message.lower()
            ), f"Unexpected error for duplicate index: {error_message}"
            logger.info("✓ Duplicate index creation correctly rejected")

    @tier2
    def test_vector_multi_dimensional_indices(self, vector_bucket_factory, mcg_obj):
        """
        Test multiple vector indices with different dimensions and cleanup scenarios:
            1. Create vector bucket
            2. Create 3-dimensional vector index
            3. Insert valid vector entry
            4. Insert valid vector entry with duplicate id (should overwrite)
            5. Insert 4-dimensional entry in 3D index (should fail)
            6. Create 4-dimensional vector index in same bucket
            7. Insert 4-dimensional vector entry in 4D index
            8. List all vector entries present in vector bucket (across all indices)
            9. Delete 3-dimensional vector index without deleting vector records first
            10. Delete vector bucket without deleting any indices (should fail)
            11. Delete all indices from bucket and then delete vector bucket (should succeed)
        """
        # Step 1: Create vector bucket
        logger.info("Step 1: Creating vector bucket")
        vector_bucket_name = vector_bucket_factory(amount=1)[0].name
        logger.info(f"✓ Vector bucket created: {vector_bucket_name}")

        obc_obj = OBC(vector_bucket_name)
        s3vectors_client = vector_utils.create_s3vectors_client(mcg_obj, obc_obj)

        # Step 2: Create 3-dimensional vector index
        logger.info("Step 2: Creating 3-dimensional vector index")
        index_3d = f"index-3d-{random.randint(1000, 9999)}"
        self._create_index(s3vectors_client, index_3d, 3, vector_bucket_name)

        # Step 3: Insert valid vector entry
        logger.info("Step 3: Inserting valid 3D vector entry")
        vector_3d_1 = {
            "key": "vector-3d-1",
            "data": {"float32": [random.uniform(0.0, 1.0) for _ in range(3)]},
            "metadata": {"description": "First 3D vector"},
        }
        vector_utils.put_vectors(
            s3vectors_client,
            [vector_3d_1],
            vectorBucketName=vector_bucket_name,
            indexName=index_3d,
        )
        logger.info("✓ Inserted vector: vector-3d-1")

        # Step 4: Insert valid vector entry with duplicate id
        logger.info(
            "Step 4: Inserting vector entry with duplicate id (should overwrite)"
        )
        vector_3d_1_dup = {
            "key": "vector-3d-1",  # Same key
            "data": {
                "float32": [random.uniform(0.0, 1.0) for _ in range(3)]
            },  # Different data
            "metadata": {"description": "Duplicate 3D vector (updated)"},
        }
        vector_utils.put_vectors(
            s3vectors_client,
            [vector_3d_1_dup],
            vectorBucketName=vector_bucket_name,
            indexName=index_3d,
        )
        logger.info("✓ Inserted duplicate key vector (expected to overwrite)")

        # Verify only one vector exists
        vectors_3d = vector_utils.list_vectors(
            s3vectors_client,
            vectorBucketName=vector_bucket_name,
            indexName=index_3d,
        )
        assert (
            len(vectors_3d.get("vectors", [])) == 1
        ), "Expected only 1 vector after duplicate insert"

        # Verify the vector was overwritten with updated data
        retrieved_vectors = vector_utils.get_vectors(
            s3vectors_client,
            keys=["vector-3d-1"],
            vectorBucketName=vector_bucket_name,
            indexName=index_3d,
            returnData=True,
            returnMetadata=True,
        )
        retrieved_vector = retrieved_vectors.get("vectors", [])[0]
        retrieved_data = retrieved_vector.get("data", {}).get("float32", [])
        retrieved_metadata = retrieved_vector.get("metadata", {})

        # Should have updated data from vector_3d_1_dup, not original from vector_3d_1
        expected_data = vector_3d_1_dup["data"]["float32"]
        expected_description = vector_3d_1_dup["metadata"]["description"]

        assert len(retrieved_data) == len(expected_data) and all(
            math.isclose(a, b, rel_tol=1e-6)
            for a, b in zip(retrieved_data, expected_data)
        ), f"Expected updated data {expected_data}, got {retrieved_data}"
        assert (
            retrieved_metadata.get("description") == expected_description
        ), f"Expected updated metadata '{expected_description}', got '{retrieved_metadata.get('description')}'"

        logger.info(
            "✓ Verified: duplicate key overwrote the original vector with updated data"
        )

        # Step 5: Try to insert 4-dimensional entry in 3D index (should fail)
        logger.info(
            "Step 5: Attempting to insert 4D vector into 3D index (should fail)"
        )
        vector_4d_wrong = {
            "key": "vector-4d-wrong",
            "data": {
                "float32": [random.uniform(0.0, 1.0) for _ in range(4)]
            },  # 4 dimensions
            "metadata": {"description": "4D vector in 3D index"},
        }
        try:
            vector_utils.put_vectors(
                s3vectors_client,
                [vector_4d_wrong],
                vectorBucketName=vector_bucket_name,
                indexName=index_3d,
            )
            assert False, "Inserting 4D vector into 3D index should have failed"
        except (ClientError, ValueError) as e:
            logger.info(
                f"✓ Correctly rejected 4D vector in 3D index: {type(e).__name__}"
            )

        # Step 6: Create 4-dimensional vector index
        logger.info("Step 6: Creating 4-dimensional vector index in same bucket")
        index_4d = f"index-4d-{random.randint(1000, 9999)}"
        self._create_index(s3vectors_client, index_4d, 4, vector_bucket_name)

        # Step 7: Insert 4-dimensional vector entry in 4D index
        logger.info("Step 7: Inserting 4D vector entry into 4D index")
        vector_4d_1 = {
            "key": "vector-4d-1",
            "data": {"float32": [random.uniform(0.0, 1.0) for _ in range(4)]},
            "metadata": {"description": "First 4D vector"},
        }
        vector_utils.put_vectors(
            s3vectors_client,
            [vector_4d_1],
            vectorBucketName=vector_bucket_name,
            indexName=index_4d,
        )
        logger.info("✓ Inserted vector: vector-4d-1")

        # Step 8: List all vector entries present in vector bucket (from both indices)
        logger.info("Step 8: Listing all vector entries from all indices")

        # List indices
        indices_response = vector_utils.list_indexes(
            s3vectors_client, vectorBucketName=vector_bucket_name
        )
        indices = indices_response.get("indexes", [])
        logger.info(f"Found {len(indices)} indices in bucket")

        total_vectors = 0
        for idx in indices:
            idx_name = idx["indexName"]
            vectors_response = vector_utils.list_vectors(
                s3vectors_client,
                vectorBucketName=vector_bucket_name,
                indexName=idx_name,
            )
            vector_count = len(vectors_response.get("vectors", []))
            total_vectors += vector_count
            logger.info(f"  Index {idx_name}: {vector_count} vectors")

        logger.info(f"✓ Total vectors across all indices: {total_vectors}")
        assert total_vectors == 2, f"Expected 2 total vectors, found {total_vectors}"

        # Step 9: Delete 3-dimensional vector index without deleting vectors first
        logger.info("Step 9: Deleting 3D index without deleting vectors first")
        vector_utils.delete_index(
            s3vectors_client,
            vectorBucketName=vector_bucket_name,
            indexName=index_3d,
        )
        logger.info(
            f"✓ Deleted index {index_3d} (vectors were automatically deleted with index)"
        )

        # Verify index is deleted
        indices_after = vector_utils.list_indexes(
            s3vectors_client, vectorBucketName=vector_bucket_name
        )
        remaining_indices = [
            idx["indexName"] for idx in indices_after.get("indexes", [])
        ]
        assert (
            index_3d not in remaining_indices
        ), f"Index {index_3d} still exists after deletion"
        logger.info("✓ Verified index deletion")

        # Step 10: Try to delete vector bucket without deleting remaining indices (should fail)
        logger.info(
            "Step 10: Attempting to delete vector bucket without deleting remaining indices"
        )
        try:
            vector_utils.delete_vector_bucket(s3vectors_client, vector_bucket_name)
            assert False, "Deleting bucket with indices should have failed"
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            logger.info(
                f"✓ Correctly rejected bucket deletion with remaining indices: {error_code}"
            )

        # Step 11: Delete all remaining indices and then delete bucket
        logger.info("Step 11: Deleting all remaining indices, then deleting bucket")

        # Delete remaining 4D index
        vector_utils.delete_index(
            s3vectors_client,
            vectorBucketName=vector_bucket_name,
            indexName=index_4d,
        )
        logger.info(f"✓ Deleted index {index_4d}")

        # Verify no indices remain
        indices_final = vector_utils.list_indexes(
            s3vectors_client, vectorBucketName=vector_bucket_name
        )
        assert (
            len(indices_final.get("indexes", [])) == 0
        ), "Expected no indices remaining"
        logger.info("✓ All indices deleted")

        # Now delete the bucket (should succeed)
        vector_utils.delete_vector_bucket(s3vectors_client, vector_bucket_name)
        logger.info(f"✓ Successfully deleted vector bucket {vector_bucket_name}")

        # Verify bucket deletion
        buckets_list = vector_utils.list_vector_buckets(s3vectors_client)
        remaining_buckets = [
            b["vectorBucketName"] for b in buckets_list.get("vectorBuckets", [])
        ]
        assert (
            vector_bucket_name not in remaining_buckets
        ), f"Bucket {vector_bucket_name} still exists after deletion"
        logger.info("✓ Verified bucket deletion")

        logger.info(
            "Multi-dimensional indices and cleanup test completed successfully!"
        )

    @tier2
    def test_vector_query_operations(self, vector_bucket_factory, mcg_obj):
        """
        Test vector query operations across 3D and 4D indices:
            1. Create vector bucket
            2-7. Repeated for 3D and 4D indices:
                2. Create dimensional vector index
                3. Run query on empty index (should return no results)
                4. Insert 100 valid vector entries
                5. Query with dimension-appropriate vector for top 5 using cosine metric
                6. Update one entry's vector data and re-run the same query
                7. Query with zero vector of same dimension
            8. Query with 5D vector [0.1, 0.2, 0.3, 0.4, 0.5] on both indices (should fail)
        """
        # Step 1: Create vector bucket
        logger.info("Step 1: Creating vector bucket")
        vector_bucket_name = vector_bucket_factory(amount=1)[0].name
        logger.info(f"✓ Vector bucket created: {vector_bucket_name}")

        obc_obj = OBC(vector_bucket_name)
        s3vectors_client = vector_utils.create_s3vectors_client(mcg_obj, obc_obj)

        num_vectors = 100
        top_k = 5
        # (dimension, query_vector, key_prefix)
        index_configs = [
            (3, [0.1, 0.2, 0.3], "vector_key"),
            (4, [0.1, 0.2, 0.3, 0.4], "vector_4d_key"),
        ]
        created_indices = {}  # dim -> index_name

        for dim, query_vector, key_prefix in index_configs:
            dim_label = f"{dim}D"
            index_name = f"index-{dim}d-{random.randint(1000, 9999)}"
            created_indices[dim] = index_name

            # Step 2: Create index
            logger.info(f"Step 2 [{dim_label}]: Creating {dim_label} vector index")
            self._create_index(s3vectors_client, index_name, dim, vector_bucket_name)

            # Step 3: Query on empty index
            logger.info(
                f"Step 3 [{dim_label}]: Running query on empty {dim_label} index"
            )
            try:
                empty_results = vector_utils.query_vectors(
                    s3vectors_client,
                    query_vector=query_vector,
                    top_k=top_k,
                    vectorBucketName=vector_bucket_name,
                    indexName=index_name,
                    returnDistance=True,
                ).get("vectors", [])
                assert (
                    len(empty_results) == 0
                ), f"Expected no results from empty {dim_label} index, got {len(empty_results)}"
                logger.info(
                    f"✓ [{dim_label}] Empty index query returned 0 results as expected"
                )
            except ClientError as e:
                error_str = str(e)
                if "InternalFailure" in error_str and (
                    "vectorSearch" in error_str
                    or "Cannot read properties of undefined" in error_str
                ):
                    logger.info(
                        f"✓ [{dim_label}] Empty index query raised InternalFailure "
                        f"(expected NooBaa behavior for uninitialized index)"
                    )
                else:
                    raise

            # Step 4: Insert 100 vectors
            logger.info(
                f"Step 4 [{dim_label}]: Inserting {num_vectors} valid {dim_label} vector entries"
            )
            vectors = vector_utils.generate_test_vectors_with_metadata(
                dimension=dim, num_vectors=num_vectors, key_prefix=key_prefix
            )
            vector_utils.put_vectors(
                s3vectors_client,
                vectors,
                vectorBucketName=vector_bucket_name,
                indexName=index_name,
            )
            logger.info(f"✓ [{dim_label}] Inserted {num_vectors} vectors")

            # Step 5: Query for top 5 using cosine metric
            logger.info(
                f"Step 5 [{dim_label}]: Querying with {query_vector} for top {top_k} closest entries"
            )
            results_step5 = vector_utils.query_vectors(
                s3vectors_client,
                query_vector=query_vector,
                top_k=top_k,
                vectorBucketName=vector_bucket_name,
                indexName=index_name,
                returnDistance=True,
                returnMetadata=True,
            ).get("vectors", [])
            assert (
                len(results_step5) == top_k
            ), f"[{dim_label}] Expected exactly {top_k} results, got {len(results_step5)}"
            logger.info(f"✓ [{dim_label}] Query returned {len(results_step5)} results")
            distances_step5 = [r.get("distance") for r in results_step5]
            assert distances_step5 == sorted(distances_step5), (
                f"[{dim_label}] Query results are not sorted by distance (ascending): "
                f"{distances_step5}"
            )
            logger.info(
                f"✓ [{dim_label}] Results are sorted by distance (ascending): {distances_step5}"
            )
            for rank, result in enumerate(results_step5, 1):
                logger.info(
                    f"  [{dim_label}] Rank {rank}: key={result.get('key')}, "
                    f"distance={result.get('distance', 'N/A')}"
                )

            # Step 6: Update one entry and re-run the same query.
            # We take the top-ranked vector and overwrite it with new random data.
            # After re-querying, we verify the update was indexed by checking that
            # either the vector's distance changed (still in top 5) or it dropped
            # out of top 5 and a different key replaced it.
            logger.info(
                f"Step 6 [{dim_label}]: Updating one vector entry and re-running the query"
            )
            target_key = results_step5[0].get("key")
            target_distance_before = results_step5[0].get("distance")
            keys_step5 = {r.get("key") for r in results_step5}

            updated_vector = {
                "key": target_key,
                "data": {"float32": [random.uniform(0.0, 1.0) for _ in range(dim)]},
                "metadata": {
                    "genre": "updated",
                    "source_text": "updated vector entry",
                    "price": 0,
                },
            }
            vector_utils.put_vectors(
                s3vectors_client,
                [updated_vector],
                vectorBucketName=vector_bucket_name,
                indexName=index_name,
            )
            logger.info(f"✓ [{dim_label}] Updated vector: {target_key}")

            results_step6 = vector_utils.query_vectors(
                s3vectors_client,
                query_vector=query_vector,
                top_k=top_k,
                vectorBucketName=vector_bucket_name,
                indexName=index_name,
                returnDistance=True,
                returnMetadata=True,
            ).get("vectors", [])
            assert len(results_step6) == top_k, (
                f"[{dim_label}] Expected exactly {top_k} results after update, "
                f"got {len(results_step6)}"
            )

            keys_step6 = [r.get("key") for r in results_step6]
            if target_key in keys_step6:
                # Updated vector is still in top 5 — its distance must have changed
                target_distance_after = next(
                    r.get("distance")
                    for r in results_step6
                    if r.get("key") == target_key
                )
                assert target_distance_before != target_distance_after, (
                    f"[{dim_label}] Distance for updated vector '{target_key}' did not change "
                    f"after update: before={target_distance_before}, after={target_distance_after}"
                )
                logger.info(
                    f"✓ [{dim_label}] Updated vector '{target_key}' still in top 5; "
                    f"distance changed {target_distance_before} → {target_distance_after}"
                )
            else:
                # Updated vector dropped out of top 5 — a new key replaced it
                new_keys = set(keys_step6) - keys_step5
                logger.info(
                    f"✓ [{dim_label}] Updated vector '{target_key}' dropped out of top 5 "
                    f"(moved farther from query); new key(s) in results: {new_keys}"
                )

            for rank, result in enumerate(results_step6, 1):
                logger.info(
                    f"  [{dim_label}] Rank {rank}: key={result.get('key')}, "
                    f"distance={result.get('distance', 'N/A')}"
                )

            # Step 7: Query with zero vector — a mathematically degenerate edge case:
            # cosine similarity is undefined for a zero-magnitude vector (division by zero),
            # and euclidean distance treats it as the origin. NooBaa may return results,
            # an empty list, or raise an error — all are acceptable outcomes.
            zero_vector = [0] * dim
            logger.info(
                f"Step 7 [{dim_label}]: Querying with zero vector {zero_vector}"
            )
            zero_results = vector_utils.query_vectors(
                s3vectors_client,
                query_vector=zero_vector,
                top_k=top_k,
                vectorBucketName=vector_bucket_name,
                indexName=index_name,
                returnDistance=True,
            ).get("vectors", [])
            logger.info(
                f"✓ [{dim_label}] Zero vector query returned {len(zero_results)} results "
                f"(0 or more are valid)"
            )
            for rank, result in enumerate(zero_results, 1):
                logger.info(
                    f"  [{dim_label}] Rank {rank}: key={result.get('key')}, "
                    f"distance={result.get('distance', 'N/A')}"
                )

        # Step 8: Query with 5D vector [0.1, 0.2, 0.3, 0.4, 0.5] (invalid for both indices)
        logger.info(
            "Step 8: Querying with 5D vector [0.1, 0.2, 0.3, 0.4, 0.5] "
            "(invalid for both 3D and 4D indices)"
        )
        for idx_dim, idx_name in created_indices.items():
            try:
                vector_utils.query_vectors(
                    s3vectors_client,
                    query_vector=[0.1, 0.2, 0.3, 0.4, 0.5],
                    top_k=top_k,
                    vectorBucketName=vector_bucket_name,
                    indexName=idx_name,
                    returnDistance=True,
                )
                assert (
                    False
                ), f"5D query on {idx_dim}D index {idx_name} should have failed"
            except (ClientError, ValueError) as e:
                logger.info(
                    f"✓ 5D query correctly rejected on {idx_dim}D index "
                    f"{idx_name}: {type(e).__name__}"
                )

        logger.info("✅ Vector query operations test completed successfully!")

    @jira("DFBUGS-7337")
    @tier2
    def test_vector_distance_metrics(self, vector_bucket_factory, mcg_obj):
        """
        Test vector distance metric behaviour and validation:
            1. Create vector bucket
            2. Create two 3D indices: one with cosine, one with euclidean distance metric
            3. Insert identical 100 vector entries in both indices
            4. Run same query [0.1, 0.2, 0.3] on both indices requesting top 5
            5. Compare result ordering and distance values between the two metrics
            6. Test edge cases: zero vectors, normalized vectors, large magnitude vectors
            7. Attempt to create index with invalid distance metrics (should fail)
        """
        import math as _math

        # Step 1: Create vector bucket
        logger.info("Step 1: Creating vector bucket")
        vector_bucket_name = vector_bucket_factory(amount=1)[0].name
        logger.info(f"✓ Vector bucket created: {vector_bucket_name}")

        obc_obj = OBC(vector_bucket_name)
        s3vectors_client = vector_utils.create_s3vectors_client(mcg_obj, obc_obj)

        num_vectors = 100
        query_vector = [0.1, 0.2, 0.3]
        top_k = 5
        metrics = ["cosine", "euclidean"]

        shared_vectors = vector_utils.generate_test_vectors_with_metadata(
            dimension=3, num_vectors=num_vectors
        )

        indices = {}  # metric -> index_name
        results = {}  # metric -> list of result vectors

        # Steps 2-4: For each metric, create index, insert vectors, run query
        for metric in metrics:
            index_name = f"index-{metric}-{random.randint(1000, 9999)}"
            indices[metric] = index_name

            # Step 2: Create index
            logger.info(f"Step 2 [{metric}]: Creating 3D {metric} index")
            self._create_index(
                s3vectors_client,
                index_name,
                3,
                vector_bucket_name,
                distance_metric=metric,
            )

            # Step 3: Insert identical vectors
            logger.info(f"Step 3 [{metric}]: Inserting {num_vectors} identical vectors")
            vector_utils.put_vectors(
                s3vectors_client,
                shared_vectors,
                vectorBucketName=vector_bucket_name,
                indexName=index_name,
            )
            logger.info(f"✓ [{metric}] Inserted {num_vectors} vectors")

            # Step 4: Query with [0.1, 0.2, 0.3] for top 5
            logger.info(
                f"Step 4 [{metric}]: Querying with {query_vector} for top {top_k}"
            )
            results[metric] = vector_utils.query_vectors(
                s3vectors_client,
                query_vector=query_vector,
                top_k=top_k,
                vectorBucketName=vector_bucket_name,
                indexName=index_name,
                returnDistance=True,
                returnMetadata=True,
            ).get("vectors", [])
            assert (
                len(results[metric]) == top_k
            ), f"[{metric}] Expected exactly {top_k} results, got {len(results[metric])}"
            logger.info(f"✓ [{metric}] Query returned {len(results[metric])} results")

        # Step 5: Compare result ordering and distance values
        logger.info("Step 5: Comparing result ordering and distances between metrics")

        cosine_keys = [r.get("key") for r in results["cosine"]]
        euclidean_keys = [r.get("key") for r in results["euclidean"]]
        cosine_distances = [r.get("distance") for r in results["cosine"]]
        euclidean_distances = [r.get("distance") for r in results["euclidean"]]

        logger.info(f"  Cosine    top keys:      {cosine_keys}")
        logger.info(f"  Euclidean top keys:      {euclidean_keys}")
        logger.info(f"  Cosine    distances:     {cosine_distances}")
        logger.info(f"  Euclidean distances:     {euclidean_distances}")

        assert cosine_distances != euclidean_distances, (
            f"Cosine and euclidean distances must differ for the same vectors — "
            f"cosine={cosine_distances}, euclidean={euclidean_distances}"
        )
        logger.info("✓ Distance values differ between cosine and euclidean as expected")

        # Step 6: Edge case vectors
        logger.info("Step 6: Testing edge case vectors")

        # Normalised (unit) vector: magnitude = 1
        magnitude = _math.sqrt(0.1**2 + 0.2**2 + 0.3**2)
        normalized_vec = [round(v / magnitude, 6) for v in [0.1, 0.2, 0.3]]

        # Large magnitude vector
        large_vec = [100.0, 200.0, 300.0]

        edge_cases = [
            ("zero-vector", [0.0, 0.0, 0.0], "zero vector"),
            ("normalized-vector", normalized_vec, "normalized (unit) vector"),
            ("large-magnitude-vector", large_vec, "large magnitude vector"),
        ]

        for key, data, description in edge_cases:
            edge_vector = {
                "key": key,
                "data": {"float32": data},
                "metadata": {"genre": "edge", "source_text": description, "price": 0},
            }
            for index_name, metric in [
                (indices["cosine"], "cosine"),
                (indices["euclidean"], "euclidean"),
            ]:
                if key == "zero-vector":
                    # Zero vector has undefined cosine similarity (zero magnitude → division
                    # by zero during normalisation). NooBaa may reject the put — that is
                    # an acceptable outcome and not a test failure.
                    try:
                        vector_utils.put_vectors(
                            s3vectors_client,
                            [edge_vector],
                            vectorBucketName=vector_bucket_name,
                            indexName=index_name,
                        )
                        logger.info(f"✓ Inserted {description} into {metric} index")
                    except ClientError as e:
                        logger.info(
                            f"  [{metric}] {description} rejected (expected for cosine): "
                            f"{e.response.get('Error', {}).get('Code', 'Unknown')}"
                        )
                else:
                    # Normalised and large-magnitude vectors are valid inputs — failures
                    # here are unexpected and must propagate as test failures.
                    vector_utils.put_vectors(
                        s3vectors_client,
                        [edge_vector],
                        vectorBucketName=vector_bucket_name,
                        indexName=index_name,
                    )
                    logger.info(f"✓ Inserted {description} into {metric} index")

        # Query with each edge case vector on both indices and log results
        for query_data, description, allow_failure in [
            ([0.0, 0.0, 0.0], "zero vector", True),
            (normalized_vec, "normalized vector", False),
            (large_vec, "large magnitude vector", False),
        ]:
            for index_name, metric in [
                (indices["cosine"], "cosine"),
                (indices["euclidean"], "euclidean"),
            ]:
                if allow_failure:
                    # Zero vector query may be rejected by NooBaa (undefined cosine).
                    try:
                        edge_results = vector_utils.query_vectors(
                            s3vectors_client,
                            query_vector=query_data,
                            top_k=top_k,
                            vectorBucketName=vector_bucket_name,
                            indexName=index_name,
                            returnDistance=True,
                        ).get("vectors", [])
                        logger.info(
                            f"  [{metric}] {description} query: {len(edge_results)} results returned"
                        )
                    except ClientError as e:
                        logger.info(
                            f"  [{metric}] {description} query raised "
                            f"{e.response.get('Error', {}).get('Code', 'Unknown')} (expected)"
                        )
                else:
                    # Normalised and large-magnitude queries must succeed.
                    edge_results = vector_utils.query_vectors(
                        s3vectors_client,
                        query_vector=query_data,
                        top_k=top_k,
                        vectorBucketName=vector_bucket_name,
                        indexName=index_name,
                        returnDistance=True,
                    ).get("vectors", [])
                    logger.info(
                        f"  [{metric}] {description} query: {len(edge_results)} results returned"
                    )

        # Step 7: Attempt to create indices with invalid distance metrics
        logger.info("Step 7: Testing invalid distance metrics")
        invalid_metrics = ["manhattan", "invalid", "dot_product"]

        for metric in invalid_metrics:
            index_name_invalid = f"index-invalid-{metric}-{random.randint(1000, 9999)}"
            logger.info(f"Attempting to create index with distance metric: '{metric}'")
            try:
                vector_utils.create_index(
                    s3vectors_client,
                    index_name=index_name_invalid,
                    data_type="float32",
                    dimension=3,
                    distance_metric=metric,
                    vectorBucketName=vector_bucket_name,
                )
                assert False, f"Index with invalid metric '{metric}' should have failed"
            except (ClientError, ParamValidationError, ValueError) as e:
                logger.info(
                    f"✓ Distance metric '{metric}' correctly rejected: {type(e).__name__}"
                )

        logger.info("✅ Vector distance metrics test completed successfully!")
