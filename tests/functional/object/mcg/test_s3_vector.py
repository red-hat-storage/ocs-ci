import logging
import random
import string

from botocore.exceptions import ClientError

from ocs_ci.framework.pytest_customization.marks import (
    tier2,
    red_squad,
    post_upgrade,
    mcg,
)
from ocs_ci.framework.testlib import MCGTest
from ocs_ci.ocs import vector_utils
from ocs_ci.ocs.resources.objectbucket import OBC
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)


@mcg
@red_squad
class TestS3Vector(MCGTest):
    """
    Test S3 Vector functionality against noobaa
    """

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
        try:
            vector_utils.create_index(
                s3vectors_client,
                index_name=index_name,
                data_type="float32",
                dimension=dimension,
                distance_metric="cosine",
                vectorBucketName=vector_bucket_name,
            )
            logger.info(f"✓ Vector index '{index_name}' created")
        except ClientError as e:
            error_message = str(e)
            if "VECTOR_INDEX_ALREADY_OWNED_BY_YOU" in error_message:
                logger.warning(
                    f"Index '{index_name}' creation got ALREADY_OWNED error. "
                    "This is a known NooBaa issue where the index is created successfully "
                    "but an error is returned, causing retries to fail. Continuing test..."
                )
            else:
                raise

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
