"""
Helper functions for S3 Vectors operations
Based on AWS S3 Vectors API: https://docs.aws.amazon.com/boto3/latest/reference/services/s3vectors.html
"""

import boto3
import logging
import random

from ocs_ci.ocs.bucket_utils import retrieve_verification_mode
from botocore.config import Config
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


def create_s3vectors_client(mcg_obj, obc_obj, nsr_name=None):
    """
    Create an S3 Vectors client using OBC credentials

    Args:
        mcg_obj (obj): MCG object containing vectors endpoint and region
        obc_obj (obj): OBC object containing access credentials
        nsr_name (str): Optional NooBaa namespace resource name for x-noobaa-nsr header

    Returns:
        boto3.client: S3 Vectors client configured with OBC credentials

    """
    if not mcg_obj.vectors_endpoint:
        raise ValueError("Vectors endpoint is not available")

    retry_cfg = Config(retries={"max_attempts": 10, "mode": "standard"})

    client = boto3.client(
        "s3vectors",
        region_name=mcg_obj.region,
        verify=retrieve_verification_mode(),
        endpoint_url=mcg_obj.vectors_endpoint,
        aws_access_key_id=obc_obj.access_key_id,
        aws_secret_access_key=obc_obj.access_key,
        config=retry_cfg,
    )

    # Add custom NooBaa header if NSR name is provided
    if nsr_name:

        def add_nsr_header(params, **kwargs):
            params["headers"]["x-noobaa-nsr"] = nsr_name

        client = boto3.client(
            "s3vectors",
            region_name=mcg_obj.region,
            verify=retrieve_verification_mode(),
            endpoint_url=mcg_obj.vectors_endpoint,
            aws_access_key_id=mcg_obj.access_key_id,
            aws_secret_access_key=mcg_obj.access_key,
            config=retry_cfg,
        )

        client.meta.events.register("before-call", add_nsr_header)

    return client


def generate_test_vectors_with_metadata(
    dimension, num_vectors, key_prefix="vector_key"
):
    """
    Generate test vectors with genre-based metadata matching reference format

    Args:
        dimension (int): Dimensionality of vectors [REQUIRED]
        num_vectors (int): Number of vectors to generate [REQUIRED]
        key_prefix (str): Prefix for vector keys (default: 'vector_key')

    Returns:
        list: List of vector objects, each with key, data (float32 array),
            and metadata (genre, source_text, price)

    """
    genres = ["scifi", "family", "drama", "action", "comedy"]
    vectors = []

    for i in range(num_vectors):
        vector_data = [random.uniform(0.0, 2.0) for _ in range(dimension)]
        vector_obj = {
            "key": f"{key_prefix}_{i+1}",
            "data": {"float32": vector_data},
            "metadata": {
                "genre": genres[i % len(genres)],
                "source_text": f"i am source text {i+1}",
                "price": (i + 1) * 10,
            },
        }
        vectors.append(vector_obj)

    return vectors


def create_vector_bucket(s3vectors_client, vector_bucket_name, **kwargs):
    """
    Create a new S3 vector bucket

    Args:
        s3vectors_client (obj): boto3 s3vectors client
        vector_bucket_name (str): Name of the vector bucket to create
        **kwargs: Optional parameters like encryptionConfiguration (dict),
            tags (dict)

    Returns:
        dict: Response with vectorBucketArn

    """
    params = {"vectorBucketName": vector_bucket_name}
    params.update(kwargs)
    return s3vectors_client.create_vector_bucket(**params)


def delete_vector_bucket(s3vectors_client, vector_bucket_name):
    """
    Delete an S3 vector bucket

    Args:
        s3vectors_client (obj): boto3 s3vectors client
        vector_bucket_name (str): Name of the vector bucket to delete

    Returns:
        dict: Response from delete_vector_bucket API

    """
    return s3vectors_client.delete_vector_bucket(vectorBucketName=vector_bucket_name)


def get_vector_bucket(s3vectors_client, vector_bucket_name):
    """
    Get information about a vector bucket

    Args:
        s3vectors_client (obj): boto3 s3vectors client
        vector_bucket_name (str): Name of the vector bucket

    Returns:
        dict: Vector bucket details

    """
    return s3vectors_client.get_vector_bucket(vectorBucketName=vector_bucket_name)


def list_vector_buckets(s3vectors_client, **kwargs):
    """
    List all vector buckets

    Args:
        s3vectors_client (obj): boto3 s3vectors client
        **kwargs: Optional parameters
            maxResults (int): Maximum number of buckets to return
            nextToken (str): Token for pagination

    Returns:
        dict: List of vector buckets with nextToken for pagination

    """
    return s3vectors_client.list_vector_buckets(**kwargs)


def create_index(
    s3vectors_client, index_name, data_type, dimension, distance_metric, **kwargs
):
    """
    Create a vector index in a vector bucket

    Args:
        s3vectors_client (obj): boto3 s3vectors client
        index_name (str): Name of the index to create [REQUIRED]
        data_type (str): Data type of vectors [REQUIRED]
        dimension (int): Dimensionality of vectors [REQUIRED]
        distance_metric (str): Distance metric ('euclidean'|'cosine') [REQUIRED]
        **kwargs: Optional parameters like vectorBucketName (str),
            vectorBucketArn (str), metadataConfiguration (dict),
            encryptionConfiguration (dict), tags (dict)

    Returns:
        dict: Response with indexArn

    """
    params = {
        "indexName": index_name,
        "dataType": data_type,
        "dimension": dimension,
        "distanceMetric": distance_metric,
    }
    params.update(kwargs)
    try:
        return s3vectors_client.create_index(**params)
    except ClientError as e:
        # botocore retries CreateIndex on InternalFailure; if the first call
        # succeeded but the response was lost, subsequent retries get
        # VECTOR_INDEX_ALREADY_OWNED_BY_YOU — the index exists and we own it,
        # which is the desired state.
        if "VECTOR_INDEX_ALREADY_OWNED_BY_YOU" in str(e):
            logger.warning(
                "Index '%s' already owned; treating create as idempotent", index_name
            )
            return {"ResponseMetadata": {"HTTPStatusCode": 200}}
        raise


def delete_index(s3vectors_client, **kwargs):
    """
    Delete a vector index

    Args:
        s3vectors_client (obj): boto3 s3vectors client
        **kwargs: Optional parameters
            vectorBucketName (str): Name of the vector bucket
            indexName (str): Name of the index to delete
            indexArn (str): ARN of the index to delete

    Returns:
        dict: Response from delete_index API

    """
    return s3vectors_client.delete_index(**kwargs)


def get_index(s3vectors_client, **kwargs):
    """
    Get information about a vector index

    Args:
        s3vectors_client (obj): boto3 s3vectors client
        **kwargs: Optional parameters
            vectorBucketName (str): Name of the vector bucket
            indexName (str): Name of the index
            indexArn (str): ARN of the index

    Returns:
        dict: Index details

    """
    return s3vectors_client.get_index(**kwargs)


def list_indexes(s3vectors_client, **kwargs):
    """
    List all indexes in a vector bucket

    Args:
        s3vectors_client (obj): boto3 s3vectors client
        **kwargs: Optional parameters
            vectorBucketName (str): Name of the vector bucket
            vectorBucketArn (str): ARN of the vector bucket
            maxResults (int): Maximum number of indexes to return
            nextToken (str): Token for pagination

    Returns:
        dict: List of indexes

    """
    return s3vectors_client.list_indexes(**kwargs)


def put_vectors(s3vectors_client, vectors, **kwargs):
    """
    Store vectors in a vector index

    Args:
        s3vectors_client (obj): boto3 s3vectors client
        vectors (list): List of vector objects [REQUIRED], each containing
            key (str), data (dict with float32 array), and optional metadata
        **kwargs: Optional parameters like vectorBucketName (str),
            indexName (str), indexArn (str)

    Returns:
        dict: Empty dict on success

    """
    params = {"vectors": vectors}
    params.update(kwargs)
    return s3vectors_client.put_vectors(**params)


def get_vectors(s3vectors_client, keys, **kwargs):
    """
    Retrieve specific vectors by their keys

    Args:
        s3vectors_client (obj): boto3 s3vectors client
        keys (list): List of vector keys to retrieve [REQUIRED]
        **kwargs: Optional parameters
            vectorBucketName (str): Name of the vector bucket
            indexName (str): Name of the index
            indexArn (str): ARN of the index

    Returns:
        dict: Retrieved vectors

    """
    params = {"keys": keys}
    params.update(kwargs)
    return s3vectors_client.get_vectors(**params)


def delete_vectors(s3vectors_client, keys, **kwargs):
    """
    Delete specific vectors from an index

    Args:
        s3vectors_client (obj): boto3 s3vectors client
        keys (list): List of vector keys to delete [REQUIRED]
        **kwargs: Optional parameters
            vectorBucketName (str): Name of the vector bucket
            indexName (str): Name of the index
            indexArn (str): ARN of the index

    Returns:
        dict: Response from delete_vectors API

    """
    params = {"keys": keys}
    params.update(kwargs)
    return s3vectors_client.delete_vectors(**params)


def list_vectors(s3vectors_client, **kwargs):
    """
    List vectors in an index

    Args:
        s3vectors_client (obj): boto3 s3vectors client
        **kwargs: Optional parameters
            vectorBucketName (str): Name of the vector bucket
            indexName (str): Name of the index
            indexArn (str): ARN of the index
            maxResults (int): Maximum number of vectors to return
            nextToken (str): Token for pagination

    Returns:
        dict: List of vectors

    """
    return s3vectors_client.list_vectors(**kwargs)


def query_vectors(s3vectors_client, query_vector, top_k, **kwargs):
    """
    Perform similarity search on vectors

    Args:
        s3vectors_client (obj): boto3 s3vectors client
        query_vector (list): Query vector as list of floats [REQUIRED]
        top_k (int): Number of nearest neighbors to return [REQUIRED]
        **kwargs: Optional parameters
            vectorBucketName (str): Name of the vector bucket
            indexName (str): Name of the index
            indexArn (str): ARN of the index
            filter (dict): Metadata filter
            returnMetadata (bool): Include metadata in response (default: False)
            returnDistance (bool): Include distance in response (default: False)

    Returns:
        dict: Query results containing vectors list with distance, key,
            and optional metadata, plus distanceMetric field

    """
    params = {"queryVector": {"float32": query_vector}, "topK": top_k}
    params.update(kwargs)
    return s3vectors_client.query_vectors(**params)


def delete_all_vectors(s3vectors_client, **kwargs):
    """
    Delete all vectors from an index

    Args:
        s3vectors_client (obj): boto3 s3vectors client
        **kwargs: Optional parameters
            vectorBucketName (str): Name of the vector bucket
            indexName (str): Name of the index
            indexArn (str): ARN of the index

    Returns:
        int: Number of vectors deleted

    """
    all_vectors = []
    next_token = None

    # List all vectors with pagination
    while True:
        list_params = dict(kwargs)
        if next_token:
            list_params["nextToken"] = next_token

        response = list_vectors(s3vectors_client, **list_params)
        vectors = response.get("vectors", [])
        all_vectors.extend([v["key"] for v in vectors])

        next_token = response.get("nextToken")
        if not next_token:
            break

    if not all_vectors:
        return 0

    # Delete in batches
    batch_size = 100
    deleted_count = 0

    for i in range(0, len(all_vectors), batch_size):
        batch_keys = all_vectors[i : i + batch_size]
        delete_vectors(s3vectors_client, batch_keys, **kwargs)
        deleted_count += len(batch_keys)

    return deleted_count


def put_vector_bucket_policy(s3vectors_client, vector_bucket_name, policy):
    """
    Attach a bucket policy to a vector bucket

    Args:
        s3vectors_client (obj): boto3 s3vectors client
        vector_bucket_name (str): Name of the vector bucket
        policy (str): Bucket policy JSON string

    Returns:
        dict: Response from put_vector_bucket_policy API

    """
    return s3vectors_client.put_vector_bucket_policy(
        vectorBucketName=vector_bucket_name, policy=policy
    )


def get_vector_bucket_policy(s3vectors_client, vector_bucket_name):
    """
    Get the bucket policy for a vector bucket

    Args:
        s3vectors_client (obj): boto3 s3vectors client
        vector_bucket_name (str): Name of the vector bucket

    Returns:
        dict: Bucket policy

    """
    return s3vectors_client.get_vector_bucket_policy(
        vectorBucketName=vector_bucket_name
    )


def delete_vector_bucket_policy(s3vectors_client, vector_bucket_name):
    """
    Remove the bucket policy from a vector bucket

    Args:
        s3vectors_client (obj): boto3 s3vectors client
        vector_bucket_name (str): Name of the vector bucket

    Returns:
        dict: Response from delete_vector_bucket_policy API

    """
    return s3vectors_client.delete_vector_bucket_policy(
        vectorBucketName=vector_bucket_name
    )
