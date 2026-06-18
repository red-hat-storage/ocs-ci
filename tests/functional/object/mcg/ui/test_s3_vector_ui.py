import logging
import random
import string

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    mcg,
    red_squad,
    tier2,
    ui,
)
from ocs_ci.framework.testlib import MCGTest
from ocs_ci.helpers.helpers import create_unique_resource_name
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.ocs import vector_utils
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.objectbucket import OBC
from ocs_ci.ocs.ui.page_objects.buckets_tab import BucketsTab

logger = logging.getLogger(__name__)


def _random_suffix(length=6):
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=length))


def cleanup_vector_buckets(created_buckets, mcg_obj):
    """
    Delete vector indices and then OBCs for all entries in *created_buckets*.

    Intended for use as a pytest finalizer in any test that creates vector OBCs
    via the UI (which does not set spec.bucketName, so the NooBaa internal
    bucket name is read from the OBC ConfigMap via obc_obj.bucket_name).

    Args:
        created_buckets (list[str]): OBC metadata names to clean up.
        mcg_obj: MCG object providing S3 credentials for the cluster.
    """
    namespace = config.ENV_DATA["cluster_namespace"]
    obc_ocp = OCP(kind="obc", namespace=namespace)
    for bkt in created_buckets:
        try:
            obc_obj = OBC(bkt)
            noobaa_bucket_name = obc_obj.bucket_name
            s3v = vector_utils.create_s3vectors_client(mcg_obj, obc_obj)
            indices = vector_utils.list_indexes(
                s3v, vectorBucketName=noobaa_bucket_name
            )
            for idx in indices.get("indexes", []):
                try:
                    vector_utils.delete_index(
                        s3v,
                        vectorBucketName=noobaa_bucket_name,
                        indexName=idx["indexName"],
                    )
                    logger.info(
                        f"Teardown: deleted index '{idx['indexName']}' "
                        f"from bucket '{noobaa_bucket_name}'"
                    )
                except Exception as e:
                    logger.warning(
                        f"Teardown: could not delete index "
                        f"'{idx['indexName']}': {e}"
                    )
        except Exception as e:
            logger.warning(f"Teardown: could not list/delete indices for '{bkt}': {e}")
        try:
            obc_ocp.delete(resource_name=bkt)
            logger.info(f"Teardown: deleted OBC '{bkt}'")
        except Exception as e:
            logger.warning(f"Teardown: could not delete OBC '{bkt}': {e}")


def cleanup_s3api_vector_buckets(created_buckets, mcg_obj):
    """
    Delete vector indices and then S3 API buckets for all entries in
    *created_buckets*.

    Used as a pytest finalizer for tests that create vector buckets via the S3
    API option (no OBC resource). The bucket name is the NooBaa internal name,
    so mcg_obj admin credentials are used for both the S3 vectors client and
    the bucket deletion.

    Args:
        created_buckets (list[str]): Bucket names to clean up.
        mcg_obj: MCG object providing S3 credentials and the vectors endpoint.
    """
    for bucket_name in created_buckets:
        try:
            s3v = vector_utils.create_s3vectors_client(mcg_obj, mcg_obj)
            indices = vector_utils.list_indexes(s3v, vectorBucketName=bucket_name)
            for idx in indices.get("indexes", []):
                try:
                    vector_utils.delete_index(
                        s3v,
                        vectorBucketName=bucket_name,
                        indexName=idx["indexName"],
                    )
                    logger.info(
                        f"Teardown: deleted index '{idx['indexName']}' "
                        f"from bucket '{bucket_name}'"
                    )
                except Exception as e:
                    logger.warning(
                        f"Teardown: could not delete index "
                        f"'{idx['indexName']}': {e}"
                    )
        except Exception as e:
            logger.warning(
                f"Teardown: could not list/delete indices for '{bucket_name}': {e}"
            )
        try:
            vector_utils.delete_vector_bucket(
                s3v,
                vector_bucket_name=bucket_name,
            )
            logger.info(f"Teardown: deleted S3 API bucket '{bucket_name}'")
        except Exception as e:
            logger.warning(
                f"Teardown: could not delete S3 API bucket '{bucket_name}': {e}"
            )


def _create_and_verify_indices(vector_ui, num_indices, dimension, vector_bucket_name):
    """
    Create *num_indices* vector indices on the currently open bucket detail page,
    verify each index detail page, and return the list of created index names.

    Alternates between 'cosine' and 'euclidean' distance metrics across indices.

    Args:
        vector_ui (S3VectorTab): Page object positioned on the bucket detail page.
        num_indices (int): Number of indices to create.
        dimension (int): Vector dimension for each index.
        vector_bucket_name (str): Expected bucket name for breadcrumb verification.

    Returns:
        list[str]: Names of all created indices.
    """
    distance_metrics = ("cosine", "euclidean")
    created = []
    for idx_num in range(num_indices):
        index_name = f"idx-{_random_suffix()}-{idx_num}"
        distance_metric = distance_metrics[idx_num % len(distance_metrics)]
        logger.info(
            f"  Index {idx_num + 1}/{num_indices}: "
            f"'{index_name}' metric={distance_metric}"
        )
        vector_ui.create_vector_index(
            index_name=index_name,
            dimension=dimension,
            distance_metric=distance_metric,
        )
        created.append(index_name)
        vector_ui.navigate_to_index(index_name)
        actual = vector_ui.verify_index_details(
            index_name=index_name,
            dimension=dimension,
            distance_metric=distance_metric,
            vector_bucket_name=vector_bucket_name,
        )
        logger.info(f"  Index '{index_name}' detail page verified: {actual}")
        if idx_num < num_indices - 1:
            vector_ui.navigate_backward()
            vector_ui.page_has_loaded(sleep_time=2)
    return created


def _assert_s3_vector_api_state(s3vectors_client, bucket_name, expected_index_names):
    """
    Assert via S3 API that *bucket_name* exists and contains exactly
    *expected_index_names* (no more, no fewer).

    Args:
        s3vectors_client: Boto3-style S3 vectors client.
        bucket_name (str): NooBaa internal bucket name.
        expected_index_names (list[str]): Index names that must be present.

    Raises:
        AssertionError: If bucket is missing, any index is absent, or count differs.
    """
    bucket_info = vector_utils.get_vector_bucket(s3vectors_client, bucket_name)
    assert (
        bucket_info["ResponseMetadata"]["HTTPStatusCode"] == 200
    ), f"Vector bucket '{bucket_name}' not found via S3 API"
    logger.info(f"  ✓ Bucket '{bucket_name}' confirmed via S3 API")

    indices_response = vector_utils.list_indexes(
        s3vectors_client, vectorBucketName=bucket_name
    )
    api_index_names = [idx["indexName"] for idx in indices_response.get("indexes", [])]
    for expected_idx in expected_index_names:
        assert expected_idx in api_index_names, (
            f"Index '{expected_idx}' not found via S3 API for bucket "
            f"'{bucket_name}'. Found: {api_index_names}"
        )
        logger.info(f"  ✓ Index '{expected_idx}' confirmed in bucket '{bucket_name}'")
    assert len(api_index_names) == len(expected_index_names), (
        f"Expected exactly {len(expected_index_names)} indices in "
        f"'{bucket_name}', got {len(api_index_names)}: {api_index_names}"
    )


@mcg
@red_squad
@ui
class TestS3VectorUI(MCGTest):
    """
    UI tests for S3 Vector bucket and index management via the ODF console.
    """

    @pytest.fixture(scope="function")
    def vector_prereqs(self, namespace_store_factory, bucket_class_factory):
        """
        Create an NSFS namespacestore and a vectorPolicy bucketclass for the
        test function.  Uses function-scoped factories so teardown runs before
        the OCS-CI environment leftover check fires at class/session end.

        Returns:
            tuple: (namespacestore object, bucketclass object)
        """
        logger.info("Pre-req: creating NSFS namespacestore (function scope)")
        nss_list = namespace_store_factory("oc", {"nsfs": [(1, "50", "nsfs")]})
        namespacestore = nss_list[0]
        logger.info(f"Created namespacestore: {namespacestore.name}")

        logger.info("Pre-req: creating vectorPolicy bucketclass (function scope)")
        bucketclass_obj = bucket_class_factory(
            {
                "interface": "OC",
                "vector_policy": {
                    "resource": namespacestore.name,
                    "vector_db_type": "lance",
                },
            }
        )
        logger.info(f"Created vector bucketclass: {bucketclass_obj.name}")
        return namespacestore, bucketclass_obj

    @tier2
    def test_s3_vector_buckets_and_indices_ui(
        self,
        request,
        setup_ui_class_factory,
        vector_prereqs,
        mcg_obj,
    ):
        """
        Create 1 S3 vector bucket via UI (Create via OBC) with 3 vector
        indices. Validate index details in the UI and confirm via S3 API that
        the bucket and its indices are present on the cluster.

        Steps:
            1.  Login to ODF console.
            2.  Navigate to Storage > Object Storage > Buckets > S3 Vector tab.
            3.  Click "Create bucket" -> select "Create via OBC".
            4.  Provide bucket name, storage class, vector bucketclass -> Create.
            5.  Click the new bucket -> click "Create vector index".
            6.  Fill in index name, dimension, distance metric -> Create.
            7.  Click the index and verify the detail page.
            8.  Repeat steps 5-7 for 3 indices.
            9.  Run S3 vector APIs and validate the bucket and its 3 indices.
        """
        NUM_BUCKETS = 1
        NUM_INDICES_PER_BUCKET = 3
        VECTOR_DIMENSION = 3

        _, bucketclass_obj = vector_prereqs
        bucketclass_name = bucketclass_obj.name
        storageclass = f"{config.ENV_DATA['cluster_namespace']}.noobaa.io"

        created_buckets = []
        created_indices_per_bucket = {}

        # Register cleanup before any UI work so teardown fires on any failure,
        # including a failure inside setup_ui_class_factory().
        def teardown():
            cleanup_vector_buckets(created_buckets, mcg_obj)

        request.addfinalizer(teardown)

        setup_ui_class_factory()

        buckets_tab = BucketsTab()

        for bucket_idx in range(NUM_BUCKETS):
            bucket_name = create_unique_resource_name(
                resource_description="vector-ui",
                resource_type="obc",
            )
            logger.info(
                f"=== Bucket {bucket_idx + 1}/{NUM_BUCKETS}: '{bucket_name}' ==="
            )

            # Steps 2–4: Navigate to S3 Vector tab and create bucket via OBC.
            # Pre-register before creation so teardown catches partial failures.
            created_buckets.append(bucket_name)
            vector_ui = buckets_tab.nav_s3_vector_tab()
            vector_ui.create_vector_bucket_via_obc(
                bucket_name=bucket_name,
                storageclass=storageclass,
                bucketclass_name=bucketclass_name,
            )
            # Wait for the OBC controller to populate the NooBaa bucket name
            # before reading it; reading immediately after form submission can
            # race on slower clusters.
            noobaa_bucket_name = None
            for noobaa_bucket_name in TimeoutSampler(
                timeout=60,
                sleep=3,
                func=lambda bn=bucket_name: OBC(bn).bucket_name,
            ):
                if noobaa_bucket_name:
                    break
            logger.info(f"  NooBaa bucket name: '{noobaa_bucket_name}'")

            # OBC creation may redirect to an OBC details page; navigate back
            # to the S3 Vector tab so navigate_to_vector_bucket can find the link.
            # Pass the resolved NooBaa bucket name — that is what the UI displays.
            vector_ui = buckets_tab.nav_s3_vector_tab()
            vector_ui.navigate_to_vector_bucket(noobaa_bucket_name)

            # Steps 5–9: Create and verify indices on the bucket detail page.
            created_indices_per_bucket[bucket_name] = _create_and_verify_indices(
                vector_ui, NUM_INDICES_PER_BUCKET, VECTOR_DIMENSION, noobaa_bucket_name
            )

        # Step 10: CLI validation via S3 vector API for all buckets
        logger.info(
            f"Validating {len(created_buckets)} vector buckets and "
            f"{NUM_INDICES_PER_BUCKET} indices each via S3 API"
        )
        for bucket_name in created_buckets:
            obc_obj = OBC(bucket_name)
            noobaa_bucket_name = obc_obj.bucket_name
            s3vectors_client = vector_utils.create_s3vectors_client(mcg_obj, obc_obj)
            _assert_s3_vector_api_state(
                s3vectors_client,
                noobaa_bucket_name,
                created_indices_per_bucket[bucket_name],
            )
        logger.info(
            f"All {len(created_buckets)} vector buckets and "
            f"{NUM_INDICES_PER_BUCKET} indices each validated via S3 API"
        )

    @tier2
    def test_s3_vector_bucket_via_s3api(
        self,
        request,
        setup_ui_class_factory,
        vector_prereqs,
        mcg_obj,
    ):
        """
        Create 1 S3 vector bucket via the "Create via S3 API" UI option, add 3
        vector indices, validate index details in the UI, and confirm via S3 API
        that the bucket and all indices are present on the cluster.

        Unlike the OBC-based test, no OBC resource is created. The bucket name
        entered in the UI is the NooBaa internal bucket name and admin mcg_obj
        credentials are used for S3 vector API calls. An NSFS namespacestore
        (provided by vector_prereqs) must exist on the cluster before creating
        a vector bucket via the S3 API path.

        Steps:
            1.  Login to ODF console.
            2.  Navigate to Storage -> Object Storage -> Buckets -> S3 Vector tab.
            3.  Click "Create bucket" -> select "Create via S3 API".
            4.  Provide bucket name and click "Create".
            5.  Click the new bucket -> click "Create vector index".
            6.  Fill in index name, dimension, distance metric -> Create.
            7.  Click the index and verify the detail page.
            8.  Repeat steps 5-7 for 3 indices.
            9.  Run S3 vector APIs and validate bucket and 3 indices on the cluster.
        """
        NUM_INDICES_PER_BUCKET = 3
        VECTOR_DIMENSION = 3

        namespacestore, _ = vector_prereqs

        bucket_name = create_unique_resource_name(
            resource_description="vector-s3",
            resource_type="bkt",
        )
        created_buckets = []

        # Register cleanup before any UI work so teardown fires on any failure,
        # including a failure inside setup_ui_class_factory().
        def teardown():
            cleanup_s3api_vector_buckets(created_buckets, mcg_obj)

        request.addfinalizer(teardown)

        setup_ui_class_factory()

        buckets_tab = BucketsTab()

        logger.info(f"=== Creating vector bucket '{bucket_name}' via S3 API ===")

        # Pre-register before creation so teardown catches partial failures.
        created_buckets.append(bucket_name)

        # Steps 2-4: Navigate to S3 Vector tab and create bucket via S3 API
        vector_ui = buckets_tab.nav_s3_vector_tab()
        vector_ui.create_vector_bucket_via_s3api(
            bucket_name=bucket_name,
            namespace_store_name=namespacestore.name,
        )

        # S3 API creation may redirect; navigate back to the tab to find the bucket.
        vector_ui = buckets_tab.nav_s3_vector_tab()
        vector_ui.navigate_to_vector_bucket(bucket_name)

        # Steps 5-8: Create and verify indices on the bucket detail page.
        created_indices = _create_and_verify_indices(
            vector_ui, NUM_INDICES_PER_BUCKET, VECTOR_DIMENSION, bucket_name
        )

        # Step 9: CLI validation via S3 vector API
        logger.info(
            f"Validating vector bucket '{bucket_name}' and "
            f"{NUM_INDICES_PER_BUCKET} indices via S3 API"
        )
        # S3 API bucket uses mcg_obj admin credentials directly (no OBC).
        s3vectors_client = vector_utils.create_s3vectors_client(mcg_obj, mcg_obj)
        _assert_s3_vector_api_state(s3vectors_client, bucket_name, created_indices)
        logger.info(
            f"Vector bucket '{bucket_name}' and {NUM_INDICES_PER_BUCKET} "
            f"indices validated via S3 API"
        )
