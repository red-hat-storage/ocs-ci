import json
import logging
import random
import string

from botocore.exceptions import ClientError

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

    @tier2
    def test_s3_vector_delete_index_and_bucket_ui(
        self,
        request,
        setup_ui_class_factory,
        vector_prereqs,
        mcg_obj,
    ):
        """
        Delete vector indices and a vector bucket via the ODF console UI.

        Pre-req (via OBC flow + S3 API):
            - 1 vector bucket created via UI (Create via OBC).
            - 3 indices created in the bucket via S3 API.

        Steps:
            1.  Navigate to Storage -> Object Storage -> Buckets -> S3 Vector tab.
            2.  Create a vector bucket via the "Create via OBC" option.
            3.  Wait for the OBC controller to populate the NooBaa bucket name.
            4.  Create 3 vector indices in the bucket via S3 API.
            5.  Navigate to the bucket detail page in the S3 Vector tab.
            6.  Select 1 index from the list and delete it via the row kebab menu.
            7.  Type the index name in the confirmation pop-up and confirm.
            8.  Verify via S3 API that exactly 2 indices remain.
            9.  Navigate back, delete each remaining index via its kebab menu.
            10. Verify via S3 API that 0 indices remain.
            11. Navigate to the S3 Vector tab and delete the vector bucket via kebab.
            12. Type the bucket name in the confirmation pop-up and confirm.
            13. Verify via S3 API that the vector bucket no longer exists.
        """
        NUM_PREREQ_INDICES = 3
        VECTOR_DIMENSION = 3

        _, bucketclass_obj = vector_prereqs
        bucketclass_name = bucketclass_obj.name
        storageclass = f"{config.ENV_DATA['cluster_namespace']}.noobaa.io"

        created_buckets = []

        def teardown():
            cleanup_vector_buckets(created_buckets, mcg_obj)

        request.addfinalizer(teardown)

        setup_ui_class_factory()
        buckets_tab = BucketsTab()

        # Steps 1-2: Navigate to S3 Vector tab and create bucket via OBC.
        # Pre-register before creation so teardown catches partial failures.
        bucket_name = create_unique_resource_name(
            resource_description="vector-del",
            resource_type="obc",
        )
        created_buckets.append(bucket_name)
        vector_ui = buckets_tab.nav_s3_vector_tab()
        vector_ui.create_vector_bucket_via_obc(
            bucket_name=bucket_name,
            storageclass=storageclass,
            bucketclass_name=bucketclass_name,
        )

        # Step 3: Wait for OBC controller to populate the NooBaa bucket name.
        noobaa_bucket_name = None
        for noobaa_bucket_name in TimeoutSampler(
            timeout=60,
            sleep=3,
            func=lambda bn=bucket_name: OBC(bn).bucket_name,
        ):
            if noobaa_bucket_name:
                break
        logger.info(f"NooBaa bucket name: '{noobaa_bucket_name}'")

        obc_obj = OBC(bucket_name)
        s3vectors_client = vector_utils.create_s3vectors_client(mcg_obj, obc_obj)

        # Step 4: Create 3 indices via S3 API.
        created_index_names = []
        distance_metrics = ("cosine", "euclidean", "cosine")
        for i in range(NUM_PREREQ_INDICES):
            index_name = f"idx-del-{_random_suffix()}-{i}"
            created_index_names.append(index_name)
            vector_utils.create_index(
                s3vectors_client,
                index_name=index_name,
                data_type="float32",
                dimension=VECTOR_DIMENSION,
                distance_metric=distance_metrics[i],
                vectorBucketName=noobaa_bucket_name,
            )
            logger.info(f"Pre-req: created index '{index_name}'")

        # Step 5: Navigate to the bucket detail page and wait for all 3 indices
        # to appear in the UI before attempting deletion — API-created indices can
        # lag behind the UI renderer.
        # The S3 Vector tab displays the NooBaa internal bucket name.
        vector_ui = buckets_tab.nav_s3_vector_tab()
        vector_ui.navigate_to_vector_bucket(noobaa_bucket_name)
        expected_indices = set(created_index_names)
        for ui_indices in TimeoutSampler(
            timeout=60,
            sleep=3,
            func=vector_ui.get_index_names_from_bucket,
        ):
            if expected_indices.issubset(set(ui_indices)):
                break

        # Steps 6-7: Delete the first index via the row kebab menu.
        index_to_delete = created_index_names[0]
        logger.info("Steps 6-7: Deleting index '%s' via UI", index_to_delete)
        vector_ui.delete_index(index_to_delete)

        # Step 8 (UI): Wait until deleted index row is absent from the UI table.
        logger.info("Step 8 (UI): Verifying '%s' row is gone from UI", index_to_delete)
        for ui_indices in TimeoutSampler(
            timeout=30,
            sleep=3,
            func=vector_ui.get_index_names_from_bucket,
        ):
            if index_to_delete not in ui_indices:
                break

        # Step 8 (API): Poll S3 API until exactly 2 indices remain — UI-driven deletes
        # propagate to the backend asynchronously, so a bare assertion can race.
        remaining_indices = created_index_names[1:]
        logger.info("Step 8: Verifying 2 indices remain via S3 API")
        for api_indices in TimeoutSampler(
            timeout=60,
            sleep=3,
            func=lambda: [
                idx["indexName"]
                for idx in vector_utils.list_indexes(
                    s3vectors_client, vectorBucketName=noobaa_bucket_name
                ).get("indexes", [])
            ],
        ):
            if set(api_indices) == set(remaining_indices):
                break
        logger.info(
            f"Confirmed: {len(remaining_indices)} indices remain after single delete"
        )

        # Step 9: Navigate back and wait for remaining indices to render in the
        # UI before deleting — same rendering lag applies after re-navigation.
        # The S3 Vector UI has no bulk-delete; indices are deleted one by one.
        logger.info("Step 9: Navigating back and deleting remaining indices one by one")
        vector_ui = buckets_tab.nav_s3_vector_tab()
        vector_ui.navigate_to_vector_bucket(noobaa_bucket_name)
        for ui_indices in TimeoutSampler(
            timeout=60,
            sleep=3,
            func=vector_ui.get_index_names_from_bucket,
        ):
            if set(remaining_indices).issubset(set(ui_indices)):
                break
        vector_ui.delete_all_indices(remaining_indices)

        # Step 10 (UI): Wait until all deleted index rows are absent from the UI table.
        logger.info("Step 10 (UI): Verifying all index rows are gone from UI")
        for ui_indices in TimeoutSampler(
            timeout=60,
            sleep=3,
            func=vector_ui.get_index_names_from_bucket,
        ):
            if not any(idx in ui_indices for idx in remaining_indices):
                break

        # Step 10 (API): Poll S3 API until 0 indices remain — same propagation lag as step 8.
        logger.info("Step 10: Verifying 0 indices remain via S3 API")
        for api_indices in TimeoutSampler(
            timeout=60,
            sleep=3,
            func=lambda: [
                idx["indexName"]
                for idx in vector_utils.list_indexes(
                    s3vectors_client, vectorBucketName=noobaa_bucket_name
                ).get("indexes", [])
            ],
        ):
            if not api_indices:
                break
        logger.info("Confirmed: 0 indices remain after individual deletion")

        # Steps 11-12: Navigate to S3 Vector tab and delete the bucket via kebab.
        logger.info(
            f"Steps 11-12: Deleting vector bucket '{noobaa_bucket_name}' via UI"
        )
        vector_ui = buckets_tab.nav_s3_vector_tab()
        vector_ui.delete_vector_bucket_from_list(noobaa_bucket_name)

        # Step 13 (UI): Wait until the bucket row is absent from the S3 Vector tab list.
        logger.info(
            "Step 13 (UI): Verifying '%s' row is gone from S3 Vector tab",
            noobaa_bucket_name,
        )
        for ui_buckets in TimeoutSampler(
            timeout=30,
            sleep=3,
            func=vector_ui.get_vector_bucket_names_from_tab,
        ):
            if noobaa_bucket_name not in ui_buckets:
                break

        # Step 13 (API): Poll S3 API until the bucket is gone — UI delete is async.
        logger.info("Step 13: Verifying vector bucket is deleted via S3 API")
        for existing_names in TimeoutSampler(
            timeout=60,
            sleep=3,
            func=lambda: [
                b["vectorBucketName"]
                for b in vector_utils.list_vector_buckets(s3vectors_client).get(
                    "vectorBuckets", []
                )
            ],
        ):
            if noobaa_bucket_name not in existing_names:
                break
        logger.info(f"Confirmed: vector bucket '{noobaa_bucket_name}' is deleted")

    @tier2
    def test_s3_vector_bucket_policy_ui(
        self,
        request,
        setup_ui_class_factory,
        vector_prereqs,
        mcg_obj,
    ):
        """
        Bucket-policy CRUD for a vector bucket via the ODF console S3 Vector tab.

        Pre-req (OBC flow + S3 API):
            - 1 vector bucket created via UI (OBC flow).
            - 3 vector indices created in the bucket via S3 API.

        Steps:
            1.  Navigate to Storage -> Object Storage -> Buckets -> S3 Vector tab.
            2.  Create a vector bucket via the "Create via OBC" option.
            3.  Wait for the OBC controller to populate the NooBaa bucket name.
            4.  Create 3 vector indices in the bucket via S3 API.
            5.  Navigate to the bucket detail page -> Bucket Policy sub-tab.
            6.  Apply an Allow policy (ListIndexes + GetIndex for *) via UI.
            7.  Verify the applied policy content via S3 API.
            8.  Verify all 3 indices remain visible in the UI (console is
                unaffected by end-user bucket policies).
            9.  Navigate back to Bucket Policy tab; edit the policy to add
                DeleteIndex to the allowed actions; apply.
            10. Verify the updated policy (3 actions) via S3 API.
            11. Delete the bucket policy via UI.
            12. Verify the policy is absent via S3 API.
        """
        NUM_PREREQ_INDICES = 3
        VECTOR_DIMENSION = 3

        _, bucketclass_obj = vector_prereqs
        bucketclass_name = bucketclass_obj.name
        storageclass = f"{config.ENV_DATA['cluster_namespace']}.noobaa.io"

        created_buckets = []

        def teardown():
            cleanup_vector_buckets(created_buckets, mcg_obj)

        request.addfinalizer(teardown)

        setup_ui_class_factory()
        buckets_tab = BucketsTab()

        # Steps 1-2: Navigate to S3 Vector tab and create bucket via OBC.
        # Pre-register before creation so teardown catches partial failures.
        bucket_name = create_unique_resource_name(
            resource_description="vector-pol",
            resource_type="obc",
        )
        created_buckets.append(bucket_name)
        vector_ui = buckets_tab.nav_s3_vector_tab()
        vector_ui.create_vector_bucket_via_obc(
            bucket_name=bucket_name,
            storageclass=storageclass,
            bucketclass_name=bucketclass_name,
        )

        # Step 3: Wait for OBC controller to populate the NooBaa bucket name.
        noobaa_bucket_name = None
        for noobaa_bucket_name in TimeoutSampler(
            timeout=60,
            sleep=3,
            func=lambda bn=bucket_name: OBC(bn).bucket_name,
        ):
            if noobaa_bucket_name:
                break
        logger.info("NooBaa bucket name: '%s'", noobaa_bucket_name)

        obc_obj = OBC(bucket_name)
        s3vectors_client = vector_utils.create_s3vectors_client(mcg_obj, obc_obj)

        # Step 4: Create 3 indices via S3 API.
        created_index_names = []
        for i in range(NUM_PREREQ_INDICES):
            index_name = f"idx-pol-{_random_suffix()}-{i}"
            created_index_names.append(index_name)
            vector_utils.create_index(
                s3vectors_client,
                index_name=index_name,
                data_type="float32",
                dimension=VECTOR_DIMENSION,
                distance_metric="cosine",
                vectorBucketName=noobaa_bucket_name,
            )
            logger.info("Pre-req: created index '%s'", index_name)

        # Step 5: Navigate to the vector bucket detail page then to the
        # Bucket Policy sub-tab.
        vector_ui = buckets_tab.nav_s3_vector_tab()
        vector_ui.navigate_to_vector_bucket(noobaa_bucket_name)
        perms_ui = vector_ui.navigate_to_bucket_policy_tab()

        # Step 6: Build initial policy (ListIndexes + GetIndex for *) and
        # apply it via the UI Monaco editor.
        initial_policy = vector_utils.gen_vector_bucket_policy(
            user_list=["*"],
            actions_list=["ListIndexes", "GetIndex"],
            resources_list=[noobaa_bucket_name, f"{noobaa_bucket_name}/*"],
            sid="initial-allow",
        )
        logger.info("Step 6: Applying initial bucket policy via UI")
        perms_ui.activate_policy_editor()
        perms_ui.set_policy_json_in_editor(json.dumps(initial_policy))
        perms_ui.apply_bucket_policy()

        # Step 7: Verify the applied policy via S3 API.
        logger.info("Step 7: Verifying initial policy via S3 API")
        policy_response = vector_utils.get_vector_bucket_policy(
            s3vectors_client, noobaa_bucket_name
        )
        applied_policy = json.loads(policy_response["policy"])
        applied_actions = applied_policy["Statement"][0].get("Action", [])
        if isinstance(applied_actions, str):
            applied_actions = [applied_actions]
        expected_initial_actions = {"s3vectors:ListIndexes", "s3vectors:GetIndex"}
        assert expected_initial_actions == set(applied_actions), (
            f"Initial policy actions mismatch. "
            f"Expected {expected_initial_actions}, got {set(applied_actions)}"
        )
        logger.info("Step 7: Initial policy verified — actions: %s", applied_actions)

        # Step 8: Verify all 3 indices remain visible in the UI — the console
        # admin view is not restricted by end-user bucket policies.
        logger.info("Step 8: Navigating to index list to verify indices still visible")
        vector_ui = buckets_tab.nav_s3_vector_tab()
        vector_ui.navigate_to_vector_bucket(noobaa_bucket_name)
        for ui_indices in TimeoutSampler(
            timeout=60,
            sleep=3,
            func=vector_ui.get_index_names_from_bucket,
        ):
            if set(created_index_names).issubset(set(ui_indices)):
                break
        logger.info(
            "Step 8: All %d indices visible — policy does not affect console",
            NUM_PREREQ_INDICES,
        )

        # Step 9: Navigate back to Bucket Policy tab and update the policy to
        # include DeleteIndex.
        perms_ui = vector_ui.navigate_to_bucket_policy_tab()
        updated_policy = vector_utils.gen_vector_bucket_policy(
            user_list=["*"],
            actions_list=["ListIndexes", "GetIndex", "DeleteIndex"],
            resources_list=[noobaa_bucket_name, f"{noobaa_bucket_name}/*"],
            sid="updated-allow",
        )
        logger.info("Step 9: Editing bucket policy via UI (adding DeleteIndex)")
        perms_ui.activate_policy_editor()
        perms_ui.set_policy_json_in_editor(json.dumps(updated_policy))
        perms_ui.apply_bucket_policy()

        # Step 10: Verify the updated policy (3 actions) via S3 API.
        logger.info("Step 10: Verifying updated policy via S3 API")
        policy_response = vector_utils.get_vector_bucket_policy(
            s3vectors_client, noobaa_bucket_name
        )
        updated_applied = json.loads(policy_response["policy"])
        updated_actions = updated_applied["Statement"][0].get("Action", [])
        if isinstance(updated_actions, str):
            updated_actions = [updated_actions]
        expected_updated_actions = {
            "s3vectors:ListIndexes",
            "s3vectors:GetIndex",
            "s3vectors:DeleteIndex",
        }
        assert expected_updated_actions == set(updated_actions), (
            f"Updated policy actions mismatch. "
            f"Expected {expected_updated_actions}, got {set(updated_actions)}"
        )
        logger.info("Step 10: Updated policy verified — actions: %s", updated_actions)

        # Step 11: Delete the bucket policy via UI.
        logger.info("Step 11: Deleting bucket policy via UI")
        perms_ui.delete_bucket_policy_ui(bucket_name=noobaa_bucket_name)

        # Step 12: Poll until the policy is absent via S3 API — the backend may
        # take a moment to propagate the UI deletion.
        logger.info("Step 12: Verifying policy is deleted via S3 API")

        def _policy_still_exists():
            try:
                vector_utils.get_vector_bucket_policy(
                    s3vectors_client, noobaa_bucket_name
                )
                return True
            except ClientError as e:
                if e.response["Error"]["Code"] == "NoSuchVectorBucketPolicy":
                    return False
                # Return unexpected errors as values — TimeoutSampler catches all
                # exceptions internally, which would mask the real error code.
                return e

        for policy_present in TimeoutSampler(
            timeout=60,
            sleep=3,
            func=_policy_still_exists,
        ):
            if isinstance(policy_present, Exception):
                raise policy_present
            if not policy_present:
                break
        logger.info("Step 12: Policy confirmed deleted via S3 API")
