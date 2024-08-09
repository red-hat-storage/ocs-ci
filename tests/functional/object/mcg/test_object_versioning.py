import logging
import pytest
import os

from uuid import uuid4

from ocs_ci.framework import config, ConfigSafeThread
from ocs_ci.framework.pytest_customization.marks import (
    tier2,
    skipif_ocs_version,
    red_squad,
    runs_on_provider,
    mcg,
    post_upgrade,
    pre_upgrade,
    polarion_id,
)
from ocs_ci.ocs.bucket_utils import (
    s3_put_bucket_versioning,
    s3_put_object,
    s3_delete_object,
    s3_head_object,
    s3_get_object,
)
from ocs_ci.ocs.resources.pod import get_pods_having_label, Pod
from ocs_ci.ocs import constants

logger = logging.getLogger(__name__)


@mcg
@red_squad
@runs_on_provider
class TestObjectVersioning:
    @pytest.fixture(scope="function")
    def setup_file_object(self, request):
        # create object file
        filename = f"file-{uuid4().hex}"
        with open(filename, "wb") as f:
            f.write(os.urandom(1000))
        logger.info(f"Created file {filename}!!")

        def teardown():
            os.remove(filename)
            logger.info(f"Removed file {filename}!!")

        request.addfinalizer(teardown)
        return filename

    @tier2
    @skipif_ocs_version("<4.10")
    @pytest.mark.parametrize(
        argnames=["versioned"],
        argvalues=[
            pytest.param(
                True,
                marks=[pytest.mark.polarion_id("OCS-4622")],
            ),
            pytest.param(
                False,
                marks=[pytest.mark.polarion_id("OCS-4623")],
            ),
        ],
    )
    def test_versioning_parallel_ops(
        self, bucket_factory, mcg_obj_session, setup_file_object, versioned
    ):
        """
        This test will check if there is more than one current version of the objects when multiple
        s3 put/delete operations are performed parallely.
        First test with the object versioning enabled on bucket and later without versioning
        enabled.
        """

        # create a bucket and generate test file object
        bucket_name = bucket_factory()[0].name
        filename = setup_file_object
        s3_obj = mcg_obj_session
        query = (
            "SELECT data->>'bucket' as bucket, data->>'key' as key,"
            "jsonb_agg(jsonb_build_object('seq', data->'version_seq', '_id', _id)) as versions "
            "FROM objectmds WHERE ((data -> 'deleted'::text) IS NULL OR "
            "(data -> 'deleted'::text) = 'null'::jsonb) "
            "AND ((data -> 'upload_started'::text) IS NULL OR "
            "(data -> 'upload_started'::text) = 'null'::jsonb) "
            "AND ((data -> 'version_enabled'::text) IS NULL OR "
            "(data -> 'version_enabled'::text) = 'null'::jsonb) GROUP BY 1,2 HAVING count(*) > 1;"
        )

        # enable versioning on the bucket
        if versioned:
            s3_put_bucket_versioning(s3_obj=s3_obj, bucketname=bucket_name)
            logger.info("Object versioning enabled!")
            query = (
                "SELECT data->>'bucket' as bucket, data->>'key' as key,"
                "jsonb_agg(jsonb_build_object('seq', data->'version_seq', '_id', _id)) as versions "
                "FROM objectmds WHERE ((data -> 'deleted'::text) IS NULL OR "
                "(data -> 'deleted'::text) = 'null'::jsonb) "
                "AND ((data -> 'upload_started'::text) IS NULL OR "
                "(data -> 'upload_started'::text) = 'null'::jsonb) "
                "AND ((data -> 'version_past'::text) IS NULL OR (data -> 'version_past'::text) = 'null'::jsonb) "
                "GROUP BY 1,2 HAVING count(*) > 1;"
            )
        command = f'psql -h 127.0.0.1 -p 5432 -U postgres -d nbcore -c "{query}"'

        # perform PUT and DELETE parallely on loop
        config_index = config.default_cluster_index
        for i in range(0, 5):
            ConfigSafeThread(
                config_index=config_index,
                target=s3_delete_object,
                args=(s3_obj, bucket_name, filename),
            ).start()
            ConfigSafeThread(
                config_index=config_index,
                target=s3_put_object,
                args=(s3_obj, bucket_name, filename, filename),
            ).start()

        # head object
        try:
            head_obj_output = s3_head_object(
                s3_obj=s3_obj, bucketname=bucket_name, object_key=filename
            )
            logger.info(
                f"Head object s3://{bucket_name}/{filename}:\n{head_obj_output}"
            )
        except Exception as err:
            logger.info(f"[Head object failed]: {err}")

        # Run query on nooba-db to see if there is more than
        # one current version of the object
        pod_data = get_pods_having_label(
            label=constants.NOOBAA_DB_LABEL_47_AND_ABOVE,
            namespace=config.ENV_DATA["cluster_namespace"],
        )[0]

        db_pod = Pod(**pod_data)
        query_out = db_pod.exec_cmd_on_pod(
            command=command,
            out_yaml_format=False,
        )
        logger.info(f"DB query output: {query_out}")
        assert "(0 rows)" in str(
            query_out
        ), "[Test failed] There are more than one versions considered to be latest version !!"
        logger.info("Test succeeded!!")


@mcg
@red_squad
@polarion_id("OCS-6177")
class TestGetObjectByVersionID:

    # Common consts between the
    # pre-upgrade and post-upgrade tests
    object_name = "file_1"
    object_data = "version 1"

    @pre_upgrade
    def test_get_object_pre_upgrade(
        self, request, bucket_factory_session, mcg_obj_session
    ):
        """
        This test will run as pre-upgrade test where we
        verify 'GetObject' by version id

        """

        # create bucket
        bucket = bucket_factory_session()[0]
        logger.info(f"Created bucket {bucket.name}")

        # enable bucket versioning
        s3_put_bucket_versioning(mcg_obj_session, bucket.name)
        logger.info(f"Enabled bucket versioning on bucket {bucket.name}")

        # upload object with some data
        s3_put_object(
            mcg_obj_session,
            bucket.name,
            object_key=self.object_name,
            data=self.object_data,
        )
        logger.info(f"uploaded {self.object_name} to {bucket.name}")

        # get object from the bucket
        ver_1 = s3_get_object(
            mcg_obj_session,
            bucket.name,
            object_key=self.object_name,
        )["VersionId"]

        # update the same object with some extra data
        new_data = "version 2"
        s3_put_object(
            mcg_obj_session,
            bucket.name,
            object_key=self.object_name,
            data=new_data,
        )
        logger.info("updated the object with some extra data")

        # get object with and without version id
        response_body = (
            s3_get_object(
                mcg_obj_session,
                bucket.name,
                object_key=self.object_name,
            )["Body"]
            .read()
            .decode("utf-8")
        )
        assert (
            response_body == new_data
        ), f"Object data doesnt match with the latest written data. Response data: {response_body}"

        response_body_v1 = (
            s3_get_object(
                mcg_obj_session,
                bucket.name,
                object_key=self.object_name,
                versionid=ver_1,
            )["Body"]
            .read()
            .decode("utf-8")
        )
        assert (
            response_body_v1 == self.object_data
        ), f"Object data doesnt match the version {ver_1} data. Response data: {response_body_v1}"
        logger.info(
            f"Verified the get object by version id. \nResponse data: {response_body_v1}"
        )

        # cache the bucket name to pass it to post upgrade verification
        request.config.cache.set("versioning_enabled_bucket", bucket.name)
        request.config.cache.set("object_version", ver_1)
        logger.info(
            "Cached the bucket name and object name for the post upgrade verification"
        )

    @post_upgrade
    def test_get_object_post_upgrade(self, request, mcg_obj_session):
        """
        Test get object by bucket versioning on a bucket
        which already has some versioned data

        """
        # fetch bucket name from the cache
        bucket_name = request.config.cache.get("versioning_enabled_bucket", None)
        ver_1 = request.config.cache.get("object_version", None)
        logger.info(f"Bucket {bucket_name} fetched detail from cache")

        # upload some data to the bucket
        new_data = "version 3"
        s3_put_object(
            mcg_obj_session, bucket_name, object_key=self.object_name, data=new_data
        )
        logger.info("Updated the object with new data")

        # get object with and without versioning
        response_body = (
            s3_get_object(
                mcg_obj_session,
                bucket_name,
                object_key=self.object_name,
            )["Body"]
            .read()
            .decode("utf-8")
        )
        assert (
            response_body == new_data
        ), f"Object data doesnt match with the latest written data. Response data: {response_body}"

        response_body_v1 = (
            s3_get_object(
                mcg_obj_session,
                bucket_name,
                object_key=self.object_name,
                versionid=ver_1,
            )["Body"]
            .read()
            .decode("utf-8")
        )
        assert (
            response_body_v1 == self.object_data
        ), f"Object data doesnt match the version {ver_1} data. Response data: {response_body_v1}"
        logger.info(
            f"Verified the get object by version id. \nResponse data: {response_body_v1}"
        )
