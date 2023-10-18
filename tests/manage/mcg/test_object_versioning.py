import logging
import pytest
import os
import threading

from uuid import uuid4

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    bugzilla,
    tier2,
    skipif_ocs_version,
    red_squad,
)
from ocs_ci.ocs.bucket_utils import (
    s3_put_bucket_versioning,
    s3_put_object,
    s3_delete_object,
    s3_head_object,
)
from ocs_ci.ocs.resources.pod import get_pods_having_label, Pod
from ocs_ci.ocs import constants

logger = logging.getLogger(__name__)


@red_squad
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
    @bugzilla("2111544")
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
        for i in range(0, 5):
            threading.Thread(
                target=s3_delete_object, args=(s3_obj, bucket_name, filename)
            ).start()
            threading.Thread(
                target=s3_put_object, args=(s3_obj, bucket_name, filename, filename)
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

    @tier2
    def test_versioning_properties_and_deletion(self, bucket_factory, mcg_obj_session):
        """
        """
        s3_obj = mcg_obj_session
        bucket = bucket_factory(interface="S3", versioning=True)[0]
        versioning_info = bucket.s3client.get_bucket_versioning(Bucket=bucket.name)
        logger.info(f"Versioning info of bucket {bucket.name}: (versioning_info)")
        object_info = bucket.s3client.list_object_versions(Bucket=bucket.name)
        logger.info(f"object info of bucket {bucket.name}: (object_info)")

        filename = f"file-{uuid4().hex}"
        with open(filename, "wb") as f:
            f.write(os.urandom(1000))
        logger.info(f"Created file {filename}!!")
        s3_put_object(s3_obj, bucket.name, filename, filename)
        versioning_info = bucket.s3client.get_bucket_versioning(Bucket=bucket.name)
        logger.info(f"Versioning info of bucket {bucket.name}: (versioning_info)")
        object_info = bucket.s3client.list_object_versions(Bucket=bucket.name)
        logger.info(f"object info of bucket {bucket.name}: (object_info)")

        s3_put_object(s3_obj, bucket.name, filename, filename)
        versioning_info = bucket.s3client.get_bucket_versioning(Bucket=bucket.name)
        logger.info(f"Versioning info of bucket {bucket.name}: (versioning_info)")
        object_info = bucket.s3client.list_object_versions(Bucket=bucket.name)
        logger.info(f"object info of bucket {bucket.name}: (object_info)")

        with open(filename, "wb") as f:
            f.write(os.urandom(1000))
        logger.info(f"Created file {filename}!!")
        s3_put_object(s3_obj, bucket.name, filename, filename)
        versioning_info = bucket.s3client.get_bucket_versioning(Bucket=bucket.name)
        logger.info(f"Versioning info of bucket {bucket.name}: (versioning_info)")
        object_info = bucket.s3client.list_object_versions(Bucket=bucket.name)
        logger.info(f"object info of bucket {bucket.name}: (object_info)")

        for i in range(5):
            s3_put_object(s3_obj, bucket.name, f"{filename}{i}", filename)
        versioning_info = bucket.s3client.get_bucket_versioning(Bucket=bucket.name)
        logger.info(f"Versioning info of bucket {bucket.name}: (versioning_info)")
        object_info = bucket.s3client.list_object_versions(Bucket=bucket.name)
        logger.info(f"object info of bucket {bucket.name}: (object_info)")

        s3_delete_object(s3_obj, bucket.name, filename)
        versioning_info = bucket.s3client.get_bucket_versioning(Bucket=bucket.name)
        logger.info(f"Versioning info of bucket {bucket.name}: (versioning_info)")
        object_info = bucket.s3client.list_object_versions(Bucket=bucket.name)
        logger.info(f"object info of bucket {bucket.name}: (object_info)")

        for i in range(5):
            s3_delete_object(s3_obj, bucket.name, f"{filename}{i}")
        versioning_info = bucket.s3client.get_bucket_versioning(Bucket=bucket.name)
        logger.info(f"Versioning info of bucket {bucket.name}: (versioning_info)")
        object_info = bucket.s3client.list_object_versions(Bucket=bucket.name)
        logger.info(f"object info of bucket {bucket.name}: (object_info)")

        os.remove(filename)

    @tier2
    def test_listing(self, bucket_factory, mcg_obj_session, setup_file_object):
        """
        """
        s3_obj = mcg_obj_session
        filename = setup_file_object
        bucket = bucket_factory(interface="S3", versioning=True)[0]
        for i in range(20):
            s3_put_object(s3_obj, bucket.name, f"{filename}{i}", filename)
        for i in range(20):
            s3_put_object(s3_obj, bucket.name, f"{i}{filename}", filename)
        #add directory
        s3_obj.s3_client.put_object(Bucket=bucket.name, Key=("testdir/"))
        #add empty directory
        s3_obj.s3_client.put_object(Bucket=bucket.name, Key=("testdir_empty/"))
        for i in range(20):
            s3_put_object(s3_obj, bucket.name, f"{filename}{i}", filename)
        for i in range(20):
            s3_put_object(s3_obj, bucket.name, f"testdir/{filename}{i}", filename)
        object_info = bucket.s3client.list_object_versions(Bucket=bucket.name)
        logger.info(f"object info of bucket {bucket.name}: (object_info)")
        object_info = bucket.s3client.list_object_versions(Bucket=bucket.name, MaxKeys=35)
        logger.info(f"object info of bucket {bucket.name}: (object_info)")
        object_info = bucket.s3client.list_object_versions(Bucket=bucket.name, MaxKeys=35, KeyMarker=object_info["NextKeyMarker"], VersionIdMarker=object_info['NextVersionIdMarker'])
        logger.info(f"object info of bucket {bucket.name}: (object_info)")
        object_info = bucket.s3client.list_object_versions(Bucket=bucket.name, MaxKeys=30, KeyMarker=object_info["NextKeyMarker"], VersionIdMarker=object_info['NextVersionIdMarker'])
        logger.info(f"object info of bucket {bucket.name}: (object_info)")
        object_info = bucket.s3client.list_object_versions(Bucket=bucket.name, MaxKeys=30, KeyMarker=object_info["NextKeyMarker"], VersionIdMarker=object_info['NextVersionIdMarker'])
        logger.info(f"object info of bucket {bucket.name}: (object_info)")
        object_info = bucket.s3client.list_object_versions(Bucket=bucket.name, Prefix="testdir/", Delimiter="/")
        logger.info(f"object info of bucket {bucket.name}: (object_info)")

    def test_locking(self):
        """
        """
        pass
