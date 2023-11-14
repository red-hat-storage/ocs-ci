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
        Test basic properties and deletion of objects in versioned bucket.

        Steps:
            1. Check an empty versioned bucked properties.
            2. Check properties of the bucket with 1 file.
            3. Check 2 versions of the same file in the bucket.
            4. Check 3 version of the same file in the bucket that contain different data.
            5. Create 5 more files in bucket and check properties of the bucket and objects.
            6. Make a generic delete request to versioned object and check properties.
            7. Delete objects with 1 version from the bucket.
            8. Deleting all object versions and delete markers.

        """
        s3_obj = mcg_obj_session
        bucket = bucket_factory(interface="S3", versioning=True)[0]
        logger.info(f"Bucket {bucket.name} created")
        versioning_info = bucket.s3client.get_bucket_versioning(Bucket=bucket.name)
        logger.info(f"Versioning info of bucket {bucket.name}: {versioning_info}")
        assert versioning_info["Status"] == "Enabled"
        object_info = bucket.s3client.list_object_versions(Bucket=bucket.name)
        logger.info(f"object info of bucket {bucket.name}: {object_info}")

        filename = f"file-{uuid4().hex}"
        with open(filename, "wb") as f:
            f.write(os.urandom(1000))
        logger.info(f"Created file {filename}")
        logger.info(f"Putting file {filename} into bucket {bucket.name}")
        s3_put_object(s3_obj, bucket.name, filename, filename)
        versioning_info = bucket.s3client.get_bucket_versioning(Bucket=bucket.name)
        logger.info(f"Versioning info of bucket {bucket.name}: {versioning_info}")
        assert versioning_info["Status"] == "Enabled"
        object_info = bucket.s3client.list_object_versions(Bucket=bucket.name)
        logger.info(f"object info of bucket {bucket.name}: {object_info}")

        logger.info("Checking that there is only one object with IsLatest set to True")
        assert len(object_info["Versions"]) == 1
        assert object_info["Versions"][0]["IsLatest"] is True
        version_ids = []
        version_ids.append(object_info["Versions"][0]["VersionId"])

        s3_put_object(s3_obj, bucket.name, filename, filename)
        versioning_info = bucket.s3client.get_bucket_versioning(Bucket=bucket.name)
        logger.info(f"Versioning info of bucket {bucket.name}: {versioning_info}")
        assert versioning_info["Status"] == "Enabled"
        object_info = bucket.s3client.list_object_versions(Bucket=bucket.name)
        logger.info(f"object info of bucket {bucket.name}: {object_info}")

        logger.info("Checking 2 versions of the same file in a bucket")
        assert len(object_info["Versions"]) == 2
        assert object_info["Versions"][0]["IsLatest"] is True
        assert object_info["Versions"][0]["VersionId"] != version_ids[0]
        assert object_info["Versions"][1]["IsLatest"] is False
        version_ids.append(object_info["Versions"][0]["VersionId"])
        assert object_info["Versions"][1]["VersionId"] == version_ids[0]
        assert version_ids[0] != version_ids[1]
        assert object_info["Versions"][0]["Key"] == object_info["Versions"][1]["Key"]
        assert object_info["Versions"][0]["Size"] == object_info["Versions"][1]["Size"]

        with open(filename, "wb") as f:
            f.write(os.urandom(100))
        logger.info(f"Created file {filename}!!")
        s3_put_object(s3_obj, bucket.name, filename, filename)

        logger.info(f"Putting smaller file {filename} into bucket {bucket.name}")
        versioning_info = bucket.s3client.get_bucket_versioning(Bucket=bucket.name)
        logger.info(f"Versioning info of bucket {bucket.name}: {versioning_info}")
        assert versioning_info["Status"] == "Enabled"
        object_info = bucket.s3client.list_object_versions(Bucket=bucket.name)
        logger.info(f"object info of bucket {bucket.name}: {object_info}")
        assert len(object_info["Versions"]) == 3
        assert object_info["Versions"][0]["IsLatest"] is True
        assert object_info["Versions"][0]["VersionId"] != version_ids[0]
        assert object_info["Versions"][1]["IsLatest"] is False
        assert object_info["Versions"][2]["IsLatest"] is False
        version_ids.append(object_info["Versions"][0]["VersionId"])
        assert object_info["Versions"][2]["VersionId"] == version_ids[0]
        assert len(set(version_ids)) == 3
        assert object_info["Versions"][0]["Key"] == object_info["Versions"][1]["Key"]
        assert object_info["Versions"][0]["Size"] < object_info["Versions"][1]["Size"]

        logger.info(f"Creating 5 more files in bucket {bucket.name}")
        for i in range(5):
            s3_put_object(s3_obj, bucket.name, f"{filename}{i}", filename)
        versioning_info = bucket.s3client.get_bucket_versioning(Bucket=bucket.name)
        logger.info(f"Versioning info of bucket {bucket.name}: {versioning_info}")
        assert versioning_info["Status"] == "Enabled"
        object_info = bucket.s3client.list_object_versions(Bucket=bucket.name)
        logger.info(f"object info of bucket {bucket.name}: {object_info}")
        assert len(object_info["Versions"]) == 8
        assert object_info["Versions"][0]["IsLatest"] is True
        assert object_info["Versions"][1]["IsLatest"] is False
        assert object_info["Versions"][2]["IsLatest"] is False
        for i in range(3, 8):
            assert object_info["Versions"][i]["IsLatest"] is True
            version_ids.append(object_info["Versions"][i]["VersionId"])
        assert len(set(version_ids)) == 8

        logger.info(
            f"Getting object {filename} from bucket {bucket.name} and checking version"
        )
        bucket_object = bucket.s3client.get_object(Bucket=bucket.name, Key=filename)
        logger.info(
            f"info of object {filename} from bucket {bucket.name}: {bucket_object}"
        )
        assert bucket_object["VersionId"] == object_info["Versions"][0]["VersionId"]

        logger.info(f"Deleting object {filename} from bucket {bucket.name}")
        s3_delete_object(s3_obj, bucket.name, filename)
        versioning_info = bucket.s3client.get_bucket_versioning(Bucket=bucket.name)
        logger.info(f"Versioning info of bucket {bucket.name}: {versioning_info}")
        assert versioning_info["Status"] == "Enabled"
        object_info = bucket.s3client.list_object_versions(Bucket=bucket.name)
        logger.info(f"object info of bucket {bucket.name}: {object_info}")
        assert object_info["Versions"][0]["IsLatest"] is False
        assert len(object_info["DeleteMarkers"]) == 1
        assert len(object_info["Versions"]) == 8

        bucket_object = bucket.s3client.get_object(Bucket=bucket.name, Key=filename)
        logger.info(
            f"info of object {filename} from bucket {bucket.name}: {bucket_object}"
        )

        logger.info(f"Deleting other objects from bucket {bucket.name}")
        for i in range(5):
            s3_delete_object(s3_obj, bucket.name, f"{filename}{i}")
        versioning_info = bucket.s3client.get_bucket_versioning(Bucket=bucket.name)
        logger.info(f"Versioning info of bucket {bucket.name}: {versioning_info}")
        assert versioning_info["Status"] == "Enabled"
        object_info = bucket.s3client.list_object_versions(Bucket=bucket.name)
        logger.info(f"object info of bucket {bucket.name}: {object_info}")
        for version in object_info["Versions"]:
            assert version["IsLatest"] is False
        assert len(object_info["DeleteMarkers"]) == 6

        logger.info("Deleting all object versions and delete markers")
        versions = object_info.get("Versions", [])
        versions.extend(object_info.get("DeleteMarkers", []))
        for version_id in [
            x["VersionId"]
            for x in versions
            if x["Key"] == filename and x["VersionId"] != "null"
        ]:
            bucket.s3client.delete_object(
                Bucket=bucket.name, Key=filename, VersionId=version_id
            )

        bucket_object = bucket.s3client.get_object(Bucket=bucket.name, Key=filename)
        logger.info(
            f"info of object {filename} from bucket {bucket.name}: {bucket_object}"
        )
        object_info = bucket.s3client.list_object_versions(Bucket=bucket.name)
        logger.info(f"object info of bucket {bucket.name}: {object_info}")

        os.remove(filename)

    @tier2
    def test_listing(self, bucket_factory, mcg_obj_session, setup_file_object):
        """
        Test listing of objects in versioned bucket.

        Steps:
            1. Create a bucket with 80 files, 20 new versions and 2 directories
            2. Check that there is 102 objects in the bucket
            3. Check listing with Key markers
            4. Check listing with prefix and delimiter

        """
        s3_obj = mcg_obj_session
        filename = setup_file_object
        logger.info(
            "Creating a bucket with 80 files, 20 new versions and 2 directories"
        )
        bucket = bucket_factory(interface="S3", versioning=True)[0]
        for i in range(20):
            s3_put_object(s3_obj, bucket.name, f"{filename}{i}", filename)
        for i in range(20):
            s3_put_object(s3_obj, bucket.name, f"{i}{filename}", filename)
        # add directory
        s3_obj.s3_client.put_object(Bucket=bucket.name, Key=("testdir/"))
        # add empty directory
        s3_obj.s3_client.put_object(Bucket=bucket.name, Key=("testdir_empty/"))
        for i in range(20):
            s3_put_object(s3_obj, bucket.name, f"{filename}{i}", filename)
        for i in range(20):
            s3_put_object(s3_obj, bucket.name, f"testdir/{filename}{i}", filename)
        # add new versions of files in directory testdir
        for i in range(20):
            s3_put_object(s3_obj, bucket.name, f"testdir/{filename}{i}", filename)

        logger.info("Check that there is 102 objects in the bucket")
        object_info = bucket.s3client.list_object_versions(Bucket=bucket.name)
        logger.info(f"object info of bucket {bucket.name}: {object_info}")
        assert len(object_info["Versions"]) == 102

        logger.info("Check listing of 35 files")
        object_info = bucket.s3client.list_object_versions(
            Bucket=bucket.name, MaxKeys=35
        )
        logger.info(f"object info of bucket {bucket.name}: {object_info}")
        version_ids = [x["VersionId"] for x in object_info["Versions"]]
        assert len(set(version_ids)) == 35
        assert object_info["NextVersionIdMarker"]
        assert object_info["NextKeyMarker"]

        logger.info(
            "Check listing of 35 files following NextKeyMarker from previous listing"
        )
        object_info = bucket.s3client.list_object_versions(
            Bucket=bucket.name, MaxKeys=35, KeyMarker=object_info["NextKeyMarker"]
        )
        logger.info(f"object info of bucket {bucket.name}: {object_info}")
        version_ids2 = [x["VersionId"] for x in object_info["Versions"]]
        assert len(set(version_ids2)) == 35
        assert not set(version_ids).intersection(version_ids2)
        assert object_info["NextVersionIdMarker"]
        assert object_info["NextKeyMarker"]

        logger.info("Check listing of following 32 files")
        object_info = bucket.s3client.list_object_versions(
            Bucket=bucket.name, MaxKeys=32, KeyMarker=object_info["NextKeyMarker"]
        )
        logger.info(f"object info of bucket {bucket.name}: {object_info}")
        version_ids3 = [x["VersionId"] for x in object_info["Versions"]]
        assert len(set(version_ids3)) == 31
        assert not set(version_ids2).intersection(version_ids3)
        assert not object_info.get("NextVersionIdMarker")
        assert not object_info.get("NextKeyMarker")

        logger.info("Check listing of following 30 files, the list should be smaller")
        object_info = bucket.s3client.list_object_versions(
            Bucket=bucket.name, MaxKeys=30, KeyMarker=object_info["NextKeyMarker"]
        )
        logger.info(f"object info of bucket {bucket.name}: {object_info}")
        version_ids4 = [x["VersionId"] for x in object_info["Versions"]]
        assert len(set(version_ids4)) == 11
        assert not set(version_ids3).intersection(version_ids4)

        logger.info("Check listing with a prefix and delimiter")
        object_info = bucket.s3client.list_object_versions(
            Bucket=bucket.name, Prefix="testdir/", Delimiter="/"
        )
        logger.info(f"object info of bucket {bucket.name}: {object_info}")

    @tier2
    @pytest.mark.parametrize(
        argnames=["retention"],
        argvalues=[
            pytest.param(*["governance"], marks=pytest.mark.polarion_id("OCS-")),
            pytest.param(*["compliance"], marks=pytest.mark.polarion_id("OCS-")),
        ],
    )
    def test_locking(self, bucket_factory, mcg_obj_session, retention):
        """
        Test S3 Object Locking in Governance and Compliance retention modes.

        """
        s3_obj = mcg_obj_session
        bucket = bucket_factory(interface="S3", versioning=True, object_lock=True)[0]
        logger.info(f"Bucket {bucket.name} created")
        filename = f"file-{uuid4().hex}"
        with open(filename, "wb") as f:
            f.write(os.urandom(1000))
        logger.info(f"Created file {filename}")
        logger.info(f"Putting file {filename} into bucket {bucket.name}")
        s3_put_object(s3_obj, bucket.name, filename, filename)

        logger.info(
            f"Setting object lock with with retention policy {retention} to {bucket.name}"
        )
        policy_response = bucket.s3client.put_object_lock_configuration(
            Bucket=bucket.name,
            ObjectLockConfiguration={
                "ObjectLockEnabled": "Enabled",
                "Rule": {"DefaultRetention": {"Mode": retention, "Days": 12}},
            },
        )
        logger.debug(f"policy response: {policy_response}")
        object_info = bucket.s3client.list_object_versions(Bucket=bucket.name)
        logger.info(f"object info of bucket {bucket.name}: {object_info}")
        put_object_response = s3_put_object(s3_obj, bucket.name, filename, filename)
        logger.info(f"put object response: {put_object_response}")

        object_info = bucket.s3client.list_object_versions(Bucket=bucket.name)
        logger.info(f"object info of bucket {bucket.name}: {object_info}")

    @tier2
    def test_version_restore(self, bucket_factory, mcg_obj_session):
        """
        According to AWS documentation about restoring object versions
        there are 2 ways:
            * Copy a previous version of the object into the same bucket.
                The copied object becomes the current version of that object an
                all object versions are preserved.
            * Permanently delete the current version of the object.
                When you delete the current object version, you, in effect, turn the
                previous version into the current version of that object.

        AWS documentation:
        https://docs.aws.amazon.com/AmazonS3/latest/userguide/RestoringPreviousVersions.html

        """
        s3_obj = mcg_obj_session
        bucket = bucket_factory(interface="S3", versioning=True)[0]
        logger.info(f"Bucket {bucket.name} created")
        filename = f"file-{uuid4().hex}"
        with open(filename, "wb") as f:
            f.write(os.urandom(1000))
        logger.info(f"Created file {filename}")
        logger.info(f"Putting file {filename} into bucket {bucket.name}")
        s3_put_object(s3_obj, bucket.name, filename, filename)
        bucket_object1 = bucket.s3client.get_object(Bucket=bucket.name, Key=filename)
        logger.info(
            f"info of object {filename} from bucket {bucket.name}: {bucket_object1}"
        )

        filename2 = f"file-{uuid4().hex}"
        with open(filename2, "wb") as f:
            f.write(os.urandom(300))
        logger.info(f"Created file {filename2}")
        logger.info(f"Putting file {filename2} into bucket {bucket.name}")
        s3_put_object(s3_obj, bucket.name, filename, filename2)
        bucket_object2 = bucket.s3client.get_object(Bucket=bucket.name, Key=filename)
        logger.info(
            f"info of object {filename} from bucket {bucket.name}: {bucket_object2}"
        )
        assert bucket_object1["body"] != bucket_object2["body"]
        object_info = bucket.s3client.list_object_versions(Bucket=bucket.name)
        logger.info(f"object info of bucket {bucket.name}: {object_info}")
        assert object_info["Versions"][1]["IsLatest"] is True
        assert bucket_object2["VersionId"] == object_info["Versions"][1]["VersionId"]

        logger.info(f"Putting file {filename} into bucket {bucket.name} again")
        s3_put_object(s3_obj, bucket.name, filename, filename)
        bucket_object3 = bucket.s3client.get_object(Bucket=bucket.name, Key=filename)
        logger.info(
            f"info of object {filename} from bucket {bucket.name}: {bucket_object3}"
        )
        assert bucket_object1["body"] == bucket_object3["body"]
        object_info = bucket.s3client.list_object_versions(Bucket=bucket.name)
        logger.info(f"object info of bucket {bucket.name}: {object_info}")
        assert object_info["Versions"][2]["IsLatest"] is True
        assert bucket_object3["VersionId"] == object_info["Versions"][2]["VersionId"]

        logger.info(
            f"Deleting object {filename} twice from bucket {bucket.name} to get to oriinal version"
        )
        s3_delete_object(s3_obj, bucket.name, filename)
        s3_delete_object(s3_obj, bucket.name, filename)
        versioning_info = bucket.s3client.get_bucket_versioning(Bucket=bucket.name)
        logger.info(f"Versioning info of bucket {bucket.name}: {versioning_info}")
        bucket_object3 = bucket.s3client.get_object(Bucket=bucket.name, Key=filename)
        logger.info(
            f"info of object {filename} from bucket {bucket.name}: {bucket_object3}"
        )
