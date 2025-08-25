import logging
import os
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor
from zipfile import ZipFile
import pytest
from flaky import flaky

from ocs_ci.framework import config, config_safe_thread_pool_task
from ocs_ci.framework.pytest_customization.marks import (
    vsphere_platform_required,
    skip_inconsistent,
    red_squad,
    runs_on_provider,
    mcg,
    skipif_fips_enabled,
    skipif_fips_enabled_on_ibm_cloud,
)
from ocs_ci.framework.testlib import (
    MCGTest,
    tier1,
    tier2,
    performance,
)
from ocs_ci.utility.utils import exec_nb_db_query
from ocs_ci.ocs import constants
from ocs_ci.ocs.bucket_utils import (
    sync_object_directory,
    retrieve_test_objects_to_pod,
    craft_s3_command,
    s3_put_object,
    s3_head_object,
    rm_object_recursive,
    write_random_test_objects_to_bucket,
)

from ocs_ci.framework.pytest_customization.marks import (
    skipif_managed_service,
    skipif_ocs_version,
    on_prem_platform_required,
    jira,
)
from ocs_ci.ocs.constants import AWSCLI_TEST_OBJ_DIR
from uuid import uuid4

logger = logging.getLogger(__name__)

PUBLIC_BUCKET = "1000genomes"
LARGE_FILE_KEY = "1000G_2504_high_coverage/data/ERR3239276/NA06985.final.cram"
FILESIZE_SKIP = pytest.mark.skip("Current test filesize is too large.")
RUNTIME_SKIP = pytest.mark.skip("Runtime is too long; Code needs to be parallelized")


def pod_io(pods):
    """
    Running IOs on rbd and cephfs pods

    Args:
        pods (Pod): List of pods

    """
    with ThreadPoolExecutor() as p:
        for pod in pods:
            p.submit(
                config_safe_thread_pool_task,
                config.default_cluster_index,
                pod.run_io,
                "fs",
                "1G",
            )


@pytest.fixture(scope="function")
def file_setup(request):
    """
    Generates test files and then zips it

    Returns:
          name of the zip file created

    """
    filename = f"random-{uuid4().hex}"
    zip_filename = f"{filename}.zip"
    with open(filename, "wb") as f:
        f.write(os.urandom(1000))
    with ZipFile(zip_filename, "w") as zip:
        zip.write(f"{filename}", compress_type=zipfile.ZIP_DEFLATED)

    def teardown():
        os.remove(f"{filename}.zip")
        os.remove(f"{filename}")
        logger.info(f"Removed files {filename} and {filename}.zip!!")

    request.addfinalizer(teardown)
    return zip_filename


@pytest.fixture(scope="class", autouse=True)
def reduce_dedup_wait_time(add_env_vars_to_noobaa_endpoint_class):
    """
    Reduce the dedup time to 0 sec
    """
    add_env_vars_to_noobaa_endpoint_class([(constants.MIN_CHUNK_AGE_FOR_DEDUP, 0)])


@mcg
@red_squad
@runs_on_provider
@skipif_managed_service
class TestBucketIO(MCGTest):
    """
    Test IO of a bucket
    """

    @pytest.mark.polarion_id("OCS-1300")
    @pytest.mark.parametrize(
        argnames="interface,bucketclass_dict",
        argvalues=[
            pytest.param(
                *[
                    "OC",
                    {"interface": "OC", "backingstore_dict": {"rgw": [(1, None)]}},
                ],
                marks=[tier1, on_prem_platform_required],
            ),
            pytest.param(
                *[
                    "CLI",
                    {"interface": "CLI", "backingstore_dict": {"rgw": [(1, None)]}},
                ],
                marks=[tier2, on_prem_platform_required],
            ),
            pytest.param(
                *[
                    "CLI",
                    {
                        "interface": "CLI",
                        "namespace_policy_dict": {
                            "type": "Single",
                            "namespacestore_dict": {"rgw": [(1, None)]},
                        },
                    },
                ],
                marks=[
                    tier2,
                    on_prem_platform_required,
                    jira("DFBUGS-1035", run=False),
                ],
            ),
        ],
        ids=[
            "RGW-OC-1",
            "RGW-CLI-1",
            "RGW-CLI-NSS-1",
        ],
    )
    @flaky
    def test_write_file_to_bucket(
        self,
        mcg_obj,
        awscli_pod_session,
        bucket_class_factory,
        bucket_factory,
        interface,
        bucketclass_dict,
    ):
        """
        Test object IO using the S3 SDK
        """
        # Retrieve a list of all objects on the test-objects bucket and
        # downloads them to the pod
        bucketname = bucket_factory(
            1, interface=interface, bucketclass=bucketclass_dict
        )[0].name
        full_object_path = f"s3://{bucketname}"
        downloaded_files = awscli_pod_session.exec_cmd_on_pod(
            f"ls -A1 {AWSCLI_TEST_OBJ_DIR}"
        ).split(" ")
        # Write all downloaded objects to the new bucket
        sync_object_directory(
            awscli_pod_session, AWSCLI_TEST_OBJ_DIR, full_object_path, mcg_obj
        )

        assert set(downloaded_files).issubset(
            obj.key for obj in mcg_obj.s3_list_all_objects_in_bucket(bucketname)
        )

    @pytest.mark.polarion_id("OCS-1949")
    @pytest.mark.parametrize(
        argnames="bucketclass_dict",
        argvalues=[
            pytest.param(
                None,
                marks=[tier1, skipif_fips_enabled_on_ibm_cloud],
            ),
            pytest.param(
                {
                    "interface": "OC",
                    "backingstore_dict": {"aws": [(1, "eu-central-1")]},
                },
                marks=[tier2],
            ),
            pytest.param(
                {"interface": "OC", "backingstore_dict": {"azure": [(1, None)]}},
                marks=[tier2],
            ),
            pytest.param(
                {"interface": "OC", "backingstore_dict": {"gcp": [(1, None)]}},
                marks=[tier2],
            ),
            pytest.param(
                {"interface": "CLI", "backingstore_dict": {"ibmcos": [(1, None)]}},
                marks=[tier2, skipif_fips_enabled],
            ),
        ],
        ids=[
            "DEFAULT-BACKINGSTORE",
            "AWS-OC-1",
            "AZURE-OC-1",
            "GCP-OC-1",
            "IBMCOS-CLI-1",
        ],
    )
    def test_mcg_data_deduplication(
        self, mcg_obj, awscli_pod_session, bucket_factory, bucketclass_dict
    ):
        """
        Test data deduplication mechanics
        Args:
            mcg_obj (obj): An object representing the current state of the MCG in the cluster
            awscli_pod_session (pod): A pod running the AWSCLI tools
            bucket_factory: Calling this fixture creates a new bucket(s)
        """

        download_dir = AWSCLI_TEST_OBJ_DIR
        file_size = int(
            awscli_pod_session.exec_cmd_on_pod(
                command=f"stat -c %s {download_dir}danny.webm", out_yaml_format=False
            )
        )
        bucketname = bucket_factory(1, bucketclass=bucketclass_dict)[0].name
        for i in range(3):
            awscli_pod_session.exec_cmd_on_pod(
                command=craft_s3_command(
                    f"cp {download_dir}danny.webm s3://{bucketname}/danny{i}.webm",
                    mcg_obj=mcg_obj,
                ),
                out_yaml_format=False,
            )
        mcg_obj.check_data_reduction(bucketname, 2 * file_size)

    @pytest.mark.polarion_id("OCS-1949")
    @pytest.mark.parametrize(
        argnames="bucketclass_dict",
        argvalues=[
            pytest.param(
                None,
                marks=[tier1, skipif_fips_enabled_on_ibm_cloud],
            ),
            pytest.param(
                {
                    "interface": "OC",
                    "backingstore_dict": {"aws": [(1, "eu-central-1")]},
                },
                marks=[tier1],
            ),
            pytest.param(
                {"interface": "OC", "backingstore_dict": {"azure": [(1, None)]}},
                marks=[tier1],
            ),
            pytest.param(
                {"interface": "OC", "backingstore_dict": {"gcp": [(1, None)]}},
                marks=[tier2],
            ),
            pytest.param(
                {"interface": "OC", "backingstore_dict": {"ibmcos": [(1, None)]}},
                marks=[tier2, skipif_fips_enabled],
            ),
        ],
        ids=[
            "DEFAULT-BACKINGSTORE",
            "AWS-OC-1",
            "AZURE-OC-1",
            "GCP-OC-1",
            "IBMCOS-OC-1",
        ],
    )
    def test_mcg_data_compression(
        self, mcg_obj, awscli_pod_session, bucket_factory, bucketclass_dict
    ):
        """
        Test data reduction mechanics
        Args:
            mcg_obj (obj): An object representing the current state of the MCG in the cluster
            awscli_pod_session (pod): A pod running the AWSCLI tools
            bucket_factory: Calling this fixture creates a new bucket(s)
        """

        download_dir = AWSCLI_TEST_OBJ_DIR
        bucketname = bucket_factory(1, bucketclass=bucketclass_dict)[0].name
        full_object_path = f"s3://{bucketname}"
        awscli_pod_session.exec_cmd_on_pod(
            command=craft_s3_command(
                f"cp {download_dir}enwik8 {full_object_path}", mcg_obj
            ),
            out_yaml_format=False,
        )
        # For this test, enwik8 is used in conjunction with Snappy compression
        # utilized by NooBaa. Snappy consistently compresses 35MB of the file.
        mcg_obj.check_data_reduction(bucketname, 35 * 1024 * 1024)

    @pytest.mark.polarion_id("OCS-1949")
    @tier2
    @performance
    @skip_inconsistent
    def deprecated_test_data_reduction_performance(
        self, mcg_obj, awscli_pod, bucket_factory
    ):
        """
        Test data reduction performance
        """
        # TODO: Privatize test bucket
        download_dir = "/aws/downloaded"
        retrieve_test_objects_to_pod(awscli_pod, download_dir)
        bucket = bucket_factory(1)[0]
        bucketname = bucket.name
        full_object_path = f"s3://{bucketname}"
        sync_object_directory(awscli_pod, download_dir, full_object_path, mcg_obj)

        assert mcg_obj.check_data_reduction(
            bucketname, 100 * 1024 * 1024
        ), "Data reduction did not work as anticipated."

    @pytest.mark.polarion_id("OCS-1945")
    @tier2
    def test_write_empty_file_to_bucket(
        self, mcg_obj, awscli_pod_session, bucket_factory, test_directory_setup
    ):
        """
        Test write empty files to bucket
        """
        data_dir = test_directory_setup.origin_dir
        bucketname = bucket_factory(1)[0].name
        full_object_path = f"s3://{bucketname}"

        # Touch create 1000 empty files in pod
        command = f"for i in $(seq 1 100); do touch {data_dir}/test$i; done"
        awscli_pod_session.exec_sh_cmd_on_pod(command=command, sh="sh")
        # Write all empty objects to the new bucket
        sync_object_directory(awscli_pod_session, data_dir, full_object_path, mcg_obj)

        obj_set = set(
            obj.key for obj in mcg_obj.s3_list_all_objects_in_bucket(bucketname)
        )
        test_set = set("test" + str(i + 1) for i in range(100))
        assert test_set == obj_set, "File name set does not match"

    @pytest.fixture()
    def setup_rbd_cephfs_pods(self, multi_pvc_factory, pod_factory):
        """
        This fixture setups the required rbd and cephfs pvcs and pods

        """
        pvc_objs_rbd = multi_pvc_factory(
            interface=constants.CEPHBLOCKPOOL, size=2, num_of_pvc=5
        )
        ns = pvc_objs_rbd[0].project

        pvc_objs_cephfs = multi_pvc_factory(
            interface=constants.CEPHFILESYSTEM, size=2, num_of_pvc=5, project=ns
        )

        pods = []
        for pvc in pvc_objs_rbd:
            pods.append(pod_factory(interface=constants.CEPHBLOCKPOOL, pvc=pvc))

        for pvc in pvc_objs_cephfs:
            pods.append(pod_factory(interface=constants.CEPHFILESYSTEM, pvc=pvc))

        return pods

    @vsphere_platform_required
    @tier2
    @pytest.mark.polarion_id("OCS-2040")
    def test_write_to_bucket_rbd_cephfs(
        self,
        verify_rgw_restart_count,
        setup_rbd_cephfs_pods,
        mcg_obj,
        awscli_pod_session,
        bucket_factory,
    ):
        """
        Test RGW restarts after running s3, rbd and cephfs IOs in parallel

        """
        bucketname = bucket_factory(1)[0].name
        full_object_path = f"s3://{bucketname}"
        target_dir = AWSCLI_TEST_OBJ_DIR
        with ThreadPoolExecutor() as p:
            p.submit(
                config_safe_thread_pool_task,
                config.default_cluster_index,
                pod_io,
                setup_rbd_cephfs_pods,
            )
            p.submit(
                config_safe_thread_pool_task,
                config.default_cluster_index,
                sync_object_directory(
                    awscli_pod_session, target_dir, full_object_path, mcg_obj
                ),
            )

    @tier2
    @skipif_ocs_version("<4.10")
    @pytest.mark.polarion_id("OCS-4000")
    def test_content_encoding_with_write(
        self, file_setup, bucket_factory, mcg_obj_session
    ):
        """
        Test s3 put object operation to see if the content-encoding is stored as object
        metadata after put
        """
        # create bucket
        bucket_name = bucket_factory()[0].name
        logger.info(f"Bucket created {bucket_name}")

        # create a random file and then zip it
        filename = file_setup
        logger.info(f"Random zip file generated : {filename}")

        # put object to the bucket created
        s3_put_object(
            s3_obj=mcg_obj_session,
            bucketname=bucket_name,
            object_key=f"{filename}",
            data=f"{filename}",
            content_encoding="zip",
        )

        # head object to see if the content-encoding is preserved
        head_obj = s3_head_object(
            s3_obj=mcg_obj_session, bucketname=bucket_name, object_key=f"{filename}"
        )
        assert (
            head_obj["ContentEncoding"] == "zip"
        ), "Put object operation doesn't store ContentEncoding!!"
        logger.info(
            "Put object operation is preserving ContentEncoding as a object metadata"
        )

    @tier2
    @pytest.mark.polarion_id("OCS-5773")
    def test_nb_db_activity_logs_on_io(
        self,
        bucket_factory,
        awscli_pod_session,
        mcg_obj,
        change_the_noobaa_log_level,
        test_directory_setup,
    ):
        """
        This test checks if the activity logs are being logged
        in the activitylogs table for every object upload and
        deletion when noobaa log is set to default. As no activity
        logs are expected for creation/deletion at defualt log level.

        """
        logger.info("Making sure noobaa log is at default_level...")
        change_the_noobaa_log_level(level="default_level")

        bucket = bucket_factory()[0]
        logger.info("successfully created bucket")

        obj_uploaded = write_random_test_objects_to_bucket(
            awscli_pod_session,
            bucket.name,
            test_directory_setup.origin_dir,
            amount=1,
            mcg_obj=mcg_obj,
        )[0]
        logger.info(f"uploaded object {obj_uploaded} to the bucket")

        rm_object_recursive(awscli_pod_session, bucket.name, mcg_obj)
        logger.info("deleted all the objects from the bucket")

        tries = 0
        while tries <= 10:
            logger.info("Checking the logs for 10 minutes if any ")
            nb_activitylogs = [
                line
                for line in exec_nb_db_query("SELECT data FROM activitylogs;")
                if obj_uploaded in line
            ]
            logger.info("successfully fetched noobaa db activitylogs data")

            assert "obj.uploaded" not in str(
                nb_activitylogs
            ), "Object upload event is being logged in activitylogs table"
            assert "obj.deleted" not in str(
                nb_activitylogs
            ), "Object deletion event is being logged in activitylogs table"
            logger.info(
                "No object upload/deletion info is being updated in the activitylogs table"
            )
            time.sleep(60)
            tries += 1
