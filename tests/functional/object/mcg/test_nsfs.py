import logging
import types
import pytest

from ocs_ci.framework.testlib import MCGTest, tier1, tier3
from ocs_ci.framework.pytest_customization.marks import (
    skipif_disconnected_cluster,
    skipif_mcg_only,
    skipif_ocs_version,
    red_squad,
    runs_on_provider,
    mcg,
    jira,
    skipif_proxy_cluster,
    tier2,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.bucket_utils import (
    list_objects_from_bucket,
    random_object_round_trip_verification,
    s3_copy_object,
    s3_head_object,
    s3_list_buckets,
    s3_put_object,
    write_random_test_objects_to_bucket,
)
from ocs_ci.ocs.exceptions import CommandFailed, UnexpectedBehaviour


from ocs_ci.ocs.resources.mcg_params import NSFS
from ocs_ci.utility.retry import retry
from tests.conftest import revert_noobaa_endpoint_scc_class

logger = logging.getLogger(__name__)


@mcg
@red_squad
@runs_on_provider
@skipif_mcg_only
@skipif_ocs_version("<4.10")
@pytest.mark.usefixtures(revert_noobaa_endpoint_scc_class.__name__)
@jira("DFBUGS-153")
class TestNSFSObjectIntegrity(MCGTest):
    """
    Test the integrity of IO operations on NSFS buckets

    """

    @pytest.mark.polarion_id("OCS-3735")
    @pytest.mark.parametrize(
        argnames="nsfs_obj",
        argvalues=[
            pytest.param(
                NSFS(
                    method="CLI",
                    pvc_size=25,
                ),
                marks=[tier1],
            ),
            pytest.param(
                NSFS(
                    method="OC",
                    pvc_size=20,
                    mount_existing_dir=True,
                ),
                marks=[tier1],
            ),
        ],
        ids=[
            "CLI-25Gi",
            "OC-20Gi-Export",
        ],
    )
    def test_nsfs_object_integrity(
        self, nsfs_bucket_factory, awscli_pod_session, test_directory_setup, nsfs_obj
    ):
        """
        Test NSFS object integrity -
        1. Write to the NSFS bucket
        2. Read the objects back
        3. Compare their checksums
        4. Also compare the checksums of the files that reside on the filesystem

        """
        nsfs_bucket_factory(nsfs_obj)
        retry(CommandFailed, tries=4, delay=10)(random_object_round_trip_verification)(
            io_pod=awscli_pod_session,
            bucket_name=nsfs_obj.bucket_name,
            upload_dir=test_directory_setup.origin_dir,
            download_dir=test_directory_setup.result_dir,
            amount=10,
            pattern="nsfs-test-obj-",
            s3_creds=nsfs_obj.s3_creds,
            result_pod=nsfs_obj.interface_pod,
            result_pod_path=nsfs_obj.mounted_bucket_path,
        )

    @pytest.mark.polarion_id("OCS-3737")
    @pytest.mark.parametrize(
        argnames="nsfs_obj",
        argvalues=[
            pytest.param(
                NSFS(
                    method="CLI",
                    pvc_size=20,
                    mount_existing_dir=True,
                    existing_dir_mode=000,
                ),
                marks=[tier3],
            ),
        ],
        ids=[
            "CLI-20Gi",
        ],
    )
    def test_nsfs_object_integrity_with_wrong_permissions(
        self, nsfs_bucket_factory, awscli_pod_session, test_directory_setup, nsfs_obj
    ):
        """
        Test NSFS object integrity -
        1. Create an NSFS bucket on top of an existing directory with wrong permissions
        2. Verify that writing fails

        """
        nsfs_bucket_factory(nsfs_obj)
        try:
            retry(CommandFailed, tries=4, delay=10)(
                random_object_round_trip_verification
            )(
                io_pod=awscli_pod_session,
                bucket_name=nsfs_obj.bucket_name,
                upload_dir=test_directory_setup.origin_dir,
                download_dir=test_directory_setup.result_dir,
                amount=10,
                pattern="nsfs-test-obj-",
                s3_creds=nsfs_obj.s3_creds,
                result_pod=nsfs_obj.interface_pod,
                result_pod_path=nsfs_obj.mounted_bucket_path,
            )
        except Exception as e:
            assert "AccessDenied" in str(
                e
            ), f"Test failed unexpectedly; Exception data: {str(e)}"

    @skipif_disconnected_cluster  # Test requires DNF
    @skipif_proxy_cluster  # Test requires DNF
    @pytest.mark.polarion_id("OCS-4513")
    @pytest.mark.polarion_id("OCS-4512")
    @tier2
    def test_nsfs_metadata(
        self,
        nsfs_bucket_factory,
    ):
        """
        Test NSFS metadata handling:

        1. Create an NSFS bucket
        2. Using S3, write an object with some metadata
        3. Using the FS interface, verify that the metadata shows in the file's extended attributes
        4. Set a new extended attribute via the FS interface
        5. Using S3, verify that the new extended attribute shows in the object's metadata
        """
        # 1. Create an NSFS bucket
        nsfs_obj = NSFS(
            method="CLI",
            pvc_size=20,
        )
        nsfs_bucket_factory(nsfs_obj)

        # 2. Using S3, write an object with some metadata
        obj_key = "test-obj"
        s3_md_key, s3_md_val = "s3md", "s3md-val"

        # Existing implementation of s3_copy_object requires a source object

        # First write to the bucket might fail due to AccessDenied
        # because allow policy is still being processed
        s3_put_object(
            s3_obj=nsfs_obj,
            bucketname=nsfs_obj.bucket_name,
            object_key=f"{obj_key}-source",
            data="test-data",
        )
        s3_copy_object(
            s3_obj=nsfs_obj,
            bucketname=nsfs_obj.bucket_name,
            source=f"/{nsfs_obj.bucket_name}/{obj_key}-source",
            object_key=obj_key,
            Metadata={s3_md_key: s3_md_val},
            MetadataDirective="REPLACE",  # Otherwise the original empty md is kept
        )

        # 3. Using the FS interface, verify that the metadata shows in the file's extended attributes
        fs_interface_pod = nsfs_obj.interface_pod
        # Use DNF to install the attr package
        fs_interface_pod.exec_cmd_on_pod("dnf install -y attr", out_yaml_format=False)
        # Verify that the metadata shows in the file's extended attributes
        try:
            response = fs_interface_pod.exec_cmd_on_pod(
                f"attr -g {s3_md_key} {nsfs_obj.mounted_bucket_path}/{obj_key}",
                out_yaml_format=False,
            )
        except CommandFailed as e:
            if "No such file or directory" in str(e):
                raise UnexpectedBehaviour("File matching the object key does not exist")
            elif "No data available" in str(e):
                raise UnexpectedBehaviour(
                    "Metadata {s3_md_key} not found in file's extended attributes"
                )
            else:
                raise e
        assert (
            s3_md_val in response
        ), "Metadata added via S3 not found in file's extended attributes"

        # 4. Set a new extended attribute via the FS interface
        fs_md_key, fs_md_val = "fsmd", "fsmd-val"
        fs_interface_pod.exec_cmd_on_pod(
            f"attr -s {fs_md_key} -V {fs_md_val} {nsfs_obj.mounted_bucket_path}/{obj_key}",
            out_yaml_format=False,
        )
        # 5. Using S3, verify that the new extended attribute shows in the object's metadata
        response = s3_head_object(
            s3_obj=nsfs_obj,
            bucketname=nsfs_obj.bucket_name,
            object_key=obj_key,
        )
        assert (
            response["Metadata"][fs_md_key] == fs_md_val
        ), "Extended attribute added via FS not found in object's metadata"

    @tier1
    @pytest.mark.polarion_id("OCS-4511")
    def test_nsfs_list_objects(
        self, nsfs_bucket_factory, awscli_pod_session, test_directory_setup
    ):
        """
        Test NSFS object listing:

        1. Create an NSFS bucket
        2. Upload some objects
        3. List the objects and verify that the original objects are listed
        """
        # 1. Create an NSFS bucket
        nsfs_obj = NSFS(
            method="CLI",
            pvc_size=20,
        )
        nsfs_bucket_factory(nsfs_obj)

        # 2. Upload some objects

        # Convert nsfs_obj.s3_creds to an S3 object with the expected attributes
        nsfs_s3_obj = types.SimpleNamespace(
            access_key_id=nsfs_obj.s3_creds["access_key_id"],
            access_key=nsfs_obj.s3_creds["access_key"],
            s3_internal_endpoint=nsfs_obj.s3_creds["endpoint"],
            region=constants.DEFAULT_AWS_REGION,  # any region will do, we don't use it
            ssl=nsfs_obj.s3_creds["ssl"],
        )
        objs_amount = 200
        uploaded_objs = write_random_test_objects_to_bucket(
            io_pod=awscli_pod_session,
            bucket_to_write=nsfs_obj.bucket_name,
            file_dir=test_directory_setup.origin_dir,
            amount=objs_amount,
            pattern="nsfs-test-obj-",
            mcg_obj=nsfs_s3_obj,
        )

        # 3. List the objects and verify that the original objects are listed
        listed_objs = list_objects_from_bucket(
            pod_obj=awscli_pod_session,
            target=f"s3://{nsfs_obj.bucket_name}",
            s3_obj=nsfs_s3_obj,
        )
        assert set(uploaded_objs).issubset(listed_objs), "Objects are not listed"

    @pytest.mark.polarion_id("OCS-4510")
    @pytest.mark.polarion_id("OCS-4509")
    @tier2
    def test_nsfs_list_buckets(
        self,
        nsfs_bucket_factory,
    ):
        """
        Test NSFS bucket listing:

        1. Create two NSFS accounts - one with NSFS_ONLY=True and one with NSFS_ONLY=False
        2. Create two NSFS buckets - one from each account
        3. Verify that the NSFS_ONLY=True account can only see the NSFS buckets
        4. Verify that the NSFS_ONLY=False account can also see first.bucket
        """
        # 1. Create two NSFS accounts - one with NSFS_ONLY=True and one with NSFS_ONLY=False
        # 2. Create two NSFS buckets - one from each account
        nsfs_obj_1 = NSFS(
            method="CLI",
            pvc_size=20,
            nsfs_only=True,
        )
        nsfs_obj_2 = NSFS(
            method="CLI",
            pvc_size=20,
            nsfs_only=False,
        )
        nsfs_bucket_factory(nsfs_obj_1)
        nsfs_bucket_factory(nsfs_obj_2)

        nsfs_buckets = {nsfs_obj_1.bucket_name, nsfs_obj_2.bucket_name}
        non_nsfs_bucket = "first.bucket"

        # 3. Verify that the NSFS_ONLY=True account can only see the NSFS buckets
        nsfs_only_acc_list = set(
            s3_list_buckets(
                s3_obj=nsfs_obj_1,
            )
        )
        assert (
            nsfs_buckets.issubset(nsfs_only_acc_list)
            and non_nsfs_bucket not in nsfs_only_acc_list
        ), "NSFS_ONLY=True account can see non-NSFS buckets"

        # 4. Verify that the NSFS_ONLY=False account can also see first.bucket
        nsfs_only_acc_list = set(
            s3_list_buckets(
                s3_obj=nsfs_obj_2,
            )
        )
        assert nsfs_buckets.union({non_nsfs_bucket}).issubset(
            nsfs_only_acc_list
        ), "NSFS_ONLY=False account can't see some of expected buckets"
