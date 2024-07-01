import base64
import logging

import pytest

from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs import constants
from ocs_ci.ocs.bucket_utils import (
    verify_s3_object_integrity,
    write_random_objects_in_pod,
)
from ocs_ci.framework.pytest_customization.marks import (
    skipif_mcg_only,
    bugzilla,
    skipif_ocs_version,
    red_squad,
    runs_on_provider,
    tier1,
    rgw,
)

logger = logging.getLogger(__name__)


@tier1
@rgw
@runs_on_provider
@skipif_mcg_only
@skipif_ocs_version("<4.16")
@bugzilla("2283643")
@red_squad
class TestRgwVirtualHostedOps:
    def prepare_cmd_and_data(
        self, bucket_name, multipart, awscli_pod_session, test_directory_setup
    ):
        """
        Prepare s3cmd command and data for upload based on multipart value

        Args:
            bucket_name (str): Bucket name
            multipart (bool): True or False
            awscli_pod_session : Fixture to create a new AWSCLI pod.
            test_directory_setup : Fixture to setup test DIRs.

        Returns:
            str : Prepared command

        """
        host_base = "rgw.data.local"
        host_bucket = f"%(bucket)s.{host_base}"

        rgw_user_creds = OCP(
            kind="secret",
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
            resource_name=f"{bucket_name}",
        ).get()
        access_key = base64.b64decode(
            rgw_user_creds["data"]["AWS_ACCESS_KEY_ID"]
        ).decode("utf-8")
        secret_key = base64.b64decode(
            rgw_user_creds["data"]["AWS_SECRET_ACCESS_KEY"]
        ).decode("utf-8")
        config_cmd = (
            f"--access_key={access_key} "
            f"--secret_key={secret_key} "
            f"--host={host_base} --host-bucket='{host_bucket}' --no-ssl"
        )
        if not multipart:
            write_random_objects_in_pod(
                awscli_pod_session, test_directory_setup.origin_dir, amount=1
            )
        else:
            write_random_objects_in_pod(
                awscli_pod_session, test_directory_setup.origin_dir, amount=1, bs="50M"
            )
            config_cmd = config_cmd + " multipart_chunk_size_mb = 5"
        return config_cmd

    @pytest.mark.parametrize(
        argnames="multipart",
        argvalues=[pytest.param(True), pytest.param(False)],
        ids=[
            "multipart-obj",
            "non-multipart-obj",
        ],
    )
    def test_host_style_obj_upload(
        self,
        awscli_pod_session,
        rgw_bucket_factory,
        test_directory_setup,
        multipart,
    ):
        """
        Test s3 operations on virtual style hosted buckets

        """
        logger.info("Creating obc")
        bucket_name = rgw_bucket_factory(amount=1, interface="RGW-OC")[0].name
        config_cmd = self.prepare_cmd_and_data(
            bucket_name, multipart, awscli_pod_session, test_directory_setup
        )
        logger.info("Uploading object to the bucket")
        awscli_pod_session.exec_sh_cmd_on_pod(
            command=f"s3cmd {config_cmd} put {test_directory_setup.origin_dir}/ObjKey-0 s3://{bucket_name}"
        )
        logger.info("Listing the objects in the bucket")
        op = awscli_pod_session.exec_sh_cmd_on_pod(
            command=f"s3cmd {config_cmd} ls s3://{bucket_name}"
        )
        logger.info(op)

    @pytest.mark.parametrize(
        argnames="multipart",
        argvalues=[pytest.param(True), pytest.param(False)],
        ids=[
            "multipart-obj",
            "non-multipart-obj",
        ],
    )
    def test_host_style_obj_download(
        self, awscli_pod_session, rgw_bucket_factory, test_directory_setup, multipart
    ):
        """
        Test download operations on virtual style hosted buckets

        """
        logger.info("Creating obc")
        bucket_name = rgw_bucket_factory(amount=1, interface="RGW-OC")[0].name
        config_cmd = self.prepare_cmd_and_data(
            bucket_name, multipart, awscli_pod_session, test_directory_setup
        )
        logger.info("Uploading object to the bucket")
        awscli_pod_session.exec_sh_cmd_on_pod(
            command=f"s3cmd {config_cmd} put {test_directory_setup.origin_dir}/ObjKey-0 s3://{bucket_name}"
        )
        logger.info("Downloading object from the bucket")
        awscli_pod_session.exec_sh_cmd_on_pod(
            command=f"s3cmd {config_cmd} get s3://{bucket_name}/ObjKey-0 {test_directory_setup.result_dir}"
        )
        verify_s3_object_integrity(
            original_object_path=f"{test_directory_setup.origin_dir}/ObjKey-0",
            result_object_path=f"{test_directory_setup.result_dir}/ObjKey-0",
            awscli_pod=awscli_pod_session,
        )

    @pytest.mark.parametrize(
        argnames="multipart",
        argvalues=[pytest.param(True), pytest.param(False)],
        ids=[
            "multipart-obj",
            "non-multipart-obj",
        ],
    )
    def test_host_style_obj_delete(
        self, awscli_pod_session, rgw_bucket_factory, test_directory_setup, multipart
    ):
        """
        Test delete operations on virtual style hosted buckets

        """
        logger.info("Creating obc")
        bucket_name = rgw_bucket_factory(amount=1, interface="RGW-OC")[0].name
        config_cmd = self.prepare_cmd_and_data(
            bucket_name, multipart, awscli_pod_session, test_directory_setup
        )
        logger.info("Uploading object to the bucket")
        awscli_pod_session.exec_sh_cmd_on_pod(
            command=f"s3cmd {config_cmd} put {test_directory_setup.origin_dir}/ObjKey-0 s3://{bucket_name}"
        )
        logger.info("Delete object from the bucket")
        awscli_pod_session.exec_sh_cmd_on_pod(
            command=f"s3cmd {config_cmd} del s3://{bucket_name}/ObjKey-0"
        )
