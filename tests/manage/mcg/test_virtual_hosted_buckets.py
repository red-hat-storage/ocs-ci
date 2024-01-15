import logging

import pytest

from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs import constants
from ocs_ci.ocs.bucket_utils import (
    verify_s3_object_integrity,
    write_random_objects_in_pod,
)
from ocs_ci.framework.pytest_customization.marks import (
    bugzilla,
    polarion_id,
    red_squad,
    tier2,
    mcg,
)

logger = logging.getLogger(__name__)


@tier2
@mcg
@bugzilla("2183092")
@polarion_id("OCS-5217")
@red_squad
class TestVirtualHostedBuckets:
    @pytest.fixture()
    def s3cmd_setup(self, awscli_pod_session):
        """
        Setup s3cmd tool in s3cli pod

        """
        awscli_pod_session.exec_sh_cmd_on_pod(command="apk add s3cmd")
        return awscli_pod_session

    def test_virtual_hosted_bucket(
        self,
        s3cmd_setup,
        bucket_factory,
        mcg_obj_session,
        teardown_factory,
        test_directory_setup,
    ):
        """
        Test s3 operations on virtual style hosted buckets

        """
        logger.info("Creating obc")
        bucket_name = bucket_factory()[0].name

        # create a route for the bucket create above
        s3_route_data = OCP(
            kind="route",
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
            resource_name="s3",
        ).get()
        host_base = f'{s3_route_data["spec"]["host"]}'
        host_bucket = f'%(bucket)s.{s3_route_data["spec"]["host"]}'

        s3_route_data["metadata"]["name"] = f"s3-{bucket_name}"
        s3_route_data["spec"]["host"] = f'{bucket_name}.{s3_route_data["spec"]["host"]}'
        new_route_obj = OCS(**s3_route_data)
        logger.info(f"Creating new route for bucket {bucket_name}")
        new_route_obj.create()
        teardown_factory(new_route_obj)

        # upload and download object and verify object integrity
        config_cmd = (
            f"--access_key={mcg_obj_session.access_key_id} "
            f"--secret_key={mcg_obj_session.access_key} "
            f"--host={host_base} --host-bucket='{host_bucket}' --no-ssl"
        )

        write_random_objects_in_pod(
            s3cmd_setup, test_directory_setup.origin_dir, amount=1
        )
        logger.info("Uploading object to the bucket")
        s3cmd_setup.exec_sh_cmd_on_pod(
            command=f"s3cmd {config_cmd} put {test_directory_setup.origin_dir}/ObjKey-0 s3://{bucket_name}"
        )
        logger.info("Listing the objects in the bucket")
        s3cmd_setup.exec_sh_cmd_on_pod(
            command=f"s3cmd {config_cmd} ls s3://{bucket_name}"
        )

        logger.info("Downloading object from the bucket")
        s3cmd_setup.exec_sh_cmd_on_pod(
            command=f"s3cmd {config_cmd} get s3://{bucket_name}/ObjKey-0 {test_directory_setup.result_dir}"
        )
        verify_s3_object_integrity(
            original_object_path=f"{test_directory_setup.origin_dir}/ObjKey-0",
            result_object_path=f"{test_directory_setup.result_dir}/ObjKey-0",
            awscli_pod=s3cmd_setup,
        )
