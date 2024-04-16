import logging
import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    tier1,
    red_squad,
    rgw,
    runs_on_provider,
    provider_client_ms_platform_required,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.bucket_utils import sync_object_directory
from ocs_ci.ocs.ocp import OCP
from ocs_ci.framework.testlib import polarion_id

log = logging.getLogger(__name__)


@pytest.fixture
def return_to_original_context():
    """
    Make sure that original context is restored after the test.
    """
    original_cluster = config.cluster_ctx.MULTICLUSTER["multicluster_index"]
    return
    log.info(f"Switching back to original cluster with index {original_cluster}")
    config.switch_ctx(original_cluster)


@rgw
@red_squad
@tier1
@runs_on_provider
@provider_client_ms_platform_required
@pytest.mark.polarion_id("OCS-5765")
def test_write_file_to_bucket_on_client(
    rgw_bucket_factory, rgw_obj, awscli_pod_client_session, return_to_original_context
):
    """
    Test object IO using the S3 SDK on rgw bucket created on provider and used on client.
    """
    awscli_pod, client_cluster = awscli_pod_client_session
    # Retrieve a list of all objects on the test-objects bucket and
    # downloads them to the pod
    bucketname = rgw_bucket_factory()[0].name
    full_object_path = f"s3://{bucketname}"

    config.switch_ctx(client_cluster)
    log.info(f"Switched to client cluster with index {client_cluster}")
    downloaded_files = awscli_pod.exec_cmd_on_pod(
        f"ls -A1 {constants.AWSCLI_TEST_OBJ_DIR}"
    ).split(" ")
    # create s3_creds structure with s3_endpoint so that s3_internal_endpoint is not used
    # TODO(fbalak): remove ssl=False option and provide correct certificate
    credentials = rgw_obj.get_credentials()
    s3_creds = {
        "access_key_id": credentials[2],
        "access_key": credentials[1],
        "endpoint": credentials[0],
        "ssl": False,
    }
    # Write all downloaded objects to the new bucket
    sync_object_directory(
        awscli_pod,
        constants.AWSCLI_TEST_OBJ_DIR,
        full_object_path,
        signed_request_creds=s3_creds,
    )

    assert set(downloaded_files).issubset(
        obj.key for obj in rgw_obj.s3_list_all_objects_in_bucket(bucketname)
    )
