import logging
import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    tier1,
    red_squad,
    run_on_all_clients_push_missing_configs,
    runs_on_provider,
    mcg,
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


@mcg
@red_squad
@runs_on_provider
@provider_client_ms_platform_required
@tier1
@polarion_id("OCS-5415")
def test_verify_backingstore_uses_rgw(mcg_obj_session):
    """
    Validates whether default MCG backingstore uses rgw endpoint
    """
    ceph_object_store = OCP(
        kind=constants.CEPHOBJECTSTORE,
        resource_name="ocs-storagecluster-cephobjectstore",
    ).get()
    log.debug(f"Ceph object store: {ceph_object_store}")
    rgw_endpoint = ceph_object_store["status"]["endpoints"]["secure"][0]
    log.info(
        f"Checking if backingstore noobaa-default-backing-store uses endpoint {rgw_endpoint}"
    )

    # Get default backingstore status
    backingstore_data = mcg_obj_session.exec_mcg_cmd(
        "backingstore status noobaa-default-backing-store"
    ).stdout
    assert f"endpoint: {rgw_endpoint}" in backingstore_data


@mcg
@red_squad
@tier1
@provider_client_ms_platform_required
@run_on_all_clients_push_missing_configs
@pytest.mark.polarion_id("OCS-5214")
def test_write_file_to_bucket_on_client(
    bucket_factory,
    mcg_obj,
    awscli_pod_client_session,
    return_to_original_context,
    cluster_index,
):
    """
    Test object IO using the S3 SDK on bucket created on provider and used on client.
    """
    awscli_pod, client_cluster = awscli_pod_client_session
    # Retrieve a list of all objects on the test-objects bucket and
    # downloads them to the pod
    with config.RunWithProviderConfigContextIfAvailable():
        bucketname = bucket_factory(1, interface="OC")[0].name
    full_object_path = f"s3://{bucketname}"

    downloaded_files = awscli_pod.exec_cmd_on_pod(
        f"ls -A1 {constants.AWSCLI_TEST_OBJ_DIR}"
    ).split(" ")
    # create s3_creds structure with s3_endpoint so that s3_internal_endpoint is not used
    # TODO(fbalak): remove ssl=False option and provide correct certificate
    s3_creds = {
        "access_key_id": mcg_obj.access_key_id,
        "access_key": mcg_obj.access_key,
        "endpoint": mcg_obj.s3_endpoint,
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
        obj.key for obj in mcg_obj.s3_list_all_objects_in_bucket(bucketname)
    )
