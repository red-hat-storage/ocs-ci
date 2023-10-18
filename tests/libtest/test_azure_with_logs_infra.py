from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import libtest
from ocs_ci.ocs.resources.mcg_replication_policy import AzureLogBasedReplicationPolicy
from ocs_ci.ocs.bucket_utils import bucket_read_api


@libtest
def test_azure_logs_based_repli_setup(bucket_factory, mcg_obj_session):
    target_bucket = bucket_factory()[0].name
    bucketclass_dict = {
        "interface": "OC",
        "namespace_policy_dict": {
            "type": "Single",
            "namespacestore_dict": {constants.AZURE_WITH_LOGS_PLATFORM: [(1, None)]},
        },
    }
    replication_policy = AzureLogBasedReplicationPolicy(
        destination_bucket=target_bucket,
        sync_deletions=True,
    )
    source_bucket = bucket_factory(
        bucketclass=bucketclass_dict, replication_policy=replication_policy
    )[0].name

    response = bucket_read_api(mcg_obj_session, source_bucket)
    assert "replication_policy_id" in response
