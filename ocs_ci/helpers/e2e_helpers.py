import json
import logging
import random
import copy
import re
import time
from datetime import datetime, timedelta

from uuid import uuid4
from ocs_ci.utility.retry import retry
from ocs_ci.ocs.bucket_utils import (
    bulk_s3_put_bucket_lifecycle_config,
    change_versions_creation_date_in_noobaa_db,
    compare_bucket_object_list,
    create_multipart_upload,
    delete_all_objects_in_batches,
    expire_multipart_upload_in_noobaa_db,
    list_objects_from_bucket,
    random_object_round_trip_verification,
    write_random_test_objects_to_bucket,
    wait_for_cache,
    sync_object_directory,
    verify_s3_object_integrity,
    s3_put_object,
    s3_put_bucket_versioning,
    s3_get_bucket_versioning,
    expire_objects_in_bucket,
    sample_if_objects_expired,
    wait_for_object_count_in_bucket,
    get_obj_versions,
    get_replication_policy,
    list_multipart_upload,
    s3_delete_object,
    s3_list_object_versions,
    upload_parts,
)
from ocs_ci.ocs.resources.mcg_lifecycle_policies import (
    ExpirationRule,
    LifecyclePolicy,
)
from ocs_ci.helpers.helpers import create_unique_resource_name
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pod import (
    get_noobaa_pods,
    get_rgw_pods,
    get_pod_logs,
    wait_for_noobaa_pods_running,
)
from ocs_ci.ocs import node as nodes
from ocs_ci.ocs.node import get_node_objs, wait_for_nodes_status
from ocs_ci.ocs.exceptions import ResourceNotFoundError
from ocs_ci.ocs.ocp import OCP, get_all_resource_of_kind_containing_string
from ocs_ci.framework import config
from ocs_ci.utility.utils import (
    exec_cmd,
    get_primary_nb_db_pod,
    get_secondary_nb_db_pod,
    run_cmd,
    TimeoutSampler,
)
from ocs_ci.ocs.cluster import (
    get_percent_used_capacity,
    get_osd_utilization,
    get_ceph_df_detail,
)

logger = logging.getLogger(__name__)


def create_muliple_types_provider_obcs(
    num_of_buckets, bucket_types, cloud_providers, bucket_factory
):
    """
    This function creates valid OBCs of different cloud providers
    and bucket types

    Args:
        num_of_buckets (int): Number of buckets
        bucket_types (dict): Dict representing mapping between
            bucket type and relevant configuration
        cloud_providers (dict): Dict representing mapping between
            cloud providers and relevant configuration
        bucket_factory (fixture): bucket_factory fixture method

    Returns:
        List: list of created buckets

    """

    def get_all_combinations_map(providers, bucket_types):
        """
        Create valid combination of cloud-providers and bucket-types

        Args:
            providers (dict): dictionary representing cloud
                providers and the respective config
            bucket_types (dict): dictionary representing different
                types of bucket and the respective config
        Returns:
            List: containing all the possible combination of buckets

        """
        all_combinations = dict()

        for provider, provider_config in providers.items():
            for bucket_type, type_config in bucket_types.items():
                if provider == "pv" and bucket_type != "data":
                    available_providers = [
                        key for key in cloud_providers.keys() if key != "pv"
                    ]
                    if available_providers:
                        provider = random.choice(available_providers)
                    else:
                        # If 'pv' is the only available provider, choose between 'aws' and 'azure'
                        provider = random.choice(["aws", "azure"])
                    provider_config = providers[provider]
                bucketclass = copy.deepcopy(type_config)

                if "backingstore_dict" in bucketclass.keys():
                    bucketclass["backingstore_dict"][provider] = [provider_config]
                elif "namespace_policy_dict" in bucketclass.keys():
                    bucketclass["namespace_policy_dict"]["namespacestore_dict"][
                        provider
                    ] = [provider_config]
                all_combinations.update({f"{bucket_type}-{provider}": bucketclass})
        return all_combinations

    all_combination_of_obcs = get_all_combinations_map(cloud_providers, bucket_types)
    buckets = list()
    num_of_buckets_each = num_of_buckets // len(all_combination_of_obcs.keys())
    buckets_left = num_of_buckets % len(all_combination_of_obcs.keys())
    if num_of_buckets_each != 0:
        for combo, combo_config in all_combination_of_obcs.items():
            buckets.extend(
                bucket_factory(
                    interface="OC",
                    amount=num_of_buckets_each,
                    bucketclass=combo_config,
                )
            )

    for index in range(0, buckets_left):
        buckets.extend(
            bucket_factory(
                interface="OC",
                amount=1,
                bucketclass=all_combination_of_obcs[
                    list(all_combination_of_obcs.keys())[index]
                ],
            )
        )

    return buckets


def setup_mcg_feature_verification_buckets(
    bucket_factory,
    mcg_obj,
    reduce_expiration_interval,
    source_bucketclass=None,
    target_bucketclass=None,
    expiration_bucketclass=None,
    versioning_bucketclass=None,
    replication_prefix="",
    expiration_interval_minutes=1,
):
    """
    Step-1 setup for MCG feature verification tests.

    Creates:
    - Two namespace buckets with uni-directional replication (no deletion sync)
    - One data bucket with an object expiration lifecycle policy
    - One data bucket with S3 versioning enabled

    Uni-directional replication is enabled on the source namespace bucket toward
    the target namespace bucket. Basic replication does not sync deletions.

    Args:
        bucket_factory (fixture): bucket_factory fixture method
        mcg_obj (MCG): MCG object with S3 credentials
        reduce_expiration_interval (callable): fixture to shorten lifecycle interval
        source_bucketclass (dict): bucketclass for the replication source bucket
        target_bucketclass (dict): bucketclass for the replication target bucket
        expiration_bucketclass (dict): bucketclass for the data expiration bucket
        versioning_bucketclass (dict): bucketclass for the versioning data bucket
        replication_prefix (str): optional replication rule prefix filter
        expiration_interval_minutes (int): noobaa lifecycle worker interval in minutes

    Returns:
        dict: {
            "source_bucket": source namespace bucket,
            "target_bucket": target namespace bucket,
            "expiration_bucket": data bucket with expiration policy,
            "versioning_bucket": data bucket with versioning enabled,
        }

    """
    source_bucketclass = source_bucketclass or {
        "interface": "OC",
        "namespace_policy_dict": {
            "type": "Single",
            "namespacestore_dict": {"aws": [(1, "eu-central-1")]},
        },
    }
    target_bucketclass = target_bucketclass or {
        "interface": "OC",
        "namespace_policy_dict": {
            "type": "Single",
            "namespacestore_dict": {"azure": [(1, None)]},
        },
    }
    expiration_bucketclass = expiration_bucketclass or {
        "interface": "OC",
        "backingstore_dict": {"aws": [(1, "eu-central-1")]},
    }
    versioning_bucketclass = versioning_bucketclass or {
        "interface": "OC",
        "backingstore_dict": {"azure": [(1, None)]},
    }

    target_bucket = bucket_factory(bucketclass=target_bucketclass)[0]
    replication_policy = (
        "basic-replication-rule",
        target_bucket.name,
        replication_prefix or None,
    )
    source_bucket = bucket_factory(
        1,
        bucketclass=source_bucketclass,
        replication_policy=replication_policy,
    )[0]

    reduce_expiration_interval(interval=expiration_interval_minutes)
    expiration_bucket = bucket_factory(bucketclass=expiration_bucketclass)[0]
    expiration_rule = LifecyclePolicy(ExpirationRule(days=1))
    bulk_s3_put_bucket_lifecycle_config(
        mcg_obj, [expiration_bucket], expiration_rule.as_dict()
    )

    versioning_bucket = bucket_factory(bucketclass=versioning_bucketclass)[0]
    s3_put_bucket_versioning(mcg_obj, versioning_bucket.name)
    versioning_status = s3_get_bucket_versioning(mcg_obj, versioning_bucket.name)
    assert (
        versioning_status.get("Status") == "Enabled"
    ), f"Versioning not enabled on bucket {versioning_bucket.name}"

    logger.info(
        "Created replication buckets %s -> %s, expiration bucket %s, "
        "versioning bucket %s",
        source_bucket.name,
        target_bucket.name,
        expiration_bucket.name,
        versioning_bucket.name,
    )
    return {
        "source_bucket": source_bucket,
        "target_bucket": target_bucket,
        "expiration_bucket": expiration_bucket,
        "versioning_bucket": versioning_bucket,
    }


def assert_mcg_feature_verification_bucket_setup(buckets, mcg_obj):
    """
    Validate step-2 verification bucket configuration.

    """
    source_bucket = buckets["source_bucket"]
    target_bucket = buckets["target_bucket"]
    expiration_bucket = buckets["expiration_bucket"]
    versioning_bucket = buckets["versioning_bucket"]

    assert source_bucket.replication_policy is not None
    assert (
        source_bucket.replication_policy["rules"][0]["destination_bucket"]
        == target_bucket.name
    )
    assert "sync_deletions" not in source_bucket.replication_policy["rules"][0]
    assert target_bucket.replication_policy is None
    assert expiration_bucket.bucketclass is not None
    assert (
        s3_get_bucket_versioning(mcg_obj, versioning_bucket.name).get("Status")
        == "Enabled"
    )
    logger.info(
        "Verification buckets ready: replication source=%s, target=%s, "
        "expiration=%s, versioning=%s",
        source_bucket.name,
        target_bucket.name,
        expiration_bucket.name,
        versioning_bucket.name,
    )


def trigger_noobaa_db_cluster_recovery(timeout=900):
    """
    Delete the CNPG NooBaa DB cluster CR and wait for NooBaa pods to recover.

    """
    db_cluster_names = get_all_resource_of_kind_containing_string(
        "noobaa-db-pg-cluster", "Cluster"
    )
    if not db_cluster_names:
        raise RuntimeError("CNPG NooBaa DB cluster not found")

    db_cluster_name = db_cluster_names[0]
    cluster_obj = OCP(kind="Cluster", namespace=config.ENV_DATA["cluster_namespace"])
    logger.info("Deleting NooBaa DB cluster %s to trigger recovery", db_cluster_name)
    cluster_obj.delete(resource_name=db_cluster_name, force=True)
    cluster_obj.wait_for_delete(resource_name=db_cluster_name)

    noobaa_pods = get_noobaa_pods()
    pod_obj = OCP(kind=constants.POD, namespace=config.ENV_DATA["cluster_namespace"])
    pod_obj.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        resource_count=len(noobaa_pods),
        selector=constants.NOOBAA_APP_LABEL,
        timeout=timeout,
    )
    logger.info("NooBaa pods are running after DB cluster recovery")


def verify_noobaa_db_config_info(db_param, ocs_storage_obj, noobaa_obj):
    """
    Verify DB backup or recovery config propagated to the NooBaa CR.

    Based on tests/functional/object/mcg/test_noobaa_db_backup_recovery.py.

    """
    ocs_storage_obj.reload_data()
    noobaa_obj.reload_data()
    info_from_ocs_storage = ocs_storage_obj.get("ocs-storagecluster")["spec"][
        "multiCloudGateway"
    ][db_param]
    info_from_noobaa_cr = noobaa_obj.get("noobaa")["spec"]["dbSpec"][db_param]
    assert (
        info_from_ocs_storage == info_from_noobaa_cr
    ), f"Mismatch in {db_param} info between ocs-storagecluster and noobaa CR"
    return info_from_noobaa_cr


def cleanup_noobaa_cli_backup_resources(backup_name):
    """
    Remove on-demand CLI backup and related volume snapshots.

    Based on tests/functional/object/mcg/test_noobaa_db_backup_recovery.py.

    """
    backup_obj = OCP(kind="Backup", namespace=config.ENV_DATA["cluster_namespace"])
    backup_names = get_all_resource_of_kind_containing_string(backup_name, "Backup")
    for bkp_name in backup_names:
        backup_obj.delete(resource_name=bkp_name, force=True)
        backup_obj.wait_for_delete(resource_name=bkp_name)
    logger.info("CLI backup resources removed")

    volumesnapshot_obj = OCP(
        kind="volumesnapshot", namespace=config.ENV_DATA["cluster_namespace"]
    )
    volumesnapshot_names = get_all_resource_of_kind_containing_string(
        backup_name, "volumesnapshot"
    )
    for volumesnapshot_name in volumesnapshot_names:
        volumesnapshot_obj.delete(resource_name=volumesnapshot_name, force=True)
        volumesnapshot_obj.wait_for_delete(resource_name=volumesnapshot_name)
    logger.info("CLI backup volume snapshots removed")


def perform_noobaa_db_backup_recovery_using_cli(
    mcg_obj,
    awscli_pod_session,
    test_directory_setup,
    noobaa_db_recovery_patch,
    buckets_for_health=None,
    buckets_with_local_objects=None,
):
    """
    CNPG NooBaa DB backup and recovery using the NooBaa CLI on-demand backup.

    Mirrors test_noobaa_db_backup_recovery_op_using_cli from:
    tests/functional/object/mcg/test_noobaa_db_backup_recovery.py

    Args:
        mcg_obj (MCG): MCG object with CLI access
        awscli_pod_session (Pod): aws-cli pod
        test_directory_setup: test directory fixture with origin_dir and result_dir
        noobaa_db_recovery_patch (callable): fixture to patch dbRecovery config
        buckets_for_health (list): bucket objects to verify_health after recovery
        buckets_with_local_objects (list): tuples of (bucket_obj, local_dir) whose
            objects are listed before backup and checksum-verified after recovery

    Returns:
        str: completed backup name

    """
    logger.info(
        "Waiting for async backup between primary and secondary NooBaa DB"
    )
    time.sleep(60)

    bucket_objects_map = {}
    if buckets_with_local_objects:
        for bucket_obj, local_dir in buckets_with_local_objects:
            bucket_objects_map[bucket_obj.name] = list_objects_from_bucket(
                pod_obj=awscli_pod_session,
                target=bucket_obj.name,
                s3_obj=mcg_obj,
                recursive=True,
            )

    ocs_storage_obj = OCP(
        kind="storagecluster",
        namespace=config.ENV_DATA["cluster_namespace"],
        resource_name=constants.DEFAULT_STORAGE_CLUSTER,
    )
    noobaa_obj = OCP(
        kind="noobaa",
        namespace=config.ENV_DATA["cluster_namespace"],
        resource_name=constants.NOOBAA_RESOURCE_NAME,
    )

    logger.info("Creating on-demand backup using NooBaa CLI")
    backup_name = create_unique_resource_name("noobaa-cli", "backup")
    mcg_obj.exec_mcg_cmd(
        cmd=f"system db-backup --name {backup_name}",
        namespace=config.ENV_DATA["cluster_namespace"],
        use_yes=True,
        ignore_error=False,
    )
    logger.info("On-demand backup command executed")

    backup_obj = OCP(kind="Backup", namespace=config.ENV_DATA["cluster_namespace"])
    backup_obj.wait_for_resource(
        "completed",
        resource_name=backup_name,
        column="PHASE",
        timeout=300,
    )
    logger.info("On-demand backup %s completed successfully", backup_name)

    noobaa_db_recovery_patch(backup_name)
    verify_noobaa_db_config_info("dbRecovery", ocs_storage_obj, noobaa_obj)
    logger.info("DB recovery configuration added to OCS Storage cluster CR")

    trigger_noobaa_db_cluster_recovery()

    if buckets_for_health:
        for bucket_obj in buckets_for_health:
            bucket_obj.verify_health(timeout=600)

    if buckets_with_local_objects:
        for bucket_obj, local_dir in buckets_with_local_objects:
            obj_download_path = f"{test_directory_setup.result_dir}/{bucket_obj.name}"
            full_object_path = f"s3://{bucket_obj.name}"
            sync_object_directory(
                podobj=awscli_pod_session,
                src=full_object_path,
                target=obj_download_path,
                s3_obj=mcg_obj,
            )
            logger.info("Objects downloaded to %s", obj_download_path)
            for obj in bucket_objects_map[bucket_obj.name]:
                assert verify_s3_object_integrity(
                    original_object_path=f"{local_dir}/{obj}",
                    result_object_path=f"{obj_download_path}/{obj}",
                    awscli_pod=awscli_pod_session,
                ), (
                    "Mismatch in checksum between original object and object "
                    "downloaded after recovery"
                )

    cleanup_noobaa_cli_backup_resources(backup_name)
    logger.info(
        "Cluster recovered successfully using CLI-created backup and validated data"
    )
    return backup_name


def shutdown_primary_noobaa_db_node(timeout=300):
    """
    Shutdown the worker node hosting the primary CNPG NooBaa DB pod.

    Based on tests/cross_functional/system_test/test_object_expiration_system.py.

    Returns:
        str: name of the shut down node

    """
    primary_nb_db_pod = get_primary_nb_db_pod()
    primary_nb_db_node = primary_nb_db_pod.get_node()
    logger.info(
        "Shutting down primary NooBaa DB pod %s on node %s",
        primary_nb_db_pod.name,
        primary_nb_db_node,
    )
    nodes.stop_nodes(nodes=get_node_objs([primary_nb_db_node]))
    wait_for_nodes_status(
        node_names=[primary_nb_db_node],
        status=constants.NODE_NOT_READY,
        timeout=timeout,
    )
    logger.info("Primary NooBaa DB node %s is NotReady", primary_nb_db_node)
    return primary_nb_db_node


def shutdown_secondary_noobaa_db_node(timeout=300):
    """
    Shutdown the worker node hosting a secondary CNPG NooBaa DB replica pod.

    Returns:
        str: name of the shut down node

    """
    secondary_nb_db_pod = get_secondary_nb_db_pod()
    secondary_nb_db_node = secondary_nb_db_pod.get_node()
    logger.info(
        "Shutting down secondary NooBaa DB pod %s on node %s",
        secondary_nb_db_pod.name,
        secondary_nb_db_node,
    )
    nodes.stop_nodes(nodes=get_node_objs([secondary_nb_db_node]))
    wait_for_nodes_status(
        node_names=[secondary_nb_db_node],
        status=constants.NODE_NOT_READY,
        timeout=timeout,
    )
    logger.info("Secondary NooBaa DB node %s is NotReady", secondary_nb_db_node)
    return secondary_nb_db_node


def start_primary_noobaa_db_node(node_name, timeout=300, noobaa_timeout=1200):
    """
    Start the worker node hosting the primary CNPG NooBaa DB pod.

    Based on tests/cross_functional/system_test/test_object_expiration_system.py.

    Returns:
        str: name of the started node

    """
    logger.info("Starting primary NooBaa DB node %s", node_name)
    nodes.start_nodes(nodes=get_node_objs([node_name]))
    wait_for_nodes_status(
        node_names=[node_name],
        status=constants.NODE_READY,
        timeout=timeout,
    )
    wait_for_noobaa_pods_running(timeout=noobaa_timeout)
    logger.info(
        "Primary NooBaa DB node %s is Ready and NooBaa pods are running",
        node_name,
    )
    return node_name


def start_secondary_noobaa_db_node(node_name, timeout=300, noobaa_timeout=1200):
    """
    Start the worker node hosting a secondary CNPG NooBaa DB replica pod.

    Based on tests/cross_functional/system_test/test_object_expiration_system.py.

    Returns:
        str: name of the started node

    """
    logger.info("Starting secondary NooBaa DB node %s", node_name)
    nodes.start_nodes(nodes=get_node_objs([node_name]))
    wait_for_nodes_status(
        node_names=[node_name],
        status=constants.NODE_READY,
        timeout=timeout,
    )
    wait_for_noobaa_pods_running(timeout=noobaa_timeout)
    logger.info(
        "Secondary NooBaa DB node %s is Ready and NooBaa pods are running",
        node_name,
    )
    return node_name


def verify_noncurrent_versions_expired(
    mcg_obj,
    awscli_pod,
    bucket_name,
    obj_key,
    timeout=600,
    sleep=30,
):
    """
    Verify all non-current object versions are expired and only the current remains.

    """
    for versions in TimeoutSampler(
        timeout=timeout,
        sleep=sleep,
        func=get_obj_versions,
        mcg_obj=mcg_obj,
        awscli_pod=awscli_pod,
        bucket_name=bucket_name,
        obj_key=obj_key,
    ):
        if len(versions) == 1 and versions[0].get("IsLatest"):
            logger.info(
                "All non-current versions expired for %s/%s; current version %s retained",
                bucket_name,
                obj_key,
                versions[0]["VersionId"],
            )
            return versions[0]

        logger.warning(
            "Waiting for non-current versions to expire for %s/%s (%s version(s) remain)",
            bucket_name,
            obj_key,
            len(versions),
        )
    else:
        remaining_versions = get_obj_versions(
            mcg_obj, awscli_pod, bucket_name, obj_key
        )
        remaining_ids = [version["VersionId"] for version in remaining_versions]
        raise AssertionError(
            f"Non-current versions were not expired for {bucket_name}/{obj_key}; "
            f"remaining versions: {remaining_ids}"
        )


def verify_noncurrent_versions_and_delete_marker_expired(
    mcg_obj,
    awscli_pod,
    bucket_name,
    obj_key,
    timeout=600,
    sleep=30,
):
    """
    Verify non-current versions expire and the delete marker is removed afterward.

    Ages remaining object versions in the NooBaa DB, waits for non-current version
    expiration, then waits for ExpiredObjectDeleteMarker to remove the marker.

    """
    remaining_versions = get_obj_versions(
        mcg_obj, awscli_pod, bucket_name, obj_key
    )
    if remaining_versions:
        latest_creation_date = datetime.fromisoformat(
            remaining_versions[0]["LastModified"].replace("Z", "+00:00")
        )
        logger.info(
            "Manually aging %s non-current version(s) for %s/%s",
            len(remaining_versions),
            bucket_name,
            obj_key,
        )
        for index, version in enumerate(remaining_versions):
            change_versions_creation_date_in_noobaa_db(
                bucket_name=bucket_name,
                object_key=obj_key,
                version_ids=[version["VersionId"]],
                new_creation_time=(
                    latest_creation_date - timedelta(days=index + 2)
                ).timestamp(),
            )

    logger.info("Waiting for non-current versions to expire for %s/%s", bucket_name, obj_key)
    for versions in TimeoutSampler(
        timeout=timeout,
        sleep=sleep,
        func=get_obj_versions,
        mcg_obj=mcg_obj,
        awscli_pod=awscli_pod,
        bucket_name=bucket_name,
        obj_key=obj_key,
    ):
        if len(versions) == 0:
            logger.info("All non-current versions expired for %s/%s", bucket_name, obj_key)
            break
        logger.warning(
            "Non-current versions still present for %s/%s (%s remain)",
            bucket_name,
            obj_key,
            len(versions),
        )
    else:
        remaining_version_ids = [
            version["VersionId"]
            for version in get_obj_versions(mcg_obj, awscli_pod, bucket_name, obj_key)
        ]
        raise AssertionError(
            f"Non-current versions were not expired for {bucket_name}/{obj_key}; "
            f"remaining versions: {remaining_version_ids}"
        )

    logger.info("Waiting for delete marker to expire for %s/%s", bucket_name, obj_key)
    for raw_versions in TimeoutSampler(
        timeout=timeout,
        sleep=sleep,
        func=s3_list_object_versions,
        s3_obj=mcg_obj,
        bucketname=bucket_name,
        prefix=obj_key,
    ):
        delete_markers = raw_versions.get("DeleteMarkers", [])
        if len(delete_markers) == 0:
            logger.info("Delete marker expired for %s/%s", bucket_name, obj_key)
            break
        logger.warning(
            "Delete marker has not expired yet for %s/%s: %s",
            bucket_name,
            obj_key,
            delete_markers,
        )
    else:
        raise AssertionError(
            f"Delete marker was not expired for {bucket_name}/{obj_key} in time"
        )

    object_versions = s3_list_object_versions(
        mcg_obj, bucket_name, prefix=obj_key
    )
    assert not object_versions.get("Versions") and not object_versions.get(
        "DeleteMarkers"
    ), (
        f"Object {obj_key} or its delete marker still exists in "
        f"{bucket_name}: {object_versions}"
    )


def stop_mcg_background_features(feature_setup_map):
    """
    Stop background MCG feature validation threads started by setup_mcg_bg_features.

    """
    event = feature_setup_map["executor"]["event"]
    if event is not None:
        event.set()
    threads = feature_setup_map["executor"]["threads"]
    if threads:
        for thread in threads:
            thread.result()
    logger.info("Stopped background MCG feature validation threads")


def verify_unidirectional_replication(
    mcg_obj,
    source_bucket,
    target_bucket,
    replication_object_keys,
    timeout=1200,
):
    """
    Verify uni-directional replication from source to target namespace bucket.

    Confirms replication policy is configured only on the source bucket, object
    lists match, and each uploaded object key is present in the target bucket.

    """
    assert source_bucket.replication_policy is not None
    assert (
        source_bucket.replication_policy["rules"][0]["destination_bucket"]
        == target_bucket.name
    )
    assert "sync_deletions" not in source_bucket.replication_policy["rules"][0]
    assert target_bucket.replication_policy is None

    assert compare_bucket_object_list(
        mcg_obj,
        source_bucket.name,
        target_bucket.name,
        timeout=timeout,
    ), (
        f"Uni-directional replication verification failed: objects in "
        f"{source_bucket.name} and {target_bucket.name} do not match"
    )

    target_object_keys = {
        obj.key for obj in mcg_obj.s3_list_all_objects_in_bucket(target_bucket.name)
    }
    for obj_key in replication_object_keys:
        assert obj_key in target_object_keys, (
            f"Replicated object {obj_key} not found in target bucket "
            f"{target_bucket.name}"
        )
    logger.info(
        "Uni-directional replication verified from %s to %s",
        source_bucket.name,
        target_bucket.name,
    )


def verify_bidirectional_replication(
    mcg_obj,
    awscli_pod,
    source_bucket,
    target_bucket,
    test_directory_setup,
    target_to_source_prefix,
    timeout=1200,
):
    """
    Verify bi-directional replication between source and target namespace buckets.

    Uploads an object in each direction and confirms both buckets stay in sync.

    """
    assert source_bucket.replication_policy is not None
    assert (
        source_bucket.replication_policy["rules"][0]["destination_bucket"]
        == target_bucket.name
    )
    assert source_bucket.name in get_replication_policy(target_bucket.name)

    assert compare_bucket_object_list(
        mcg_obj,
        source_bucket.name,
        target_bucket.name,
        timeout=timeout,
    ), (
        f"Bi-directional replication verification failed: objects in "
        f"{source_bucket.name} and {target_bucket.name} do not match"
    )

    source_objects = write_random_test_objects_to_bucket(
        io_pod=awscli_pod,
        bucket_to_write=source_bucket.name,
        file_dir=test_directory_setup.origin_dir,
        amount=1,
        pattern="bidi-verify-source-",
        mcg_obj=mcg_obj,
    )
    assert compare_bucket_object_list(
        mcg_obj,
        source_bucket.name,
        target_bucket.name,
        timeout=timeout,
    ), (
        f"Source-to-target replication failed after uploading to {source_bucket.name}"
    )
    target_object_keys = {
        obj.key for obj in mcg_obj.s3_list_all_objects_in_bucket(target_bucket.name)
    }
    assert source_objects[0] in target_object_keys, (
        f"Object {source_objects[0]} uploaded to {source_bucket.name} "
        f"was not replicated to {target_bucket.name}"
    )

    target_objects = write_random_test_objects_to_bucket(
        io_pod=awscli_pod,
        bucket_to_write=target_bucket.name,
        file_dir=test_directory_setup.origin_dir,
        amount=1,
        pattern="bidi-verify-target-",
        prefix=target_to_source_prefix,
        mcg_obj=mcg_obj,
    )
    expected_target_keys = {
        f"{target_to_source_prefix}/{obj_key}" for obj_key in target_objects
    }
    assert compare_bucket_object_list(
        mcg_obj,
        source_bucket.name,
        target_bucket.name,
        timeout=timeout,
    ), (
        f"Target-to-source replication failed after uploading to {target_bucket.name}"
    )
    source_object_keys = {
        obj.key for obj in mcg_obj.s3_list_all_objects_in_bucket(source_bucket.name)
    }
    assert expected_target_keys <= source_object_keys, (
        f"Objects uploaded to {target_bucket.name} were not replicated "
        f"to {source_bucket.name}"
    )
    logger.info(
        "Bi-directional replication verified between %s and %s",
        source_bucket.name,
        target_bucket.name,
    )


def verify_deletion_sync_between_replication_buckets(
    mcg_obj,
    awscli_pod,
    source_bucket,
    target_bucket,
    test_directory_setup,
    target_to_source_prefix,
    timeout=1200,
):
    """
    Verify deletion sync in both directions between replication buckets.

    Uploads an object to each bucket, deletes it, and confirms the deletion
    is synchronized to the peer bucket.

    """
    for bucket_name in (source_bucket.name, target_bucket.name):
        replication_policy = json.loads(get_replication_policy(bucket_name))
        assert replication_policy["rules"][0]["sync_deletions"] is True, (
            f"Deletion sync is not enabled on replication bucket {bucket_name}"
        )

    source_objects = write_random_test_objects_to_bucket(
        io_pod=awscli_pod,
        bucket_to_write=source_bucket.name,
        file_dir=test_directory_setup.origin_dir,
        amount=1,
        pattern="deletion-sync-source-",
        mcg_obj=mcg_obj,
    )
    source_object_to_delete = source_objects[0]
    assert compare_bucket_object_list(
        mcg_obj,
        source_bucket.name,
        target_bucket.name,
        timeout=timeout,
    ), (
        f"Replication failed before source deletion sync test on "
        f"{source_bucket.name}"
    )
    logger.info(
        "Deleting %s from source bucket %s to verify deletion sync",
        source_object_to_delete,
        source_bucket.name,
    )
    s3_delete_object(mcg_obj, source_bucket.name, source_object_to_delete)
    assert compare_bucket_object_list(
        mcg_obj,
        source_bucket.name,
        target_bucket.name,
        timeout=timeout,
    ), (
        f"Deletion sync failed from {source_bucket.name} to {target_bucket.name}"
    )
    target_object_keys = {
        obj.key for obj in mcg_obj.s3_list_all_objects_in_bucket(target_bucket.name)
    }
    assert source_object_to_delete not in target_object_keys, (
        f"Object {source_object_to_delete} was not deleted from "
        f"{target_bucket.name}"
    )

    target_objects = write_random_test_objects_to_bucket(
        io_pod=awscli_pod,
        bucket_to_write=target_bucket.name,
        file_dir=test_directory_setup.origin_dir,
        amount=1,
        pattern="deletion-sync-target-",
        prefix=target_to_source_prefix,
        mcg_obj=mcg_obj,
    )
    target_object_to_delete = f"{target_to_source_prefix}/{target_objects[0]}"
    assert compare_bucket_object_list(
        mcg_obj,
        source_bucket.name,
        target_bucket.name,
        timeout=timeout,
    ), (
        f"Replication failed before target deletion sync test on "
        f"{target_bucket.name}"
    )
    logger.info(
        "Deleting %s from target bucket %s to verify deletion sync",
        target_object_to_delete,
        target_bucket.name,
    )
    s3_delete_object(mcg_obj, target_bucket.name, target_object_to_delete)
    assert compare_bucket_object_list(
        mcg_obj,
        source_bucket.name,
        target_bucket.name,
        timeout=timeout,
    ), (
        f"Deletion sync failed from {target_bucket.name} to {source_bucket.name}"
    )
    source_object_keys = {
        obj.key for obj in mcg_obj.s3_list_all_objects_in_bucket(source_bucket.name)
    }
    assert target_object_to_delete not in source_object_keys, (
        f"Object {target_object_to_delete} was not deleted from "
        f"{source_bucket.name}"
    )
    logger.info(
        "Deletion sync verified between %s and %s",
        source_bucket.name,
        target_bucket.name,
    )


def verify_multipart_upload_aborted_and_cleaned_up(
    mcg_obj,
    awscli_pod,
    bucket_name,
    test_directory_setup,
    object_key="multipart-abort-verify-obj",
    parts_amount=3,
    timeout=600,
    sleep=30,
):
    """
    Verify an incomplete multipart upload is aborted and cleaned up by lifecycle.

    Based on tests/functional/object/mcg/test_lifecycle_configuration.py.

    """
    origin_dir = test_directory_setup.origin_dir
    result_dir = test_directory_setup.result_dir

    upload_id = create_multipart_upload(mcg_obj, bucket_name, object_key)
    awscli_pod.exec_cmd_on_pod(
        f'sh -c "dd if=/dev/urandom of={origin_dir}/{object_key} '
        f'bs=1MB count={parts_amount}; '
        f'split -b 1m {origin_dir}/{object_key} {result_dir}/part"'
    )
    parts = awscli_pod.exec_cmd_on_pod(f'sh -c "ls -1 {result_dir}"').split()
    upload_parts(
        mcg_obj,
        awscli_pod,
        bucket_name,
        object_key,
        result_dir,
        upload_id,
        parts,
    )

    multipart_uploads = list_multipart_upload(mcg_obj, bucket_name)
    assert "Uploads" in multipart_uploads, "No in-progress multipart uploads found"
    assert any(
        upload["UploadId"] == upload_id and upload["Key"] == object_key
        for upload in multipart_uploads["Uploads"]
    ), (
        f"Multipart upload {upload_id} not found on {bucket_name}/{object_key}"
    )

    logger.info("Expiring multipart upload %s in NooBaa DB", upload_id)
    expire_multipart_upload_in_noobaa_db(upload_id)

    logger.info(
        "Waiting for multipart upload %s to be aborted on %s",
        upload_id,
        bucket_name,
    )
    for multipart_response in TimeoutSampler(
        timeout=timeout,
        sleep=sleep,
        func=list_multipart_upload,
        s3_obj=mcg_obj,
        bucketname=bucket_name,
    ):
        uploads = multipart_response.get("Uploads", [])
        if not any(upload["UploadId"] == upload_id for upload in uploads):
            logger.info(
                "Multipart upload %s aborted and cleaned up on %s",
                upload_id,
                bucket_name,
            )
            return upload_id

        logger.warning(
            "Multipart upload %s has not been aborted yet on %s: %s",
            upload_id,
            bucket_name,
            uploads,
        )
    else:
        raise AssertionError(
            f"Multipart upload {upload_id} was not aborted on {bucket_name} in time"
        )


def cleanup_all_test_bucket_objects(
    mcg_obj,
    awscli_pod,
    buckets,
    timeout=600,
):
    """
    Delete all objects from the provided test buckets, including versioned objects.

    """
    unique_bucket_names = sorted({bucket.name for bucket in buckets})
    for bucket_name in unique_bucket_names:
        logger.info("Deleting all objects from bucket %s", bucket_name)
        delete_all_objects_in_batches(mcg_obj.s3_resource, bucket_name)

        object_versions = s3_list_object_versions(mcg_obj, bucket_name)
        for version in object_versions.get("Versions", []):
            s3_delete_object(
                mcg_obj,
                bucket_name,
                version["Key"],
                versionid=version["VersionId"],
            )
        for marker in object_versions.get("DeleteMarkers", []):
            s3_delete_object(
                mcg_obj,
                bucket_name,
                marker["Key"],
                versionid=marker["VersionId"],
            )

        assert wait_for_object_count_in_bucket(
            io_pod=awscli_pod,
            expected_count=0,
            bucket_name=bucket_name,
            s3_obj=mcg_obj,
            timeout=timeout,
            sleep=10,
        ), f"Bucket {bucket_name} is not empty after cleanup"

    logger.info("Cleaned up objects from %s buckets", len(unique_bucket_names))


def verify_mcg_features_after_db_recovery(
    mcg_obj,
    awscli_pod_session,
    source_bucket,
    target_bucket,
    expiration_bucket,
    versioning_bucket,
    replication_object_keys,
    versioning_object_key,
    test_directory_setup,
    expiration_prefix="to_expire",
):
    """
    Verify replication, expiration, and object versioning after NooBaa DB recovery.

    """
    verify_unidirectional_replication(
        mcg_obj=mcg_obj,
        source_bucket=source_bucket,
        target_bucket=target_bucket,
        replication_object_keys=replication_object_keys,
    )

    assert wait_for_object_count_in_bucket(
        io_pod=awscli_pod_session,
        expected_count=0,
        bucket_name=expiration_bucket.name,
        prefix=expiration_prefix,
        s3_obj=mcg_obj,
        timeout=120,
        sleep=10,
    ), f"Expired prefix {expiration_prefix} is not empty after DB recovery"

    write_random_test_objects_to_bucket(
        io_pod=awscli_pod_session,
        bucket_to_write=expiration_bucket.name,
        file_dir=test_directory_setup.origin_dir,
        amount=1,
        prefix=expiration_prefix,
        mcg_obj=mcg_obj,
    )
    post_recovery_expire_objects = awscli_pod_session.exec_cmd_on_pod(
        f"ls -A1 {test_directory_setup.origin_dir}"
    ).split(" ")
    expire_objects_in_bucket(
        expiration_bucket.name,
        post_recovery_expire_objects,
        prefix=expiration_prefix,
    )
    assert wait_for_object_count_in_bucket(
        io_pod=awscli_pod_session,
        expected_count=0,
        bucket_name=expiration_bucket.name,
        prefix=expiration_prefix,
        s3_obj=mcg_obj,
        timeout=600,
        sleep=30,
    ), "Expiration did not work after DB recovery"
    logger.info("Expiration verified on bucket %s", expiration_bucket.name)

    obj_versions = get_obj_versions(
        mcg_obj,
        awscli_pod_session,
        versioning_bucket.name,
        versioning_object_key,
    )
    assert len(obj_versions) >= 2, (
        f"Expected at least 2 versions for {versioning_object_key}, "
        f"found {len(obj_versions)}"
    )
    version_ids = {version["VersionId"] for version in obj_versions}
    assert len(version_ids) == len(obj_versions), (
        f"Duplicate version IDs found for {versioning_object_key}"
    )
    logger.info(
        "Object versioning verified on %s/%s (%s versions)",
        versioning_bucket.name,
        versioning_object_key,
        len(obj_versions),
    )


def validate_mcg_bucket_replicaton(
    awscli_pod_session,
    mcg_obj_session,
    source_target_map,
    uploaded_objects_dir,
    downloaded_obejcts_dir,
    event,
    run_in_bg=False,
    object_amount=5,
):
    """
    Validate MCG bucket replication feature

    Args:
        awscli_pod_session (Pod): Pod object representing aws-cli pod
        mcg_obj_session (MCG): MCG object
        source_target_map (Dict): Dictionary consisting of source - target buckets
        uploaded_objects_dir (str): directory where uploaded objects are kept
        downloaded_obejcts_dir (str): directory where downloaded objects are kept
        event (threading.Event()): Event() object
        run_in_bg (bool): If True, validation is run in background
        object_amount (int): Amounts of objects

    """
    bidi_uploaded_objs_dir_1 = uploaded_objects_dir + "/bidi_1"
    bidi_uploaded_objs_dir_2 = uploaded_objects_dir + "/bidi_2"
    bidi_downloaded_objs_dir_1 = downloaded_obejcts_dir + "/bidi_1"
    bidi_downloaded_objs_dir_2 = downloaded_obejcts_dir + "/bidi_2"

    # Verify replication is working as expected by performing a two-way round-trip object verification
    while True:
        for first_bucket, second_bucket in source_target_map.items():
            random_object_round_trip_verification(
                io_pod=awscli_pod_session,
                bucket_name=first_bucket.name,
                upload_dir=bidi_uploaded_objs_dir_1,
                download_dir=bidi_downloaded_objs_dir_1,
                amount=object_amount,
                pattern=f"FirstBiDi-{uuid4().hex}",
                prefix="bidi_1",
                wait_for_replication=True,
                second_bucket_name=second_bucket.name,
                mcg_obj=mcg_obj_session,
                cleanup=True,
                timeout=1200,
            )

            random_object_round_trip_verification(
                io_pod=awscli_pod_session,
                bucket_name=second_bucket.name,
                upload_dir=bidi_uploaded_objs_dir_2,
                download_dir=bidi_downloaded_objs_dir_2,
                amount=object_amount,
                pattern=f"SecondBiDi-{uuid4().hex}",
                prefix="bidi_2",
                wait_for_replication=True,
                second_bucket_name=first_bucket.name,
                mcg_obj=mcg_obj_session,
                cleanup=True,
                timeout=1200,
            )
            if event.is_set():
                run_in_bg = False
                break

        if not run_in_bg:
            logger.info("Verified bi-direction replication successfully")
            logger.warning("Stopping bi-direction replication verification")
            break
        time.sleep(30)


def validate_mcg_caching(
    awscli_pod_session,
    mcg_obj_session,
    cld_mgr,
    cache_buckets,
    uploaded_objects_dir,
    downloaded_obejcts_dir,
    event,
    run_in_bg=False,
):
    """
    Validate noobaa caching feature against the cache buckets

    Args:
        awscli_pod_session (Pod): Pod object representing aws-cli pod
        mcg_obj_session (MCG): MCG object
        cld_mgr (cld_mgr): cld_mgr object
        cache_buckets (List): List consisting of cache buckets
        uploaded_objects_dir (str): directory where uploaded objects are kept
        downloaded_obejcts_dir (str): directory where downloaded objects are kept
        event (threading.Event()): Event() object
        run_in_bg (bool): If True, validation is run in background

    """
    while True:
        for bucket in cache_buckets:
            cache_uploaded_objs_dir = uploaded_objects_dir + "/cache"
            cache_uploaded_objs_dir_2 = uploaded_objects_dir + "/cache_2"
            cache_downloaded_objs_dir = downloaded_obejcts_dir + "/cache"
            underlying_bucket_name = bucket.bucketclass.namespacestores[0].uls_name

            # Upload a random object to the bucket
            logger.info(f"Uploading to the cache bucket: {bucket.name}")
            obj_name = f"Cache-{uuid4().hex}"
            objs_written_to_cache_bucket = write_random_test_objects_to_bucket(
                awscli_pod_session,
                bucket.name,
                cache_uploaded_objs_dir,
                pattern=obj_name,
                mcg_obj=mcg_obj_session,
            )
            wait_for_cache(
                mcg_obj_session,
                bucket.name,
                objs_written_to_cache_bucket,
                timeout=300,
            )

            # Write a random, larger object directly to the underlying storage of the bucket
            logger.info(
                f"Uploading to the underlying bucket {underlying_bucket_name} directly"
            )
            write_random_test_objects_to_bucket(
                awscli_pod_session,
                underlying_bucket_name,
                cache_uploaded_objs_dir_2,
                pattern=obj_name,
                s3_creds=cld_mgr.aws_client.nss_creds,
                bs="2M",
            )

            # Download the object from the cache bucket
            awscli_pod_session.exec_cmd_on_pod(f"mkdir -p {cache_downloaded_objs_dir}")
            sync_object_directory(
                awscli_pod_session,
                f"s3://{bucket.name}",
                cache_downloaded_objs_dir,
                mcg_obj_session,
            )

            assert verify_s3_object_integrity(
                original_object_path=f"{cache_uploaded_objs_dir}/{obj_name}0",
                result_object_path=f"{cache_downloaded_objs_dir}/{obj_name}0",
                awscli_pod=awscli_pod_session,
            ), "The uploaded and downloaded cached objects have different checksums"

            assert (
                verify_s3_object_integrity(
                    original_object_path=f"{cache_uploaded_objs_dir_2}/{obj_name}0",
                    result_object_path=f"{cache_downloaded_objs_dir}/{obj_name}0",
                    awscli_pod=awscli_pod_session,
                )
                is False
            ), "The cached object was replaced by the new one before the TTL has expired"
            logger.info(f"Verified caching for bucket: {bucket.name}")

            if event.is_set():
                run_in_bg = False
                break

        if not run_in_bg:
            logger.warning("Stopping noobaa caching verification")
            break
        time.sleep(30)


def validate_rgw_kafka_notification(kafka_rgw_dict, event, run_in_bg=False):
    """
    Validate kafka notifications for RGW buckets

    Args:
        kafka_rgw_dict (Dict): Dict consisting of rgw bucket,
        kafka_topic, kafkadrop_host etc
        event (threading.Event()): Event() object
        run_in_bg (Bool): True if you want to run in the background

    """
    s3_client = kafka_rgw_dict["s3client"]
    bucketname = kafka_rgw_dict["kafka_rgw_bucket"]
    notify_cmd = kafka_rgw_dict["notify_cmd"]
    data = kafka_rgw_dict["data"]
    kafkadrop_host = kafka_rgw_dict["kafkadrop_host"]
    kafka_topic = kafka_rgw_dict["kafka_topic"]

    while True:
        data = data + f"{uuid4().hex}"

        def put_object_to_bucket(bucket_name, key, body):
            return s3_client.put_object(Bucket=bucket_name, Key=key, Body=body)

        assert put_object_to_bucket(
            bucketname, "key-1", data
        ), "Failed: Put object: key-1"
        exec_cmd(notify_cmd)

        # Validate rgw logs notification are sent
        # No errors are seen
        pattern = "ERROR: failed to create push endpoint"
        rgw_pod_obj = get_rgw_pods()
        rgw_log = get_pod_logs(pod_name=rgw_pod_obj[0].name, container="rgw")
        assert re.search(pattern=pattern, string=rgw_log) is None, (
            f"Error: {pattern} msg found in the rgw logs."
            f"Validate {pattern} found on rgw logs and also "
            f"rgw bucket notification is working correctly"
        )
        assert put_object_to_bucket(
            bucketname, "key-2", data
        ), "Failed: Put object: key-2"
        exec_cmd(notify_cmd)

        # Validate message are received Kafka side using curl command
        # A temporary way to check from Kafka side, need to check from UI
        @retry(Exception, tries=5, delay=5)
        def validate_kafa_for_message():
            curl_command = (
                f"curl -X GET {kafkadrop_host}/topic/{kafka_topic.name} "
                "-H 'content-type: application/vnd.kafka.json.v2+json'"
            )
            json_output = run_cmd(cmd=curl_command)
            # logger.info("Json output:" f"{json_output}")
            new_string = json_output.split()
            messages = new_string[new_string.index("messages</td>") + 1]
            logger.info("Messages:" + str(messages))
            if messages.find("1") == -1:
                raise Exception(
                    "Error: Messages are not recieved from Kafka side."
                    "RGW bucket notification is not working as expected."
                )

        validate_kafa_for_message()

        if event.is_set() or not run_in_bg:
            logger.warning("Stopping kafka rgw notification verification")
            break
        time.sleep(30)


def validate_mcg_object_expiration(
    mcg_obj,
    buckets,
    event,
    run_in_bg=False,
    object_amount=5,
):
    """
    Validates objects expiration for MCG buckets

    Args:
        mcg_obj (MCG): MCG object
        buckets (List): List of MCG buckets
        event (threading.Event()): Event() object
        run_in_bg (Bool): True if wants to run in background
        object_amount (Int): Amount of objects
        prefix (str): Any prefix used for objects

    """
    while True:
        for bucket in buckets:

            for i in range(object_amount):
                s3_put_object(
                    mcg_obj,
                    bucket.name,
                    f"obj-key-{uuid4().hex}",
                    "Some random data",
                )
            expire_objects_in_bucket(bucket.name)
            sample_if_objects_expired(mcg_obj, bucket.name)
            if event.is_set():
                run_in_bg = False
                break

        if not run_in_bg:
            logger.warning("Stopping MCG object expiration verification")
            break
        time.sleep(30)


def validate_mcg_nsfs_feature():
    logger.info("This is not implemented")


def verify_osd_used_capacity_greater_than_expected(expected_used_capacity):
    """
    Verify OSD percent used capacity greate than ceph_full_ratio

    Args:
        expected_used_capacity (float): expected used capacity

    Returns:
         bool: True if used_capacity greater than expected_used_capacity, False otherwise

    """
    used_capacity = get_percent_used_capacity()
    logger.info(f"Used Capacity is {used_capacity}%")
    ceph_df_detail = get_ceph_df_detail()
    logger.info(f"ceph df detail: {ceph_df_detail}")
    osds_utilization = get_osd_utilization()
    logger.info(f"osd utilization: {osds_utilization}")
    for osd_id, osd_utilization in osds_utilization.items():
        if osd_utilization > expected_used_capacity:
            logger.info(f"OSD ID:{osd_id}:{osd_utilization} greater than 85%")
            return True
    return False
