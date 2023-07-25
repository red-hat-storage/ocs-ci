import logging
import pytest

from ocs_ci.ocs import constants, defaults
from ocs_ci.framework import config
from ocs_ci.ocs.bucket_utils import (
    compare_object_checksums_between_bucket_and_local,
    compare_directory,
    patch_replication_policy_to_bucket,
    random_object_round_trip_verification,
    sync_object_directory,
    wait_for_cache,
    write_random_test_objects_to_bucket,
)
from ocs_ci.ocs.benchmark_operator_fio import BenchmarkOperatorFIO
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources import pod, pvc
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.helpers.helpers import (
    wait_for_resource_state,
    modify_statefulset_replica_count,
    validate_pv_delete,
)

logger = logging.getLogger(__name__)


@pytest.fixture()
def noobaa_db_backup_and_recovery(request, snapshot_factory):
    """
    Verify noobaa backup and recovery

    1. Take snapshot db-noobaa-db-0 PVC and retore it to PVC
    2. Scale down the statefulset noobaa-db
    3. Get the yaml of the current PVC, db-noobaa-db-0 and
       change the parameter persistentVolumeReclaimPolicy to Retain for restored PVC
    4. Delete both PVCs, the PV for the original claim db-noobaa-db-0 will be removed.
       The PV for claim db-noobaa-db-0-snapshot-restore will move to ‘Released’
    5. Edit again restore PV and remove the claimRef section.
       The volume will transition to Available.
    6. Edit the yaml db-noobaa-db-0.yaml and change the setting volumeName to restored PVC.
    7. Scale up the stateful set again and the pod should be running

    """
    restore_pvc_objs = []

    def factory(snapshot_factory=snapshot_factory):
        # Get noobaa pods before execution
        noobaa_pods = pod.get_noobaa_pods()

        # Get noobaa PVC before execution
        noobaa_pvc_obj = pvc.get_pvc_objs(pvc_names=["db-noobaa-db-pg-0"])
        noobaa_pv_name = noobaa_pvc_obj[0].get("spec").get("spec").get("volumeName")

        # Take snapshot db-noobaa-db-0 PVC
        logger.info(f"Creating snapshot of the {noobaa_pvc_obj[0].name} PVC")
        snap_obj = snapshot_factory(
            pvc_obj=noobaa_pvc_obj[0],
            wait=True,
            snapshot_name=f"{noobaa_pvc_obj[0].name}-snapshot",
        )
        logger.info(f"Successfully created snapshot {snap_obj.name} and in Ready state")

        # Restore it to PVC
        logger.info(f"Restoring snapshot {snap_obj.name} to create new PVC")
        sc_name = noobaa_pvc_obj[0].get().get("spec").get("storageClassName")
        pvc_size = (
            noobaa_pvc_obj[0]
            .get()
            .get("spec")
            .get("resources")
            .get("requests")
            .get("storage")
        )
        restore_pvc_obj = pvc.create_restore_pvc(
            sc_name=sc_name,
            snap_name=snap_obj.name,
            namespace=snap_obj.namespace,
            size=pvc_size,
            pvc_name=f"{snap_obj.name}-restore",
            volume_mode=snap_obj.parent_volume_mode,
            access_mode=snap_obj.parent_access_mode,
        )
        restore_pvc_objs.append(restore_pvc_obj)
        wait_for_resource_state(restore_pvc_obj, constants.STATUS_BOUND)
        restore_pvc_obj.reload()
        logger.info(
            f"Succeesfuly created PVC {restore_pvc_obj.name} "
            f"from snapshot {snap_obj.name}"
        )

        # Scale down the statefulset noobaa-db
        modify_statefulset_replica_count(
            statefulset_name=constants.NOOBAA_DB_STATEFULSET, replica_count=0
        ), f"Failed to scale down the statefulset {constants.NOOBAA_DB_STATEFULSET}"

        # Get the noobaa-db PVC
        pvc_obj = OCP(
            kind=constants.PVC, namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        noobaa_pvc_yaml = pvc_obj.get(resource_name=noobaa_pvc_obj[0].name)

        # Get the restored noobaa PVC and
        # change the parameter persistentVolumeReclaimPolicy to Retain
        restored_noobaa_pvc_obj = pvc.get_pvc_objs(
            pvc_names=[f"{snap_obj.name}-restore"]
        )
        restored_noobaa_pv_name = (
            restored_noobaa_pvc_obj[0].get("spec").get("spec").get("volumeName")
        )
        pv_obj = OCP(kind=constants.PV, namespace=constants.OPENSHIFT_STORAGE_NAMESPACE)
        params = '{"spec":{"persistentVolumeReclaimPolicy":"Retain"}}'
        assert pv_obj.patch(resource_name=restored_noobaa_pv_name, params=params), (
            "Failed to change the parameter persistentVolumeReclaimPolicy"
            f" to Retain {restored_noobaa_pv_name}"
        )

        # Delete both PVCs
        pvc.delete_pvcs(pvc_objs=[noobaa_pvc_obj[0], restored_noobaa_pvc_obj[0]])

        # Validate original claim db-noobaa-db-0 removed
        assert validate_pv_delete(
            pv_name=noobaa_pv_name
        ), f"PV not deleted, still exist {noobaa_pv_name}"

        # Validate PV for claim db-noobaa-db-0-snapshot-restore is in Released state
        pv_obj.wait_for_resource(
            condition=constants.STATUS_RELEASED, resource_name=restored_noobaa_pv_name
        )

        # Edit again restore PV and remove the claimRef section
        logger.info(f"Remove the claimRef section from PVC {restored_noobaa_pv_name}")
        params = '[{"op": "remove", "path": "/spec/claimRef"}]'
        pv_obj.patch(
            resource_name=restored_noobaa_pv_name, params=params, format_type="json"
        )
        logger.info(
            f"Successfully removed claimRef section from PVC {restored_noobaa_pv_name}"
        )

        # Validate PV is in Available state
        pv_obj.wait_for_resource(
            condition=constants.STATUS_AVAILABLE, resource_name=restored_noobaa_pv_name
        )

        # Edit the yaml db-noobaa-db-0.yaml and change the
        # setting volumeName to restored PVC
        noobaa_pvc_yaml["spec"]["volumeName"] = restored_noobaa_pv_name
        noobaa_pvc_yaml = OCS(**noobaa_pvc_yaml)
        noobaa_pvc_yaml.create()

        # Validate noobaa PVC is in bound state
        pvc_obj.wait_for_resource(
            condition=constants.STATUS_BOUND,
            resource_name=noobaa_pvc_obj[0].name,
            timeout=120,
        )

        # Scale up the statefulset again
        assert modify_statefulset_replica_count(
            statefulset_name=constants.NOOBAA_DB_STATEFULSET, replica_count=1
        ), f"Failed to scale up the statefulset {constants.NOOBAA_DB_STATEFULSET}"

        # Validate noobaa pod is up and running
        pod_obj = OCP(kind=constants.POD, namespace=defaults.ROOK_CLUSTER_NAMESPACE)
        pod_obj.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            resource_count=len(noobaa_pods),
            selector=constants.NOOBAA_APP_LABEL,
        )

        # Change the parameter persistentVolumeReclaimPolicy to Delete again
        params = '{"spec":{"persistentVolumeReclaimPolicy":"Delete"}}'
        assert pv_obj.patch(resource_name=restored_noobaa_pv_name, params=params), (
            "Failed to change the parameter persistentVolumeReclaimPolicy"
            f" to Delete {restored_noobaa_pv_name}"
        )
        logger.info(
            "Changed the parameter persistentVolumeReclaimPolicy to Delete again"
        )

    def finalizer():
        # Get the statefulset replica count
        sst_obj = OCP(
            kind=constants.STATEFULSET,
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
        )
        noobaa_db_sst_obj = sst_obj.get(resource_name=constants.NOOBAA_DB_STATEFULSET)
        if noobaa_db_sst_obj["spec"]["replicas"] != 1:
            modify_statefulset_replica_count(
                statefulset_name=constants.NOOBAA_DB_STATEFULSET, replica_count=1
            ), f"Failed to scale up the statefulset {constants.NOOBAA_DB_STATEFULSET}"

        try:
            restore_pvc_objs[0].delete()
        except CommandFailed as ex:
            if f'"{restore_pvc_objs[0].name}" not found' not in str(ex):
                raise ex

    request.addfinalizer(finalizer)
    return factory


@pytest.fixture()
def setup_mcg_system(
    request,
    awscli_pod_session,
    mcg_obj_session,
    bucket_factory,
    cld_mgr,
    test_directory_setup,
):
    # E2E TODO: Have a cluster with FIPS, KMS for RGW and Hugepages enabled
    # E2E TODO: Please add the necessary skips to verify that all prerequisites are met

    def mcg_system_setup(bucket_amount=5, object_amount=10):
        # Create standard MCG buckets
        test_buckets = bucket_factory(
            amount=bucket_amount,
            interface="CLI",
        )

        uploaded_objects_dir = test_directory_setup.origin_dir
        downloaded_obejcts_dir = test_directory_setup.result_dir

        test_buckets_pattern = "RandomObject-"
        first_bidirectional_pattern = "FirstBidi-"
        second_bidirectional_pattern = "SecondBidi-"
        cache_pattern = "Cache-"

        # Perform a round-trip object verification -
        # 1. Generate random objects in uploaded_objects_dir
        # 2. Upload the objects to the bucket
        # 3. Download the objects from the bucket
        # 4. Compare the object checksums in downloaded_obejcts_dir
        # with the ones in uploaded_objects_dir
        for count, bucket in enumerate(test_buckets):
            assert random_object_round_trip_verification(
                io_pod=awscli_pod_session,
                bucket_name=bucket.name,
                upload_dir=uploaded_objects_dir + f"Bucket{count}",
                download_dir=downloaded_obejcts_dir + f"Bucket{count}",
                amount=object_amount,
                pattern=test_buckets_pattern,
                mcg_obj=mcg_obj_session,
            ), "Some or all written objects were not found in the list of downloaded objects"

        # E2E TODO: Create RGW kafka notification & see the objects are notified to kafka

        # Create two MCG buckets with a bidirectional replication policy
        bucketclass = {
            "interface": "OC",
            "backingstore_dict": {"aws": [(1, "eu-central-1")]},
        }
        first_bidi_bucket_name = bucket_factory(bucketclass=bucketclass)[0].name
        replication_policy = ("basic-replication-rule", first_bidi_bucket_name, None)
        second_bidi_bucket_name = bucket_factory(
            1, bucketclass=bucketclass, replication_policy=replication_policy
        )[0].name
        patch_replication_policy_to_bucket(
            first_bidi_bucket_name, "basic-replication-rule-2", second_bidi_bucket_name
        )

        bidi_uploaded_objs_dir_1 = uploaded_objects_dir + "/bidi_1"
        bidi_uploaded_objs_dir_2 = uploaded_objects_dir + "/bidi_2"
        bidi_downloaded_objs_dir_1 = downloaded_obejcts_dir + "/bidi_1"
        bidi_downloaded_objs_dir_2 = downloaded_obejcts_dir + "/bidi_2"

        # Verify replication is working as expected by performing a two-way round-trip object verification
        random_object_round_trip_verification(
            io_pod=awscli_pod_session,
            bucket_name=first_bidi_bucket_name,
            upload_dir=bidi_uploaded_objs_dir_1,
            download_dir=bidi_downloaded_objs_dir_1,
            amount=object_amount,
            pattern=first_bidirectional_pattern,
            wait_for_replication=True,
            second_bucket_name=second_bidi_bucket_name,
            mcg_obj=mcg_obj_session,
        )

        random_object_round_trip_verification(
            io_pod=awscli_pod_session,
            bucket_name=second_bidi_bucket_name,
            upload_dir=bidi_uploaded_objs_dir_2,
            download_dir=bidi_downloaded_objs_dir_2,
            amount=object_amount,
            pattern=second_bidirectional_pattern,
            wait_for_replication=True,
            second_bucket_name=first_bidi_bucket_name,
            mcg_obj=mcg_obj_session,
        )

        # Create a cache bucket
        cache_bucketclass = {
            "interface": "OC",
            "namespace_policy_dict": {
                "type": "Cache",
                "ttl": 3600000,
                "namespacestore_dict": {
                    "aws": [(1, "eu-central-1")],
                },
            },
            "placement_policy": {
                "tiers": [{"backingStores": [constants.DEFAULT_NOOBAA_BACKINGSTORE]}]
            },
        }
        cache_bucket = bucket_factory(bucketclass=cache_bucketclass)[0]

        cache_uploaded_objs_dir = uploaded_objects_dir + "/cache"
        cache_uploaded_objs_dir_2 = uploaded_objects_dir + "/cache_2"
        cache_downloaded_objs_dir = downloaded_obejcts_dir + "/cache"
        underlying_bucket_name = cache_bucket.bucketclass.namespacestores[0].uls_name

        # Upload a random object to the bucket
        objs_written_to_cache_bucket = write_random_test_objects_to_bucket(
            awscli_pod_session,
            cache_bucket.name,
            cache_uploaded_objs_dir,
            pattern=cache_pattern,
            mcg_obj=mcg_obj_session,
        )
        wait_for_cache(mcg_obj_session, cache_bucket.name, objs_written_to_cache_bucket)
        # Write a random, larger object directly to the underlying storage of the bucket
        write_random_test_objects_to_bucket(
            awscli_pod_session,
            underlying_bucket_name,
            cache_uploaded_objs_dir_2,
            pattern=cache_pattern,
            s3_creds=cld_mgr.aws_client.nss_creds,
        )
        # Download the object from the cache bucket
        sync_object_directory(
            awscli_pod_session,
            f"s3://{cache_bucket.name}",
            cache_downloaded_objs_dir,
            mcg_obj_session,
        )
        # Make sure the cached object was returned, and not the one that was written to the underlying storage
        assert compare_directory(
            awscli_pod_session,
            cache_uploaded_objs_dir,
            cache_downloaded_objs_dir,
            amount=1,
            pattern=cache_pattern,
        ), "The uploaded and downloaded cached objects have different checksums"
        assert (
            compare_directory(
                awscli_pod_session,
                cache_uploaded_objs_dir_2,
                cache_downloaded_objs_dir,
                amount=1,
                pattern=cache_pattern,
            )
            is False
        ), "The cached object was replaced by the new one before the TTL has expired"
        return {
            "test_buckets": test_buckets,
            "test_buckets_upload_dir": uploaded_objects_dir,
            "object_amount": object_amount,
            "test_buckets_pattern": test_buckets_pattern,
            "first_bidi_bucket_name": first_bidi_bucket_name,
            "bidi_downloaded_objs_dir_2": bidi_downloaded_objs_dir_2,
            "first_bidirectional_pattern": first_bidirectional_pattern,
            "second_bidi_bucket_name": second_bidi_bucket_name,
            "second_bidirectional_pattern": second_bidirectional_pattern,
            "cache_bucket_name": cache_bucket.name,
            "cache_pattern": cache_pattern,
            "cache_downloaded_objs_dir": cache_downloaded_objs_dir,
        }

    return mcg_system_setup


@pytest.fixture()
def verify_mcg_system_recovery(
    request,
    awscli_pod_session,
    mcg_obj_session,
):
    def mcg_system_recovery_check(mcg_sys_setup_dict):
        # Giving the dict an alias for readability
        a = mcg_sys_setup_dict

        # Verify the integrity of all objects in all buckets post-recovery
        for count, bucket in enumerate(a["test_buckets"]):
            compare_object_checksums_between_bucket_and_local(
                awscli_pod_session,
                mcg_obj_session,
                bucket.name,
                a["test_buckets_upload_dir"] + f"Bucket{count}",
                amount=a["object_amount"],
                pattern=a["test_buckets_pattern"],
            )

        compare_object_checksums_between_bucket_and_local(
            awscli_pod_session,
            mcg_obj_session,
            a["first_bidi_bucket_name"],
            a["bidi_downloaded_objs_dir_2"],
            amount=a["object_amount"],
            pattern=a["first_bidirectional_pattern"],
        )
        compare_object_checksums_between_bucket_and_local(
            awscli_pod_session,
            mcg_obj_session,
            a["second_bidi_bucket_name"],
            a["bidi_downloaded_objs_dir_2"],
            amount=a["object_amount"],
            pattern=a["second_bidirectional_pattern"],
        )

        compare_object_checksums_between_bucket_and_local(
            awscli_pod_session,
            mcg_obj_session,
            a["cache_bucket_name"],
            a["cache_downloaded_objs_dir"],
            pattern=a["cache_pattern"],
        )

    return mcg_system_recovery_check


@pytest.fixture(scope="class")
def benchmark_fio_factory_fixture(request):
    bmo_fio_obj = BenchmarkOperatorFIO()

    def factory(
        total_size=2,
        jobs="read",
        read_runtime=30,
        bs="4096KiB",
        storageclass=constants.DEFAULT_STORAGECLASS_RBD,
        timeout_completed=2400,
    ):
        bmo_fio_obj.setup_benchmark_fio(
            total_size=total_size,
            jobs=jobs,
            read_runtime=read_runtime,
            bs=bs,
            storageclass=storageclass,
            timeout_completed=timeout_completed,
        )
        bmo_fio_obj.run_fio_benchmark_operator()

    def finalizer():
        """
        Clean up

        """
        # Clean up
        bmo_fio_obj.cleanup()

    request.addfinalizer(finalizer)
    return factory


def pytest_collection_modifyitems(items):
    """
    A pytest hook to
    Args:
        items: list of collected tests
    """
    skip_list = [
        "test_create_scale_pods_and_pvcs_using_kube_job_ms",
        "test_create_scale_pods_and_pvcs_with_ms_consumer",
        "test_create_scale_pods_and_pvcs_with_ms_consumers",
        "test_create_and_delete_scale_pods_and_pvcs_with_ms_consumers",
    ]
    if not config.ENV_DATA["platform"].lower() in constants.MANAGED_SERVICE_PLATFORMS:
        for item in items.copy():
            if str(item.name) in skip_list:
                logger.debug(
                    f"Test {item} is removed from the collected items"
                    f" since it requires Managed service platform"
                )
                items.remove(item)
