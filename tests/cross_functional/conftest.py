import os
import logging
import boto3
import pytest

from concurrent.futures import ThreadPoolExecutor
from threading import Event

from ocs_ci.utility import version
from ocs_ci.utility.retry import retry
from ocs_ci.framework import config
from ocs_ci.helpers.e2e_helpers import (
    create_muliple_types_provider_obcs,
    validate_mcg_bucket_replicaton,
    validate_mcg_caching,
    validate_mcg_object_expiration,
    validate_rgw_kafka_notification,
    validate_mcg_nsfs_feature,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.amq import AMQ
from ocs_ci.ocs.bucket_utils import (
    compare_object_checksums_between_bucket_and_local,
    compare_directory,
    patch_replication_policy_to_bucket,
    random_object_round_trip_verification,
    sync_object_directory,
    wait_for_cache,
    write_random_test_objects_to_bucket,
    retrieve_verification_mode,
    s3_list_objects_v2,
    bulk_s3_put_bucket_lifecycle_config,
)

from ocs_ci.ocs.benchmark_operator_fio import BenchmarkOperatorFIO
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources import pod, pvc
from ocs_ci.ocs.resources.objectbucket import OBC
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.resources.pod import (
    Pod,
    get_pods_having_label,
)
from ocs_ci.ocs.resources.deployment import Deployment
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.helpers.helpers import (
    wait_for_resource_state,
    modify_statefulset_replica_count,
    validate_pv_delete,
    default_storage_class,
)

from ocs_ci.utility.kms import is_kms_enabled

from ocs_ci.utility.utils import clone_notify

logger = logging.getLogger(__name__)


def restore_mcg_reconcilation(ocs_storagecluster_obj):
    params = '{"spec": {"multiCloudGateway": {"reconcileStrategy": "manage"}}}'
    ocs_storagecluster_obj.patch(
        resource_name=constants.DEFAULT_CLUSTERNAME,
        params=params,
        format_type="merge",
    )


def start_noobaa_services(noobaa_endpoint_dc, noobaa_operator_dc):
    if noobaa_endpoint_dc.get()["spec"]["replicas"] == 0:
        noobaa_endpoint_dc.scale(replicas=1)
    if noobaa_operator_dc.get()["spec"]["replicas"] == 0:
        noobaa_operator_dc.scale(replicas=1)
    modify_statefulset_replica_count(
        statefulset_name=constants.NOOBAA_CORE_STATEFULSET, replica_count=1
    )


@pytest.fixture()
def noobaa_db_backup_and_recovery_locally(
    request, bucket_factory, awscli_pod_session, mcg_obj_session
):
    """
    Test to verify Backup and Restore for Multicloud Object Gateway database locally
    Backup procedure:
        * Create a test bucket and write some data
        * Backup noobaa secrets to local folder OR store it in secret objects
        * Backup the PostgreSQL database and save it to a local folder
        * For testing, write new data to show a little data loss between backup and restore
    Restore procedure:
        * Stop MCG reconciliation
        * Stop the NooBaa Service before restoring the NooBaa DB.
          There will be no object service after this point
        * Verify that all NooBaa components (except NooBaa DB) have 0 replicas
        * Login to the NooBaa DB pod and cleanup potential database clients to nbcore
        * Restore DB from a local folder
        * Delete current noobaa secrets and restore them from a local folder OR secrets objects.
        * Restore MCG reconciliation
        * Start the NooBaa service
        * Restart the NooBaa DB pod
        * Check that the old data exists, but not s3://testloss/

    """
    # OCS storagecluster object
    ocs_storagecluster_obj = OCP(
        namespace=config.ENV_DATA["cluster_namespace"],
        kind=constants.STORAGECLUSTER,
    )

    # OCP object for kind deployment
    ocp_deployment_obj = OCP(
        kind=constants.DEPLOYMENT, namespace=config.ENV_DATA["cluster_namespace"]
    )

    # Noobaa operator & noobaa endpoint deployments objects
    nb_operator_dc = Deployment(
        **ocp_deployment_obj.get(resource_name=constants.NOOBAA_OPERATOR_DEPLOYMENT)
    )
    nb_endpoint_dc = Deployment(
        **ocp_deployment_obj.get(resource_name=constants.NOOBAA_ENDPOINT_DEPLOYMENT)
    )

    secrets_obj = []

    def factory(
        bucket_factory=bucket_factory,
        awscli_pod_session=awscli_pod_session,
        mcg_obj_session=mcg_obj_session,
    ):
        global secrets_obj

        # create bucket and write some objects to it
        test_bucket = bucket_factory()[0]
        write_random_test_objects_to_bucket(
            io_pod=awscli_pod_session,
            file_dir="test_dir",
            pattern="test-object",
            bucket_to_write=test_bucket.name,
            mcg_obj=mcg_obj_session,
        )

        # Backup secrets
        ocp_secret_obj = OCP(
            kind="secret", namespace=config.ENV_DATA["cluster_namespace"]
        )
        secrets = [
            "noobaa-admin",
            "noobaa-operator",
            "noobaa-db",
            "noobaa-server",
            "noobaa-endpoints",
        ]

        if (
            version.get_semantic_ocs_version_from_config() >= version.VERSION_4_14
            and not is_kms_enabled()
        ):
            secrets.extend(
                ["noobaa-root-master-key-backend", "noobaa-root-master-key-volume"]
            )
        elif not is_kms_enabled():
            secrets.append("noobaa-root-master-key")

        secrets_yaml = [
            ocp_secret_obj.get(resource_name=f"{secret}") for secret in secrets
        ]
        secrets_obj = [OCS(**secret_yaml) for secret_yaml in secrets_yaml]
        logger.info("Backed up secrets as secret objects!")

        # Backup the PostgreSQL database and save it to a local folder
        noobaa_db_pod = Pod(
            **get_pods_having_label(
                label=constants.NOOBAA_DB_LABEL_47_AND_ABOVE,
                namespace=config.ENV_DATA["cluster_namespace"],
            )[0]
        )
        noobaa_db_pod.exec_cmd_on_pod(
            command="pg_dump nbcore -f /tmp/test.db -F custom"
        )
        OCP(namespace=config.ENV_DATA["cluster_namespace"]).exec_oc_cmd(
            command=f"cp --retries=-1 {noobaa_db_pod.name}:/tmp/test.db ./mcg.bck",
            out_yaml_format=False,
        )
        logger.info("Backed up PostgreSQL and stored it in local folder!")

        # For testing, write new data to show a little data loss between backup and restore
        testloss_bucket = bucket_factory()[0]
        write_random_test_objects_to_bucket(
            io_pod=awscli_pod_session,
            file_dir="testloss_dir",
            pattern="testloss-object",
            bucket_to_write=testloss_bucket.name,
            mcg_obj=mcg_obj_session,
        )

        # Stop MCG reconcilation
        params = '{"spec": {"multiCloudGateway": {"reconcileStrategy": "ignore"}}}'
        ocs_storagecluster_obj.patch(
            resource_name=constants.DEFAULT_CLUSTERNAME,
            params=params,
            format_type="merge",
        )
        logger.info("Stopped MCG reconcilation!")

        # Stop the NooBaa Service before restoring the NooBaa DB. There will be no object service after this point
        nb_operator_dc.scale(replicas=0)
        nb_endpoint_dc.scale(replicas=0)
        modify_statefulset_replica_count(
            statefulset_name=constants.NOOBAA_CORE_STATEFULSET, replica_count=0
        )
        logger.info(
            "Stopped the noobaa service: Noobaa endpoint, Noobaa core, Noobaa operator pods!!"
        )

        # Login to the NooBaa DB pod and cleanup potential database clients to nbcore
        query = "SELECT pg_terminate_backend (pid) FROM pg_stat_activity WHERE datname = 'nbcore';"
        noobaa_db_pod.exec_cmd_on_pod(
            command=f'psql -h 127.0.0.1 -p 5432 -U postgres -c "{query}"'
        )
        logger.info("Cleaned up potential database clients to nbcore!")

        # Restore DB from a local folder
        OCP(namespace=config.ENV_DATA["cluster_namespace"]).exec_oc_cmd(
            command=f"cp ./mcg.bck {noobaa_db_pod.name}:/tmp/test.db"
        )
        noobaa_db_pod.exec_cmd_on_pod(command="pg_restore -d nbcore /tmp/test.db -c")
        logger.info("Restored DB from the local folder!")

        # Delete secrets and restore them from a local folder.
        # Please note that verify that there are no errors before you proceed to the next steps.
        for secret in secrets_obj:
            secret.delete()
        logger.info(f"Deleted current Noobaa secrets: {secrets}!")
        for secret in secrets_obj:
            secret.create()
        logger.info(f"Restored old Noobaa secrets: {secrets}")

        # Restore MCG reconciliation
        restore_mcg_reconcilation(ocs_storagecluster_obj)
        logger.info("Restored MCG reconcilation!")

        # Start the NooBaa service
        nb_operator_dc.scale(replicas=1)
        nb_endpoint_dc.scale(replicas=1)
        modify_statefulset_replica_count(
            statefulset_name=constants.NOOBAA_CORE_STATEFULSET, replica_count=1
        )
        logger.info(
            "Started noobaa services: Noobaa endpoint, Noobaa core, Noobaa operator pods!"
        )

        # Restart the NooBaa DB pod
        noobaa_db_pod.delete()
        logger.info("Restarted noobaa-db pod!")

        # Make sure the testloss bucket doesn't exists and test bucket consists all the data
        @retry(Exception, tries=10, delay=5)
        def check_for_buckets_content(bucket):
            try:
                response = s3_list_objects_v2(
                    s3_obj=mcg_obj_session, bucketname=bucket.name
                )
                logger.info(response)
                return response
            except Exception as err:
                if "The specified bucket does not exist" in err.args[0]:
                    return err.args[0]
                else:
                    raise

        assert "The specified bucket does not exist" in check_for_buckets_content(
            testloss_bucket
        ), "Test loss bucket exists even though it shouldn't be present in the recovered db"

        assert (
            check_for_buckets_content(test_bucket)["KeyCount"] == 1
        ), "test bucket doesnt consists of data post db recovery"

    def finalizer():

        global secrets_obj

        # remove the local copy of ./mcg.bck
        if os.path.exists("./mcg.bck"):
            os.remove("mcg.bck")
            logger.info("Removed the local copy of mcg.bck")

        # create the secrets if they're deleted
        for secret in secrets_obj:
            if secret.is_deleted:
                secret.create()
            else:
                logger.info(f"{secret.name} is not deleted!")

        # restore MCG reconcilation if not restored already
        if (
            ocs_storagecluster_obj.get(resource_name=constants.DEFAULT_CLUSTERNAME)[
                "spec"
            ]["multiCloudGateway"]["reconcileStrategy"]
            != "manage"
        ):
            restore_mcg_reconcilation(ocs_storagecluster_obj)
            logger.info("MCG reconcilation restored!")

        # start noobaa services if its down
        ocp_deployment_obj = OCP(
            kind=constants.DEPLOYMENT, namespace=config.ENV_DATA["cluster_namespace"]
        )
        nb_operator_dc = Deployment(
            **ocp_deployment_obj.get(resource_name=constants.NOOBAA_OPERATOR_DEPLOYMENT)
        )
        nb_endpoint_dc = Deployment(
            **ocp_deployment_obj.get(resource_name=constants.NOOBAA_ENDPOINT_DEPLOYMENT)
        )
        start_noobaa_services(nb_endpoint_dc, nb_operator_dc)

    request.addfinalizer(finalizer)
    return factory


@pytest.fixture()
def noobaa_db_backup(request, snapshot_factory):
    restore_pvc_objs = []

    def factory(noobaa_pvc_obj):

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
        return restore_pvc_objs, snap_obj

    return factory


@pytest.fixture()
def noobaa_db_recovery_from_backup(request):
    def factory(snap_obj, noobaa_pvc_obj, noobaa_pods):
        noobaa_pv_name = noobaa_pvc_obj[0].get("spec").get("spec").get("volumeName")

        # Scale down the statefulset noobaa-db
        modify_statefulset_replica_count(
            statefulset_name=constants.NOOBAA_DB_STATEFULSET, replica_count=0
        ), f"Failed to scale down the statefulset {constants.NOOBAA_DB_STATEFULSET}"

        # Get the noobaa-db PVC
        pvc_obj = OCP(
            kind=constants.PVC, namespace=config.ENV_DATA["cluster_namespace"]
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
        pv_obj = OCP(kind=constants.PV, namespace=config.ENV_DATA["cluster_namespace"])
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
        pod_obj = OCP(
            kind=constants.POD, namespace=config.ENV_DATA["cluster_namespace"]
        )
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

    return factory


@pytest.fixture()
def noobaa_db_backup_and_recovery(
    request, snapshot_factory, noobaa_db_backup, noobaa_db_recovery_from_backup
):
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
        nonlocal restore_pvc_objs
        # Get noobaa pods before execution
        noobaa_pods = pod.get_noobaa_pods()

        # Get noobaa PVC before execution
        noobaa_pvc_obj = pvc.get_pvc_objs(pvc_names=["db-noobaa-db-pg-0"])

        restore_pvc_objs, snap_obj = noobaa_db_backup(noobaa_pvc_obj)
        noobaa_db_recovery_from_backup(snap_obj, noobaa_pvc_obj, noobaa_pods)

    def finalizer():
        # Get the statefulset replica count
        sst_obj = OCP(
            kind=constants.STATEFULSET,
            namespace=config.ENV_DATA["cluster_namespace"],
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
        wait_for_cache(
            mcg_obj_session,
            cache_bucket.name,
            objs_written_to_cache_bucket,
            timeout=600,
        )
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


@pytest.fixture()
def setup_mcg_replication_feature_buckets(request, bucket_factory):
    """
    This fixture does the setup for validating MCG replication
    feature

    """

    def factory(number_of_buckets, bucket_types, cloud_providers):
        """
        factory function implementing the fixture

        Args:
            number_of_buckets (int): number of buckets
            bucket_types (dict): dictionary mapping bucket types and
                configuration
            cloud_providers (dict): dictionary mapping cloud provider
                and configuration

        Returns:
            Dict: source bucket to target bucket map

        """
        all_buckets = create_muliple_types_provider_obcs(
            number_of_buckets, bucket_types, cloud_providers, bucket_factory
        )

        if len(all_buckets) % 2 != 0:
            all_buckets[len(all_buckets) - 1].delete()
            all_buckets.remove(all_buckets[len(all_buckets) - 1])

        source_target_map = dict()
        index = 0
        for i in range(len(all_buckets) // 2):
            source_target_map[all_buckets[index]] = all_buckets[index + 1]
            patch_replication_policy_to_bucket(
                all_buckets[index].name,
                "basic-replication-rule-1",
                all_buckets[index + 1].name,
                prefix="bidi_1",
            )
            patch_replication_policy_to_bucket(
                all_buckets[index + 1].name,
                "basic-replication-rule-2",
                all_buckets[index].name,
                prefix="bidi_2",
            )

            index += 2

        logger.info(
            f"Buckets created under replication setup: {[bucket.name for bucket in all_buckets]}"
        )
        return all_buckets, source_target_map

    return factory


@pytest.fixture()
def setup_mcg_caching_feature_buckets(request, bucket_factory):
    """
    This fixture does the setup for Noobaa cache buckets validation

    """

    def factory(number_of_buckets, bucket_types, cloud_providers):
        """
        factory function implementing fixture

        Args:
            number_of_buckets (int): number of buckets
            bucket_types (dict): dictionary mapping bucket types and
                configuration
            cloud_providers (dict): dictionary mapping cloud provider
                and configuration

        Returns:
            List: List of cache buckets

        """
        cache_type = dict()
        cache_type["cache"] = bucket_types["cache"]
        all_buckets = create_muliple_types_provider_obcs(
            number_of_buckets, cache_type, cloud_providers, bucket_factory
        )
        logger.info(
            f"These are the cache buckets created: {[bucket.name for bucket in all_buckets]}"
        )
        return all_buckets

    return factory


@pytest.fixture()
def setup_mcg_expiration_feature_buckets(
    request, bucket_factory, mcg_obj, reduce_expiration_interval
):
    """
    This fixture does the setup for validating MCG replication
    feature

    """

    def factory(number_of_buckets, bucket_types, cloud_providers):
        """
        Factory function implementing the fixture

        Args:
            number_of_buckets (int): number of buckets
            bucket_types (dict): dictionary mapping bucket types and
                configuration
            cloud_providers (dict): dictionary mapping cloud provider
                and configuration

        Returns:
            List: list of buckets

        """
        type = dict()
        type["data"] = bucket_types["data"]
        reduce_expiration_interval(interval=1)
        logger.info("Changed noobaa lifecycle interval to 1 minute")

        expiration_rule = {
            "Rules": [
                {
                    "Expiration": {
                        "Days": 1,
                        "ExpiredObjectDeleteMarker": False,
                    },
                    "Filter": {"Prefix": ""},
                    "ID": "data-expire",
                    "Status": "Enabled",
                }
            ]
        }

        all_buckets = create_muliple_types_provider_obcs(
            number_of_buckets, type, cloud_providers, bucket_factory
        )

        bulk_s3_put_bucket_lifecycle_config(mcg_obj, all_buckets, expiration_rule)

        logger.info(
            f"Buckets created under expiration setup: {[bucket.name for bucket in all_buckets]}"
        )
        return all_buckets

    return factory


@pytest.fixture()
def setup_mcg_nsfs_feature_buckets(request):
    def factory():
        pass


@pytest.fixture()
def setup_rgw_kafka_notification(request, rgw_bucket_factory, rgw_obj):
    """
    This fixture does the setup for validating RGW kafka
    notification feature

    """

    # setup AMQ
    amq = AMQ()

    kafka_topic = kafkadrop_pod = kafkadrop_svc = kafkadrop_route = None

    # get storageclass
    storage_class = default_storage_class(interface_type=constants.CEPHBLOCKPOOL)

    # setup AMQ cluster
    amq.setup_amq_cluster(storage_class.name)

    # create kafka topic
    kafka_topic = amq.create_kafka_topic()

    # create kafkadrop pod
    (
        kafkadrop_pod,
        kafkadrop_svc,
        kafkadrop_route,
    ) = amq.create_kafkadrop()

    def factory():
        """
        Factory function implementing the fixture

        Returns:
            Dict: This consists of mapping of rgw buckets,
                kafka_topic, kafkadrop_host objects etc

        """

        # get the kafkadrop route
        kafkadrop_host = kafkadrop_route.get().get("spec").get("host")

        # create the bucket
        bucketname = rgw_bucket_factory(amount=1, interface="RGW-OC")[0].name

        # get RGW credentials
        rgw_endpoint, access_key, secret_key = rgw_obj.get_credentials()

        # clone notify repo
        notify_path = clone_notify()

        # initilize to upload data
        data = "A random string data to write on created rgw bucket"
        obc_obj = OBC(bucketname)
        s3_resource = boto3.resource(
            "s3",
            verify=retrieve_verification_mode(),
            endpoint_url=rgw_endpoint,
            aws_access_key_id=obc_obj.access_key_id,
            aws_secret_access_key=obc_obj.access_key,
        )
        s3_client = s3_resource.meta.client

        # Initialize notify command to run
        notify_cmd = (
            f"python {notify_path} -e {rgw_endpoint} -a {obc_obj.access_key_id} "
            f"-s {obc_obj.access_key} -b {bucketname} "
            f"-ke {constants.KAFKA_ENDPOINT} -t {kafka_topic.name}"
        )

        kafka_rgw_dict = {
            "s3client": s3_client,
            "kafka_rgw_bucket": bucketname,
            "notify_cmd": notify_cmd,
            "data": data,
            "kafkadrop_host": kafkadrop_host,
            "kafka_topic": kafka_topic,
        }

        return kafka_rgw_dict

    def finalizer():
        if kafka_topic:
            kafka_topic.delete()
        if kafkadrop_pod:
            kafkadrop_pod.delete()
        if kafkadrop_svc:
            kafkadrop_svc.delete()
        if kafkadrop_route:
            kafkadrop_route.delete()

        amq.cleanup()

    request.addfinalizer(finalizer)
    return factory


@pytest.fixture()
def validate_mcg_bg_features(
    request, awscli_pod_session, mcg_obj_session, test_directory_setup, cld_mgr
):
    """
    This fixture validates specified features provided neccesary
    feature setup map. It has option to run the validation to run
    in the background while not blocking the execution of rest of
    the code

    """

    def factory(
        feature_setup_map,
        run_in_bg=False,
        skip_any_features=None,
        object_amount=5,
    ):
        """
        factory functon implementing the fixture

        Args:
            feature_setup_map (Dict): This has feature to setup of buckets map
                consists of buckets, executor, event objects
            run_in_bg (Bool): True if want to run the validation in background
            skip_any_features (List): List consisting of features that dont need
                to be validated
            object_amount (int): Number of objects that you wanna use while doing
                the validation

        Returns:
            Event(): this is a threading.Event() object used to send signals to the
                threads to stop
            List: List consisting of all the futures objects, ie. threads

        """
        uploaded_objects_dir = test_directory_setup.origin_dir
        downloaded_obejcts_dir = test_directory_setup.result_dir
        futures_obj = list()

        # if any already running background validation threads
        # then stop those threads
        if feature_setup_map["executor"]["event"] is not None:
            feature_setup_map["executor"]["event"].set()
            for t in feature_setup_map["executor"]["threads"]:
                t.result()

        event = Event()
        executor = ThreadPoolExecutor(
            max_workers=(
                5 - len(skip_any_features) if skip_any_features is not None else 5
            )
        )
        skip_any_features = list() if skip_any_features is None else skip_any_features

        if "replication" not in skip_any_features:
            validate_replication = executor.submit(
                validate_mcg_bucket_replicaton,
                awscli_pod_session,
                mcg_obj_session,
                feature_setup_map["replication"],
                uploaded_objects_dir,
                downloaded_obejcts_dir,
                event,
                run_in_bg=run_in_bg,
                object_amount=object_amount,
            )
            futures_obj.append(validate_replication)

        if "caching" not in skip_any_features:
            validate_caching = executor.submit(
                validate_mcg_caching,
                awscli_pod_session,
                mcg_obj_session,
                cld_mgr,
                feature_setup_map["caching"],
                uploaded_objects_dir,
                downloaded_obejcts_dir,
                event,
                run_in_bg=run_in_bg,
            )
            futures_obj.append(validate_caching)

        if "expiration" not in skip_any_features:
            validate_expiration = executor.submit(
                validate_mcg_object_expiration,
                mcg_obj_session,
                feature_setup_map["expiration"],
                event,
                run_in_bg=run_in_bg,
                object_amount=object_amount,
            )
            futures_obj.append(validate_expiration)

        if "rgw kafka" not in skip_any_features:
            validate_rgw_kafka = executor.submit(
                validate_rgw_kafka_notification,
                feature_setup_map["rgw kafka"],
                event,
                run_in_bg=run_in_bg,
            )
            futures_obj.append(validate_rgw_kafka)

        if "nsfs" not in skip_any_features:
            validate_nsfs = executor.submit(
                validate_mcg_nsfs_feature,
            )
            futures_obj.append(validate_nsfs)

        # if not run in background we wait until the
        # threads are finsihed executing, ie. single iteration
        if not run_in_bg:
            for t in futures_obj:
                t.result()
            event = None

        return event, futures_obj

    return factory


@pytest.fixture()
def setup_mcg_bg_features(
    request,
    test_directory_setup,
    awscli_pod_session,
    mcg_obj_session,
    setup_mcg_replication_feature_buckets,
    setup_mcg_caching_feature_buckets,
    setup_mcg_nsfs_feature_buckets,
    setup_mcg_expiration_feature_buckets,
    # setup_rgw_kafka_notification,
    validate_mcg_bg_features,
):
    """
    Fixture to setup MCG features buckets, run IOs, validate IOs

    1. Bucket replication
    2. Noobaa caching
    3. Object expiration
    4. MCG NSFS
    5. RGW kafka notification

    """

    def factory(
        num_of_buckets=10,
        object_amount=5,
        is_disruptive=True,
        skip_any_type=None,
        skip_any_provider=None,
        skip_any_features=None,
    ):
        """
        Args:
            num_of_buckets(int): Number of buckets for each MCG features
            is_disruptive(bool): Is the test calling this has disruptive flow?
            skip_any_type(list): If you want to skip any types of OBCs
            skip_any_provider(list): If you want to skip any cloud provider
            skip_any_features(list): If you want to skip any MCG features

        Returns:
            Dict: Representing all the buckets created for the respective features,
            executor and event objects

        """

        bucket_types = {
            "data": {
                "interface": "OC",
                "backingstore_dict": {},
            },
            "namespace": {
                "interface": "OC",
                "namespace_policy_dict": {
                    "type": "Single",
                    "namespacestore_dict": {},
                },
            },
            "cache": {
                "interface": "OC",
                "namespace_policy_dict": {
                    "type": "Cache",
                    "ttl": 300000,
                    "namespacestore_dict": {},
                },
                "placement_policy": {
                    "tiers": [
                        {"backingStores": [constants.DEFAULT_NOOBAA_BACKINGSTORE]}
                    ]
                },
            },
        }

        # skip if any bucket types one wants to skip
        if skip_any_type is not None:
            for type in skip_any_type:
                if type not in bucket_types.keys():
                    logger.error(
                        f"Bucket type {type} you asked to skip is not valid type "
                        f"and valid are {list(bucket_types.keys())}"
                    )
                else:
                    bucket_types.pop(type)

        cloud_providers = {
            "aws": (1, "eu-central-1"),
            "azure": (1, None),
            "pv": (
                1,
                constants.MIN_PV_BACKINGSTORE_SIZE_IN_GB,
                "ocs-storagecluster-ceph-rbd",
            ),
        }

        # skip any cloud providers if one wants to skip
        if skip_any_provider is not None:
            for provider in skip_any_provider:
                if provider not in cloud_providers.keys():
                    logger.error(
                        f"Bucket type {provider} you asked to skip is not valid type "
                        f"and valid are {list(cloud_providers.keys())}"
                    )
                else:
                    cloud_providers.pop(provider)

        all_buckets = list()
        feature_setup_map = dict()
        feature_setup_map["executor"] = dict()
        feature_setup_map["executor"]["event"] = None

        # skip any features if one wants to skip
        features = ["replication", "caching", "expiration", "nsfs", "rgw kafka"]
        assert isinstance(skip_any_features, list) and set(skip_any_features).issubset(
            set(features)
        ), f"Features asked to skip either not present or you havent provided through a list, valid: {features}"

        if "replication" not in skip_any_features:
            buckets, source_target_map = setup_mcg_replication_feature_buckets(
                num_of_buckets, bucket_types, cloud_providers
            )
            all_buckets.extend(buckets)
            feature_setup_map["replication"] = source_target_map

        if "caching" not in skip_any_features:
            cache_buckets = setup_mcg_caching_feature_buckets(
                num_of_buckets, bucket_types, cloud_providers
            )
            all_buckets.extend(cache_buckets)
            feature_setup_map["caching"] = cache_buckets

        if "expiration" not in skip_any_features:
            buckets_with_expiration_policy = setup_mcg_expiration_feature_buckets(
                num_of_buckets, bucket_types, cloud_providers
            )
            all_buckets.extend(buckets_with_expiration_policy)
            feature_setup_map["expiration"] = buckets_with_expiration_policy

        if "nsfs" not in skip_any_features:
            setup_mcg_nsfs_feature_buckets()
            feature_setup_map["nsfs"] = None

        # if "rgw kafka" not in skip_any_features:
        #     kafka_rgw_dict = setup_rgw_kafka_notification()
        #     all_buckets.extend([OBC(kafka_rgw_dict["kafka_rgw_bucket"])])
        #     feature_setup_map["rgw kafka"] = kafka_rgw_dict

        uploaded_objects_dir = test_directory_setup.origin_dir
        downloaded_obejcts_dir = test_directory_setup.result_dir

        for count, bucket in enumerate(all_buckets):
            assert random_object_round_trip_verification(
                io_pod=awscli_pod_session,
                bucket_name=bucket.name,
                upload_dir=uploaded_objects_dir + f"Bucket{count}",
                download_dir=downloaded_obejcts_dir + f"Bucket{count}",
                amount=1,
                pattern="Random_object",
                mcg_obj=mcg_obj_session,
                cleanup=True,
            ), "Some or all written objects were not found in the list of downloaded objects"
        logger.info("Successful object round trip verification")

        event, threads = validate_mcg_bg_features(
            feature_setup_map,
            run_in_bg=not is_disruptive,
            skip_any_features=skip_any_features,
            object_amount=object_amount,
        )
        feature_setup_map["executor"]["event"] = event
        feature_setup_map["executor"]["threads"] = threads
        feature_setup_map["all_buckets"] = all_buckets
        return feature_setup_map

    return factory
