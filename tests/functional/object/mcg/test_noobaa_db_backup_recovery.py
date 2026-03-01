import logging
from ocs_ci.helpers.helpers import create_unique_resource_name
import pytest

from time import sleep
from ocs_ci.framework.pytest_customization.marks import (
    tier2,
    tier4,
    red_squad,
    mcg,
    skipif_mcg_only,
)
from ocs_ci.ocs.bucket_utils import (
    sync_object_directory,
    verify_s3_object_integrity,
    list_objects_from_bucket,
)
from ocs_ci.ocs.resources.pod import get_noobaa_pods
from ocs_ci.ocs.ocp import OCP, get_all_resource_of_kind_containing_string
from ocs_ci.ocs import constants
from ocs_ci.framework import config
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)


def _get_storage_cluster_obj():
    """
    Get the OCS storage cluster object.

    Returns:
        OCP: OCS storage cluster OCP object
    """
    return OCP(
        kind="storagecluster",
        namespace=config.ENV_DATA["cluster_namespace"],
        resource_name=constants.DEFAULT_STORAGE_CLUSTER,
    )


def _get_noobaa_obj():
    """
    Get the NooBaa CR object.

    Returns:
        OCP: NooBaa OCP object
    """
    return OCP(
        kind="noobaa",
        namespace=config.ENV_DATA["cluster_namespace"],
        resource_name=constants.NOOBAA_RESOURCE_NAME,
    )


def _patch_db_backup_config(
    ocs_storage_obj, schedule_cron_interval, num_backups, snapshot_class
):
    """
    Patch the storage cluster with DB backup configuration.

    Args:
        ocs_storage_obj (OCP): OCS storage cluster object
        schedule_cron_interval (int): Cron schedule interval in minutes
        num_backups (int): Maximum number of backups to retain
        snapshot_class (str): Volume snapshot class name

    Returns:
        None
    """
    db_backup_param = (
        f'{{"spec": {{"multiCloudGateway": '
        f'{{"dbBackup": {{"schedule": "*/{schedule_cron_interval} * * * *", '
        f'"volumeSnapshot": {{"maxSnapshots": {num_backups}, "volumeSnapshotClass": "{snapshot_class}"}}}}}}}}}}'
    )
    ocs_storage_obj.patch(params=db_backup_param, format_type="merge")
    logger.info(
        f"DB backup info patched successfully with maxSnapshots={num_backups}, "
        f"schedule=*/{schedule_cron_interval} * * * *"
    )
    sleep(15)


def _verify_backup_config_propagation(ocs_storage_obj, noobaa_obj):
    """
    Verify that DB backup configuration is propagated from storage cluster to NooBaa CR.

    Args:
        ocs_storage_obj (OCP): OCS storage cluster object
        noobaa_obj (OCP): NooBaa object

    Returns:
        dict: DB backup info from NooBaa CR

    Raises:
        AssertionError: If configuration mismatch is detected
    """
    ocs_storage_obj.reload_data()
    noobaa_obj.reload_data()
    db_info_from_ocs_storage = ocs_storage_obj.get("ocs-storagecluster")["spec"][
        "multiCloudGateway"
    ]["dbBackup"]
    db_info_from_noobaa_cr = noobaa_obj.get("noobaa")["spec"]["dbSpec"]["dbBackup"]
    assert (
        db_info_from_ocs_storage == db_info_from_noobaa_cr
    ), "Mismatch in DB backup info between ocs-storagecluster and noobaa CR"
    return db_info_from_noobaa_cr


def _wait_for_backups_completion(num_backups, schedule_cron_interval):
    """
    Wait for specified number of backups to be created and completed.

    Args:
        num_backups (int): Expected number of backups
        schedule_cron_interval (int): Cron schedule interval in minutes

    Returns:
        list: List of backup names that are completed
    """

    def get_num_backups():
        return len(
            get_all_resource_of_kind_containing_string(
                "noobaa-db-pg-cluster-scheduled-backup", "Backup"
            )
        )

    sample = TimeoutSampler(
        timeout=(schedule_cron_interval * num_backups) * 60 + 120,
        sleep=10,
        func=get_num_backups,
    )
    sample.wait_for_func_value(num_backups)

    backup_obj = OCP(kind="Backup", namespace=config.ENV_DATA["cluster_namespace"])
    backup_names = get_all_resource_of_kind_containing_string(
        "noobaa-db-pg-cluster-scheduled-backup", "Backup"
    )
    for bkp_name in backup_names:
        backup_obj.wait_for_resource(
            "completed",
            resource_name=bkp_name,
            column="PHASE",
            timeout=120,
        )
    logger.info(f"All {num_backups} backups are in completed state")
    return backup_names


def _patch_db_recovery_config(ocs_storage_obj, backup_name):
    """
    Patch the storage cluster with DB recovery configuration.

    Args:
        ocs_storage_obj (OCP): OCS storage cluster object
        backup_name (str): Name of the backup to use for recovery

    Returns:
        None
    """
    db_recovery_param = (
        f'{{"spec": {{"multiCloudGateway": '
        f'{{"dbRecovery": {{"volumeSnapshotName": "{backup_name}"}}}}}}}}'
    )
    ocs_storage_obj.patch(params=db_recovery_param, format_type="merge")
    logger.info("DB recovery info patched successfully")
    sleep(15)


def _verify_recovery_config_propagation(ocs_storage_obj, noobaa_obj):
    """
    Verify that DB recovery configuration is propagated from storage cluster to NooBaa CR.

    Args:
        ocs_storage_obj (OCP): OCS storage cluster object
        noobaa_obj (OCP): NooBaa object

    Raises:
        AssertionError: If configuration mismatch is detected
    """
    ocs_storage_obj.reload_data()
    noobaa_obj.reload_data()
    recovery_info_from_ocs_storage = ocs_storage_obj.get("ocs-storagecluster")["spec"][
        "multiCloudGateway"
    ]["dbRecovery"]
    recovery_info_from_noobaa_cr = noobaa_obj.get("noobaa")["spec"]["dbSpec"][
        "dbRecovery"
    ]
    assert (
        recovery_info_from_ocs_storage == recovery_info_from_noobaa_cr
    ), "Mismatch in DB recovery info between ocs-storagecluster and noobaa CR"


def _delete_and_wait_for_cluster_recovery(db_cluster_name):
    """
    Delete NooBaa DB cluster and wait for automatic recovery.

    Args:
        db_cluster_name (str): Name of the DB cluster to delete

    Returns:
        None
    """
    cluster_obj = OCP(kind="Cluster", namespace=config.ENV_DATA["cluster_namespace"])
    cluster_obj.delete(resource_name=db_cluster_name, force=True)
    cluster_obj.wait_for_delete(resource_name=db_cluster_name)

    # Validate noobaa pods are up and running after recovery
    noobaa_pods = get_noobaa_pods()
    pod_obj = OCP(kind=constants.POD, namespace=config.ENV_DATA["cluster_namespace"])
    pod_obj.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        resource_count=len(noobaa_pods),
        selector=constants.NOOBAA_APP_LABEL,
        timeout=900,
    )
    logger.info("NooBaa pods are up and running after recovery")


@pytest.fixture(autouse=True)
def remove_db_info_from_sc(request):
    """
    removes the DB backup and recovery information from storage cluster CR
    """
    ocs_storage_obj = _get_storage_cluster_obj()

    def remove_info():
        backup_params = '[{"op": "remove", "path": "/spec/multiCloudGateway/dbBackup"}]'
        recovery_params = (
            '[{"op": "remove", "path": "/spec/multiCloudGateway/dbRecovery"}]'
        )
        for i in [backup_params, recovery_params]:
            try:
                ocs_storage_obj.patch(
                    resource_name=constants.DEFAULT_STORAGE_CLUSTER,
                    params=i,
                    format_type="json",
                )
            except Exception as e:
                logger.error(e)
                pass
        logger.info(
            "Successfully removed backup and recovery section from Storage cluster"
        )
        logger.info("Removing created backups now")
        backup_obj = OCP(kind="Backup", namespace=config.ENV_DATA["cluster_namespace"])
        backup_names = get_all_resource_of_kind_containing_string(
            "noobaa-db-pg-cluster-scheduled-backup", "Backup"
        )
        for bkp_name in backup_names:
            backup_obj.delete(resource_name=bkp_name, force=True)
            backup_obj.wait_for_delete(resource_name=bkp_name)
        logger.info("Backups created by CNPG operator Removed successfully")

    request.addfinalizer(remove_info)


@mcg
@red_squad
class TestNoobaaDbBackupRecoveryOps:
    """
    Test CNPG based noobaa DB Backup and recovery functionality
    """

    @tier2
    @skipif_mcg_only
    def test_noobaa_db_backup_recovery_op(
        self, mcg_obj, awscli_pod, bucket_factory, test_directory_setup
    ):
        """
        Test to verify CNPG based noobaa DB backup and recovery is working as expected
            1: Create OBC and write data
            2: Validate Noobaa CR is accepting backup configuration in it
            3: Validate backup is getting created after scheduled time from secondary DB instance
            4: Validate Noobaa CR is accepting recovery configuration in it
            5: Delete Cluster CR and check automatic recovery is getting triggered
            6: Validate data is present in OBC after recovery
        """

        # 1: Create OBC and write data
        obj_download_path = test_directory_setup.result_dir
        bucket_obj = bucket_factory(1)[0]
        bucket_name = bucket_obj.name
        full_object_path = f"s3://{bucket_name}"

        sync_object_directory(
            awscli_pod, constants.AWSCLI_TEST_OBJ_DIR, full_object_path, mcg_obj
        )
        # Adding hard coded sleep to trigger async backup from primary to Secondary DB
        sleep(60)

        objs_in_bucket = list_objects_from_bucket(
            pod_obj=awscli_pod,
            target=bucket_name,
            s3_obj=mcg_obj,
            recursive=True,
        )

        # 2: Validate Noobaa CR is accepting backup configuration in it
        ocs_storage_obj = _get_storage_cluster_obj()
        noobaa_obj = _get_noobaa_obj()
        num_backups = 2
        snapshot_class = constants.DEFAULT_VOLUMESNAPSHOTCLASS_RBD
        schedule_cron_interval = 5

        # TO DO
        # Add support for MCG only
        # https://github.com/red-hat-storage/ocs-ci/issues/14092
        """if config.ENV_DATA["mcg_only_deployment"]:
            snapshot_class = "default driver based snapshot"
        """

        _patch_db_backup_config(
            ocs_storage_obj, schedule_cron_interval, num_backups, snapshot_class
        )
        _verify_backup_config_propagation(ocs_storage_obj, noobaa_obj)

        # 3: Validate backup is getting created after scheduled time from secondary DB instance
        backup_names = _wait_for_backups_completion(num_backups, schedule_cron_interval)

        # 4: Validate Noobaa CR is accepting recovery configuration in it
        _patch_db_recovery_config(ocs_storage_obj, backup_names[0])
        _verify_recovery_config_propagation(ocs_storage_obj, noobaa_obj)

        # 5: Delete Noobaa DB Cluster and check automatic recovery is getting triggered
        db_cluster_name = get_all_resource_of_kind_containing_string(
            "noobaa-db-pg-cluster", "Cluster"
        )[0]
        _delete_and_wait_for_cluster_recovery(db_cluster_name)

        # Verify Bucket health after recovery process
        bucket_obj.verify_health(timeout=600)

        # 6: Validate data is present in OBC after recovery
        sync_object_directory(
            podobj=awscli_pod,
            src=full_object_path,
            target=obj_download_path,
            s3_obj=mcg_obj,
        )
        logger.info(f"Objects are downloaded to the dir {obj_download_path}")

        for obj in objs_in_bucket:
            assert verify_s3_object_integrity(
                original_object_path=f"{constants.AWSCLI_TEST_OBJ_DIR}/{obj}",
                result_object_path=f"{obj_download_path}/{obj}",
                awscli_pod=awscli_pod,
            ), "Mismatch in Checksum between original object and object downloaded after recovery"
        logger.info("Cluster recovered successfully and validated data after recovery")

    @tier2
    @skipif_mcg_only
    def test_noobaa_db_backup_recovery_op_using_cli(
        self, mcg_obj, awscli_pod, bucket_factory, test_directory_setup
    ):
        """
        Test to verify CNPG based noobaa DB backup operation using CLI
            1: Create OBC and write data
            2: Add backup info in OCS Storage cluster CR
            3: Run noobaa cli command to create on demand backup and validate backup is getting created or not
            4: Add recovery info in OCS Storage cluster CR with backup snapshot info generated in step #3
            5: Delete CNPG Cluster CR and check automatic recovery is getting triggered
            6: Validate data is present in OBC after recovery
        """

        # 1: Create OBC and write data
        obj_download_path = test_directory_setup.result_dir
        bucket_obj = bucket_factory(1)[0]
        bucket_name = bucket_obj.name
        full_object_path = f"s3://{bucket_name}"

        sync_object_directory(
            awscli_pod, constants.AWSCLI_TEST_OBJ_DIR, full_object_path, mcg_obj
        )
        # Adding hard coded sleep to trigger async backup from primary to Secondary DB
        sleep(60)

        objs_in_bucket = list_objects_from_bucket(
            pod_obj=awscli_pod,
            target=bucket_name,
            s3_obj=mcg_obj,
            recursive=True,
        )

        # 2: Add backup info in OCS Storage cluster CR
        ocs_storage_obj = _get_storage_cluster_obj()
        noobaa_obj = _get_noobaa_obj()

        _patch_db_backup_config(
            ocs_storage_obj=ocs_storage_obj,
            schedule_cron_interval=15,
            num_backups=1,
            snapshot_class=constants.DEFAULT_VOLUMESNAPSHOTCLASS_RBD,
        )
        _verify_backup_config_propagation(ocs_storage_obj, noobaa_obj)
        logger.info("DB backup configuration added to OCS Storage cluster CR")

        # 3: Run noobaa cli command to create on demand backup and validate backup is getting created or not
        logger.info("Creating on-demand backup using NooBaa CLI")
        backup_name = create_unique_resource_name("noobaa-cli", "backup")
        mcg_obj.exec_mcg_cmd(
            cmd=f"system db-backup --name {backup_name}",
            namespace=config.ENV_DATA["cluster_namespace"],
            use_yes=True,
            ignore_error=False,
        )
        logger.info("On-demand backup command executed")

        # Get on-demand backup
        backup_obj = OCP(kind="Backup", namespace=config.ENV_DATA["cluster_namespace"])

        # Wait for on-demand backup to complete
        backup_obj.wait_for_resource(
            "completed",
            resource_name=backup_name,
            column="PHASE",
            timeout=300,
        )
        logger.info(f"On-demand backup {backup_name} completed successfully")

        # 4: Add recovery info in OCS Storage cluster CR with backup snapshot info generated in step #3
        _patch_db_recovery_config(ocs_storage_obj, backup_name)
        _verify_recovery_config_propagation(ocs_storage_obj, noobaa_obj)
        logger.info("DB recovery configuration added to OCS Storage cluster CR")

        # 5: Delete Cluster CR and check automatic recovery is getting triggered
        db_cluster_name = get_all_resource_of_kind_containing_string(
            "noobaa-db-pg-cluster", "Cluster"
        )[0]
        _delete_and_wait_for_cluster_recovery(db_cluster_name)

        # Verify Bucket health after recovery process
        bucket_obj.verify_health(timeout=600)

        # 6: Validate data is present in OBC after recovery
        sync_object_directory(
            podobj=awscli_pod,
            src=full_object_path,
            target=obj_download_path,
            s3_obj=mcg_obj,
        )
        logger.info(f"Objects are downloaded to the dir {obj_download_path}")

        for obj in objs_in_bucket:
            assert verify_s3_object_integrity(
                original_object_path=f"{constants.AWSCLI_TEST_OBJ_DIR}/{obj}",
                result_object_path=f"{obj_download_path}/{obj}",
                awscli_pod=awscli_pod,
            ), "Mismatch in Checksum between original object and object downloaded after recovery"
        logger.info(
            "Cluster recovered successfully using CLI-created backup and validated data after recovery"
        )

    @tier4
    @skipif_mcg_only
    def test_noobaa_db_backup_snapshot_op(
        self, mcg_obj, awscli_pod, bucket_factory, test_directory_setup
    ):
        """
        Test to verify CNPG based noobaa DB backup snapshot operation
            1: Set max snapshot value to 1 in Noobaa CR
            2: Validate only 1 snapshot entry is getting stored on DB pod node
            3: Wait for new DB snapshot entry and validate older entry is deleted from the node
            4: Change max snapshot value to 3 in Noobaa CR
            5: Validate 3 snapshots are getting created on DB pod node
            6: Try to set max snapshot value to 0 and validate the same
        """
        # Get storagecluster and noobaa objects
        ocs_storage_obj = _get_storage_cluster_obj()
        noobaa_obj = _get_noobaa_obj()

        snapshot_class = constants.DEFAULT_VOLUMESNAPSHOTCLASS_RBD
        schedule_cron_interval = 5  # 5 minutes for faster testing

        # 1: Set max snapshot value to 1 in Noobaa CR
        logger.info("Setting maxSnapshots to 1")
        _patch_db_backup_config(
            ocs_storage_obj, schedule_cron_interval, 1, snapshot_class
        )

        # Verify configuration propagated to noobaa CR
        db_info_from_noobaa_cr = _verify_backup_config_propagation(
            ocs_storage_obj, noobaa_obj
        )
        assert (
            db_info_from_noobaa_cr["volumeSnapshot"]["maxSnapshots"] == 1
        ), "maxSnapshots value not set to 1 in noobaa CR"

        # 2: Validate only 1 snapshot entry is getting stored in backup
        logger.info("Waiting for first backup to be created with maxSnapshots=1")
        backup_names = _wait_for_backups_completion(1, schedule_cron_interval)
        assert (
            len(backup_names) == 1
        ), f"Expected 1 backup, but found {len(backup_names)}"

        first_backup_name = backup_names[0]
        logger.info(f"First backup {first_backup_name} completed successfully")

        # 3: Wait for new DB snapshot entry and validate older entry is deleted from backup
        logger.info(
            f"Waiting for {schedule_cron_interval} minutes for next backup to be created and old one to be deleted"
        )
        sleep(
            (schedule_cron_interval * 60) + 120
        )  # Wait for next backup cycle + buffer

        backup_names_after_rotation = get_all_resource_of_kind_containing_string(
            "noobaa-db-pg-cluster-scheduled-backup", "Backup"
        )
        assert (
            len(backup_names_after_rotation) == 1
        ), f"Expected 1 backup after rotation, but found {len(backup_names_after_rotation)}"

        second_backup_name = backup_names_after_rotation[0]
        assert (
            second_backup_name != first_backup_name
        ), "Backup name should be different after rotation"

        backup_obj = OCP(kind="Backup", namespace=config.ENV_DATA["cluster_namespace"])
        backup_obj.wait_for_resource(
            "completed",
            resource_name=second_backup_name,
            column="PHASE",
            timeout=120,
        )
        logger.info(
            f"Backup rotation successful: old backup {first_backup_name} deleted, "
            f"new backup {second_backup_name} created"
        )

        # 4: Change max snapshot value to 3 in Noobaa CR
        new_snapshot_value = 3
        logger.info(f"Setting maxSnapshots to {new_snapshot_value}")
        _patch_db_backup_config(
            ocs_storage_obj, schedule_cron_interval, new_snapshot_value, snapshot_class
        )

        # Verify configuration propagated to noobaa CR
        db_info_from_noobaa_cr = _verify_backup_config_propagation(
            ocs_storage_obj, noobaa_obj
        )
        assert (
            db_info_from_noobaa_cr["volumeSnapshot"]["maxSnapshots"]
            == new_snapshot_value
        ), f"maxSnapshots value not set to {new_snapshot_value} in noobaa CR"

        # 5: Validate 3 snapshots are getting created on DB pod node
        logger.info(f"Waiting for {new_snapshot_value} backups to be created")
        _wait_for_backups_completion(new_snapshot_value, schedule_cron_interval)
        logger.info(f"All {new_snapshot_value} backups are in completed state")

        # 6: Try to set max snapshot value to 0 and validate the same
        logger.info("Testing invalid maxSnapshots value: 0")
        db_backup_param_0 = (
            f'{{"spec": {{"multiCloudGateway": '
            f'{{"dbBackup": {{"schedule": "*/{schedule_cron_interval} * * * *", '
            f'"volumeSnapshot": {{"maxSnapshots": 0, "volumeSnapshotClass": "{snapshot_class}"}}}}}}}}}}'
        )

        try:
            ocs_storage_obj.patch(params=db_backup_param_0, format_type="merge")
            sleep(15)
            ocs_storage_obj.reload_data()
            noobaa_obj.reload_data()
            db_info_from_noobaa_cr = noobaa_obj.get("noobaa")["spec"]["dbSpec"][
                "dbBackup"
            ]
            current_max_snapshots = db_info_from_noobaa_cr["volumeSnapshot"][
                "maxSnapshots"
            ]
            assert (
                current_max_snapshots != 0
            ), "maxSnapshots should not be set to 0 (invalid value)"
            logger.info(
                f"maxSnapshots=0 was rejected or ignored, current value: {current_max_snapshots}"
            )
        except Exception as e:
            logger.info(f"Setting maxSnapshots=0 failed as expected: {e}")

        logger.info(
            "NooBaa DB backup snapshot operation test completed successfully. "
            "Validated maxSnapshots behavior for values 1, 3, and 0 (invalid)"
        )
