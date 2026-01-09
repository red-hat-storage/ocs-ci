import logging
import pytest

from time import sleep
from ocs_ci.framework.pytest_customization.marks import (
    tier2,
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


@pytest.fixture(autouse=True)
def remove_db_info_from_sc(request):
    """
    removes the DB backup and recovery information from storage cluster CR
    """
    ocs_storage_obj = OCP(
        kind="storagecluster",
        namespace=config.ENV_DATA["cluster_namespace"],
        resource_name=constants.DEFAULT_STORAGE_CLUSTER,
    )

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
        # get storagecluster object
        ocs_storage_obj = OCP(
            kind="storagecluster",
            namespace=config.ENV_DATA["cluster_namespace"],
            resource_name=constants.DEFAULT_STORAGE_CLUSTER,
        )
        # patch storagecluster object
        num_backups = 2
        snapshot_class = "vpc-block-snapshot"
        schedule_cron_interval = 5

        # TO DO
        # Add support for MCG only
        # https://github.com/red-hat-storage/ocs-ci/issues/14092
        """if config.ENV_DATA["mcg_only_deployment"]:
            snapshot_class = "default driver based snapshot"
        """

        db_backup_param = (
            f'{{"spec": {{"multiCloudGateway": '
            f'{{"dbBackup": {{"schedule": "*/{schedule_cron_interval} * * * *", '
            f'"volumeSnapshot": {{"maxSnapshots": {num_backups}, "volumeSnapshotClass": "{snapshot_class}"}}}}}}}}}}'
        )

        ocs_storage_obj.patch(params=db_backup_param, format_type="merge")
        logger.info("DB backup info patched successfully")
        # Add sleep to reflect patched values in respective CRs
        sleep(15)
        ocs_storage_obj.reload_data()
        db_info_from_ocs_storage = ocs_storage_obj.get("ocs-storagecluster")["spec"][
            "multiCloudGateway"
        ]["dbBackup"]

        # get noobaa CR object
        noobaa_obj = OCP(
            kind="noobaa",
            namespace=config.ENV_DATA["cluster_namespace"],
            resource_name=constants.NOOBAA_RESOURCE_NAME,
        )
        db_info_from_noobaa_cr = noobaa_obj.get("noobaa")["spec"]["dbSpec"]["dbBackup"]
        assert (
            db_info_from_ocs_storage == db_info_from_noobaa_cr
        ), "Mismatch in DB backup info between ocs-storagecluster and noobaa CR"

        # 3: Validate backup is getting created after scheduled time from secondary DB instance
        def get_num_backups():
            return len(
                get_all_resource_of_kind_containing_string(
                    "noobaa-db-pg-cluster-scheduled-backup", "Backup"
                )
            )

        sample = TimeoutSampler(
            timeout=(schedule_cron_interval * num_backups) * 60 + 60,
            sleep=5,
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
                timeout=100,
            )

        # 4: Validate Noobaa CR is accepting recovery configuration in it
        db_recovery_param = (
            f'{{"spec": {{"multiCloudGateway": '
            f'{{"dbRecovery": {{"volumeSnapshotName": "{backup_names[0]}"}}}}}}}}'
        )

        ocs_storage_obj.patch(params=db_recovery_param, format_type="merge")
        logger.info("DB recovery info patched successfully")
        ocs_storage_obj.reload_data()
        recovery_info_from_ocs_storage = ocs_storage_obj.get("ocs-storagecluster")[
            "spec"
        ]["multiCloudGateway"]["dbRecovery"]
        sleep(15)
        noobaa_obj.reload_data()
        recovery_info_from_noobaa_cr = noobaa_obj.get("noobaa")["spec"]["dbSpec"][
            "dbRecovery"
        ]
        assert (
            recovery_info_from_ocs_storage == recovery_info_from_noobaa_cr
        ), "Mismatch in DB recovery info between ocs-storagecluster and noobaa CR"

        # 5: Delete Noobaa DB Cluster and check automatic recovery is getting triggered
        # get noobaa DB cluster info
        cluster_obj = OCP(
            kind="Cluster", namespace=config.ENV_DATA["cluster_namespace"]
        )
        db_cluster_name = get_all_resource_of_kind_containing_string(
            "noobaa-db-pg-cluster", "Cluster"
        )[0]
        cluster_obj.delete(resource_name=db_cluster_name, force=True)
        cluster_obj.wait_for_delete(resource_name=db_cluster_name)

        # Validate noobaa pods are up and running after considering recovery
        noobaa_pods = get_noobaa_pods()
        pod_obj = OCP(
            kind=constants.POD, namespace=config.ENV_DATA["cluster_namespace"]
        )
        pod_obj.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            resource_count=len(noobaa_pods),
            selector=constants.NOOBAA_APP_LABEL,
            timeout=900,
        )
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
