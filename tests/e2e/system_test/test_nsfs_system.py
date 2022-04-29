import logging

import pytest

from ocs_ci.framework.testlib import MCGTest, system_test
from ocs_ci.framework.pytest_customization.marks import skipif_mcg_only
from ocs_ci.ocs import constants, defaults
from ocs_ci.ocs.bucket_utils import (
    random_object_round_trip_verification,
    compare_directory,
)
from ocs_ci.ocs.resources import pod

from ocs_ci.ocs.resources.mcg_params import NSFS
from ocs_ci.ocs.resources.pod import get_plugin_pods, get_mds_pods
from tests.conftest import snapshot_factory
from tests.e2e.conftest import noobaa_db_backup_and_recovery

logger = logging.getLogger(__name__)


@system_test
@skipif_mcg_only
class TestNSFSSystem(MCGTest):
    """"""

    @pytest.mark.polarion_id("")
    def test_nsfs_system(
        self, nsfs_bucket_factory, awscli_pod_session, test_directory_setup, nsfs_obj
    ):
        """"""
        nsfs_obj_new = NSFS(
            method="OC",
            pvc_size=25,
        )
        nsfs_bucket_factory(nsfs_obj_new)
        nsfs_obj_existing = NSFS(
            method="OC",
            pvc_size=20,
            mount_existing_dir=True,
        )
        nsfs_bucket_factory(nsfs_obj_existing)

        random_object_round_trip_verification(
            io_pod=awscli_pod_session,
            bucket_name=nsfs_obj.bucket_name,
            upload_dir=test_directory_setup.origin_dir,
            download_dir=test_directory_setup.result_dir,
            amount=10,
            pattern="nsfs-test-obj-",
            s3_creds=nsfs_obj.s3_creds,
            result_pod=nsfs_obj.interface_pod,
            result_pod_path=nsfs_obj.mount_path + "/" + nsfs_obj.bucket_name,
        )
        compare_directory(
            awscli_pod=awscli_pod_session,
            original_dir=test_directory_setup.origin_dir,
            result_dir=nsfs_obj.mount_path + "/" + nsfs_obj.bucket_name,
            amount=10,
            pattern="nsfs-obj-",
            result_pod=nsfs_obj.interface_pod,
        )

        pods_to_respin = [
            pod.Pod(
                **pod.get_pods_having_label(
                    label=constants.NOOBAA_CORE_POD_LABEL,
                    namespace=defaults.ROOK_CLUSTER_NAMESPACE,
                )[0]
            ),
            get_plugin_pods(constants.CEPHFILESYSTEM),
            get_mds_pods()[0],
        ]
        for pods in pods_to_respin:
            pods.delete()
            logger.info(f"Validating integrity of object post {pods.name} resping")
            compare_directory(
                awscli_pod=awscli_pod_session,
                original_dir=test_directory_setup.origin_dir,
                result_dir=nsfs_obj.mount_path + "/" + nsfs_obj.bucket_name,
                amount=10,
                pattern="nsfs-obj-",
                result_pod=nsfs_obj.interface_pod,
            )
        # TODO: Partial cluster down and validate
        # TODO: Different S3 ops on nsfs buckets

        noobaa_db_backup_and_recovery(snapshot_factory=snapshot_factory)
        compare_directory(
            awscli_pod=awscli_pod_session,
            original_dir=test_directory_setup.origin_dir,
            result_dir=nsfs_obj.mount_path + "/" + nsfs_obj.bucket_name,
            amount=10,
            pattern="nsfs-obj-",
            result_pod=nsfs_obj.interface_pod,
        )
