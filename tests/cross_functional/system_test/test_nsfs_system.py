import logging
import pytest
import uuid

from ocs_ci.framework import config
from ocs_ci.framework.testlib import MCGTest, system_test
from ocs_ci.framework.pytest_customization.marks import (
    skipif_mcg_only,
    ignore_leftovers,
    skipif_ocs_version,
    magenta_squad,
)
from ocs_ci.helpers.helpers import wait_for_resource_state
from ocs_ci.ocs import constants
from ocs_ci.ocs.bucket_utils import (
    random_object_round_trip_verification,
    compare_directory,
    sync_object_directory,
    s3_put_object,
    s3_get_object,
    s3_list_objects_v1,
    s3_copy_object,
    s3_head_object,
    s3_delete_objects,
)
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.resources.mcg_params import NSFS
from ocs_ci.ocs.resources.pod import get_mds_pods, wait_for_storage_pods
from time import sleep
from tests.conftest import revert_noobaa_endpoint_scc_class

logger = logging.getLogger(__name__)


@magenta_squad
@system_test
@skipif_mcg_only
@ignore_leftovers
@skipif_ocs_version("<4.10")
@pytest.mark.usefixtures(revert_noobaa_endpoint_scc_class.__name__)
class TestNSFSSystem(MCGTest):
    """
    NSFS system test

    """

    @pytest.mark.polarion_id("OCS-3952")
    def test_nsfs(
        self,
        mcg_obj,
        nsfs_bucket_factory,
        awscli_pod_session,
        test_directory_setup,
        noobaa_db_backup_and_recovery_locally,
    ):
        """
        The objectives of this test case are:
        1) To verify S3 operations on NSFS buckets
        2) NSFS based data is accessible/intact when cluster related operations pod failures are performed
        3) Noobaa DB backup and recovery does not impact data on NSFS

        """
        s3_ops_obj = "obj-key"
        s3_ops_copy_obj = "copy-obj-key"
        s3_ops_obj_data = "object data-" + str(uuid.uuid4().hex)
        nsfs_obj_pattern = "nsfs-obj"

        logger.test_step("Create NSFS buckets with different configurations")
        nsfs_objs = [
            NSFS(
                method="OC",
                pvc_size=10,
            ),
            NSFS(
                method="OC",
                pvc_size=10,
                mount_existing_dir=True,
            ),
        ]
        for nsfs_obj in nsfs_objs:
            nsfs_bucket_factory(nsfs_obj)
            logger.info(f"Successfully created NSFS bucket: {nsfs_obj.bucket_name}")
        logger.info("All NSFS buckets created successfully")

        logger.info("Waiting 60 seconds for changes to propagate")
        sleep(60)

        logger.test_step(
            "Perform S3 operations on NSFS buckets (Put, Get, Copy, Head, List, Delete)"
        )
        for nsfs_obj in nsfs_objs:
            logger.info(f"Put and Get object operation on {nsfs_obj.bucket_name}")
            assert s3_put_object(
                s3_obj=nsfs_obj,
                bucketname=nsfs_obj.bucket_name,
                object_key=s3_ops_obj,
                data=s3_ops_obj_data,
            ), "Failed: PutObject"
            get_res = s3_get_object(
                s3_obj=nsfs_obj, bucketname=nsfs_obj.bucket_name, object_key=s3_ops_obj
            )

            logger.info(f"Head object operation on {nsfs_obj.bucket_name}")
            assert s3_head_object(
                s3_obj=nsfs_obj,
                bucketname=nsfs_obj.bucket_name,
                object_key=s3_ops_obj,
                if_match=get_res["ETag"],
            ), "ETag does not match with the head object"

            logger.info(f"Copy object operation on {nsfs_obj.bucket_name}")
            assert s3_copy_object(
                s3_obj=nsfs_obj,
                bucketname=nsfs_obj.bucket_name,
                source=f"/{nsfs_obj.bucket_name}/{s3_ops_obj}",
                object_key=s3_ops_copy_obj,
            ), "Failed: CopyObject"
            get_copy_res = s3_get_object(
                s3_obj=nsfs_obj,
                bucketname=nsfs_obj.bucket_name,
                object_key=s3_ops_copy_obj,
            )
            logger.info(
                f"Verifying Etag of {s3_ops_copy_obj} from Get object operations"
            )
            assert get_copy_res["ETag"] == get_res["ETag"], "Incorrect object key"

            logger.info(f"List object operation on {nsfs_obj.bucket_name}")
            list_response = s3_list_objects_v1(
                s3_obj=nsfs_obj, bucketname=nsfs_obj.bucket_name
            )
            logger.info(f"Validating keys are listed on {nsfs_obj.bucket_name}")
            page_keys = [item["Key"] for item in list_response["Contents"]]
            assert s3_ops_obj and s3_ops_copy_obj in page_keys, "keys not listed"

            logger.info(
                f"Deleting {s3_ops_obj} and {s3_ops_copy_obj} and verifying response"
            )
            del_res = s3_delete_objects(
                s3_obj=nsfs_obj,
                bucketname=nsfs_obj.bucket_name,
                object_keys=[{"Key": f"{s3_ops_obj}"}, {"Key": f"{s3_ops_copy_obj}"}],
            )
            for i, key in enumerate([s3_ops_obj, s3_ops_copy_obj]):
                assert (
                    key == del_res["Deleted"][i]["Key"]
                ), "Object key not found/not-deleted"
        logger.info(
            "S3 operations (Put, Get, Copy, Head, List, Delete) verified successfully on all buckets"
        )

        logger.test_step("Perform random object round trip verification")
        for nsfs_obj in nsfs_objs:
            random_object_round_trip_verification(
                io_pod=awscli_pod_session,
                bucket_name=nsfs_obj.bucket_name,
                upload_dir=f"{test_directory_setup.origin_dir}/{nsfs_obj.bucket_name}",
                download_dir=f"{test_directory_setup.result_dir}/{nsfs_obj.bucket_name}",
                amount=5,
                pattern=nsfs_obj_pattern,
                s3_creds=nsfs_obj.s3_creds,
                result_pod=nsfs_obj.interface_pod,
                result_pod_path=nsfs_obj.mounted_bucket_path,
            )
        logger.info("Random object round trip verification completed successfully")

        logger.test_step("Respin NooBaa core pod and MDS pod")
        pods_to_respin = [
            pod.Pod(
                **pod.get_pods_having_label(
                    label=constants.NOOBAA_CORE_POD_LABEL,
                    namespace=config.ENV_DATA["cluster_namespace"],
                )[0]
            ),
            get_mds_pods()[0],
        ]
        for pod_del in pods_to_respin:
            logger.info(f"Deleting pod {pod_del.name}")
            pod_del.delete()
        logger.info("Pods deleted, waiting 30 seconds for respawn")
        sleep(30)

        logger.test_step("Validate pods are running after respin")
        pods_to_validate = [
            pod.Pod(
                **pod.get_pods_having_label(
                    label=constants.NOOBAA_CORE_POD_LABEL,
                    namespace=config.ENV_DATA["cluster_namespace"],
                )[0]
            ),
            get_mds_pods()[0],
        ]
        for pod_val in pods_to_validate:
            wait_for_resource_state(
                resource=pod_val, state=constants.STATUS_RUNNING, timeout=300
            )
        logger.info("All pods are running after respin")

        logger.test_step("Validate data integrity after pod respin")
        for nsfs_obj in nsfs_objs:
            logger.info(
                f"Downloading objects and validating integrity on {nsfs_obj.bucket_name} after pod respin"
            )
            sync_object_directory(
                podobj=awscli_pod_session,
                src=f"s3://{nsfs_obj.bucket_name}",
                target=f"{test_directory_setup.result_dir}/{nsfs_obj.bucket_name}/a",
                signed_request_creds=nsfs_obj.s3_creds,
            )

            compare_directory(
                awscli_pod=awscli_pod_session,
                original_dir=f"{test_directory_setup.origin_dir}/{nsfs_obj.bucket_name}",
                result_dir=f"{test_directory_setup.result_dir}/{nsfs_obj.bucket_name}/a",
                amount=5,
                pattern=nsfs_obj_pattern,
            )
        logger.info("Data integrity validated successfully after pod respin")

        logger.test_step("Scale down Ceph cluster and validate NSFS data access")
        logger.info("Scaling down Ceph cluster (mon, osd, mds) to 0 replicas")
        scale_ceph(replica=0)
        logger.info("Ceph cluster scaled down")

        logger.info("Validating NSFS data access with Ceph cluster down")
        for nsfs_obj in nsfs_objs:
            sync_object_directory(
                podobj=awscli_pod_session,
                src=f"s3://{nsfs_obj.bucket_name}",
                target=f"{test_directory_setup.result_dir}/{nsfs_obj.bucket_name}/b",
                signed_request_creds=nsfs_obj.s3_creds,
            )
            compare_directory(
                awscli_pod=awscli_pod_session,
                original_dir=f"{test_directory_setup.origin_dir}/{nsfs_obj.bucket_name}",
                result_dir=f"{test_directory_setup.result_dir}/{nsfs_obj.bucket_name}/b",
                amount=5,
                pattern=nsfs_obj_pattern,
            )
        logger.info("Data access validated successfully with Ceph cluster down")

        logger.test_step("Scale up Ceph cluster and validate storage pods")
        logger.info("Scaling Ceph cluster back to normal (1 replica)")
        scale_ceph(replica=1)
        logger.info("Waiting 60 seconds for cluster stabilization")
        sleep(60)
        wait_for_storage_pods()
        logger.info("All storage pods are running after Ceph scale up")

        logger.test_step("Perform NooBaa DB backup and recovery")
        logger.info("Starting NooBaa DB backup and recovery")
        noobaa_db_backup_and_recovery_locally()
        logger.info("NooBaa DB backup and recovery completed successfully")

        logger.test_step("Validate data integrity after NooBaa DB recovery")
        for nsfs_obj in nsfs_objs:
            logger.info(
                f"Downloading objects and validating integrity on {nsfs_obj.bucket_name} after NooBaa DB recovery"
            )
            sync_object_directory(
                podobj=awscli_pod_session,
                src=f"s3://{nsfs_obj.bucket_name}",
                target=f"{test_directory_setup.result_dir}/{nsfs_obj.bucket_name}/c",
                signed_request_creds=nsfs_obj.s3_creds,
            )
            compare_directory(
                awscli_pod=awscli_pod_session,
                original_dir=f"{test_directory_setup.origin_dir}/{nsfs_obj.bucket_name}",
                result_dir=f"{test_directory_setup.result_dir}/{nsfs_obj.bucket_name}/c",
                amount=5,
                pattern=nsfs_obj_pattern,
            )
        logger.info("Data integrity validated successfully after NooBaa DB recovery")


def scale_ceph(replica=1):
    """
    Scales down/up mon, osd and mds pods

    Args:
        replica(int): Replica count

    """
    dep_ocp = OCP(
        kind=constants.DEPLOYMENT, namespace=config.ENV_DATA["cluster_namespace"]
    )
    deployments = [
        constants.ROOK_CEPH_OPERATOR,
        "rook-ceph-mon-a",
        "rook-ceph-osd-0",
        "rook-ceph-mds-ocs-storagecluster-cephfilesystem-a",
    ]
    for dep in deployments:
        logger.info(f"Scaling {dep} to replica {replica}")
        dep_ocp.exec_oc_cmd(f"scale deployment {dep} --replicas={replica}")
