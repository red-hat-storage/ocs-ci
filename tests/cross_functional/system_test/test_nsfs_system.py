import logging
import uuid
from time import sleep

import pytest

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

logger = logging.getLogger(__name__)


@magenta_squad
@system_test
@skipif_mcg_only
@ignore_leftovers
@skipif_ocs_version("<4.10")
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
        snapshot_factory,
        noobaa_db_backup_and_recovery,
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

        # Put, Get, Copy, Head, list and Delete S3 operations
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
                result_pod_path=nsfs_obj.mount_path + "/" + nsfs_obj.bucket_name,
            )
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
        sleep(30)
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
        for nsfs_obj in nsfs_objs:
            logger.info(
                f"Downloading the objects and validating the integrity on {nsfs_obj.bucket_name} "
                f"post pod re-spins"
            )
            sync_object_directory(
                podobj=awscli_pod_session,
                src=f"s3://{nsfs_obj.bucket_name}",
                target=nsfs_obj.mount_path + "/" + nsfs_obj.bucket_name,
                signed_request_creds=nsfs_obj.s3_creds,
            )
            compare_directory(
                awscli_pod=awscli_pod_session,
                original_dir=f"{test_directory_setup.origin_dir}/{nsfs_obj.bucket_name}",
                result_dir=nsfs_obj.mount_path + "/" + nsfs_obj.bucket_name,
                amount=5,
                pattern=nsfs_obj_pattern,
                result_pod=nsfs_obj.interface_pod,
            )
        logger.info("Partially bringing the ceph cluster down")
        scale_ceph(replica=0)
        for nsfs_obj in nsfs_objs:
            sync_object_directory(
                podobj=awscli_pod_session,
                src=f"s3://{nsfs_obj.bucket_name}",
                target=nsfs_obj.mount_path + "/" + nsfs_obj.bucket_name,
                signed_request_creds=nsfs_obj.s3_creds,
            )
            compare_directory(
                awscli_pod=awscli_pod_session,
                original_dir=f"{test_directory_setup.origin_dir}/{nsfs_obj.bucket_name}",
                result_dir=nsfs_obj.mount_path + "/" + nsfs_obj.bucket_name,
                amount=5,
                pattern=nsfs_obj_pattern,
                result_pod=nsfs_obj.interface_pod,
            )
        logger.info(
            "Scaling the ceph cluster back to normal and validating all storage pods"
        )
        scale_ceph(replica=1)
        sleep(15)
        wait_for_storage_pods()

        logger.info("Performing noobaa db backup/recovery")
        noobaa_db_backup_and_recovery(snapshot_factory=snapshot_factory)
        for nsfs_obj in nsfs_objs:
            logger.info(
                f"Downloading the objects and validating the integrity on {nsfs_obj.bucket_name} "
                f"post noobaa-db recovery"
            )
            sync_object_directory(
                podobj=awscli_pod_session,
                src=f"s3://{nsfs_obj.bucket_name}",
                target=nsfs_obj.mount_path + "/" + nsfs_obj.bucket_name,
                signed_request_creds=nsfs_obj.s3_creds,
            )
            compare_directory(
                awscli_pod=awscli_pod_session,
                original_dir=f"{test_directory_setup.origin_dir}/{nsfs_obj.bucket_name}",
                result_dir=nsfs_obj.mount_path + "/" + nsfs_obj.bucket_name,
                amount=5,
                pattern=nsfs_obj_pattern,
                result_pod=nsfs_obj.interface_pod,
            )


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
