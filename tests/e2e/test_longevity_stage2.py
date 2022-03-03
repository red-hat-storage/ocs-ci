import logging
import pytest

from concurrent.futures import ThreadPoolExecutor
from ocs_ci.framework.testlib import ManageTest
from ocs_ci.ocs import constants
from ocs_ci.ocs.bucket_utils import (
    compare_bucket_object_list,
    sync_object_directory,
    wait_for_cache,
)
from ocs_ci.ocs.resources.pvc import delete_pvcs
from ocs_ci.ocs.resources.pod import delete_pods

logger = logging.getLogger(__name__)


def write_empty_files_to_bucket(
    mcg_obj, awscli_pod_session, bucketname, test_directory_setup
):
    """
    Write empty files to bucket
    """

    full_object_path = f"s3://{bucketname}"
    data_dir = test_directory_setup.origin_dir

    # Touch create 3 empty files in pod
    command = f"for i in $(seq 1 3); do touch {data_dir}/test$i; done"
    awscli_pod_session.exec_sh_cmd_on_pod(command=command, sh="sh")
    # Write all empty objects to the bucket
    sync_object_directory(awscli_pod_session, data_dir, full_object_path, mcg_obj)

    logger.info(f"Successfully created files.")

    obj_set = set(
        obj.key for obj in mcg_obj.s3_list_all_objects_in_bucket(bucketname)
    )
    test_set = set("test" + str(i + 1) for i in range(3))
    assert test_set == obj_set, "File name set does not match"
    return obj_set


def seq(
    interface, multi_pvc_factory, pod_factory, teardown_factory, bucket_factory, mcg_obj, awscli_pod_session,
    mcg_obj_session, test_directory_setup, num_of_pvcs, pvc_size, access_modes, num_of_ns, num_of_bs, num_of_cache,
    num_of_replica_pair
):
    """
    Function to handle automation of Longevity Stage 2 Sequential Steps
    """

    if interface == constants.CEPHFILESYSTEM:
        access_modes.append(constants.ACCESS_MODE_RWX)
    else:
        access_modes.append(constants.ACCESS_MODE_RWO + '-' + constants.VOLUME_MODE_BLOCK)
        access_modes.append(constants.ACCESS_MODE_RWX + '-' + constants.VOLUME_MODE_BLOCK)

    executor = ThreadPoolExecutor(max_workers=1)

    # Create PVCs
    pvc_objs = multi_pvc_factory(
        interface=interface,
        size=pvc_size,
        access_modes=access_modes,
        status=constants.STATUS_BOUND,
        num_of_pvc=num_of_pvcs,
        wait_each=True,
    )
    logger.info(pvc_objs[0].get_pvc_vol_mode)
    logger.info("PVC creation was successful.")

    for pvc_obj in pvc_objs:
        teardown_factory(pvc_obj)
        teardown_factory(pvc_obj.backed_pv_obj)

    # Create PODs
    pod_objs = list()
    for pvc_obj in pvc_objs:
        if pvc_obj.get_pvc_vol_mode == constants.VOLUME_MODE_BLOCK:
            pod_objs.append(pod_factory(
                pvc=pvc_obj,
                raw_block_pv=True,
                pod_dict_path=constants.CSI_RBD_RAW_BLOCK_POD_YAML
            ))
        else:
            pod_objs.append(pod_factory(pvc=pvc_obj))
    logger.info("POD creation was successful.")

    for pod in pod_objs:
        teardown_factory(pod)

    # Run FIO on PODs
    for pod in pod_objs:
        pod.run_io("fs", "500M")
    logger.info("POD FIO was successful.")

    # Delete PODs
    pod_bulk_delete = executor.submit(delete_pods, pod_objs, wait=False)
    pod_bulk_delete.result()

    for pod_obj in pod_objs:
        assert pod_obj.ocp.wait_for_delete(
            pod_obj.name, 180
        ), f"Pod {pod_obj.name} is not deleted"
    logger.info("Verified: Pods are deleted.")

    # Delete PVCs
    pvc_delete = executor.submit(delete_pvcs, pvc_objs)
    res = pvc_delete.result()
    assert res, "Deletion of PVCs failed"
    logger.info("PVC deletion was successful.")
    for pvc in pvc_objs:
        pvc.ocp.wait_for_delete(resource_name=pvc.name)
    logger.info(f"Successfully deleted initial {num_of_pvcs} PVCs")


    # Create OBCs - bs, ns, cached and create random files
    obc_objs=list()
    obc_params = [
        ("OC", {
            "interface": "OC",
            "namespace_policy_dict": {
                "type": "Single",
                "namespacestore_dict": {"rgw": [(1, None)]},
                },
        }, num_of_ns),
        ("OC", None, num_of_bs),
        ("OC", {
            "interface": "OC",
            "namespace_policy_dict": {
                "type": "Cache",
                "ttl": 3600,
                "namespacestore_dict": {"rgw": [(1, None)]},
            },
            "placement_policy": {
                "tiers": [
                    {"backingStores": [constants.DEFAULT_NOOBAA_BACKINGSTORE]}
                ]
            },
        }, num_of_cache)
     ]

    for _interface, _bucketclass, _num in obc_params:
        if _num>0:
            buckets=bucket_factory(
                amount=_num,
                interface=_interface,
                bucketclass=_bucketclass,
            )
            obc_objs.extend(buckets)
            written_objs_names = write_empty_files_to_bucket(mcg_obj, awscli_pod_session, buckets[0].name, test_directory_setup)
            if _bucketclass["namespace_policy_dict"]["type"]=="Cache":
                wait_for_cache(mcg_obj, buckets[0].name, list(written_objs_names))


    # Create OBCs - Replica Pair, create random files and verify replication
    target_bucketclass = {
        "interface": "OC",
        "namespace_policy_dict": {
            "type": "Single",
            "namespacestore_dict": {"rgw": [(1, None)]},
        }
    }

    source_bucketclass = {
        "interface": "OC",
        "namespace_policy_dict": {
            "type": "Single",
            "namespacestore_dict": {"rgw": [(1, None)]},
        }
    }

    for _num in range(num_of_replica_pair):
        target_bucket=bucket_factory(bucketclass=target_bucketclass)[0]
        obc_objs.append(target_bucket)
        target_bucket_name = target_bucket.name

        replication_policy = ("basic-replication-rule", target_bucket_name, None)
        source_bucket = bucket_factory(1, bucketclass=source_bucketclass, replication_policy=replication_policy)[0]
        obc_objs.append(source_bucket)

        write_empty_files_to_bucket(mcg_obj, awscli_pod_session, source_bucket.name, test_directory_setup)
        compare_bucket_object_list(
            mcg_obj_session, source_bucket.name, target_bucket_name
        )

    # Delete OBCs
    for bucket in obc_objs:
        logger.info(f"Deleting bucket: {bucket.name}")
        bucket.delete()


class TestLongevityStage2(ManageTest):
    num_of_pvcs = 1
    pvc_size = 2
    access_modes = [constants.ACCESS_MODE_RWO]
    num_of_ns = 1
    num_of_bs = 1
    num_of_cache = 1
    num_of_replica_pair = 1

    @pytest.mark.parametrize(
        argnames="interface",
        argvalues=[
            pytest.param(
                *[constants.CEPHBLOCKPOOL]
            ),
            pytest.param(
                *[constants.CEPHFILESYSTEM]
            )
        ],
    )
    def test_seq(
        self, interface, multi_pvc_factory, pod_factory, teardown_factory, bucket_factory, mcg_obj, awscli_pod_session,
        mcg_obj_session, test_directory_setup
    ):
        """
        Test Longevity Stage 2
        """

        seq(interface, multi_pvc_factory, pod_factory, teardown_factory, bucket_factory, mcg_obj, awscli_pod_session,
            mcg_obj_session, test_directory_setup, self.num_of_pvcs, self.pvc_size, self.access_modes, self.num_of_ns,
            self.num_of_bs, self.num_of_cache, self.num_of_replica_pair)
