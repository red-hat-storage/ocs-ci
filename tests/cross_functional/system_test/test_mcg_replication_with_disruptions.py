import logging

import pytest
import random
import time
from concurrent.futures import ThreadPoolExecutor

from ocs_ci.framework.testlib import (
    E2ETest,
    skipif_ocs_version,
    skipif_mcg_only,
    skipif_external_mode,
)
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    tier2,
    system_test,
    skipif_vsphere_ipi,
    magenta_squad,
    mcg,
)
from ocs_ci.ocs.node import get_worker_nodes, get_node_objs
from ocs_ci.ocs.bucket_utils import (
    compare_bucket_object_list,
    patch_replication_policy_to_bucket,
    write_random_test_objects_to_bucket,
    upload_test_objects_to_source_and_wait_for_replication,
    update_replication_policy,
    remove_replication_policy,
)
from ocs_ci.ocs import ocp
from ocs_ci.ocs.resources.pvc import get_pvc_objs
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.ocs.resources.pod import (
    delete_pods,
    wait_for_pods_to_be_running,
    get_rgw_pods,
    get_noobaa_db_pod,
    get_noobaa_core_pod,
    wait_for_storage_pods,
    get_noobaa_pods,
)
from ocs_ci.utility.retry import retry
from ocs_ci.ocs.exceptions import CommandFailed, ResourceWrongStatusException

logger = logging.getLogger(__name__)


@mcg
@magenta_squad
@system_test
@skipif_ocs_version("<4.9")
@skipif_vsphere_ipi
@skipif_external_mode
@skipif_mcg_only
class TestMCGReplicationWithDisruptions(E2ETest):

    """
    The objectives of this test case are:
    1) To verify that namespace buckets can be replicated across MCG clusters
    2) To verify that the user can change from unidirectional MCG bucket replication to bidirectional successfully
    3) To verify that the Data restore functionality works
    4) To verify that the Certain admin/disruptive operations do not impact the replication
    """

    @pytest.mark.parametrize(
        argnames=["source_bucketclass", "target_bucketclass"],
        argvalues=[
            pytest.param(
                {
                    "interface": "oc",
                    "namespace_policy_dict": {
                        "type": "Multi",
                        "namespacestore_dict": {
                            "aws": [(1, "eu-central-1")],
                            "azure": [(1, None)],
                        },
                    },
                },
                {
                    "interface": "oc",
                    "namespace_policy_dict": {
                        "type": "Single",
                        "namespacestore_dict": {"rgw": [(1, None)]},
                    },
                },
                marks=[tier2, pytest.mark.polarion_id("OCS-3906")],
            ),
        ],
        ids=[
            "AZUREtoAWS-NS-CLI",
        ],
    )
    def test_replication_with_disruptions(
        self,
        awscli_pod_session,
        mcg_obj_session,
        cld_mgr,
        bucket_factory,
        source_bucketclass,
        target_bucketclass,
        test_directory_setup,
        nodes,
    ):
        # check uni bucket replication from multi (aws+azure) namespace bucket to s3-compatible namespace bucket
        target_bucket_name = bucket_factory(bucketclass=target_bucketclass)[0].name
        replication_policy = ("basic-replication-rule", target_bucket_name, None)
        source_bucket_name = bucket_factory(
            bucketclass=source_bucketclass, replication_policy=replication_policy
        )[0].name
        written_random_objects = write_random_test_objects_to_bucket(
            awscli_pod_session,
            source_bucket_name,
            test_directory_setup.origin_dir,
            mcg_obj=mcg_obj_session,
            amount=5,
            pattern="first-write-",
        )
        logger.info(f"Written objects: {written_random_objects}")

        compare_bucket_object_list(
            mcg_obj_session, source_bucket_name, target_bucket_name
        )
        logger.info("Uni-directional bucket replication working as expected")

        # change from uni-directional to bi-directional replication policy
        logger.info("Changing the replication policy from uni to bi-directional!")
        patch_replication_policy_to_bucket(
            target_bucket_name, "basic-replication-rule-2", source_bucket_name
        )
        logger.info(
            "Patch ran successfully! Changed the replication policy from uni to bi directional"
        )

        # write objects to the second bucket and see if it's replicated on the other
        logger.info("checking if bi-directional replication works!!")
        written_random_objects = write_random_test_objects_to_bucket(
            awscli_pod_session,
            target_bucket_name,
            test_directory_setup.origin_dir,
            mcg_obj=mcg_obj_session,
            amount=3,
            pattern="second-write-",
        )
        logger.info(f"Written objects: {written_random_objects}")
        compare_bucket_object_list(
            mcg_obj_session, source_bucket_name, target_bucket_name
        )
        logger.info("Bi directional bucket replication working as expected")

        # delete all the s3-compatible namespace buckets objects and then recover it from other namespace bucket on
        # write
        logger.info(
            "checking replication when one of the bucket's objects are deleted!!"
        )
        try:
            mcg_obj_session.s3_resource.Bucket(
                target_bucket_name
            ).objects.all().delete()
        except CommandFailed as e:
            logger.error(f"[Error] while deleting objects: {e}")
        if len(mcg_obj_session.s3_list_all_objects_in_bucket(target_bucket_name)) != 0:
            assert (
                False
            ), f"[Error] Unexpectedly objects were not deleted from {target_bucket_name}"
        logger.info("All the objects in RGW namespace buckets are deleted!!!")

        written_random_objects = write_random_test_objects_to_bucket(
            awscli_pod_session,
            target_bucket_name,
            test_directory_setup.origin_dir,
            mcg_obj=mcg_obj_session,
            amount=1,
            pattern="third-write-",
        )
        logger.info(f"Written objects: {written_random_objects}")

        compare_bucket_object_list(
            mcg_obj_session, source_bucket_name, target_bucket_name
        )
        logger.info(
            "All the objects retrieved back to s3-compatible bucket on new write!!"
        )

        # restart RGW pods and then see if object sync still works
        logger.info(
            "Checking if the replication works when there is RGW pod restarts!!"
        )
        written_random_objects = write_random_test_objects_to_bucket(
            awscli_pod_session,
            target_bucket_name,
            test_directory_setup.origin_dir,
            mcg_obj=mcg_obj_session,
            amount=1,
            pattern="fourth-write-",
        )
        logger.info(f"Written objects: {written_random_objects}")

        pod_names = get_pod_name_by_pattern(
            "rgw", namespace=config.ENV_DATA["cluster_namespace"]
        )
        pod_objs = get_rgw_pods(namespace=config.ENV_DATA["cluster_namespace"])
        delete_pods(pod_objs=pod_objs)
        wait_for_pods_to_be_running(
            pod_names=pod_names, namespace=config.ENV_DATA["cluster_namespace"]
        )

        compare_bucket_object_list(
            mcg_obj_session, source_bucket_name, target_bucket_name
        )
        logger.info("Object sync works after the RGW pod restarted!!")

        # write some object to any of the bucket, followed by immediate cluster restart
        logger.info("Checking replication when there is a cluster reboot!!")
        written_random_objects = write_random_test_objects_to_bucket(
            awscli_pod_session,
            target_bucket_name,
            test_directory_setup.origin_dir,
            mcg_obj=mcg_obj_session,
            amount=1,
            pattern="fifth-write-",
        )
        logger.info(f"Written objects: {written_random_objects}")

        node_list = get_worker_nodes()
        node_objs = get_node_objs(node_list)
        nodes.restart_nodes(node_objs, timeout=500)
        retry(
            (CommandFailed, TimeoutError, AssertionError, ResourceWrongStatusException),
            tries=60,
            delay=15,
        )(ocp.wait_for_cluster_connectivity(tries=400))
        wait_for_pods_to_be_running(
            namespace=config.ENV_DATA["cluster_namespace"], timeout=800
        )
        logger.info("Nodes rebooted successfully!!")

        compare_bucket_object_list(
            mcg_obj_session, source_bucket_name, target_bucket_name
        )
        logger.info("Objects sync works even when the cluster is rebooted")


@system_test
@magenta_squad
@skipif_vsphere_ipi
class TestLogBasedReplicationWithDisruptions:
    def test_log_based_replication_with_disruptions(
        self,
        mcg_obj_session,
        log_based_replication_setup,
        noobaa_db_backup,
        noobaa_db_recovery_from_backup,
        setup_mcg_bg_features,
        validate_mcg_bg_features,
    ):
        """
        This is a system test flow to test log based bucket replication
        deletion sync is not impacted due to some noobaa specific disruptions
        like noobaa pod restarts, noobaa db backup & recovery etc

        1. Setup log based bucket replication between the buckets
        2. Upload some objects and make sure replication works
        3. Keep deleting some objects from the source bucket and make sure
           deletion sync works as expected through out.
        4. In another thread, restart the noobaa pods (db & core), make sure
           deletion sync works for the step-3 deletion works as expected
        5. Now take backup of Noobaa db using PV backup method
        6. Remove the log based replication rules, perform some deletion in
           source bucket. make sure deletion sync doesn't work
        7. Recover noobaa db from the backup taken in step-5
        8. Now check if deletion sync works by deleting some objects from
           source bucket. Note: Expectation is still unclear
        9. Now patch the bucket to remove complete replication policy and
           make sure no replication - no deletion sync works

        """

        # entry criteria setup
        feature_setup_map = setup_mcg_bg_features(
            num_of_buckets=5,
            object_amount=5,
            is_disruptive=True,
            skip_any_features=["nsfs", "rgw kafka", "caching"],
        )

        mockup_logger, source_bucket, target_bucket = log_based_replication_setup()

        # upload test objects to the bucket and verify replication
        upload_test_objects_to_source_and_wait_for_replication(
            mcg_obj_session,
            source_bucket,
            target_bucket,
            mockup_logger,
            600,
        )

        # Delete objects in the first set in a batch and perform noobaa pod
        # restarts at the same time and make sure deletion sync works

        objs_in_bucket = mockup_logger.standard_test_obj_list
        objs_to_delete = random.sample(objs_in_bucket, 3)

        from ocs_ci.utility.retry import retry

        @retry(Exception, tries=10, delay=5)
        def delete_objs_in_batch():
            for obj in objs_to_delete:
                mockup_logger.delete_objs_and_log(source_bucket.name, [obj])
                time.sleep(5)
            logger.info(f"Successfully deleted these objects: {objs_to_delete}")

        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(delete_objs_in_batch)

            # Restart noobaa pods
            nb_core_pod = get_noobaa_core_pod()
            nb_db_pod = get_noobaa_db_pod()
            nb_core_pod.delete()
            nb_db_pod.delete()
            wait_for_pods_to_be_running(pod_names=[nb_core_pod.name, nb_db_pod.name])

            # Wait for the object deletion worker in the BG to completion
            future.result()
            assert compare_bucket_object_list(
                mcg_obj_session,
                source_bucket.name,
                target_bucket.name,
                timeout=600,
            ), f"Deletion sync failed to complete for the objects {objs_to_delete} deleted in the first bucket set"

        # Take noobaa db backup and remove deletion replication policy for the second bucket set
        # Get noobaa pods before execution
        noobaa_pods = get_noobaa_pods()

        # Get noobaa PVC before execution
        noobaa_pvc_obj = get_pvc_objs(pvc_names=["db-noobaa-db-pg-0"])

        _, snap_obj = noobaa_db_backup(noobaa_pvc_obj)

        disable_deletion_sync = source_bucket.replication_policy
        disable_deletion_sync["rules"][0]["sync_deletions"] = False
        update_replication_policy(source_bucket.name, disable_deletion_sync)
        logger.info("Deleting all the objects from the second bucket")
        mockup_logger.delete_all_objects_and_log(source_bucket.name)
        assert not compare_bucket_object_list(
            mcg_obj_session,
            source_bucket.name,
            target_bucket.name,
            timeout=600,
        ), "Deletion sync was done but not expected"

        # Do noobaa db recovery and see if the deletion sync works now
        noobaa_db_recovery_from_backup(snap_obj, noobaa_pvc_obj, noobaa_pods)
        wait_for_storage_pods()

        assert compare_bucket_object_list(
            mcg_obj_session,
            source_bucket.name,
            target_bucket.name,
            timeout=600,
        ), "Deletion sync was not done but expected"

        # Remove replication policy and upload some objects to the bucket
        # make sure the replication itself doesn't take place
        remove_replication_policy(source_bucket.name)
        logger.info("Uploading test objects and waiting for replication to complete")
        mockup_logger.upload_test_objs_and_log(source_bucket.name)

        logger.info(
            "Resetting the noobaa-core pod to trigger the replication background worker"
        )

        assert not compare_bucket_object_list(
            mcg_obj_session,
            source_bucket.name,
            target_bucket.name,
            timeout=600,
        ), f"Standard replication completed even though replication policy is removed"

        validate_mcg_bg_features(
            feature_setup_map,
            run_in_bg=False,
            skip_any_features=["nsfs", "rgw kafka", "caching"],
            object_amount=5,
        )
        logger.info("No issues seen with the MCG bg feature validation")
