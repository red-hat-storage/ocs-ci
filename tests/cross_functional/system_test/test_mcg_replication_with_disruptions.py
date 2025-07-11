import json
import logging

import pytest
import random
import time
from concurrent.futures import ThreadPoolExecutor

from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import (
    E2ETest,
    skipif_ocs_version,
    skipif_mcg_only,
    skipif_external_mode,
)
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    system_test,
    skipif_vsphere_ipi,
    magenta_squad,
    mcg,
    polarion_id,
    skipif_aws_creds_are_missing,
    skipif_disconnected_cluster,
)
from ocs_ci.ocs.node import get_worker_nodes, get_node_objs
from ocs_ci.ocs.bucket_utils import (
    compare_bucket_object_list,
    patch_replication_policy_to_bucket,
    write_random_test_objects_to_bucket,
    upload_test_objects_to_source_and_wait_for_replication,
    update_replication_policy,
    upload_random_objects_to_source_and_wait_for_replication,
    get_replication_policy,
    s3_put_bucket_versioning,
    wait_for_object_versions_match,
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
    get_noobaa_pods,
    wait_for_noobaa_pods_running,
    get_pod_node,
    get_noobaa_endpoint_pods,
)

from ocs_ci.utility.retry import retry
from ocs_ci.ocs.exceptions import (
    CommandFailed,
    ResourceWrongStatusException,
    TimeoutExpiredError,
)


logger = logging.getLogger(__name__)


@mcg
@magenta_squad
@system_test
@skipif_ocs_version("<4.9")
@skipif_vsphere_ipi
@skipif_external_mode
@skipif_aws_creds_are_missing
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
            ),
        ],
        ids=[
            "AZUREtoAWS-NS-CLI",
        ],
    )
    @polarion_id("OCS-3906")
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
        prefix_site_1 = "site1"
        target_bucket_name = bucket_factory(bucketclass=target_bucketclass)[0].name
        replication_policy = (
            "basic-replication-rule",
            target_bucket_name,
            prefix_site_1,
        )
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
            prefix=prefix_site_1,
        )
        logger.info(f"Written objects: {written_random_objects}")

        assert compare_bucket_object_list(
            mcg_obj_session, source_bucket_name, target_bucket_name
        )
        logger.info("Uni-directional bucket replication working as expected")

        # change from uni-directional to bi-directional replication policy
        logger.info("Changing the replication policy from uni to bi-directional!")
        prefix_site_2 = "site2"
        patch_replication_policy_to_bucket(
            target_bucket_name,
            "basic-replication-rule-2",
            source_bucket_name,
            prefix=prefix_site_2,
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
            prefix=prefix_site_2,
        )
        logger.info(f"Written objects: {written_random_objects}")
        assert compare_bucket_object_list(
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
            prefix=prefix_site_2,
        )
        logger.info(f"Written objects: {written_random_objects}")

        assert compare_bucket_object_list(
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
            prefix=prefix_site_2,
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

        assert compare_bucket_object_list(
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
            prefix=prefix_site_2,
        )
        logger.info(f"Written objects: {written_random_objects}")

        node_list = get_worker_nodes()
        node_objs = get_node_objs(node_list)
        nodes.restart_nodes(node_objs, timeout=500)
        retry(
            (CommandFailed, TimeoutError, AssertionError, ResourceWrongStatusException),
            tries=28,
            delay=15,
        )(ocp.wait_for_cluster_connectivity(tries=400))
        wait_for_pods_to_be_running(
            namespace=config.ENV_DATA["cluster_namespace"], timeout=800
        )
        logger.info("Nodes rebooted successfully!!")

        assert compare_bucket_object_list(
            mcg_obj_session, source_bucket_name, target_bucket_name
        )
        logger.info("Objects sync works even when the cluster is rebooted")


@system_test
@magenta_squad
@skipif_vsphere_ipi
class TestLogBasedReplicationWithDisruptions:
    @retry(Exception, tries=10, delay=5)
    def delete_objs_in_batch(self, objs_to_delete, mockup_logger, source_bucket):
        """
        This function deletes objects in a batch
        """
        for obj in objs_to_delete:
            mockup_logger.delete_objs_and_log(source_bucket.name, [obj])
            # adding momentary sleep just to slowdown the deletion
            # process
            time.sleep(5)
        logger.info(f"Successfully deleted these objects: {objs_to_delete}")

    @polarion_id("OCS-5457")
    def test_log_based_replication_with_disruptions(
        self,
        mcg_obj_session,
        aws_log_based_replication_setup,
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

        mockup_logger, _, source_bucket, target_bucket = (
            aws_log_based_replication_setup()
        )

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

        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(
                self.delete_objs_in_batch, objs_to_delete, mockup_logger, source_bucket
            )

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
        noobaa_pvc_obj = get_pvc_objs(pvc_names=[constants.NOOBAA_DB_PVC_NAME])

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
            timeout=300,
        ), "Deletion sync was done but not expected"

        # Do noobaa db recovery and see if the deletion sync works now
        noobaa_db_recovery_from_backup(snap_obj, noobaa_pvc_obj, noobaa_pods)
        wait_for_noobaa_pods_running(timeout=420)

        assert compare_bucket_object_list(
            mcg_obj_session,
            source_bucket.name,
            target_bucket.name,
            timeout=600,
        ), "Deletion sync was not done but expected"

        # Remove replication policy and upload some objects to the bucket
        # make sure the replication itself doesn't take place
        source_bucket.replication_policy = ""
        update_replication_policy(source_bucket.name, None)

        logger.info("Uploading test objects and waiting for replication to complete")
        mockup_logger.upload_test_objs_and_log(source_bucket.name)

        assert not compare_bucket_object_list(
            mcg_obj_session,
            source_bucket.name,
            target_bucket.name,
            timeout=300,
        ), "Standard replication completed even though replication policy is removed"

        validate_mcg_bg_features(
            feature_setup_map,
            run_in_bg=False,
            skip_any_features=["nsfs", "rgw kafka", "caching"],
            object_amount=5,
        )
        logger.info("No issues seen with the MCG bg feature validation")


@mcg
@magenta_squad
@system_test
@skipif_aws_creds_are_missing
@skipif_disconnected_cluster
class TestMCGReplicationWithVersioningSystemTest:

    @retry(CommandFailed, tries=7, delay=30)
    def upload_objects_with_retry(
        self,
        mcg_obj_session,
        source_bucket,
        target_bucket,
        mockup_logger,
        file_dir,
        pattern,
        prefix,
        num_versions=1,
    ):
        """
        Upload random objects to the bucket and retry if fails with
        CommandFailed exception.

        Args:
            mcg_obj_session (MCG): MCG object
            source_bucket (OBC): Bucket object
            target_bucket (OBC): Bucket object
            mockup_logger (MockupLogger): Mockup logger object
            file_dir (str): Source for generating objects
            pattern (str): File object pattern
            prefix (str): Prefix under which objects need to be uploaded
            num_versions (int): Number of object versions

        """
        upload_random_objects_to_source_and_wait_for_replication(
            mcg_obj_session,
            source_bucket,
            target_bucket,
            mockup_logger,
            file_dir,
            pattern=pattern,
            amount=1,
            num_versions=num_versions,
            prefix=prefix,
            timeout=600,
        )

    @pytest.fixture(autouse=True)
    def teardown(self, request, nodes):
        """
        Make sure all nodes are up again

        """

        def finalizer():
            nodes.restart_nodes_by_stop_and_start_teardown()

        request.addfinalizer(finalizer)

    @polarion_id("OCS-6407")
    def test_bucket_replication_with_versioning_system_test(
        self,
        awscli_pod_session,
        mcg_obj_session,
        bucket_factory,
        reduce_replication_delay,
        nodes,
        noobaa_db_backup_locally,
        noobaa_db_recovery_from_local,
        aws_log_based_replication_setup,
        test_directory_setup,
        setup_mcg_bg_features,
        validate_mcg_bg_features,
    ):
        """
        System test to verify the bucket replication with versioning when there
        are some disruptive and backup operations are performed.

        Steps:

        1. Run MCG background feature setup and validation
        2. Setup two buckets with bi-directional replication enabled
        3. Upload object and verify replication works between the buckets
        4. Enable versioning on the buckets and also enable sync_versions=True on
           replication policy as well
        5. Upload objects to second bucket and verify replication, version sync works
        6. Upload objects to first bucket and shutdown noobaa core pod node. Verify
           replication and version sync works
        7. Upload objects to the second bucket and restart all noobaa pods. Verify
           replication and version sync works
        8. Take the backup of Noobaa DB.
        9. Upload objects to the first bucket and verify replication works but not the
           version sync
        10. Recover Noobaa DB from the backup
        11. Upload objects to the second bucket. Verify replication and version sync works

        """

        feature_setup_map = setup_mcg_bg_features(
            num_of_buckets=5,
            object_amount=5,
            is_disruptive=True,
            skip_any_features=["nsfs", "rgw kafka", "caching"],
        )

        prefix_1 = "site_1"
        prefix_2 = "site_2"
        object_key = "ObjectKey-"

        # Reduce the replication delay to 1 minute
        logger.info("Reduce the bucket replication delay cycle to 1 minute")
        reduce_replication_delay()

        # Setup two buckets with bi-directional replication enabled
        # deletion sync disabled
        bucketclass_dict = {
            "interface": "OC",
            "backingstore_dict": {"aws": [(1, "eu-central-1")]},
        }
        mockup_logger_source, mockup_logger_target, bucket_1, bucket_2 = (
            aws_log_based_replication_setup(
                bucketclass_dict=bucketclass_dict,
                bidirectional=True,
                prefix_source=prefix_1,
                prefix_target=prefix_2,
                deletion_sync=False,
            )
        )

        # Upload object and verify that bucket replication works
        logger.info(f"Uploading object {object_key} to the bucket {bucket_1.name}")
        self.upload_objects_with_retry(
            mcg_obj_session,
            bucket_1,
            bucket_2,
            mockup_logger_source,
            test_directory_setup.origin_dir,
            pattern=object_key,
            prefix=prefix_1,
        )

        # Enable object versioning on both the buckets
        s3_put_bucket_versioning(mcg_obj_session, bucket_1.name)
        s3_put_bucket_versioning(mcg_obj_session, bucket_2.name)
        logger.info("Enabled object versioning for both the buckets")

        # Enable sync versions in both buckets replication policy
        replication_1 = json.loads(get_replication_policy(bucket_name=bucket_2.name))
        replication_2 = json.loads(get_replication_policy(bucket_name=bucket_1.name))
        replication_1["rules"][0]["sync_versions"] = True
        replication_2["rules"][0]["sync_versions"] = True

        update_replication_policy(bucket_2.name, replication_1)
        update_replication_policy(bucket_1.name, replication_2)
        logger.info(
            "Enabled sync versions in the replication policy for both the buckets"
        )

        # Update previously uploaded object with new data and new version
        self.upload_objects_with_retry(
            mcg_obj_session,
            bucket_2,
            bucket_1,
            mockup_logger_target,
            test_directory_setup.origin_dir,
            pattern=object_key,
            prefix=prefix_2,
        )
        logger.info(
            f"Updated object {object_key} with new version data in bucket {bucket_2.name}"
        )

        wait_for_object_versions_match(
            mcg_obj_session,
            awscli_pod_session,
            bucket_1.name,
            bucket_2.name,
            obj_key=f"{prefix_2}/{object_key}",
        )
        logger.info(
            f"Replication works from {bucket_2.name} to {bucket_1.name} and has all the versions of object {object_key}"
        )

        # Will perform disruptive operations and object uploads, version verifications
        # parallely.
        with ThreadPoolExecutor(max_workers=1) as executor:

            # Update object uploaded previously from the second bucket and
            # then shutdown the noobaa core and db pod nodes
            noobaa_pods = [
                get_noobaa_core_pod(),
                get_noobaa_db_pod(),
            ] + get_noobaa_endpoint_pods()
            noobaa_pod_nodes = [get_pod_node(pod_obj) for pod_obj in noobaa_pods]
            aws_cli_pod_node = get_pod_node(awscli_pod_session).name
            for node_obj in noobaa_pod_nodes:
                if node_obj.name != aws_cli_pod_node:
                    node_to_shutdown = [node_obj]
                    break

            logger.info(
                f"Updating object {object_key} with new version data in bucket {bucket_1.name}"
            )
            future = executor.submit(
                self.upload_objects_with_retry,
                mcg_obj_session,
                bucket_1,
                bucket_2,
                mockup_logger_source,
                test_directory_setup.origin_dir,
                pattern=object_key,
                prefix=prefix_1,
            )

            nodes.stop_nodes(node_to_shutdown)
            logger.info(f"Stopped these noobaa pod nodes {node_to_shutdown}")

            # Wait for the upload to finish
            future.result()

            wait_for_object_versions_match(
                mcg_obj_session,
                awscli_pod_session,
                bucket_1.name,
                bucket_2.name,
                obj_key=f"{prefix_1}/{object_key}",
            )
            logger.info(
                f"Replication works from {bucket_1.name} to {bucket_2.name} and"
                f" has all the versions of object {object_key}"
            )

            logger.info("Starting nodes now...")
            nodes.start_nodes(nodes=node_to_shutdown)
            wait_for_noobaa_pods_running()

            # Update object uploaded previously from the first bucket and then restart the noobaa pods
            noobaa_pods = get_noobaa_pods()
            logger.info(
                f"Updating object {object_key} with new version data in bucket {bucket_2.name}"
            )
            future = executor.submit(
                self.upload_objects_with_retry,
                mcg_obj_session,
                bucket_2,
                bucket_1,
                mockup_logger_target,
                test_directory_setup.origin_dir,
                pattern=object_key,
                prefix=prefix_2,
            )
            for pod_obj in noobaa_pods:
                pod_obj.delete(force=True)
                logger.info(f"Deleted noobaa pod {pod_obj.name}")
            logger.info("Restarted all Noobaa pods")

            # Wait for the upload to finish
            future.result()

            wait_for_object_versions_match(
                mcg_obj_session,
                awscli_pod_session,
                bucket_1.name,
                bucket_2.name,
                obj_key=f"{prefix_2}/{object_key}",
            )
            logger.info(
                f"Replication works from {bucket_2.name} to {bucket_1.name} "
                f"and has all the versions of object {object_key}"
            )
            future.result()

        # Take the noobaa db backup and then disable the sync versions
        # make sure no version sync happens
        logger.info("Taking backup of noobaa db")
        cnpg_cluster_yaml, original_db_replica_count, secrets_obj = (
            noobaa_db_backup_locally()
        )

        logger.info("Disabling version sync for both the buckets")
        replication_1["rules"][0]["sync_versions"] = False
        replication_2["rules"][0]["sync_versions"] = False

        update_replication_policy(bucket_2.name, replication_1)
        update_replication_policy(bucket_1.name, replication_2)

        # Change the replication cycle delay to 3 minutes
        logger.info("Reduce the bucket replication delay cycle to 5 minutes")
        reduce_replication_delay(interval=5)

        # Update previously uploaded object with new data and new version
        self.upload_objects_with_retry(
            mcg_obj_session,
            bucket_1,
            bucket_2,
            mockup_logger_source,
            test_directory_setup.origin_dir,
            pattern=object_key,
            prefix=prefix_1,
            num_versions=4,
        )
        logger.info(
            f"Updated object {object_key} with new version data in bucket {bucket_1.name}"
        )

        try:
            wait_for_object_versions_match(
                mcg_obj_session,
                awscli_pod_session,
                bucket_1.name,
                bucket_2.name,
                obj_key=f"{prefix_1}/{object_key}",
            )
        except TimeoutExpiredError:
            logger.info(
                f"Sync versions didnt work as expected, both {bucket_1.name} "
                f"and {bucket_2.name} have different versions"
            )
        else:
            assert False, "Sync version worked even when sync_versions was disabled!!"

        # Recover the noobaa db from the backup and perform
        # object deletion and verify deletion sync works
        logger.info("Recovering noobaa db from backup")
        noobaa_db_recovery_from_local(
            cnpg_cluster_yaml, original_db_replica_count, secrets_obj
        )
        wait_for_noobaa_pods_running(timeout=420)

        logger.info("Enabling version sync for both the buckets")
        replication_1["rules"][0]["sync_versions"] = True
        replication_2["rules"][0]["sync_versions"] = True

        update_replication_policy(bucket_2.name, replication_1)
        update_replication_policy(bucket_1.name, replication_2)

        # Update previously uploaded object with new data and new version
        self.upload_objects_with_retry(
            mcg_obj_session,
            bucket_1,
            bucket_2,
            mockup_logger_source,
            test_directory_setup.origin_dir,
            pattern=object_key,
            prefix=prefix_1,
            num_versions=4,
        )
        logger.info(
            f"Updated object {object_key} with new version data in bucket {bucket_1.name}"
        )

        wait_for_object_versions_match(
            mcg_obj_session,
            awscli_pod_session,
            bucket_1.name,
            bucket_2.name,
            obj_key=f"{prefix_1}/{object_key}",
        )
        logger.info(
            f"Replication works from {bucket_1.name} to {bucket_2.name} and"
            f" has all the versions of object {object_key}"
        )

        validate_mcg_bg_features(
            feature_setup_map,
            run_in_bg=False,
            skip_any_features=["nsfs", "rgw kafka", "caching"],
            object_amount=5,
        )
        logger.info("No issues seen with the MCG bg feature validation")
