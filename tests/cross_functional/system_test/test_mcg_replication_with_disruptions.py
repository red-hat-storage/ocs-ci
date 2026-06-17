import json
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
        logger.test_step("Setup uni-directional bucket replication")
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
        logger.info(
            f"Source bucket: {source_bucket_name}, Target bucket: {target_bucket_name}"
        )

        logger.test_step("Write objects and verify uni-directional replication")
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
        logger.info("Uni-directional bucket replication verified successfully")

        logger.test_step("Change from uni-directional to bi-directional replication")
        logger.info("Updating replication policy to bi-directional")
        prefix_site_2 = "site2"
        patch_replication_policy_to_bucket(
            target_bucket_name,
            "basic-replication-rule-2",
            source_bucket_name,
            prefix=prefix_site_2,
        )
        logger.info("Replication policy updated to bi-directional successfully")

        logger.test_step(
            "Write objects to target bucket and verify bi-directional replication"
        )
        logger.info(
            "Writing objects to target bucket to test bi-directional replication"
        )
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
        logger.info("Bi-directional bucket replication verified successfully")

        logger.test_step(
            "Delete all objects from target bucket and verify recovery on write"
        )
        logger.info("Deleting all objects from target bucket to test data recovery")
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
        logger.info("All objects successfully recovered to target bucket on new write")

        logger.test_step("Restart RGW pods and verify replication still works")
        logger.info("Writing new objects and restarting RGW pods")
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
        logger.info("Object replication verified successfully after RGW pod restart")

        logger.test_step("Reboot cluster and verify replication works")
        logger.info("Writing objects and rebooting cluster")
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
        logger.info("Object replication verified successfully after cluster reboot")


@system_test
@magenta_squad
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
        noobaa_db_backup_locally,
        noobaa_db_recovery_from_local,
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
        logger.test_step("Setup MCG background features for entry criteria")
        feature_setup_map = setup_mcg_bg_features(
            num_of_buckets=5,
            object_amount=5,
            is_disruptive=True,
            skip_any_features=["nsfs", "rgw kafka", "caching"],
        )
        logger.info("MCG background features setup completed")

        logger.test_step("Setup log-based replication between buckets")
        mockup_logger, _, source_bucket, target_bucket = (
            aws_log_based_replication_setup()
        )
        logger.info(
            f"Log-based replication setup: {source_bucket.name} -> {target_bucket.name}"
        )

        logger.test_step("Upload objects and verify log-based replication")
        upload_test_objects_to_source_and_wait_for_replication(
            mcg_obj_session,
            source_bucket,
            target_bucket,
            mockup_logger,
            600,
        )
        logger.info("Initial replication verified successfully")

        logger.test_step("Delete objects and restart NooBaa pods, verify deletion sync")
        objs_in_bucket = mockup_logger.standard_test_obj_list
        objs_to_delete = random.sample(objs_in_bucket, 3)
        logger.info(
            f"Deleting {len(objs_to_delete)} objects while restarting NooBaa pods"
        )

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
        logger.info("Deletion sync verified successfully after NooBaa pod restart")

        logger.test_step("Take NooBaa DB backup")
        logger.info("Starting NooBaa DB backup")
        ocs_storage_obj, backup_name, noobaa_obj = noobaa_db_backup_locally()
        logger.info("NooBaa DB backup completed successfully")

        logger.test_step("Disable deletion sync and verify it doesn't work")
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
        logger.info("Deletion sync correctly disabled as expected")

        logger.test_step("Recover NooBaa DB from backup and verify deletion sync works")
        logger.info("Starting NooBaa DB recovery from backup")
        noobaa_db_recovery_from_local(ocs_storage_obj, backup_name, noobaa_obj)

        wait_for_noobaa_pods_running(timeout=420)

        assert compare_bucket_object_list(
            mcg_obj_session,
            source_bucket.name,
            target_bucket.name,
            timeout=600,
        ), "Deletion sync was not done but expected"
        logger.info("Deletion sync verified successfully after NooBaa DB recovery")

        logger.test_step("Remove replication policy and verify replication stops")
        source_bucket.replication_policy = ""
        update_replication_policy(source_bucket.name, None)
        logger.info("Replication policy removed")

        logger.info("Uploading test objects to verify replication doesn't occur")
        mockup_logger.upload_test_objs_and_log(source_bucket.name)

        assert not compare_bucket_object_list(
            mcg_obj_session,
            source_bucket.name,
            target_bucket.name,
            timeout=300,
        ), "Standard replication completed even though replication policy is removed"
        logger.info("Verified replication stopped after policy removal")

        logger.test_step("Validate MCG background features")
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

        logger.test_step("Setup MCG background features for entry criteria")
        feature_setup_map = setup_mcg_bg_features(
            num_of_buckets=5,
            object_amount=5,
            is_disruptive=True,
            skip_any_features=["nsfs", "rgw kafka", "caching"],
        )
        logger.info("MCG background features setup completed")

        prefix_1 = "site_1"
        prefix_2 = "site_2"
        object_key = "ObjectKey-"

        logger.test_step("Reduce bucket replication delay to 1 minute")
        logger.info("Reducing replication delay cycle to 1 minute")
        reduce_replication_delay()
        logger.info("Replication delay reduced successfully")

        logger.test_step("Setup bi-directional replication between two buckets")
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
        logger.info(
            f"Bi-directional replication setup: {bucket_1.name} <-> {bucket_2.name}"
        )

        logger.test_step("Upload object and verify initial replication")
        logger.info(f"Uploading object {object_key} to bucket {bucket_1.name}")
        self.upload_objects_with_retry(
            mcg_obj_session,
            bucket_1,
            bucket_2,
            mockup_logger_source,
            test_directory_setup.origin_dir,
            pattern=object_key,
            prefix=prefix_1,
        )
        logger.info("Initial replication verified successfully")

        logger.test_step("Enable object versioning on both buckets")
        s3_put_bucket_versioning(mcg_obj_session, bucket_1.name)
        s3_put_bucket_versioning(mcg_obj_session, bucket_2.name)
        logger.info("Object versioning enabled for both buckets")

        logger.test_step("Enable version sync in replication policy")
        replication_1 = json.loads(get_replication_policy(bucket_name=bucket_2.name))
        replication_2 = json.loads(get_replication_policy(bucket_name=bucket_1.name))
        replication_1["rules"][0]["sync_versions"] = True
        replication_2["rules"][0]["sync_versions"] = True

        update_replication_policy(bucket_2.name, replication_1)
        update_replication_policy(bucket_1.name, replication_2)
        logger.info("Version sync enabled in replication policy for both buckets")

        logger.test_step("Update object with new version and verify version sync")
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
        logger.info(f"Version sync verified: {bucket_2.name} -> {bucket_1.name}")

        logger.test_step("Perform disruptive operations with version sync verification")
        with ThreadPoolExecutor(max_workers=1) as executor:
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
                f"Version sync verified after node shutdown: {bucket_1.name} -> {bucket_2.name}"
            )

            logger.info("Starting nodes")
            nodes.start_nodes(nodes=node_to_shutdown)
            wait_for_noobaa_pods_running()
            logger.info("Nodes started and NooBaa pods running")
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
                f"Version sync verified after NooBaa pod restart: {bucket_2.name} -> {bucket_1.name}"
            )
            future.result()

        logger.test_step("Take NooBaa DB backup, disable version sync, verify no sync")
        logger.info("Starting NooBaa DB backup")
        ocs_storage_obj, backup_name, noobaa_obj = noobaa_db_backup_locally()
        logger.info("NooBaa DB backup completed")

        logger.info("Disabling version sync for both buckets")
        replication_1["rules"][0]["sync_versions"] = False
        replication_2["rules"][0]["sync_versions"] = False

        update_replication_policy(bucket_2.name, replication_1)
        update_replication_policy(bucket_1.name, replication_2)
        logger.info("Version sync disabled")

        logger.info("Increasing replication delay cycle to 5 minutes")
        reduce_replication_delay(interval=5)
        logger.info("Replication delay updated")
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
        logger.info("Verified version sync is disabled")

        logger.test_step("Recover NooBaa DB and verify version sync works again")
        logger.info("Starting NooBaa DB recovery from backup")
        noobaa_db_recovery_from_local(ocs_storage_obj, backup_name, noobaa_obj)

        wait_for_noobaa_pods_running(timeout=420)
        logger.info("NooBaa DB recovery completed, pods running")

        logger.info("Re-enabling version sync for both buckets")
        replication_1["rules"][0]["sync_versions"] = True
        replication_2["rules"][0]["sync_versions"] = True

        update_replication_policy(bucket_2.name, replication_1)
        update_replication_policy(bucket_1.name, replication_2)
        logger.info("Version sync re-enabled")
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
            f"Version sync verified after DB recovery: {bucket_1.name} -> {bucket_2.name}"
        )

        logger.test_step("Validate MCG background features")
        validate_mcg_bg_features(
            feature_setup_map,
            run_in_bg=False,
            skip_any_features=["nsfs", "rgw kafka", "caching"],
            object_amount=5,
        )
        logger.info("No issues seen with the MCG bg feature validation")
