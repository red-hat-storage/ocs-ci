import logging

import pytest

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
    mcg,
)
from ocs_ci.ocs.node import get_worker_nodes, get_node_objs
from ocs_ci.ocs.bucket_utils import (
    compare_bucket_object_list,
    patch_replication_policy_to_bucket,
    write_random_test_objects_to_bucket,
)
from ocs_ci.ocs import ocp
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.ocs.resources.pod import (
    delete_pods,
    wait_for_pods_to_be_running,
    get_rgw_pods,
)
from ocs_ci.utility.retry import retry
from ocs_ci.ocs.exceptions import CommandFailed, ResourceWrongStatusException

logger = logging.getLogger(__name__)


@mcg
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
