import logging
from concurrent.futures import ThreadPoolExecutor
import pytest


from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs import defaults
from ocs_ci.ocs.resources import pod as pod_helpers
from ocs_ci.framework.testlib import tier2, ignore_leftovers, ManageTest
from ocs_ci.helpers import cluster_exp_helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.bucket_utils import s3_io_create_delete, obc_io_create_delete
from ocs_ci.ocs import cluster as cluster_helpers
from ocs_ci.ocs.resources import storage_cluster
from ocs_ci.utility.utils import ceph_health_check
from ocs_ci.framework import config
from ocs_ci.helpers.pvc_ops import test_create_delete_pvcs
from ocs_ci.ocs.resources.storage_cluster import osd_encryption_verification

logger = logging.getLogger(__name__)

# TO DO: replace/remove this with actual workloads like couchbase, amq and
# pgsql later


@pytest.mark.parametrize(
    argnames=["percent_to_fill"],
    argvalues=[
        pytest.param(*[10], marks=pytest.mark.polarion_id("OCS-2131")),
    ],
)
@ignore_leftovers
@tier2
@pytest.mark.skipif(
    config.ENV_DATA["platform"].lower() == constants.VSPHERE_PLATFORM,
    reason=(
        "Skipping add capacity test on vsphere cause of issue: "
        "https://github.com/red-hat-storage/ocs-ci/issues/2356"
    ),
)
class TestAddCapacity(ManageTest):
    def test_add_capacity(
        self,
        project_factory,
        multi_dc_pod,
        multi_pvc_factory,
        pod_factory,
        mcg_obj,
        awscli_pod,
        bucket_factory,
        percent_to_fill,
    ):

        #####################################
        #           ENTRY CRITERIA          #
        #####################################
        # Prepare initial configuration : logger, cluster filling, loop for creating & deleting of PVCs and Pods,
        # noobaa IOs etc.,

        # Perform Health checks:
        # Make sure cluster is healthy
        assert ceph_health_check(
            defaults.ROOK_CLUSTER_NAMESPACE
        ), "Entry criteria FAILED: Cluster is Unhealthy"

        # All OCS pods are in running state:
        # ToDo https://github.com/red-hat-storage/ocs-ci/issues/2361
        assert (
            pod_helpers.check_pods_in_running_state()
        ), "Entry criteria FAILED: one or more OCS pods are not in running state"
        # Create the namespace under which this test will execute:
        project = project_factory()

        # total pvc created will be 'num_of_pvcs' * 4 types of pvcs(rbd-rwo,rwx
        # & cephfs-rwo,rwx)
        num_of_pvcs = 40

        rwo_rbd_pods = multi_dc_pod(
            num_of_pvcs=num_of_pvcs,
            pvc_size=175,
            project=project,
            access_mode="RWO",
            pool_type="rbd",
            timeout=360,
        )
        # Note: Skipping cephfs pods creation
        # observing bug https://bugzilla.redhat.com/show_bug.cgi?id=1785399,
        # https://bugzilla.redhat.com/show_bug.cgi?id=1779421#c14
        # Todo: https://github.com/red-hat-storage/ocs-ci/issues/2360

        # Create rwx-rbd pods
        pods_ios_rwx_rbd = multi_dc_pod(
            num_of_pvcs=10,
            pvc_size=175,
            project=project,
            access_mode="RWX-BLK",
            pool_type="rbd",
            timeout=360,
        )

        cluster_fill_io_pods = rwo_rbd_pods
        logger.info("The DC pods are up. Running IOs from them to fill the cluster")
        filler = cluster_exp_helpers.ClusterFiller(
            cluster_fill_io_pods, percent_to_fill, project.namespace
        )
        assert filler.cluster_filler(), "IOs failed"

        # create separate threadpool for running IOs in the background
        executor_run_bg_ios_ops = ThreadPoolExecutor()

        bg_wrap = cluster_exp_helpers.BackgroundOps()
        status_cluster_ios = []
        pods_for_copy = rwo_rbd_pods[0:5] + pods_ios_rwx_rbd

        for p in pods_for_copy:
            logger.info(f"running IOs on {p.name}")
            if p.pod_type == "rbd_block_rwx":
                status_cluster_ios.append(
                    executor_run_bg_ios_ops.submit(
                        bg_wrap.wrap, cluster_exp_helpers.raw_block_io, p, iterations=10
                    )
                )
            else:
                status_cluster_ios.append(
                    executor_run_bg_ios_ops.submit(
                        bg_wrap.wrap,
                        cluster_exp_helpers.cluster_copy_ops,
                        p,
                        iterations=200,
                    )
                )

        # Start pvc ops in the background.:
        logger.info("Started pvc create delete operations")
        executor_run_bg_ios_ops.submit(
            bg_wrap.wrap,
            test_create_delete_pvcs,
            multi_pvc_factory,
            pod_factory,
            project,
            iterations=200,
        )

        # Start NooBaa IOs in the background.:
        logger.info("Started s3_io_create_delete...")

        executor_run_bg_ios_ops.submit(
            bg_wrap.wrap,
            s3_io_create_delete,
            mcg_obj,
            awscli_pod,
            bucket_factory,
            iterations=200,
        )

        logger.info("Started obc_io_create_delete...")

        executor_run_bg_ios_ops.submit(
            bg_wrap.wrap,
            obc_io_create_delete,
            mcg_obj,
            awscli_pod,
            bucket_factory,
            iterations=200,
        )

        # All ocs nodes are in Ready state (including master):
        executor_run_bg_ios_ops.submit(
            bg_wrap.wrap, cluster_exp_helpers.check_nodes_status, iterations=100
        )

        # Get restart count of ocs pods before expanstion
        restart_count_before = pod_helpers.get_pod_restarts_count(
            defaults.ROOK_CLUSTER_NAMESPACE
        )

        # Get osd pods before expansion
        osd_pods_before = pod_helpers.get_osd_pods()

        # Get the total space in cluster before expansion
        ct_pod = pod_helpers.get_ceph_tools_pod()
        output = ct_pod.exec_ceph_cmd(ceph_cmd="ceph osd df")
        total_space_b4_expansion = int(output.get("summary").get("total_kb"))
        logger.info(f"total_space_b4_expansion == {total_space_b4_expansion}")

        logger.info("############## Calling add_capacity $$$$$$$$$$")

        #####################
        # Call add_capacity #
        #####################
        osd_size = storage_cluster.get_osd_size()
        result = storage_cluster.add_capacity(osd_size)
        pod = OCP(kind=constants.POD, namespace=config.ENV_DATA["cluster_namespace"])

        # New osd (all) pods corresponding to the additional capacity should be
        # in running state
        pod.wait_for_resource(
            timeout=1200,
            condition=constants.STATUS_RUNNING,
            selector="app=rook-ceph-osd",
            resource_count=result * 3,
        )

        #################################
        # Exit criteria verification:   #
        #################################
        cluster_exp_helpers.BackgroundOps.EXPANSION_COMPLETED = True

        # No ocs pods should get restarted unexpectedly
        # Get restart count of ocs pods after expansion and see any pods got
        # restated
        restart_count_after = pod_helpers.get_pod_restarts_count(
            defaults.ROOK_CLUSTER_NAMESPACE
        )
        #
        # # TO DO
        # # Handle Bug 1814254 - All Mons respinned during add capacity and OSDs took longtime to come up
        # # implement function to make sure no pods are respun after expansion

        logger.info(
            f"sum(restart_count_before.values()) = {sum(restart_count_before.values())}"
        )
        logger.info(
            f" sum(restart_count_after.values()) = {sum(restart_count_after.values())}"
        )
        assert sum(restart_count_before.values()) == sum(
            restart_count_after.values()
        ), "Exit criteria verification FAILED: One or more pods got restarted"

        logger.info("Exit criteria verification Success: No pods were restarted")
        # Make sure right number of OSDs are added:
        #   Get osd pods after expansion
        osd_pods_after = pod_helpers.get_osd_pods()
        number_of_osds_added = len(osd_pods_after) - len(osd_pods_before)
        logger.info(
            f"### number_of_osds_added = {number_of_osds_added}, "
            f"before = {len(osd_pods_before)}, after = {len(osd_pods_after) }"
        )
        # If the difference b/w updated count of osds and old osd count is not
        # 3 then expansion failed
        assert (
            number_of_osds_added == 3
        ), "Exit criteria verification FAILED: osd count mismatch"

        logger.info(
            "Exit criteria verification Success: Correct number of OSDs are added"
        )

        # The newly added capacity takes into effect at the storage level
        ct_pod = pod_helpers.get_ceph_tools_pod()
        output = ct_pod.exec_ceph_cmd(ceph_cmd="ceph osd df")
        total_space_after_expansion = int(output.get("summary").get("total_kb"))
        osd_size = int(output.get("nodes")[0].get("kb"))
        expanded_space = osd_size * 3  # 3 OSDS are added of size = 'osd_size'
        logger.info(f"space output == {output} ")
        logger.info(f"osd size == {osd_size} ")
        logger.info(f"total_space_after_expansion == {total_space_after_expansion} ")
        expected_total_space_after_expansion = total_space_b4_expansion + expanded_space
        logger.info(
            f"expected_total_space_after_expansion == {expected_total_space_after_expansion} "
        )
        assert (
            total_space_after_expansion == expected_total_space_after_expansion
        ), "Exit criteria verification FAILED: Expected capacity mismatch"

        logger.info(
            "Exit criteria verification Success: Newly added capacity took into effect"
        )

        logger.info("Exit criteria verification Success: IOs completed successfully")
        # 'ceph osd tree' should show the new osds under right nodes/hosts
        #   Verification is different for 3 AZ and 1 AZ configs
        ct_pod = pod_helpers.get_ceph_tools_pod()
        tree_output = ct_pod.exec_ceph_cmd(ceph_cmd="ceph osd tree")
        logger.info(f"### OSD tree output = {tree_output}")
        if config.ENV_DATA["platform"].lower() == constants.VSPHERE_PLATFORM:
            assert cluster_helpers.check_osd_tree_1az_vmware(
                tree_output, len(osd_pods_after)
            ), "Exit criteria verification FAILED: Incorrect ceph osd tree formation found"

        aws_number_of_zones = 3
        if config.ENV_DATA["platform"].lower() == constants.AWS_PLATFORM:
            # parse the osd tree. if it contains a node 'rack' then it's a
            # AWS_1AZ cluster. Else, 3 AWS_3AZ cluster
            for i in range(len(tree_output["nodes"])):
                if tree_output["nodes"][i]["name"] in "rack0":
                    aws_number_of_zones = 1
            if aws_number_of_zones == 1:
                assert cluster_helpers.check_osd_tree_1az_cloud(
                    tree_output, len(osd_pods_after)
                ), "Exit criteria verification FAILED: Incorrect ceph osd tree formation found"
            else:
                assert cluster_helpers.check_osd_tree_3az_cloud(
                    tree_output, len(osd_pods_after)
                ), "Exit criteria verification FAILED: Incorrect ceph osd tree formation found"

        logger.info("Exit criteria verification Success: osd tree verification success")

        # Make sure new pvcs and pods can be created and IOs can be run from
        # the pods
        num_of_pvcs = 1
        rwo_rbd_pods = multi_dc_pod(
            num_of_pvcs=num_of_pvcs,
            pvc_size=5,
            project=project,
            access_mode="RWO",
            pool_type="rbd",
        )
        rwo_cephfs_pods = multi_dc_pod(
            num_of_pvcs=num_of_pvcs,
            pvc_size=5,
            project=project,
            access_mode="RWO",
            pool_type="cephfs",
        )
        rwx_cephfs_pods = multi_dc_pod(
            num_of_pvcs=num_of_pvcs,
            pvc_size=5,
            project=project,
            access_mode="RWX",
            pool_type="cephfs",
        )
        # Create rwx-rbd pods
        pods_ios_rwx_rbd = multi_dc_pod(
            num_of_pvcs=num_of_pvcs,
            pvc_size=5,
            project=project,
            access_mode="RWX-BLK",
            pool_type="rbd",
        )
        cluster_io_pods = (
            rwo_rbd_pods + rwo_cephfs_pods + rwx_cephfs_pods + pods_ios_rwx_rbd
        )

        with ThreadPoolExecutor() as pod_ios_executor:
            for p in cluster_io_pods:
                if p.pod_type == "rbd_block_rwx":
                    logger.info(f"Calling block fio on pod {p.name}")
                    pod_ios_executor.submit(cluster_exp_helpers.raw_block_io, p, "100M")
                else:
                    logger.info(f"calling file fio on pod {p.name}")
                    pod_ios_executor.submit(p.run_io, "fs", "100M")

        for pod_io in cluster_io_pods:
            pod_helpers.get_fio_rw_iops(pod_io)

        # Verify OSDs are encrypted
        if config.ENV_DATA.get("encryption_at_rest"):
            osd_encryption_verification()

        cluster_obj = cluster_helpers.CephCluster()
        assert (
            cluster_obj.get_ceph_health() != "HEALTH_ERR"
        ), "Ceph cluster health checking failed"

        logger.info("ALL Exit criteria verification successfully")
        logger.info(
            "********************** TEST PASSED *********************************"
        )
