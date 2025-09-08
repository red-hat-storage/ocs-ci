import logging
import pytest

from time import sleep

from ocs_ci.framework import config
from ocs_ci.framework.testlib import skipif_ocs_version, tier1
from ocs_ci.framework.pytest_customization.marks import (
    rdr,
    turquoise_squad,
)
from ocs_ci.helpers import dr_helpers, helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.acm.acm import AcmAddClusters
from ocs_ci.helpers.dr_helpers_ui import (
    failover_relocate_ui,
    check_dr_status,
)
from ocs_ci.ocs.ui.validation_ui import ValidationUI
from ocs_ci.ocs.utils import (
    get_primary_cluster_config,
    get_non_acm_cluster_config,
)
from ocs_ci.utility.utils import ceph_health_check
from ocs_ci.ocs.resources.drpc import DRPC

logger = logging.getLogger(__name__)


@rdr
@tier1
@turquoise_squad
class TestShowReplicationDelays:
    """
    Test class for RDR health status for ACM Managed application

    """

    params = [
        pytest.param(
            constants.CEPHBLOCKPOOL,
            id="CephBlockPool",
        ),
        pytest.param(
            constants.CEPHFILESYSTEM,
            id="CephFileSystem",
        ),
    ]

    @pytest.mark.parametrize(
        argnames=[
            "pvc_interface",
        ],
        argvalues=params,
    )
    @skipif_ocs_version("<4.19")
    def test_rdr_replication_delays(
        self, setup_acm_ui, dr_workload, scale_up_deployment, pvc_interface
    ):
        """
        Test to verify the display of DR health status of appset based and
        subscription based applications on ACM UI.

        Healthy: The last group sync time is less than 2X that of the sync interval.
        Warning: The last group sync time is greater than 2X and less than 3X of the sync interval.
        Critical: The last group sync time is greater than or equal to 3X of the sync interval.

        """

        config.switch_acm_ctx()

        global primary_index
        global secondary_index

        primary_config = get_primary_cluster_config()
        primary_index = primary_config.MULTICLUSTER.get("multicluster_index")
        secondary_index = [
            s.MULTICLUSTER["multicluster_index"]
            for s in get_non_acm_cluster_config()
            if s.MULTICLUSTER["multicluster_index"] != primary_index
        ][0]

        workload_names = []
        rdr_workload = dr_workload(
            num_of_subscription=1, num_of_appset=1, pvc_interface=pvc_interface
        )
        workload_names.append(f"{rdr_workload[0].workload_name}-1")

        if pvc_interface == constants.CEPHBLOCKPOOL:
            workload_names.append(f"{rdr_workload[1].workload_name}-1")
        else:
            workload_names.append(f"{rdr_workload[1].workload_name}-1-cephfs")
        drpc_subscription = DRPC(namespace=rdr_workload[0].workload_namespace)
        drpc_appset = DRPC(
            namespace=constants.GITOPS_CLUSTER_NAMESPACE,
            resource_name=f"{rdr_workload[1].appset_placement_name}-drpc",
        )
        drpc_objs = [drpc_subscription, drpc_appset]

        logger.info(f"Workload names are {workload_names}")

        dr_helpers.set_current_primary_cluster_context(
            rdr_workload[0].workload_namespace
        )

        scheduling_interval = dr_helpers.get_scheduling_interval(
            rdr_workload[0].workload_namespace
        )
        wait_time = 2 * scheduling_interval  # Time in minutes
        logger.info(f"Waiting for {wait_time} minutes to run IOs")
        sleep(wait_time * 60)

        primary_cluster_name = dr_helpers.get_current_primary_cluster_name(
            rdr_workload[0].workload_namespace
        )
        secondary_cluster_name = dr_helpers.get_current_secondary_cluster_name(
            rdr_workload[0].workload_namespace, rdr_workload[0].workload_type
        )

        acm_obj = AcmAddClusters()
        page_nav = ValidationUI()
        page_nav.refresh_web_console()

        config.switch_to_cluster_by_name(primary_cluster_name)
        before_failover_last_group_sync_time = []
        for obj in drpc_objs:
            before_failover_last_group_sync_time.append(
                dr_helpers.verify_last_group_sync_time(obj, scheduling_interval)
            )
        logger.info("Verified lastGroupSyncTime in CLI")

        logger.info("Verifying the DR health status on UI")
        check_dr_status(
            acm_obj, workload_names, rdr_workload, expected_status="healthy"
        )

        # Bring DOWN the corresponding deployment
        if pvc_interface == constants.CEPHBLOCKPOOL:
            logger.info("Change replica count to 0 for rbd-mirror on the secondary ")
            modify_rbd_replica_count(replica_count=0)
        else:
            logger.info("Change replica count to 0 for mds deployment on the primary ")
            modify_mds_replica_count(replica_count=0)

        # Wait for the range of interval between 2x - 3x interval to
        # validate the message "warning"
        # on the UI under Application -> DR Status
        logger.info(
            "Waiting for interval between more than the sync interval time x and 2x of sync interval"
            " to validate 'warning' state"
        )
        wait_time = scheduling_interval + 1
        sleep(wait_time * 60)

        check_dr_status(
            acm_obj, workload_names, rdr_workload, expected_status="warning"
        )

        logger.info("Waiting to validate 'critical' state")
        sleep(scheduling_interval * 60)

        check_dr_status(
            acm_obj, workload_names, rdr_workload, expected_status="critical"
        )

        # Bring UP the corresponding deployment
        if pvc_interface == constants.CEPHBLOCKPOOL:
            logger.info("Change replica count to 1 for rbd-mirror on the secondary ")
            modify_rbd_replica_count(replica_count=1)
        else:
            logger.info("Change replica count to 1 for mds deployment on the primary ")
            modify_mds_replica_count(replica_count=1)

        logger.info(
            "Waiting for the first sync to happen after scaling up the deployment"
        )
        sleep(scheduling_interval * 60)

        check_dr_status(
            acm_obj, workload_names, rdr_workload, expected_status="healthy"
        )

        # Navigate to failover modal via ACM UI
        logger.info("Navigate to failover modal via ACM UI")
        for workload in rdr_workload:
            failover_relocate_ui(
                acm_obj,
                scheduling_interval=scheduling_interval,
                workload_to_move=f"{workload.workload_name}-1",
                policy_name=workload.dr_policy_name,
                action=constants.ACTION_FAILOVER,
                failover_or_preferred_cluster=secondary_cluster_name,
                workload_type=workload.workload_type,
            )
            validate_after_failover = check_dr_status(
                acm_obj,
                [workload.workload_name],
                [workload],
                primary_cluster_name=primary_cluster_name,
                target_cluster_name=secondary_cluster_name,
                expected_status="FailingOver",
            )

        logger.info(
            f"Waiting for {scheduling_interval} minutes to run IOs post failover"
        )
        wait_time = 2 * scheduling_interval
        sleep(wait_time * 60)
        check_dr_status(
            acm_obj, workload_names, rdr_workload, expected_status="healthy"
        )

        for workload in rdr_workload:
            failover_relocate_ui(
                acm_obj,
                scheduling_interval=scheduling_interval,
                workload_to_move=f"{workload.workload_name}-1",
                policy_name=workload.dr_policy_name,
                action=constants.ACTION_RELOCATE,
                failover_or_preferred_cluster=primary_cluster_name,
                workload_type=workload.workload_type,
            )
            validate_after_relocate = check_dr_status(
                acm_obj,
                [workload.workload_name],
                [workload],
                primary_cluster_name=secondary_cluster_name,
                target_cluster_name=primary_cluster_name,
                expected_status="Relocating",
            )

        logger.info(
            f"Waiting for {scheduling_interval} minutes to run IOs post relocate"
        )
        sleep(scheduling_interval * 60)
        check_dr_status(
            acm_obj, workload_names, rdr_workload, expected_status="healthy"
        )

        if validate_after_failover or validate_after_relocate:
            logger.error("Failover or relocate has failed to get expected UI status")
            logger.error(
                f"Exception after failover {validate_after_failover}"
                f" and after relocate {validate_after_relocate}"
            )
            logger.info("This behaviour will be addressed in 4.21 in RHSTOR-5897")


def modify_mds_replica_count(replica_count=1):
    """
    Function that modifies the deployment count of
    mds daemons on primary cluster

    Args:
        replica(int): 1, by default that sets the replica count to 1
    """

    config.switch_ctx(primary_index)

    helpers.modify_deployment_replica_count(
        deployment_name=constants.MDS_DAEMON_DEPLOYMENT_ONE,
        replica_count=replica_count,
    )
    helpers.modify_deployment_replica_count(
        deployment_name=constants.MDS_DAEMON_DEPLOYMENT_TWO,
        replica_count=replica_count,
    )

    if replica_count == 1:
        ceph_health_check(tries=10, delay=30)


def modify_rbd_replica_count(replica_count=1):
    """
    Function that modifies the deployment count of rbd mirror
    on secondary cluster

    Args:
        replica(int): 1, by default that sets the replica count to 1
    """
    config.switch_ctx(secondary_index)

    helpers.modify_deployment_replica_count(
        deployment_name=constants.RBD_MIRROR_DAEMON_DEPLOYMENT,
        replica_count=replica_count,
    )


@pytest.fixture
def scale_up_deployment(request):
    def teardown():
        modify_rbd_replica_count(replica_count=1)
        modify_mds_replica_count(replica_count=1)

    request.addfinalizer(teardown)
