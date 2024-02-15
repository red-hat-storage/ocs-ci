import logging
from time import sleep

from ocs_ci.deployment.deployment import Deployment

from ocs_ci.framework import config
from ocs_ci.framework.testlib import tier1, brownfield_mode_required
from ocs_ci.framework.pytest_customization.marks import turquoise_squad
from ocs_ci.helpers import dr_helpers
from ocs_ci.helpers.dr_helpers import (
    brown_field_migation,
    check_mirroring_status_ok,
    validate_bluestore_rdr_osd,
)
from ocs_ci.ocs import constants


logger = logging.getLogger(__name__)


@tier1
@brownfield_mode_required
@turquoise_squad
class TestBrownField:
    """
    Test Brown Field

    """

    def test_brownfield(self, dr_workload):
        """
        Test Brown Field

        """
        rdr_workload_rbd = dr_workload(
            num_of_subscription=0, num_of_appset=3, dr_enable=False
        )
        rdr_workload_cephfs = dr_workload(
            num_of_subscription=0,
            num_of_appset=3,
            pvc_interface=constants.CEPHFILESYSTEM,
            dr_enable=False,
        )
        workload_type = constants.APPLICATION_SET
        logger.info("Sleeping for 1800 sec / 30 Min to fill up ceph cluster")
        sleep(1800)
        logger.info("Migration OSD for RDR")
        brown_field_migation()
        logger.info("Validating OSD migration status")
        validate_bluestore_rdr_osd()
        logger.info("Enabling RDR on both cluster")
        deployment_oj = Deployment()
        deployment_oj.do_deploy_rdr()
        logger.info("Checking mirroring Status")
        check_mirroring_status_ok()
        logger.info("Making Worload DR Protected")
        for rbd_workload in rdr_workload_cephfs:
            rbd_workload.add_annotation_to_placement()
            rbd_workload.apply_dr_policy()
            rbd_workload.verify_workload_deployment()

        logger.info("Wating for Sync to get complete------RBD-Failover")
        dr_helpers.wait_for_groupsync(
            workload_type=workload_type,
            workload_placement_name=rdr_workload_rbd[0].appset_placement_name,
        )

        do_failover(workload_instance=rdr_workload_rbd[0], workload_type=workload_type)
        logger.info("Wating for Sync to get complete------RBD-Relocate")
        dr_helpers.wait_for_groupsync(
            workload_type=workload_type,
            workload_placement_name=rdr_workload_rbd[2].appset_placement_name,
        )
        logger.info("Running Relocate of workload")
        do_relocate(workload_instance=rdr_workload_rbd[2], workload_type=workload_type)

        logger.info("Wating for Sync to get complete------FS-Failover")
        dr_helpers.wait_for_groupsync(
            workload_type=workload_type,
            workload_placement_name=rdr_workload_cephfs[3].appset_placement_name,
        )

        do_failover(
            workload_instance=rdr_workload_cephfs[3], workload_type=workload_type
        )
        logger.info("Wating for Sync to get complete------FS-Relocate")
        dr_helpers.wait_for_groupsync(
            workload_type=workload_type,
            workload_placement_name=rdr_workload_cephfs[4].appset_placement_name,
        )
        logger.info("Running Relocate of workload")
        do_relocate(
            workload_instance=rdr_workload_cephfs[4], workload_type=workload_type
        )


def do_failover(
    workload_instance,
    workload_type,
):

    primary_cluster_name = dr_helpers.get_current_primary_cluster_name(
        workload_instance.workload_namespace, workload_type=workload_type
    )

    secondary_cluster_name = dr_helpers.get_current_secondary_cluster_name(
        workload_instance.workload_namespace, workload_type=workload_type
    )
    dr_helpers.failover(
        failover_cluster=secondary_cluster_name,
        namespace=workload_instance.workload_namespace,
        workload_type=workload_type,
        workload_placement_name=workload_instance.appset_placement_name,
    )
    do_verication(primary_cluster_name, secondary_cluster_name, workload_instance)


def do_relocate(
    workload_instance,
    workload_type,
):

    primary_cluster_name = dr_helpers.get_current_primary_cluster_name(
        workload_instance.workload_namespace, workload_type=workload_type
    )

    secondary_cluster_name = dr_helpers.get_current_secondary_cluster_name(
        workload_instance.workload_namespace, workload_type=workload_type
    )
    dr_helpers.relocate(
        preferred_cluster=secondary_cluster_name,
        namespace=workload_instance.workload_namespace,
        workload_type=workload_type,
        workload_placement_name=workload_instance.appset_placement_name,
    )
    do_verication(primary_cluster_name, secondary_cluster_name, workload_instance)


def do_verication(primary_cluster_name, secondary_cluster_name, workload_instance):

    config.switch_to_cluster_by_name(secondary_cluster_name)
    dr_helpers.wait_for_all_resources_creation(
        workload_instance.workload_pvc_count,
        workload_instance.workload_pod_count,
        workload_instance.workload_namespace,
    )

    # Verify resources deletion from primary cluster
    config.switch_to_cluster_by_name(primary_cluster_name)

    dr_helpers.wait_for_all_resources_deletion(workload_instance.workload_namespace)
