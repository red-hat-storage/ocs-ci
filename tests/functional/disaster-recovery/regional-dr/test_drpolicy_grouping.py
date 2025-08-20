import logging

from ocs_ci.framework import config
from ocs_ci.framework.testlib import (
    acceptance,
    post_ocs_upgrade,
    rdr,
    skipif_ocs_version,
    tier1,
    turquoise_squad,
)
from ocs_ci.helpers import dr_helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.utils import get_non_acm_cluster_config

logger = logging.getLogger(__name__)


@rdr
@tier1
@acceptance
@turquoise_squad
@post_ocs_upgrade
@skipif_ocs_version("<4.20")
class TestDRPolicyGrouping:
    """
    Validate DRPolicy has grouping=true for every storageClass in status.async.peerClasses
    and that a VGRC exists on each managed cluster per scheduling interval.

    """

    def test_drpolicy_grouping(self):
        config.switch_acm_ctx()

        drpolicies = dr_helpers.get_all_drpolicy()
        scheduling_intervals = []

        for drp in drpolicies:
            drp_name = drp.get("metadata").get("name")
            peer_classes = drp.get("status").get("async").get("peerClasses")

            # Assert grouping is true for every storageClass in peerClasses
            logger.info(f"Check grouping for storageClasses in DRPolicy: {drp_name}")
            sc_with_grouping = [
                pc.get("storageClassName")
                for pc in peer_classes
                if pc.get("grouping", False)
            ]
            logger.info(f"Grouping is true for storageClasses: {sc_with_grouping}")
            sc_without_grouping = [
                pc.get("storageClassName")
                for pc in peer_classes
                if not pc.get("grouping", False)
            ]
            assert (
                not sc_without_grouping
            ), f"Grouping is not true for storageClasses: {sc_without_grouping}"

            logger.info(
                f"Verified grouping is true for every storageClass in DRPolicy: {drp_name}"
            )

            scheduling_interval = drp.get("spec").get("schedulingInterval")
            scheduling_intervals.append(scheduling_interval)

        scheduling_intervals = list(set(scheduling_intervals))
        logger.info(f"Scheduling Intervals: {scheduling_intervals}")

        for cluster in get_non_acm_cluster_config():
            config.switch_ctx(cluster.MULTICLUSTER["multicluster_index"])
            vgrc_count = dr_helpers.get_resource_count(
                kind=constants.VOLUME_GROUP_REPLICATION_CLASS
            )
            logger.info(
                f"{constants.VOLUME_GROUP_REPLICATION_CLASS} count in {cluster.ENV_DATA['cluster_name']}= {vgrc_count}"
            )
            assert vgrc_count == len(scheduling_intervals)
