"""
Bootstrap CephX Key Cleanup Verification

Verify Rook removes bootstrap CephX keys after they are no longer needed and
that client.bootstrap-osd is recreated during OSD provisioning then cleaned up.
"""

import logging

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    green_squad,
    skipif_external_mode,
    skipif_ocs_version,
    tier1,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pod import (
    get_osd_pods,
    get_pods_having_label,
    wait_for_new_osd_pods_to_come_up,
)
from ocs_ci.ocs.resources.storage_cluster import add_capacity, get_osd_size
from ocs_ci.utility.utils import ceph_health_check

log = logging.getLogger(__name__)


@skipif_external_mode
@skipif_ocs_version("<4.19")
@green_squad
@pytest.mark.order("last")
class TestCephXBootstrapKeyCleanup:
    @tier1
    def test_cephx_bootstrap_key_cleanup(self, cephx_bootstrap_setup):
        """
        Verify bootstrap CephX keys are cleaned up and OSD bootstrap is ephemeral.

        Steps:
            1. Wait for cluster/daemons Ready (fixture).
            2. Record bootstrap keys, trigger reconcile, wait for non-OSD cleanup.
            3. Add OSD capacity and verify bootstrap-osd is recreated then removed.
            4. Verify operator deletion logs and idempotent re-reconcile behavior.
        """
        rotator = cephx_bootstrap_setup
        namespace = config.ENV_DATA["cluster_namespace"]

        ceph_health_check(namespace=namespace)

        pre_bootstrap_entities = rotator.discover_bootstrap_auth_entities()
        if pre_bootstrap_entities:
            log.info(
                "Bootstrap keys before cleanup: " f"{', '.join(pre_bootstrap_entities)}"
            )
        else:
            log.info("No bootstrap keys present before reconcile")

        rotator.trigger_cephcluster_reconcile()
        rotator.wait_for_post_mon_startup_bootstrap_cleanup(timeout=900)
        rotator.assert_bootstrap_keys_absent(constants.CEPHX_BOOTSTRAP_NON_OSD_KEYS)

        rotator.verify_operator_bootstrap_deletion_logs(
            [
                entity
                for entity in constants.CEPHX_BOOTSTRAP_NON_OSD_KEYS
                if entity in pre_bootstrap_entities
            ]
        )

        osd_pods_before = get_osd_pods(namespace=namespace)
        osd_count_before = len(osd_pods_before)
        log.info(f"OSD pod count before add_capacity: {osd_count_before}")

        try:
            osd_size = get_osd_size()
            log.info(f"Adding OSD capacity (device set size={osd_size})")
            add_capacity(osd_size)
        except Exception as exc:
            pytest.skip(f"Cannot add OSD capacity on this cluster: {exc}")

        bootstrap_osd_seen = rotator.wait_for_bootstrap_key_present(
            "client.bootstrap-osd", timeout=300
        )
        wait_for_new_osd_pods_to_come_up(osd_count_before)

        running_osd_count = len(
            get_pods_having_label(
                constants.OSD_APP_LABEL,
                namespace=namespace,
                statuses=[constants.STATUS_RUNNING],
            )
        )
        assert running_osd_count > osd_count_before, (
            f"Expected more Running OSD pods after add_capacity; "
            f"before={osd_count_before} after={running_osd_count}"
        )
        log.info(f"OSD pod count after add_capacity: {running_osd_count}")

        rotator.wait_for_bootstrap_osd_key_absent(timeout=1200)
        rotator.assert_bootstrap_keys_absent()

        if bootstrap_osd_seen:
            rotator.verify_operator_bootstrap_deletion_logs(["client.bootstrap-osd"])

        rotator.trigger_cephcluster_reconcile()
        rotator.wait_for_bootstrap_keys_absent(timeout=300)
        rotator.verify_no_bootstrap_deletion_errors()
        rotator.assert_bootstrap_keys_absent()

        ceph_health_check(namespace=namespace)
        rotator.wait_for_cluster_ready()
        rotator.wait_for_pgs_active_clean()

        log.info("Bootstrap CephX key cleanup verification completed successfully")
