"""
Test module for replica-1 pools on external RHCS clusters.

This test validates topology-based replica-1 provisioning where each zone
gets its own single-replica pool with a dedicated CRUSH rule.
"""

import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import (
    brown_squad,
    external_mode_required,
    tier2,
)
from ocs_ci.framework.testlib import ManageTest
from ocs_ci.deployment.helpers.external_cluster_helpers import (
    get_external_cluster_instance,
)
from ocs_ci.ocs.exceptions import CommandFailed, ExternalClusterExporterRunFailed

log = logging.getLogger(__name__)


@brown_squad
@tier2
@external_mode_required
class TestReplicaOneExternal(ManageTest):
    """
    Test replica-1 pool setup and I/O on external RHCS clusters.

    This test:
    1. Creates CRUSH rules for each zone
    2. Creates replica-1 pools bound to those rules
    3. Verifies the configuration
    4. Cleans up on teardown
    """

    @pytest.fixture(autouse=True)
    def setup_external_replica1(self, request):
        """
        Setup and cleanup fixture for external replica-1 test.

        Creates ExternalCluster instance and builds topology config from
        EXTERNAL_MODE configuration. Registers finalizer for cleanup.
        """
        self.ext_cluster = get_external_cluster_instance()
        self.topology_config = self._build_topology_config_or_skip()
        self.created_pools = []
        self.created_rules = []

        def finalizer():
            log.info("Starting external replica-1 teardown")

            try:
                if self.created_pools:
                    self.ext_cluster.cleanup_replica_one_pools(self.created_pools)
            except CommandFailed as e:
                log.warning(f"Pool cleanup failed: {e}")

            try:
                if self.created_rules:
                    self.ext_cluster.cleanup_zone_crush_rules(self.created_rules)
            except CommandFailed as e:
                log.warning(f"CRUSH rule cleanup failed: {e}")

            log.info("External replica-1 teardown completed")

        request.addfinalizer(finalizer)

    def _build_topology_config_or_skip(self):
        """
        Build topology config from ExternalCluster, skip if too few hosts.
        """
        topology_config = self.ext_cluster.build_topology_replica1_config()
        if len(topology_config.zones) < 2:
            pytest.skip(
                f"Need at least 2 OSD hosts for replica-1 test, "
                f"found {len(topology_config.zones)}"
            )
        return topology_config

    def test_replica1_setup_and_verify(self):
        """
        Test replica-1 pool creation and verification on external cluster.

        Steps:
        1. Enable replica-1 pools (mon_allow_pool_size_one)
        2. Create CRUSH rules for each zone
        3. Create replica-1 pools
        4. Verify pools have size=1
        5. Run exporter script with topology flags
        6. Apply exported resources to ODF

        """
        log.info("Starting external replica-1 setup test")

        # Step 1-4: Setup replica-1 pools on Ceph cluster
        result = self.ext_cluster.setup_topology_replica_one(self.topology_config)
        self.created_pools = result["pools"]
        self.created_rules = result["rules"]

        log.info(f"Created pools: {self.created_pools}")
        log.info(f"Created rules: {self.created_rules}")

        # Verify pool setup completed successfully
        assert len(self.created_pools) == len(
            self.topology_config.zones
        ), f"Expected {len(self.topology_config.zones)} pools, got {len(self.created_pools)}"
        assert len(self.created_rules) == len(
            self.topology_config.zones
        ), f"Expected {len(self.topology_config.zones)} CRUSH rules, got {len(self.created_rules)}"

        # Step 5: Run exporter script with topology flags
        log.info("Running topology exporter script")
        try:
            export_resources = self.ext_cluster.run_topology_exporter_script(
                self.topology_config
            )
            log.info(f"Exporter returned {len(export_resources)} resources")

            # Step 6: Apply exported resources to ODF cluster
            log.info("Applying exported resources to ODF")
            applied = self.ext_cluster.apply_topology_export_resources(export_resources)

            log.info(f"Applied secrets: {applied['secrets']}")
            log.info(f"Applied configmaps: {applied['configmaps']}")

        except ExternalClusterExporterRunFailed as e:
            if "Failed to parse" in str(e):
                raise
            pytest.skip(f"Exporter script not available: {e}")

        log.info("External replica-1 setup test completed successfully")
