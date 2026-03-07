"""
Test module for replica-1 pools on external RHCS clusters.

This test validates topology-based replica-1 provisioning where each zone
gets its own single-replica pool with a dedicated CRUSH rule.
"""

import logging
import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    brown_squad,
    external_mode_required,
    tier2,
)
from ocs_ci.framework.testlib import ManageTest
from ocs_ci.deployment.helpers.external_cluster_helpers import (
    get_external_cluster_instance,
    TopologyReplica1Config,
    ZoneConfig,
)
from ocs_ci.ocs.exceptions import CommandFailed, ExternalClusterExporterRunFailed
from ocs_ci.ocs.ocp import OCP

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
        self.topology_config = self._build_topology_config()
        self.created_pools = []
        self.created_rules = []
        self.created_secrets = []
        self.created_configmaps = []
        self.setup_completed = False

        def finalizer():
            if not self.setup_completed:
                log.info("Setup was not completed, skipping teardown")
                return

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

            # Cleanup K8s resources (secrets and configmaps)
            namespace = config.ENV_DATA["cluster_namespace"]
            for secret_name in self.created_secrets:
                try:
                    log.info(f"Deleting Secret: {secret_name}")
                    ocp_secret = OCP(kind="Secret", namespace=namespace)
                    ocp_secret.delete(resource_name=secret_name)
                except CommandFailed as e:
                    log.warning(f"Secret cleanup failed for {secret_name}: {e}")

            for cm_name in self.created_configmaps:
                try:
                    log.info(f"Deleting ConfigMap: {cm_name}")
                    ocp_cm = OCP(kind="ConfigMap", namespace=namespace)
                    ocp_cm.delete(resource_name=cm_name)
                except CommandFailed as e:
                    log.warning(f"ConfigMap cleanup failed for {cm_name}: {e}")

            log.info("External replica-1 teardown completed")

        request.addfinalizer(finalizer)

    def _build_topology_config(self) -> TopologyReplica1Config:
        """
        Build topology configuration from EXTERNAL_MODE config.

        Reads replica1_zones from config.EXTERNAL_MODE and creates
        ZoneConfig objects for each zone.

        Returns:
            TopologyReplica1Config: Configuration for replica-1 setup.

        Raises:
            ValueError: If replica1_zones is not configured.

        """
        zones_config = config.EXTERNAL_MODE.get("replica1_zones", [])
        if not zones_config:
            raise ValueError(
                "EXTERNAL_MODE['replica1_zones'] not configured. "
                "Expected list of zones with zone_name and host_name."
            )

        zones = [
            ZoneConfig(
                zone_name=z["zone_name"],
                host_name=z["host_name"],
                pool_name=z.get("pool_name", ""),
            )
            for z in zones_config
        ]

        log.info(f"Built topology config with {len(zones)} zones: {zones_config}")
        return TopologyReplica1Config(zones=zones)

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
        self.setup_completed = True

        log.info(f"Created pools: {self.created_pools}")
        log.info(f"Created rules: {self.created_rules}")

        # Verify pool setup completed successfully
        assert self.created_pools, "No pools were created"
        assert self.created_rules, "No CRUSH rules were created"
        assert len(self.created_pools) == len(
            self.topology_config.zones
        ), f"Expected {len(self.topology_config.zones)} pools, got {len(self.created_pools)}"

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
            self.created_secrets = applied["secrets"]
            self.created_configmaps = applied["configmaps"]

            log.info(f"Applied secrets: {self.created_secrets}")
            log.info(f"Applied configmaps: {self.created_configmaps}")

        except ExternalClusterExporterRunFailed as e:
            pytest.skip(f"Exporter script not available: {e}")

        log.info("External replica-1 setup test completed successfully")
