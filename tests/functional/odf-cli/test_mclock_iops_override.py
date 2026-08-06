import logging

import pytest

from ocs_ci.ocs.cluster import (
    ceph_health_check,
    get_mclock_max_capacity_iops_config_key,
)
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.resources import pod
from ocs_ci.utility.retry import catch_exceptions
from ocs_ci.framework.testlib import (
    tier1,
    brown_squad,
    skipif_ocs_version,
    skipif_external_mode,
    runs_on_provider,
    ManageTest,
)

log = logging.getLogger(__name__)


@tier1
@brown_squad
@skipif_ocs_version("<4.15")
@skipif_external_mode
@runs_on_provider
class TestMclockIopsOverride(ManageTest):
    """
    Test OSD mClock max capacity IOPS override via ODF CLI.

    Covers reading, setting (per-OSD and global), persisting
    across OSD pod restart, and removing the override.
    """

    @pytest.fixture(autouse=True)
    def setup(self, request, odf_cli_setup):
        """
        Determine the mclock IOPS config key and default value
        based on the cluster disk type.
        """
        self.odf_cli = odf_cli_setup
        self.custom_iops_value = "80000"
        self.config_key, self.default_value = get_mclock_max_capacity_iops_config_key()
        log.info(
            "Using config key %s with default %s",
            self.config_key,
            self.default_value,
        )

        osd_pods = pod.get_osd_pods()
        self.osd_ids = sorted([pod.get_osd_pod_id(p) for p in osd_pods])
        log.info("Discovered OSD IDs: %s", self.osd_ids)

        def finalizer():
            log.info("Cleaning up mclock IOPS overrides")
            safe_rm = catch_exceptions(CommandFailed)(self.odf_cli.run_ceph_config_rm)
            for osd_id in self.osd_ids:
                safe_rm(f"osd.{osd_id}", self.config_key)
            safe_rm("global", self.config_key)

        request.addfinalizer(finalizer)

    def test_get_mclock_iops_value(self):
        """
        Test Case 1: Get osd_mclock_max_capacity_iops value via
        ODF CLI.

        Steps:
            1. Get the cluster-wide IOPS value via
               ``odf ceph config get``.
            2. Get the per-OSD value via ``odf ceph config get``
               for each OSD.
            3. Verify the returned values match the expected
               default and are consistent across OSDs.

        """
        value = self.odf_cli.run_ceph_config_get("osd", self.config_key)
        log.info(
            "Cluster-wide %s value: %s",
            self.config_key,
            value,
        )
        assert value, f"odf ceph config get returned empty " f"for {self.config_key}"
        assert float(value) == self.default_value, (
            f"Expected default {self.default_value}, " f"got {value}"
        )

        for osd_id in self.osd_ids:
            osd_value = self.odf_cli.run_ceph_config_get(
                f"osd.{osd_id}", self.config_key
            )
            assert float(osd_value) == self.default_value, (
                f"osd.{osd_id} expected {self.default_value}, " f"got {osd_value}"
            )
            log.info(
                "osd.%s %s = %s (matches default)",
                osd_id,
                self.config_key,
                osd_value,
            )

    def test_override_mclock_iops_per_osd(self):
        """
        Test Case 2: Override osd_mclock_max_capacity_iops per OSD
        via ODF CLI.

        Steps:
            1. Set a custom IOPS value for osd.0.
            2. Verify osd.0 reports the new value.
            3. Verify other OSDs still have the original value.
            4. Verify the override appears in ceph config dump.

        """
        target_osd = self.osd_ids[0]
        other_osd = self.osd_ids[1]

        self.odf_cli.run_ceph_config_set(
            f"osd.{target_osd}",
            self.config_key,
            self.custom_iops_value,
        )
        self.odf_cli.wait_for_ceph_config_value(
            f"osd.{target_osd}",
            self.config_key,
            self.custom_iops_value,
        )

        other_value = self.odf_cli.run_ceph_config_get(
            f"osd.{other_osd}", self.config_key
        )
        assert float(other_value) == self.default_value, (
            f"osd.{other_osd} should still be "
            f"{self.default_value}, got {other_value}"
        )

        dump_output = self.odf_cli.run_ceph_config_dump()
        assert (
            self.config_key in dump_output
        ), f"{self.config_key} not found in ceph config dump"
        log.info("Per-OSD override visible in config dump")

    def test_override_mclock_iops_cluster_wide(self):
        """
        Test Case 3: Override osd_mclock_max_capacity_iops
        cluster-wide.

        Steps:
            1. Remove any per-OSD overrides.
            2. Set a global IOPS value.
            3. Verify all OSDs report the new value.

        """
        for osd_id in self.osd_ids:
            self.odf_cli.run_ceph_config_rm(f"osd.{osd_id}", self.config_key)

        self.odf_cli.run_ceph_config_set(
            "global", self.config_key, self.custom_iops_value
        )
        log.info(
            "Set global %s=%s",
            self.config_key,
            self.custom_iops_value,
        )

        for osd_id in self.osd_ids:
            self.odf_cli.wait_for_ceph_config_value(
                f"osd.{osd_id}",
                self.config_key,
                self.custom_iops_value,
            )

    def test_mclock_iops_persists_after_osd_restart(self):
        """
        Test Case 4: IOPS override persists after OSD pod restart.

        Steps:
            1. Set a custom per-OSD IOPS value for osd.0.
            2. Verify the override is in place.
            3. Delete the OSD pod for osd.0 to trigger a restart.
            4. Wait for the OSD pod to come back up.
            5. Verify the IOPS value is still the custom value.
            6. Verify cluster health returns to HEALTH_OK.

        """
        target_osd = self.osd_ids[0]

        self.odf_cli.run_ceph_config_set(
            f"osd.{target_osd}",
            self.config_key,
            self.custom_iops_value,
        )
        self.odf_cli.wait_for_ceph_config_value(
            f"osd.{target_osd}",
            self.config_key,
            self.custom_iops_value,
        )

        osd_pod_list = pod.get_osd_pods_having_ids([target_osd])
        assert osd_pod_list, f"No OSD pod found for osd.{target_osd}"
        osd_pod_obj = osd_pod_list[0]
        log.info("Deleting OSD pod %s", osd_pod_obj.name)
        osd_pod_obj.delete(wait=True)

        log.info("Waiting for OSD pod to restart")
        pod.wait_for_pods_to_be_running(timeout=300)

        self.odf_cli.wait_for_ceph_config_value(
            f"osd.{target_osd}",
            self.config_key,
            self.custom_iops_value,
        )

        dump_output = self.odf_cli.run_ceph_config_dump()
        assert (
            self.config_key in dump_output
        ), f"{self.config_key} not in config dump after restart"

        ceph_health_check(tries=20, delay=30)
        log.info("Cluster health is OK after OSD restart")

    def test_remove_mclock_iops_override_reverts_to_default(self):
        """
        Test Case 5: Remove IOPS override reverts to default.

        Steps:
            1. Set a custom per-OSD IOPS value for osd.0.
            2. Verify the override is in place.
            3. Remove the override.
            4. Verify the value reverts to the default.
            5. Verify ceph config dump no longer shows a per-OSD
               entry.

        """
        target_osd = self.osd_ids[0]

        self.odf_cli.run_ceph_config_set(
            f"osd.{target_osd}",
            self.config_key,
            self.custom_iops_value,
        )
        self.odf_cli.wait_for_ceph_config_value(
            f"osd.{target_osd}",
            self.config_key,
            self.custom_iops_value,
        )

        self.odf_cli.run_ceph_config_rm(f"osd.{target_osd}", self.config_key)
        log.info("Removed override for osd.%s", target_osd)

        value_after = self.odf_cli.run_ceph_config_get("osd", self.config_key)
        assert float(value_after) == self.default_value, (
            f"After removal expected {self.default_value}, " f"got {value_after}"
        )
        log.info("Value reverted to default: %s", value_after)

        dump_output = self.odf_cli.run_ceph_config_dump()
        osd_entry = f"osd.{target_osd}"
        for line in dump_output.splitlines():
            if osd_entry in line and self.config_key in line:
                pytest.fail(
                    f"Per-OSD entry for {osd_entry} still in "
                    f"config dump after removal"
                )
        log.info(
            "No per-OSD %s entry in config dump",
            self.config_key,
        )
