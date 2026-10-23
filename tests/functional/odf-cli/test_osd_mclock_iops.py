import logging

from ocs_ci.framework.testlib import (
    tier1,
    tier2,
    brown_squad,
    polarion_id,
    skipif_ocs_version,
    skipif_external_mode,
    runs_on_provider,
)
from ocs_ci.utility.retry import retry

logger = logging.getLogger(__name__)


@retry(AssertionError, tries=6, delay=5, backoff=1)
def assert_osd_iops(odf_cli, osd_ids, key, expected, context):
    """
    Assert that every given OSD daemon is running with `expected` for `key`.

    Retried because the mon pushes a config change to the daemons
    asynchronously, so the value can lag a `ceph config set`/`rm` by a moment.

    Args:
        odf_cli (ODFCliRunner): ODF CLI runner.
        osd_ids (list): OSD ids to check.
        key (str): mClock max-capacity-IOPS config key.
        expected (float): The value every OSD is expected to be running with.
        context (str): Short description included in the failure message.

    """
    for osd_id in osd_ids:
        value = odf_cli.get_osd_running_iops(osd_id, key)
        assert (
            value == expected
        ), f"osd.{osd_id} {key} is {value}, expected {expected} ({context})"


@brown_squad
@skipif_ocs_version("<4.23")
@skipif_external_mode
@runs_on_provider
class TestOSDMclockMaxCapacityIOPS:
    """
    Manual override of osd_mclock_max_capacity_iops via the ODF CLI (RHSTOR-8677).
    """

    CUSTOM_IOPS_VALUE = 80000
    GLOBAL_IOPS_VALUE = 60000

    @tier2
    @polarion_id("OCS-8256")
    def test_read_default_iops_value(self, mclock_iops_context):
        """
        Read osd_mclock_max_capacity_iops through the ODF CLI and verify the
        cluster-wide value and every running OSD report a usable value. Per-OSD
        values are allowed to differ, each OSD benchmarks its own device at
        first boot.

        Args:
            mclock_iops_context: Fixture giving (ODFCliRunner, osd_ids, key).

        """
        odf_cli, osd_ids, key = mclock_iops_context

        cluster_value = float(odf_cli.get_ceph_config_value("osd", key))
        assert cluster_value > 0, f"{key} is {cluster_value}, expected a positive value"
        logger.info(f"Cluster-wide {key} = {cluster_value}")

        for osd_id in osd_ids:
            value = odf_cli.get_osd_running_iops(osd_id, key)
            assert (
                value > 0
            ), f"osd.{osd_id} {key} is {value}, expected a positive value"
            logger.info(f"osd.{osd_id} is running with {key} = {value}")

    @tier1
    @polarion_id("OCS-8257")
    def test_per_osd_iops_override_and_removal(self, mclock_config_cleanup):
        """
        Override osd_mclock_max_capacity_iops on a single OSD via the ODF CLI,
        verify the other OSDs are unaffected, then remove it and verify revert.

        Args:
            mclock_config_cleanup: Fixture giving (ODFCliRunner, osd_ids, key)
                with all mClock IOPS entries cleared.

        """
        odf_cli, osd_ids, key = mclock_config_cleanup
        target_osd, other_osds = osd_ids[0], osd_ids[1:]

        baseline = odf_cli.get_settled_osd_iops(osd_ids, key)
        logger.info(f"Baseline {key} on every OSD = {baseline}")
        assert baseline != self.CUSTOM_IOPS_VALUE, (
            f"Baseline {key} already equals the test value "
            f"{self.CUSTOM_IOPS_VALUE}; pick a different value"
        )

        logger.test_step(f"Setting osd.{target_osd} {key} = {self.CUSTOM_IOPS_VALUE}")
        odf_cli.run_ceph_config(f"set osd.{target_osd} {key} {self.CUSTOM_IOPS_VALUE}")

        assert_osd_iops(
            odf_cli, [target_osd], key, self.CUSTOM_IOPS_VALUE, "per-OSD override"
        )
        assert_osd_iops(
            odf_cli, other_osds, key, baseline, f"unaffected by osd.{target_osd}"
        )

        entries = odf_cli.get_ceph_config_dump_entries(key)
        assert [who for who, _ in entries] == [f"osd.{target_osd}"], (
            f"Expected a single osd.{target_osd} entry in `ceph config dump`, "
            f"found {entries}"
        )

        logger.test_step(
            f"Removing osd.{target_osd} {key} override and verifying revert"
        )
        odf_cli.run_ceph_config(f"rm osd.{target_osd} {key}")
        assert_osd_iops(odf_cli, [target_osd], key, baseline, "after override removal")

        entries = odf_cli.get_ceph_config_dump_entries(key)
        assert not entries, f"{key} still present in `ceph config dump`: {entries}"

    @tier2
    @polarion_id("OCS-8258")
    def test_per_osd_override_precedence_over_global(self, mclock_config_cleanup):
        """
        Verify a per-OSD osd_mclock_max_capacity_iops override takes precedence
        over a cluster-wide one set through the ODF CLI.

        Args:
            mclock_config_cleanup: Fixture giving (ODFCliRunner, osd_ids, key)
                with all mClock IOPS entries cleared.

        """
        odf_cli, osd_ids, key = mclock_config_cleanup
        target_osd, other_osds = osd_ids[0], osd_ids[1:]

        logger.test_step(
            f"Setting global {key} = {self.GLOBAL_IOPS_VALUE} and "
            f"osd.{target_osd} {key} = {self.CUSTOM_IOPS_VALUE}"
        )
        odf_cli.run_ceph_config(f"set global {key} {self.GLOBAL_IOPS_VALUE}")
        odf_cli.run_ceph_config(f"set osd.{target_osd} {key} {self.CUSTOM_IOPS_VALUE}")

        assert_osd_iops(
            odf_cli,
            [target_osd],
            key,
            self.CUSTOM_IOPS_VALUE,
            "per-OSD override wins over global",
        )
        assert_osd_iops(
            odf_cli,
            other_osds,
            key,
            self.GLOBAL_IOPS_VALUE,
            "follows the global override",
        )

        entries = odf_cli.get_ceph_config_dump_entries(key)
        assert {who for who, _ in entries} == {"global", f"osd.{target_osd}"}, (
            f"Expected global and osd.{target_osd} entries in "
            f"`ceph config dump`, found {entries}"
        )

    @tier2
    @polarion_id("OCS-8259")
    def test_global_osd_mclock_iops_override(self, mclock_config_cleanup):
        """
        Set a cluster-wide osd_mclock_max_capacity_iops override via the ODF
        CLI, verify every OSD reports it, then confirm removal reverts it.

        Args:
            mclock_config_cleanup: Fixture giving (ODFCliRunner, osd_ids, key)
                with all mClock IOPS entries cleared.

        """
        odf_cli, osd_ids, key = mclock_config_cleanup

        baseline = odf_cli.get_settled_osd_iops(osd_ids, key)
        logger.info(f"Baseline {key} on every OSD = {baseline}")
        assert baseline != self.CUSTOM_IOPS_VALUE, (
            f"Baseline {key} already equals the test value "
            f"{self.CUSTOM_IOPS_VALUE}; pick a different value"
        )

        logger.info(f"Setting global {key} = {self.CUSTOM_IOPS_VALUE}")
        odf_cli.run_ceph_config(f"set global {key} {self.CUSTOM_IOPS_VALUE}")
        assert_osd_iops(
            odf_cli, osd_ids, key, self.CUSTOM_IOPS_VALUE, "global override"
        )

        entries = odf_cli.get_ceph_config_dump_entries(key)
        assert any(
            who == "global" for who, _ in entries
        ), f"No global {key} entry in `ceph config dump`: {entries}"

        logger.test_step(f"Removing global {key} override and verifying revert")
        odf_cli.run_ceph_config(f"rm global {key}")
        assert_osd_iops(
            odf_cli, osd_ids, key, baseline, "after global override removal"
        )

        entries = odf_cli.get_ceph_config_dump_entries(key)
        assert not entries, f"{key} still present in `ceph config dump`: {entries}"
