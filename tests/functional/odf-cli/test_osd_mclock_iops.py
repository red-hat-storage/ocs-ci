import logging

import pytest

from ocs_ci.framework.testlib import (
    tier1,
    brown_squad,
    polarion_id,
    skipif_ocs_version,
    skipif_external_mode,
    runs_on_provider,
)
from ocs_ci.ocs.resources.pod import get_osd_pods, get_osd_pod_id
from ocs_ci.utility.retry import retry

logger = logging.getLogger(__name__)

MCLOCK_IOPS_KEYS = (
    "osd_mclock_max_capacity_iops_ssd",
    "osd_mclock_max_capacity_iops_hdd",
)


def get_osd_running_iops(odf_cli, osd_id, key):
    """
    Read the mClock max-capacity-IOPS value an OSD daemon is actually running
    with, as opposed to what the mon config DB would resolve to.

    Args:
        odf_cli (ODFCliRunner): ODF CLI runner.
        osd_id (str): OSD id.
        key (str): mClock max-capacity-IOPS config key.

    Returns:
        float: The value reported by `ceph config show osd.<id>`.

    """
    return float(odf_cli.get_ceph_config_value(f"osd.{osd_id}", key, subcommand="show"))


@retry(AssertionError, tries=6, delay=5, backoff=1)
def get_settled_osd_iops(odf_cli, osd_ids, key):
    """
    Return the mClock max-capacity-IOPS value every OSD daemon is running with,
    once they all agree on it.

    Retried because the mon pushes config changes to the daemons
    asynchronously, so the OSDs converge a moment after the overrides are
    cleared rather than instantly.

    Args:
        odf_cli (ODFCliRunner): ODF CLI runner.
        osd_ids (list): OSD ids to check.
        key (str): mClock max-capacity-IOPS config key.

    Returns:
        float: The value shared by every OSD.

    """
    values = {get_osd_running_iops(odf_cli, osd_id, key) for osd_id in osd_ids}
    assert len(values) == 1, f"OSDs have not settled on a single {key} value: {values}"
    return values.pop()


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
        value = get_osd_running_iops(odf_cli, osd_id, key)
        assert (
            value == expected
        ), f"osd.{osd_id} {key} is {value}, expected {expected} ({context})"


@pytest.fixture
def mclock_iops_context(odf_cli_setup):
    """
    Resolve the OSDs and the mClock max-capacity-IOPS key they use.

    Ceph mClock selects the `_ssd` or `_hdd` key by the OSD bluestore device
    type, which can differ from the OSD's CRUSH device class.

    Args:
        odf_cli_setup: ODF CLI runner fixture.

    Returns:
        tuple: (ODFCliRunner, list of OSD ids, mClock IOPS config key).

    """
    odf_cli = odf_cli_setup
    osd_ids = [get_osd_pod_id(pod) for pod in get_osd_pods()]
    assert osd_ids, "No OSD pods found on the cluster"

    bdev_types = {odf_cli.get_osd_bdev_type(osd_id) for osd_id in osd_ids}
    if len(bdev_types) > 1:
        pytest.skip(f"Mixed OSD bluestore device types {bdev_types} are not supported")

    key = f"osd_mclock_max_capacity_iops_{bdev_types.pop()}"
    logger.info(f"OSDs {osd_ids} report device type -> config key '{key}'")
    return odf_cli, osd_ids, key


@pytest.fixture
def mclock_config_cleanup(mclock_iops_context, request):
    """
    Clear the mClock max-capacity-IOPS entries for the test and put the
    pre-existing ones back afterwards.

    Ceph stores each OSD's boot benchmark result in the mon config DB under
    these same keys, so the entries are saved before being cleared and are
    restored on teardown - dropping them would leave the cluster running on
    the compiled-in mClock defaults until the OSDs restart.

    Args:
        mclock_iops_context: Fixture giving (ODFCliRunner, osd_ids, key).
        request: Pytest request, used to register the teardown.

    Returns:
        tuple: (ODFCliRunner, list of OSD ids, mClock IOPS config key).

    """
    odf_cli, osd_ids, key = mclock_iops_context

    original_entries = {
        (who, iops_key): value
        for iops_key in MCLOCK_IOPS_KEYS
        for who, value in odf_cli.get_ceph_config_dump_entries(iops_key)
    }
    logger.info(f"Saved the original mClock IOPS entries: {original_entries}")

    def clear_entries():
        for iops_key in MCLOCK_IOPS_KEYS:
            for who, _ in odf_cli.get_ceph_config_dump_entries(iops_key):
                logger.info(f"Clearing {who} {iops_key}")
                odf_cli.run_ceph_config(f"rm {who} {iops_key}")

    def restore_entries():
        clear_entries()
        for (who, iops_key), value in original_entries.items():
            logger.info(f"Restoring {who} {iops_key} = {value}")
            odf_cli.run_ceph_config(f"set {who} {iops_key} {value}")

    clear_entries()
    request.addfinalizer(restore_entries)
    return odf_cli, osd_ids, key


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

    @tier1
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
            value = get_osd_running_iops(odf_cli, osd_id, key)
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

        baseline = get_settled_osd_iops(odf_cli, osd_ids, key)
        logger.info(f"Baseline {key} on every OSD = {baseline}")
        assert baseline != self.CUSTOM_IOPS_VALUE, (
            f"Baseline {key} already equals the test value "
            f"{self.CUSTOM_IOPS_VALUE}; pick a different value"
        )

        logger.info(f"Setting osd.{target_osd} {key} = {self.CUSTOM_IOPS_VALUE}")
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

        logger.info(f"Removing osd.{target_osd} {key} override and verifying revert")
        odf_cli.run_ceph_config(f"rm osd.{target_osd} {key}")
        assert_osd_iops(odf_cli, [target_osd], key, baseline, "after override removal")

        entries = odf_cli.get_ceph_config_dump_entries(key)
        assert not entries, f"{key} still present in `ceph config dump`: {entries}"

    @tier1
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

        logger.info(
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

    @tier1
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

        baseline = get_settled_osd_iops(odf_cli, osd_ids, key)
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

        logger.info(f"Removing global {key} override and verifying revert")
        odf_cli.run_ceph_config(f"rm global {key}")
        assert_osd_iops(
            odf_cli, osd_ids, key, baseline, "after global override removal"
        )

        entries = odf_cli.get_ceph_config_dump_entries(key)
        assert not entries, f"{key} still present in `ceph config dump`: {entries}"
