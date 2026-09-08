import logging

import pytest

from ocs_ci.framework.testlib import tier1, brown_squad, polarion_id
from ocs_ci.ocs.resources.pod import get_osd_pods, get_osd_pod_id
from ocs_ci.utility.utils import ceph_health_check

logger = logging.getLogger(__name__)

MCLOCK_IOPS_KEYS = (
    "osd_mclock_max_capacity_iops_ssd",
    "osd_mclock_max_capacity_iops_hdd",
)
# Ceph's documented defaults. Only logged, never asserted - the effective value
# is whatever the OSD benchmark measured at deployment time.
DOCUMENTED_IOPS_DEFAULTS = {
    "osd_mclock_max_capacity_iops_ssd": 21500,
    "osd_mclock_max_capacity_iops_hdd": 315,
}


def get_mclock_iops_key(odf_cli, osd_ids):
    """
    Resolve the mClock max-capacity-IOPS key from the OSDs' device type.

    Args:
        odf_cli (ODFCliRunner): ODF CLI runner.
        osd_ids (list): OSD ids to inspect.

    Returns:
        str: "osd_mclock_max_capacity_iops_ssd" or "..._hdd".

    """
    bdev_types = {odf_cli.get_osd_bdev_type(osd_id) for osd_id in osd_ids}
    if len(bdev_types) > 1:
        pytest.skip(f"Mixed OSD bluestore device types {bdev_types} are not supported")
    return f"osd_mclock_max_capacity_iops_{bdev_types.pop()}"


@pytest.fixture
def mclock_config_cleanup(odf_cli_setup, request):
    """
    Clear every mClock max-capacity-IOPS override before and after the test.

    Args:
        odf_cli_setup: ODF CLI runner fixture.
        request: Pytest request, used to register the teardown.

    Returns:
        tuple: (ODFCliRunner, list of OSD ids).

    """
    odf_cli = odf_cli_setup
    osd_ids = [get_osd_pod_id(pod) for pod in get_osd_pods()]
    assert osd_ids, "No OSD pods found on the cluster"

    def clear_overrides():
        for key in MCLOCK_IOPS_KEYS:
            for who, _ in odf_cli.get_ceph_config_dump_entries(key):
                logger.info(f"Clearing {who} {key}")
                odf_cli.run_ceph_config(f"rm {who} {key}")

    clear_overrides()
    request.addfinalizer(clear_overrides)
    return odf_cli, osd_ids


@brown_squad
class TestOSDMclockMaxCapacityIOPS:
    """
    Manual override of osd_mclock_max_capacity_iops via the ODF CLI (RHSTOR-8677).
    """

    CUSTOM_IOPS_VALUE = 80000
    GLOBAL_IOPS_VALUE = 60000

    @tier1
    @polarion_id("OCS-8256")
    def test_read_default_iops_value(self, mclock_config_cleanup):
        """
        Read osd_mclock_max_capacity_iops via the ODF CLI with no override in
        place and verify every OSD reports the same usable value.

        Args:
            mclock_config_cleanup: Fixture giving (ODFCliRunner, osd_ids) with
                all mClock IOPS overrides cleared.

        """
        odf_cli, osd_ids = mclock_config_cleanup
        key = get_mclock_iops_key(odf_cli, osd_ids)
        logger.info(f"OSDs report device type -> config key '{key}'")

        entries = odf_cli.get_ceph_config_dump_entries(key)
        assert not entries, f"{key} is overridden, expected no entry: {entries}"

        cluster_value = float(odf_cli.get_ceph_config_value("osd", key))
        assert cluster_value > 0, f"{key} is {cluster_value}, expected a positive value"

        documented = DOCUMENTED_IOPS_DEFAULTS[key]
        if cluster_value != documented:
            logger.warning(
                f"{key} is {cluster_value}, not the documented Ceph default "
                f"{documented} - benchmark result or a pre-existing setting"
            )

        for osd_id in osd_ids:
            value = float(odf_cli.get_ceph_config_value(f"osd.{osd_id}", key))
            assert value == cluster_value, (
                f"osd.{osd_id} {key} is {value}, expected the cluster-wide "
                f"value {cluster_value}"
            )
        logger.info(f"All OSDs report {key} = {cluster_value}")

    @tier1
    @polarion_id("OCS-8257")
    def test_per_osd_iops_override_and_removal(self, mclock_config_cleanup):
        """
        Override osd_mclock_max_capacity_iops on a single OSD via the ODF CLI,
        verify the other OSDs are unaffected, then remove it and verify revert.

        Args:
            mclock_config_cleanup: Fixture giving (ODFCliRunner, osd_ids) with
                all mClock IOPS overrides cleared.

        """
        odf_cli, osd_ids = mclock_config_cleanup
        key = get_mclock_iops_key(odf_cli, osd_ids)
        target_osd, other_osds = osd_ids[0], osd_ids[1:]

        baseline = float(odf_cli.get_ceph_config_value(f"osd.{target_osd}", key))
        logger.info(f"Baseline osd.{target_osd} {key} = {baseline}")
        assert baseline != float(self.CUSTOM_IOPS_VALUE), (
            f"Baseline {key} already equals the test value "
            f"{self.CUSTOM_IOPS_VALUE}; pick a different value"
        )

        logger.info(f"Setting osd.{target_osd} {key} = {self.CUSTOM_IOPS_VALUE}")
        odf_cli.run_ceph_config(f"set osd.{target_osd} {key} {self.CUSTOM_IOPS_VALUE}")

        value = float(odf_cli.get_ceph_config_value(f"osd.{target_osd}", key))
        assert value == float(self.CUSTOM_IOPS_VALUE), (
            f"osd.{target_osd} {key} not overridden: expected "
            f"{self.CUSTOM_IOPS_VALUE}, found {value}"
        )
        for osd_id in other_osds:
            value = float(odf_cli.get_ceph_config_value(f"osd.{osd_id}", key))
            assert value == baseline, (
                f"osd.{osd_id} {key} changed by the override on "
                f"osd.{target_osd}: expected {baseline}, found {value}"
            )
        logger.info(f"Only osd.{target_osd} is overridden")

        entries = odf_cli.get_ceph_config_dump_entries(key)
        assert [who for who, _ in entries] == [f"osd.{target_osd}"], (
            f"Expected a single osd.{target_osd} entry in `ceph config dump`, "
            f"found {entries}"
        )

        logger.info(f"Removing osd.{target_osd} {key} override and verifying revert")
        odf_cli.run_ceph_config(f"rm osd.{target_osd} {key}")
        reverted = float(odf_cli.get_ceph_config_value(f"osd.{target_osd}", key))
        assert reverted == baseline, (
            f"osd.{target_osd} {key} did not revert: expected {baseline}, "
            f"found {reverted}"
        )

        entries = odf_cli.get_ceph_config_dump_entries(key)
        assert not entries, f"{key} still present in `ceph config dump`: {entries}"

        ceph_health_check()

    @tier1
    @polarion_id("OCS-8258")
    def test_per_osd_override_precedence_over_global(self, mclock_config_cleanup):
        """
        Verify a per-OSD osd_mclock_max_capacity_iops override takes precedence
        over a cluster-wide one set through the ODF CLI.

        Args:
            mclock_config_cleanup: Fixture giving (ODFCliRunner, osd_ids) with
                all mClock IOPS overrides cleared.

        """
        odf_cli, osd_ids = mclock_config_cleanup
        key = get_mclock_iops_key(odf_cli, osd_ids)
        target_osd, other_osds = osd_ids[0], osd_ids[1:]

        logger.info(
            f"Setting global {key} = {self.GLOBAL_IOPS_VALUE} and "
            f"osd.{target_osd} {key} = {self.CUSTOM_IOPS_VALUE}"
        )
        odf_cli.run_ceph_config(f"set global {key} {self.GLOBAL_IOPS_VALUE}")
        odf_cli.run_ceph_config(f"set osd.{target_osd} {key} {self.CUSTOM_IOPS_VALUE}")

        value = float(odf_cli.get_ceph_config_value(f"osd.{target_osd}", key))
        assert value == float(self.CUSTOM_IOPS_VALUE), (
            f"Per-OSD override did not win on osd.{target_osd}: expected "
            f"{self.CUSTOM_IOPS_VALUE}, found {value}"
        )
        for osd_id in other_osds:
            value = float(odf_cli.get_ceph_config_value(f"osd.{osd_id}", key))
            assert value == float(self.GLOBAL_IOPS_VALUE), (
                f"osd.{osd_id} {key} should follow the global override: "
                f"expected {self.GLOBAL_IOPS_VALUE}, found {value}"
            )
        logger.info("Per-OSD override wins; other OSDs follow the global value")

        entries = odf_cli.get_ceph_config_dump_entries(key)
        assert {who for who, _ in entries} == {"global", f"osd.{target_osd}"}, (
            f"Expected global and osd.{target_osd} entries in "
            f"`ceph config dump`, found {entries}"
        )

        ceph_health_check()

    @tier1
    @polarion_id("OCS-8259")
    def test_global_osd_mclock_iops_override(self, mclock_config_cleanup):
        """
        Set a cluster-wide osd_mclock_max_capacity_iops override via the ODF
        CLI, verify every OSD reports it, then confirm removal reverts it.

        Args:
            mclock_config_cleanup: Fixture giving (ODFCliRunner, osd_ids) with
                all mClock IOPS overrides cleared.

        """
        odf_cli, osd_ids = mclock_config_cleanup
        key = get_mclock_iops_key(odf_cli, osd_ids)
        logger.info(f"OSDs report device type -> config key '{key}'")

        baseline = float(odf_cli.get_ceph_config_value("osd", key))
        logger.info(f"Baseline {key} = {baseline}")
        assert baseline != float(self.CUSTOM_IOPS_VALUE), (
            f"Baseline {key} already equals the test value "
            f"{self.CUSTOM_IOPS_VALUE}; pick a different value"
        )

        logger.info(f"Setting global {key} = {self.CUSTOM_IOPS_VALUE}")
        odf_cli.run_ceph_config(f"set global {key} {self.CUSTOM_IOPS_VALUE}")
        for osd_id in osd_ids:
            value = float(odf_cli.get_ceph_config_value(f"osd.{osd_id}", key))
            assert value == float(self.CUSTOM_IOPS_VALUE), (
                f"osd.{osd_id} {key} not overridden: expected "
                f"{self.CUSTOM_IOPS_VALUE}, found {value}"
            )
        logger.info("All OSDs report the overridden value")

        entries = odf_cli.get_ceph_config_dump_entries(key)
        assert any(
            who == "global" for who, _ in entries
        ), f"No global {key} entry in `ceph config dump`: {entries}"

        logger.info(f"Removing global {key} override and verifying revert")
        odf_cli.run_ceph_config(f"rm global {key}")
        for osd_id in osd_ids:
            reverted = float(odf_cli.get_ceph_config_value(f"osd.{osd_id}", key))
            assert reverted == baseline, (
                f"osd.{osd_id} {key} did not revert: expected {baseline}, "
                f"found {reverted}"
            )

        entries = odf_cli.get_ceph_config_dump_entries(key)
        assert not entries, f"{key} still present in `ceph config dump`: {entries}"

        ceph_health_check()
