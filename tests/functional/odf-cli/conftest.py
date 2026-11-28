import logging
from functools import partial

import pytest

from ocs_ci.ocs.resources.pod import get_osd_pods, get_osd_pod_id

logger = logging.getLogger(__name__)

MCLOCK_IOPS_KEYS = (
    "osd_mclock_max_capacity_iops_ssd",
    "osd_mclock_max_capacity_iops_hdd",
)


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
        logger.test_step("Clear mClock IOPS entries")
        for iops_key in MCLOCK_IOPS_KEYS:
            for who, _ in odf_cli.get_ceph_config_dump_entries(iops_key):
                odf_cli.run_ceph_config(f"rm {who} {iops_key}")

    def restore_entry(who, iops_key, value):
        odf_cli.run_ceph_config(f"set {who} {iops_key} {value}")

    for (who, iops_key), value in reversed(original_entries.items()):
        request.addfinalizer(partial(restore_entry, who, iops_key, value))
    # Finalizers run in reverse order, so clear entries before restoring them.
    request.addfinalizer(clear_entries)

    clear_entries()
    return odf_cli, osd_ids, key
