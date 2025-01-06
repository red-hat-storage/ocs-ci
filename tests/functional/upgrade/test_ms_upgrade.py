import logging

from ocs_ci.ocs.resources.pod import cal_md5sum
from ocs_ci.helpers.managed_services import verify_provider_topology
from ocs_ci.framework.pytest_customization.marks import yellow_squad
from ocs_ci.framework.testlib import (
    pre_upgrade,
    post_upgrade,
    ms_consumer_required,
    ms_provider_required,
)

logger = logging.getLogger(name=__file__)


@yellow_squad
@pre_upgrade
@ms_consumer_required
def test_prepare_block_md5_before_upgrade(block_md5):
    """
    Prepare md5 results for utilized RBD PVC.

    """
    pass


@yellow_squad
@pre_upgrade
@ms_consumer_required
def test_prepare_fs_md5_before_upgrade(fs_md5):
    """
    Prepare md5 results for utilized Ceph FS PVC.

    """
    pass


@yellow_squad
@post_upgrade
@ms_consumer_required
def test_verify_block_md5_after_upgrade(block_md5, block_pod):
    """
    Check that md5 checksum of file on RBD PVC did not changed during upgrade.

    """
    md5_after_upgrade = cal_md5sum(
        pod_obj=block_pod,
        file_name="fio-rand-write",
        block=False,
    )
    logger.info(f"RBD file md5 after upgrade: {md5_after_upgrade}")
    assert md5_after_upgrade == block_md5


@yellow_squad
@post_upgrade
@ms_consumer_required
def test_verify_fs_md5_after_upgrade(fs_md5, fs_pod):
    """
    Check that md5 checksum of file on Ceph FS PVC did not changed during upgrade.

    """
    md5_after_upgrade = cal_md5sum(
        pod_obj=fs_pod,
        file_name="fio-rand-write",
        block=False,
    )
    logger.info(f"Ceph FS file md5 after upgrade: {md5_after_upgrade}")
    assert md5_after_upgrade == fs_md5


@yellow_squad
@post_upgrade
@ms_provider_required
def test_verify_provider_topology_after_upgrade():
    """
    Verify topology in a Managed Services provider cluster after upgrade

    """
    verify_provider_topology()
