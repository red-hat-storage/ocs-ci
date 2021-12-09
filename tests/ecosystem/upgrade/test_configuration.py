import logging

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    pre_upgrade,
    post_upgrade,
    brown_squad,
)
from ocs_ci.ocs.resources import pod

log = logging.getLogger(__name__)


def get_crush_map():
    """
    Get decompiled CRUSH map from ceph toolbox pod.

    Returns:
        str: Multiline string representing current Ceph CRUSH map
    """
    ct_pod = pod.get_ceph_tools_pod()
    file_comp = "/tmp/crush_comp"
    file_decomp = "/tmp/crush_decomp"
    ct_pod.exec_ceph_cmd(f"ceph osd getcrushmap -o {file_comp}")
    ct_pod.exec_ceph_cmd(f"crushtool -d {file_comp} -o {file_decomp}")
    return ct_pod.exec_sh_cmd_on_pod(f"cat {file_decomp}")


@pytest.fixture(scope="session")
def pre_upgrade_crush_map():
    """
    Loads CRUSH map before upgrade by `test_load_crush_map` test case.

    Returns:
        str: String consisting of CRUSH map before upgrade
    """
    crush_map = get_crush_map()
    log.info(f"Pre upgrade CRUSH map: {crush_map}")
    return crush_map


@pre_upgrade
@brown_squad
def test_load_crush_map(pre_upgrade_crush_map):
    """
    Load CRUSH map.
    """
    assert pre_upgrade_crush_map


@post_upgrade
@brown_squad
@pytest.mark.polarion_id("OCS-1936")
def test_crush_map_unchanged(pre_upgrade_crush_map):
    """
    Test that CRUSH map loaded before upgrade is the same as CRUSH map after
    upgrade.
    """
    pre_upgrade_crush_map == get_crush_map()
