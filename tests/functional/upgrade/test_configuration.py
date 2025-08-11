import logging

import pytest
import time
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    pre_upgrade,
    post_upgrade,
    runs_on_provider,
    brown_squad,
    skipif_mcg_only,
    tier1,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.resources.daemonset import DaemonSet
from ocs_ci.utility.utils import exec_cmd
from ocs_ci.helpers.helpers import get_logs_rook_ceph_operator
from ocs_ci.ocs.ocs_upgrade import set_update_strategy

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
@runs_on_provider
@skipif_mcg_only
def test_load_crush_map(pre_upgrade_crush_map):
    """
    Load CRUSH map.
    """
    assert pre_upgrade_crush_map


@post_upgrade
@brown_squad
@skipif_mcg_only
@pytest.mark.polarion_id("OCS-1936")
@runs_on_provider
def test_crush_map_unchanged(pre_upgrade_crush_map):
    """
    Test that CRUSH map loaded before upgrade is the same as CRUSH map after
    upgrade.
    """
    pre_upgrade_crush_map == get_crush_map()


@post_upgrade
@pytest.mark.polarion_id("OCS-6275")
@brown_squad
@runs_on_provider
def test_max_unavaialable_rbd(upgrade_stats):
    """
    Test that the number of unavailable RBD daemonset plugin pods during ODF
    upgrade corresponds to the value set in rook-ceph-operator-config configmap.
    """
    configmap = OCP(
        kind=constants.CONFIGMAP,
        namespace=config.ENV_DATA["cluster_namespace"],
        resource_name=constants.ROOK_OPERATOR_CONFIGMAP,
    ).get()
    if config.UPGRADE.get("csi_rbd_plugin_update_strategy_max_unavailable") is not None:
        config_value = configmap.get("data").get(
            "CSI_RBD_PLUGIN_UPDATE_STRATEGY_MAX_UNAVAILABLE"
        )
        assert config_value == upgrade_stats["odf_upgrade"]["rbd_max_unavailable"]


@post_upgrade
@pytest.mark.polarion_id("OCS-6278")
@brown_squad
@runs_on_provider
def test_max_unavaialable_cephfs(upgrade_stats):
    """
    Test that the number of unavailable CephFS daemonset plugin pods during ODF
    upgrade corresponds to the value set in rook-ceph-operator-config configmap.
    """
    configmap = OCP(
        kind=constants.CONFIGMAP,
        namespace=config.ENV_DATA["cluster_namespace"],
        resource_name=constants.ROOK_OPERATOR_CONFIGMAP,
    ).get()
    if (
        config.UPGRADE.get("csi_cephfs_plugin_update_strategy_max_unavailable")
        is not None
    ):
        config_value = configmap.get("data").get(
            "CSI_CEPHFS_PLUGIN_UPDATE_STRATEGY_MAX_UNAVAILABLE"
        )
        assert config_value == upgrade_stats["odf_upgrade"]["cephfs_max_unavailable"]


@pytest.mark.parametrize(
    argnames=["daemonset", "value_to_set", "expected_value"],
    argvalues=[
        pytest.param(
            "csi-rbdplugin", 2, 2, marks=[tier1, pytest.mark.polarion_id("OCS-6276")]
        ),
        pytest.param(
            "csi-cephfsplugin", 2, 2, marks=[tier1, pytest.mark.polarion_id("OCS-6277")]
        ),
    ],
)
@brown_squad
@runs_on_provider
def test_update_strategy_config_change(
    daemonset, value_to_set, expected_value, rook_operator_configmap_cleanup
):
    """
    Test that tested value added to configmap rook-ceph-operator-config is
    reflected in respective daemonset.
    """
    if daemonset == "csi-rbdplugin":
        set_update_strategy(rbd_max_unavailable=value_to_set)
    elif daemonset == "csi-cephfsplugin":
        set_update_strategy(cephfs_max_unavailable=value_to_set)

    ds_obj = DaemonSet(
        resource_name=daemonset, namespace=config.ENV_DATA["cluster_namespace"]
    )
    time.sleep(10)  # wait a few second
    results = ds_obj.get_update_strategy()
    assert str(expected_value) == str(results["rollingUpdate"]["maxUnavailable"])


@pytest.mark.parametrize(
    argnames=["daemonset", "value_to_set"],
    argvalues=[
        pytest.param(
            "csi-rbdplugin",
            0,
            marks=[pytest.mark.tier4a, pytest.mark.polarion_id("OCS-6517")],
        ),
        pytest.param(
            "csi-rbdplugin",
            "0%",
            marks=[pytest.mark.tier4a, pytest.mark.polarion_id("OCS-6518")],
        ),
        pytest.param(
            "csi-cephfsplugin",
            0,
            marks=[pytest.mark.tier4a, pytest.mark.polarion_id("OCS-6517")],
        ),
        pytest.param(
            "csi-cephfsplugin",
            "0%",
            marks=[pytest.mark.tier4a, pytest.mark.polarion_id("OCS-6518")],
        ),
    ],
)
@brown_squad
@runs_on_provider
def test_max_unavailable_zero_update_strategy_config(
    daemonset, value_to_set, rook_operator_configmap_cleanup
):
    """
    Test that tested value added to configmap rook-ceph-operator-config is
    updated with zero or zero percent value and generate the respective error
    in rook-ceph-operator log
    """
    if daemonset == "csi-rbdplugin":
        parameter_name = "CSI_RBD_PLUGIN_UPDATE_STRATEGY_MAX_UNAVAILABLE"
    elif daemonset == "csi-cephfsplugin":
        parameter_name = "CSI_CEPHFS_PLUGIN_UPDATE_STRATEGY_MAX_UNAVAILABLE"

    config_map_patch = f'\'{{"data": {{"{parameter_name}": "{value_to_set}"}}}}\''
    exec_cmd(
        f"oc patch configmap -n {config.ENV_DATA['cluster_namespace']} "
        f"{constants.ROOK_OPERATOR_CONFIGMAP} -p {config_map_patch}"
    )

    # Verify the error in rook-ceph-operator-logs
    rook_ceph_operator_log = get_logs_rook_ceph_operator()

    expected_error = (
        "failed to reconcile failed to configure ceph csi: "
        "failed to start ceph csi drivers: failed to start"
    )
    if daemonset == "csi-rbdplugin":
        expected_error += (
            ' rbdplugin daemonset "csi-rbdplugin": failed to '
            'start csi-rbdplugin daemonset: DaemonSet.apps "csi-rbdplugin"'
        )
    elif daemonset == "csi-cephfsplugin":
        expected_error += (
            ' cephfs plugin daemonset "csi-cephfsplugin": to '
            'start csi-cephfsplugin daemonset: DaemonSet.apps "csi-cephfsplugin"'
        )
    expected_error += (
        "is invalid: spec.updateStrategy.rollingUpdate.maxUnavailable: "
        "Required value: cannot be 0 when maxSurge is 0"
    )
    msg = f'Expected Error for "{parameter_name}": "{value_to_set}" value in rook-ceph-operator log'

    assert expected_error not in rook_ceph_operator_log, msg
