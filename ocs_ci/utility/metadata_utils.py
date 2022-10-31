"""
Module that contains all operations related to add metadata feature in a cluster

"""

import logging
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources import pod


log = logging.getLogger(__name__)


def enable_metadata(
    config_map_obj,
    pod_obj,
):
    """
    Enable CSI_ENABLE_METADATA
    Steps:
    1:- Enable CSI_ENABLE_METADATA flag via patch request
    2:- Check csi-cephfsplugin provisioner and csi-rbdplugin-provisioner
    pods are up and running
    3:- Check 'setmatadata' is set for csi-cephfsplugin-provisioner
    and csi-rbdplugin-provisioner pods

    """
    enable_metadata = '{"data":{"CSI_ENABLE_METADATA": "true"}}'

    # Enable metadata feature for rook-ceph-operator-config using patch command
    assert config_map_obj.patch(
        resource_name="rook-ceph-operator-config",
        params=enable_metadata,
    ), "configmap/rook-ceph-operator-config not patched"

    # Check csi-cephfsplugin provisioner and csi-rbdplugin-provisioner pods are up and running
    assert pod_obj.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector="app=csi-cephfsplugin",
        dont_allow_other_resources=True,
        timeout=60,
    )

    assert pod_obj.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector="app=csi-rbdplugin",
        dont_allow_other_resources=True,
        timeout=60,
    )

    # Check 'setmatadata' is set for csi-cephfsplugin-provisioner and csi-rbdplugin-provisioner pods
    plugin_provisioner_pod_objs = pod.get_all_pods(
        namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
        selector=["csi-cephfsplugin-provisioner", "csi-rbdplugin-provisioner"],
    )
    log.info(f"plugin provisioner pods-----{plugin_provisioner_pod_objs}")

    for plugin_provisioner_pod in plugin_provisioner_pod_objs:
        args = pod_obj.exec_oc_cmd(
            "get pod "
            + plugin_provisioner_pod.name
            + " --output jsonpath='{.spec.containers[4].args}'"
        )
        assert (
            "--setmetadata=true" in args
        ), "'setmatadata' not set for csi-cephfsplugin-provisioner and csi-rbdplugin-provisioner pods"

    args = pod_obj.exec_oc_cmd(
        "get pod "
        + plugin_provisioner_pod_objs[0].name
        + " --output jsonpath='{.spec.containers[4].args}'"
    )
    for arg in args:
        if "--clustername" in arg:
            return arg[14:]


def disable_metadata(
    config_map_obj,
    pod_obj,
):
    """
    Disable CSI_ENABLE_METADATA
    Steps:
    1:- Disable CSI_ENABLE_METADATA flag via patch request
    2:- Check csi-cephfsplugin provisioner and csi-rbdplugin-provisioner
    pods are up and running
    3:- Check 'setmatadata' is not set for csi-cephfsplugin-provisioner
    and csi-rbdplugin-provisioner pods

    """
    disable_metadata = '{"data":{"CSI_ENABLE_METADATA": "false"}}'

    # Disable metadata feature for rook-ceph-operator-config using patch command
    assert config_map_obj.patch(
        resource_name="rook-ceph-operator-config",
        params=disable_metadata,
    ), "configmap/rook-ceph-operator-config not patched"

    # Check csi-cephfsplugin provisioner and csi-rbdplugin-provisioner pods are up and running
    assert pod_obj.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector="app=csi-cephfsplugin",
        dont_allow_other_resources=True,
        timeout=60,
    )

    assert pod_obj.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector="app=csi-rbdplugin",
        dont_allow_other_resources=True,
        timeout=60,
    )

    # Check 'setmatadata' is not set for csi-cephfsplugin-provisioner and csi-rbdplugin-provisioner pods
    plugin_provisioner_pod_objs = pod.get_all_pods(
        namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
        selector=["csi-cephfsplugin-provisioner", "csi-rbdplugin-provisioner"],
    )
    log.info(f"plugin provisioner pods-----{plugin_provisioner_pod_objs}")

    for plugin_provisioner_pod in plugin_provisioner_pod_objs:
        args = pod_obj.exec_oc_cmd(
            "get pod "
            + plugin_provisioner_pod.name
            + " --output jsonpath='{.spec.containers[4].args}'"
        )
        assert (
            "--setmetadata=true" not in args
        ), "'setmatadata' is set for csi-cephfsplugin-provisioner and csi-rbdplugin-provisioner pods"


def validate_metadata(
    metadata,
    clustername,
    pv_name=None,
    pvc_name=None,
    namespace=None,
    volumesnapshot_name=None,
    volumesnapshot_content=None,
):
    """
    Enable CSI_ENABLE_METADATA
    Steps:
    1:- Enable CSI_ENABLE_METADATA flag via patch request
    2:- Check csi-cephfsplugin provisioner and csi-rbdplugin-provisioner
    pods are up and running
    3:- Check 'setmatadata' is set for csi-cephfsplugin-provisioner
    and csi-rbdplugin-provisioner pods

    """
    assert (
        clustername == metadata["csi.ceph.com/cluster/name"]
    ), "Error: cluster name is not as expected"
    if pv_name:
        assert (
            pv_name == metadata["csi.storage.k8s.io/pv/name"]
        ), "Error: pv name is not as expected"
    if pvc_name:
        assert (
            pvc_name == metadata["csi.storage.k8s.io/pvc/name"]
        ), "Error: pvc name is not as expected"
        assert (
            namespace == metadata["csi.storage.k8s.io/pvc/namespace"]
        ), "Error: namespace is not as expected"
    if volumesnapshot_name:
        assert (
            volumesnapshot_name == metadata["csi.storage.k8s.io/volumesnapshot/name"]
        ), "Error: volumesnapshot name is not as expected"
    if volumesnapshot_content:
        assert (
            volumesnapshot_content
            == metadata["csi.storage.k8s.io/volumesnapshotcontent/name"]
        ), "Error: snapshot content name is not as expected"
        assert (
            namespace == metadata["csi.storage.k8s.io/volumesnapshot/namespace"]
        ), "Error: namespace is not as expected"
