import logging

from ocs_ci.framework import config
from ocs_ci.framework.testlib import deployment, destroy
from ocs_ci.ocs import defaults
from ocs_ci.ocs import constants, ocp
from ocs_ci.utility.utils import is_cluster_running, ceph_health_check

log = logging.getLogger(__name__)


# @destroy marker is added only for smooth transition in CI/Jenkins jobs,
# will be removed in one or two weeks
@destroy
@deployment
def test_deployment():
    deploy = config.RUN['cli_params'].get('deploy')
    teardown = config.RUN['cli_params'].get('teardown')
    if not teardown or deploy:
        log.info("Verifying OCP cluster is running")
        assert is_cluster_running(config.ENV_DATA['cluster_path'])
        if not config.ENV_DATA['skip_ocs_deployment']:
            ocs_install_verification()

    if teardown:
        log.info(
            "Cluster will be destroyed during teardown part of this test."
        )


def ocs_install_verification():
    """
    Perform steps necessary to verify a successful OCS installation
    """
    log.info("Verifying OCS installation")
    namespace = config.ENV_DATA['cluster_namespace']

    # Verify OCS Operator and Local Storage Operator in Succeeded phase
    log.info("Verifying OCS and Local Storage Operators")
    csv = ocp.OCP(kind='csv', namespace=namespace)
    csvs = csv.get()
    for item in csvs['items']:
        name = item['metadata']['name']
        log.info("Checking status of %s", name)
        assert item['status']['phase'] == 'Succeeded', (
            f"Operator {name} not 'Succeeded'"
        )

    # Verify OCS Cluster Service (ocs-storagecluster) is Ready
    log.info("Verifying OCS Cluster service")
    storage_cluster = ocp.OCP(kind='StorageCluster', namespace=namespace)
    storage_clusters = storage_cluster.get()
    for item in storage_clusters['items']:
        name = item['metadata']['name']
        log.info("Checking status of %s", name)
        assert item['status']['phase'] == 'Ready', (
            f"StorageCluster {name} not 'Ready'"
        )

    # Verify pods in running state and proper counts
    log.info("Verifying pod states and counts")
    pod = ocp.OCP(
        kind=constants.POD, namespace=namespace
    )
    timeout = 0
    # ocs-operator
    assert pod.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector=constants.OCS_OPERATOR_LABEL,
        timeout=timeout
    )
    # rook-ceph-operator
    assert pod.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector=constants.OPERATOR_LABEL,
        timeout=timeout
    )
    # noobaa
    assert pod.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector=constants.NOOBAA_APP_LABEL,
        resource_count=2,
        timeout=timeout
    )
    # local-storage-operator
    assert pod.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector=constants.LOCAL_STORAGE_OPERATOR_LABEL,
        timeout=timeout
    )
    # mons
    assert pod.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector=constants.MON_APP_LABEL,
        resource_count=3,
        timeout=timeout
    )
    # csi-cephfsplugin
    assert pod.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector=constants.CSI_CEPHFSPLUGIN_LABEL,
        resource_count=3,
        timeout=timeout
    )
    # csi-cephfsplugin-provisioner
    assert pod.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector=constants.CSI_CEPHFSPLUGIN_PROVISIONER_LABEL,
        resource_count=2,
        timeout=timeout
    )
    # csi-rbdplugin
    assert pod.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector=constants.CSI_RBDPLUGIN_LABEL,
        resource_count=3,
        timeout=timeout
    )
    # csi-rbdplugin-profisioner
    assert pod.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector=constants.CSI_RBDPLUGIN_PROVISIONER_LABEL,
        resource_count=2,
        timeout=timeout
    )
    # osds
    assert pod.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector=constants.OSD_APP_LABEL,
        resource_count=3,
        timeout=timeout
    )
    # mgr
    assert pod.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector=constants.MGR_APP_LABEL,
        timeout=timeout
    )
    # mds
    assert pod.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector=constants.MDS_APP_LABEL,
        resource_count=2,
        timeout=timeout
    )

    # Verify ceph health
    log.info("Verifying ceph health")
    assert ceph_health_check(namespace=namespace)

    # Verify StorageClasses (1 ceph-fs, 1 ceph-rbd)
    log.info("Verifying storage classes")
    storage_class = ocp.OCP(
        kind=constants.STORAGECLASS, namespace=namespace
    )
    storage_cluster_name = config.ENV_DATA['storage_cluster_name']
    required_storage_classes = {
        f'{storage_cluster_name}-cephfs',
        f'{storage_cluster_name}-ceph-rbd'
    }
    storage_classes = storage_class.get()
    storage_class_names = {
        item['metadata']['name'] for item in storage_classes['items']
    }
    assert required_storage_classes.issubset(storage_class_names)

    # Verify OSD's are distributed
    log.info("Verifying OSD's are distributed evenly across worker nodes")
    ocp_pod_obj = ocp.OCP(kind=constants.POD, namespace=defaults.ROOK_CLUSTER_NAMESPACE)
    osds = ocp_pod_obj.get(selector='app=rook-ceph-osd')['items']
    node_names = [osd['spec']['nodeName'] for osd in osds]
    for node in node_names:
        assert not node_names.count(node) > 1, (
            "OSD's are not distributed evenly across worker nodes"
        )
