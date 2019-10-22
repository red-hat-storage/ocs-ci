import logging

from ocs_ci.framework import config
from ocs_ci.framework.testlib import deployment, destroy
from ocs_ci.ocs import constants, ocp
from ocs_ci.utility.utils import is_cluster_running

log = logging.getLogger(__name__)


# @destroy marker is added only for smooth transition in CI/Jenkins jobs,
# will be removed in one or two weeks
@destroy
@deployment
def test_deployment():
    deploy = config.RUN['cli_params'].get('deploy')
    teardown = config.RUN['cli_params'].get('teardown')
    if not teardown or deploy:
        ocs_install_verification()

    if teardown:
        log.info(
            "Cluster will be destroyed during teardown part of this test."
        )


def ocs_install_verification():
    """
    Perform steps necessary to verify a successful OCS installation
    """
    namespace = config.ENV_DATA['cluster_namespace']
    storage_cluster_name = config.ENV_DATA['storage_cluster_name']

    # Verify cluster is running
    assert is_cluster_running(config.ENV_DATA['cluster_path'])

    # Verify OCS Operator and Local Storage Operator in Succeeded phase
    csv = ocp.OCP(kind='csv', namespace=namespace)
    csvs = csv.get()
    for item in csvs['items']:
        name = item['metadata']['name']
        log.info("Checking status of %s", name)
        assert item['status']['phase'] == 'Succeeded', (
            f"Operator {name} not 'Succeeded'"
        )

    # Verify OCS Cluster Service (ocs-storagecluster) is Ready
    storage_cluster = ocp.OCP(kind='StorageCluster', namespace=namespace)
    storage_clusters = storage_cluster.get()
    for item in storage_clusters['items']:
        name = item['metadata']['name']
        log.info("Checking status of %s", name)
        assert item['status']['phase'] == 'Ready', (
            f"StorageCluster {name} not 'Ready'"
        )

    # Verify pods in running state and proper counts
    pod = ocp.OCP(
        kind=constants.POD, namespace=namespace
    )
    timeout = 0
    # ocs-operator
    assert pod.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector='name=ocs-operator',
        timeout=timeout
    )
    # storagecluster-operator
    assert pod.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector=f'app={storage_cluster_name}-operator',
        timeout=timeout
    )
    # nooba
    assert pod.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector='app=noobaa',
        resource_count=2,
        timeout=timeout
    )
    # local-storage-operator
    assert pod.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector='name=local-storage-operator',
        timeout=timeout
    )
    # mons
    assert pod.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector=f'app={storage_cluster_name}-mon',
        resource_count=3,
        timeout=timeout
    )
    # csi-cephfsplugin
    assert pod.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector='app=csi-cephfsplugin',
        resource_count=3
    )
    # csi-cephfsplugin-provisioner
    assert pod.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector='app=csi-cephfsplugin-provisioner',
        resource_count=2,
        timeout=timeout
    )
    # csi-rbdplugin
    assert pod.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector='app=csi-rbdplugin',
        resource_count=3,
        timeout=timeout
    )
    # csi-rbdplugin-profisioner
    assert pod.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector='app=csi-rbdplugin-provisioner',
        resource_count=2,
        timeout=timeout
    )
    # osds
    assert pod.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector=f'app={storage_cluster_name}-osd',
        resource_count=3,
        timeout=timeout
    )
    # mgr
    assert pod.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector=f'app={storage_cluster_name}-mgr',
        timeout=timeout
    )
    # mds
    assert pod.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector=f'app={storage_cluster_name}-mds',
        resource_count=2,
        timeout=timeout
    )

    # Verify StorageClasses (1 ceph-fs, 1 ceph-rbd)
    storage_class = ocp.OCP(
        kind=constants.STORAGECLASS, namespace=namespace
    )
    required_storage_classes = {
        f'{storage_cluster_name}-cephfs',
        f'{storage_cluster_name}-ceph-rbd'
    }
    storage_classes = storage_class.get()
    storage_class_names = {
        item['metadata']['name'] for item in storage_classes['items']
    }
    assert required_storage_classes.issubset(storage_class_names)
