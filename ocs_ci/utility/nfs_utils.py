"""
Module that contains all operations related to nfs feature in a cluster

"""

import logging
import yaml
import time
import pytest
from ocs_ci.ocs import constants, resources
from ocs_ci.helpers import helpers
from ocs_ci.ocs.resources import pod
from ocs_ci.utility.retry import retry
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.framework import config
from ocs_ci.utility import version as version_module


log = logging.getLogger(__name__)


def nfs_enable(
    storage_cluster_obj,
    config_map_obj,
    pod_obj,
    namespace,
):
    """
    Enable nfs feature and ROOK_CSI_ENABLE_NFS
    Steps:
    1:- Enable nfs feature for storage-cluster
    2:- Enable ROOK_CSI_ENABLE_NFS via patch request
    3:- Check nfs-ganesha server is up and running
    4:- Check csi-nfsplugin pods are up and running

    Args:
        storage_cluster_obj (obj): storage cluster object
        config_map_obj (obj): config map object
        pod_obj (obj): pod object
        namespace (str): namespace name

    Returns:
        str: nfs-ganesha pod name

    """
    nfs_spec_enable = '{"spec": {"nfs":{"enable": true}}}'
    rook_csi_config_enable = '{"data":{"ROOK_CSI_ENABLE_NFS": "true"}}'

    # Enable nfs feature for storage-cluster using patch command
    assert storage_cluster_obj.patch(
        resource_name="ocs-storagecluster",
        params=nfs_spec_enable,
        format_type="merge",
    ), "storagecluster.ocs.openshift.io/ocs-storagecluster not patched"

    # Enable ROOK_CSI_ENABLE_NFS via patch request
    assert config_map_obj.patch(
        resource_name="rook-ceph-operator-config",
        params=rook_csi_config_enable,
        format_type="merge",
    ), "configmap/rook-ceph-operator-config not patched"

    # Check nfs-ganesha server is up and running
    assert pod_obj.wait_for_resource(
        resource_count=1,
        condition=constants.STATUS_RUNNING,
        selector="app=rook-ceph-nfs",
        dont_allow_other_resources=True,
        timeout=60,
    )

    provisioner_list = provisioner_selectors(nfs_plugins=True)

    # Check csi-nfsplugin and csi-nfsplugin-provisioner pods are up and running

    for provisioner in provisioner_list:
        assert pod_obj.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            selector=provisioner,
            dont_allow_other_resources=True,
            timeout=60,
        )

    # Fetch the nfs-ganesha pod name
    pod_objs = pod.get_all_pods(namespace=namespace, selector=["rook-ceph-nfs"])
    log.info(f"pod objects---{pod_objs[0]}")
    nfs_ganesha_pod_name = pod_objs[0].name
    log.info(f"pod objects---{nfs_ganesha_pod_name}")

    return nfs_ganesha_pod_name


def nfs_disable(
    storage_cluster_obj,
    config_map_obj,
    pod_obj,
    sc,
    nfs_ganesha_pod_name,
):
    """
    Disable nfs feature and ROOK_CSI_ENABLE_NFS
    Steps:
    1:- Disable nfs feature for storage-cluster
    2:- Disable ROOK_CSI_ENABLE_NFS via patch request
    3:- Delete CephNFS, ocs nfs Service and the nfs StorageClass
    4:- Wait untill nfs-ganesha pod deleted

    Args:
        storage_cluster_obj (obj): storage cluster object
        config_map_obj (obj): config map object
        pod_obj (obj): pod object
        sc (str): nfs storage class
        nfs_ganesha_pod_name (str): rook-ceph-nfs * pod name

    """

    nfs_spec_disable = '{"spec": {"nfs":{"enable": false}}}'
    rook_csi_config_disable = '{"data":{"ROOK_CSI_ENABLE_NFS": "false"}}'

    assert storage_cluster_obj.patch(
        resource_name="ocs-storagecluster",
        params=nfs_spec_disable,
        format_type="merge",
    ), "storagecluster.ocs.openshift.io/ocs-storagecluster not patched"

    # Disable ROOK_CSI_ENABLE_NFS via patch request
    assert config_map_obj.patch(
        resource_name="rook-ceph-operator-config",
        params=rook_csi_config_disable,
        format_type="merge",
    ), "configmap/rook-ceph-operator-config not patched"

    # Delete the nfs StorageClass
    sc.delete()

    # Delete CephNFS
    cmd_delete_cephnfs = "delete CephNFS ocs-storagecluster-cephnfs"
    storage_cluster_obj.exec_oc_cmd(cmd_delete_cephnfs)

    # Wait untill nfs-ganesha pod deleted
    pod_obj.wait_for_delete(resource_name=nfs_ganesha_pod_name)


def create_nfs_load_balancer_service(
    storage_cluster_obj,
):
    """
    Create the nfs loadbalancer service

    Args:
        storage_cluster_obj (obj): storage cluster object

    """
    # Create loadbalancer service for nfs
    log.info("----create loadbalancer service----")
    service = """
            apiVersion: v1
            kind: Service
            metadata:
              name: rook-ceph-nfs-my-nfs-load-balancer
              namespace: openshift-storage
            spec:
              ports:
              - name: nfs
                port: 2049
              type: LoadBalancer
              externalTrafficPolicy: Local
              selector:
                app: rook-ceph-nfs
                ceph_nfs: ocs-storagecluster-cephnfs
            """

    nfs_service_data = yaml.safe_load(service)
    helpers.create_resource(**nfs_service_data)
    time.sleep(30)
    ingress_add = storage_cluster_obj.exec_oc_cmd(
        "get service rook-ceph-nfs-my-nfs-load-balancer"
        + " --output jsonpath='{.status.loadBalancer.ingress}'"
    )

    host_details = ingress_add[0]
    if "hostname" in host_details:
        hostname_add = host_details["hostname"]
        log.info(f"ingress hostname, {hostname_add}")
        return hostname_add
    elif "ip" in host_details:
        host_ip = host_details["ip"]
        log.info(f"ingress host ip, {host_ip}")
        return host_ip
    else:
        log.error("host details unavailable")


def delete_nfs_load_balancer_service(
    storage_cluster_obj,
):
    """
    Delete the nfs loadbalancer service

    Args:
        storage_cluster_obj (obj): storage cluster object

    """
    # Delete ocs nfs Service
    cmd_delete_nfs_service = "delete service rook-ceph-nfs-my-nfs-load-balancer"
    storage_cluster_obj.exec_oc_cmd(cmd_delete_nfs_service)


def skip_test_if_nfs_client_unavailable(nfs_client_ip):
    """
    Skip the tests if a valid nfs client ip is not
    available for outside mounts

    Args:
        nfs_client_ip(str): nfs client ip address

    """
    if not nfs_client_ip:
        pytest.skip(
            "Skipped the test as a valid nfs client ip is required, "
            " for nfs outcluster export validation. "
        )


def unmount(con, test_folder):
    """
    Unmount existing mount points

    Args:
        con (obj): connection object
        test_folder (str) : Mount path

    """
    retry(
        (CommandFailed),
        tries=600,
        delay=10,
        backoff=1,
    )(con.exec_cmd(cmd="umount -f " + test_folder))

    # Check mount point unmounted successfully
    retcode, _, _ = con.exec_cmd("findmnt -M " + test_folder)
    assert retcode == 1


def provisioner_selectors(nfs_plugins=False, cephfs_plugin=False):
    """
    This method returns the provisioner pod selectors

    Args:
        nfs_plugins (bool): if True returns nfs_plugin provisooner list
        cephfs_plugin (bool): if True returns cephfs_plugin provisooner list

    Returns:
        nfs_provisioner_list(list): list of nfs provisioner selectors
        cephfs_provisioner_list(list): list of cephfs provisioner selectors

    """
    hci_platform_conf = (
        config.ENV_DATA["platform"].lower() in constants.HCI_PROVIDER_CLIENT_PLATFORMS
    )
    # csi provisioner pods were renamed starting from 4.18 for Provider mode and 4.19 for every mode (Converged mode)
    if (
        version_module.get_semantic_ocs_version_from_config()
        >= version_module.VERSION_4_18
        and hci_platform_conf
        or version_module.get_semantic_ocs_version_from_config()
        >= version_module.VERSION_4_19
    ):
        nfs_provisioner_list = [
            constants.NFS_CSI_CTRLPLUGIN_LABEL_419,
            constants.NFS_CSI_NODEPLUGIN_LABEL_419,
        ]
        cephfs_provisioner_list = [
            constants.CSI_CEPHFSPLUGIN_PROVISIONER_LABEL_419,
            constants.CSI_RBDPLUGIN_PROVISIONER_LABEL_419,
        ]
    else:
        nfs_provisioner_list = [
            constants.NFS_CSI_PLUGIN_PROVISIONER_LABEL,
            constants.NFS_CSI_PLUGIN_LABEL,
        ]
        cephfs_provisioner_list = [
            constants.CSI_CEPHFSPLUGIN_PROVISIONER_LABEL,
            constants.CSI_RBDPLUGIN_PROVISIONER_LABEL,
        ]
    if nfs_plugins:
        return nfs_provisioner_list
    elif cephfs_plugin:
        return cephfs_provisioner_list


def create_nfs_sc_retain(sc_name):
    """
    This method is to create nfs retain storageclass.

    Args:
        sc_name (str): name of the storageclass to create

    Returns:
        retain_nfs_sc(obj): returns storageclass obj created

    """
    # Create storage class
    retain_nfs_sc = resources.ocs.OCS(
        kind=constants.STORAGECLASS, metadata={"name": "ocs-storagecluster-cephfs"}
    )
    retain_nfs_sc.reload()
    retain_nfs_sc.data["reclaimPolicy"] = constants.RECLAIM_POLICY_RETAIN
    retain_nfs_sc.data["metadata"]["name"] = sc_name
    retain_nfs_sc.data["metadata"]["ownerReferences"] = None
    retain_nfs_sc._name = retain_nfs_sc.data["metadata"]["name"]
    retain_nfs_sc.create()
    return retain_nfs_sc


def nfs_access_for_clients(
    storage_cluster_obj,
    config_map_obj,
    pod_obj,
    namespace,
    distribute_storage_classes_to_all_consumers_factory,
):
    """
    This method is for client clusters to be able to access nfs
    """
    # switch to provider
    config.switch_to_provider()

    # Enable nfs
    nfs_enable(storage_cluster_obj, config_map_obj, pod_obj, namespace)

    # Create nfs-load balancer service
    if config.ENV_DATA.get("platform", "").lower() == constants.HCI_BAREMETAL:
        # Create loadbalancer service for nfs
        hostname_add = create_nfs_load_balancer_service(storage_cluster_obj)
        print("######Amtita#######")
        print(f"hostname: {hostname_add}")

    # Distribute the scs to consumers
    distribute_storage_classes_to_all_consumers_factory()

    # switch to consumer
    config.switch_to_consumer()
