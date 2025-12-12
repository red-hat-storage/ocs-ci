"""
Module that contains all operations related to nfs feature in a cluster

"""

import logging
import yaml
import time
import pytest
from ocs_ci.ocs import constants, resources, ocp
from ocs_ci.helpers import helpers
from ocs_ci.ocs.resources import pod
from ocs_ci.utility.retry import retry
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.framework import config
from ocs_ci.utility import version as version_module
from ocs_ci.utility.utils import convert_device_size
from ocs_ci.deployment.hub_spoke import get_autodistributed_storage_classes

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
        timeout=120,
    )

    provisioner_list = provisioner_selectors(nfs_plugins=True)

    # Check csi-nfsplugin and csi-nfsplugin-provisioner pods are up and running

    for provisioner in provisioner_list:
        assert pod_obj.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            selector=provisioner,
            dont_allow_other_resources=True,
            timeout=120,
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
        sc (obj): nfs storage class object
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

    Returns:
        str: host details

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


def create_nfs_sc(
    sc_name_to_create,
    sc_name_to_copy="ocs-storagecluster-cephfs",
    retain_reclaim_policy=False,
    server=None,
):
    """
    This method is to create a new storageclass.

    Args:
        sc_name_to_create (str): name of the storageclass to create
        sc_name_to_copy (str): name of the sc to copy
        retain_reclaim_policy (bool): if true set retain reclaim policy
        server (str): if server details passed set that value

    Returns:
        new_sc(obj): returns storageclass obj created

    """
    # Create storage class
    new_sc = resources.ocs.OCS(
        kind=constants.STORAGECLASS, metadata={"name": sc_name_to_copy}
    )
    new_sc.reload()
    if retain_reclaim_policy:
        new_sc.data["reclaimPolicy"] = constants.RECLAIM_POLICY_RETAIN
    if server:
        new_sc.data["parameters"]["server"] = server
    new_sc.data["metadata"]["name"] = sc_name_to_create
    new_sc.data["metadata"]["ownerReferences"] = None
    new_sc._name = new_sc.data["metadata"]["name"]
    new_sc.create()
    return new_sc


def distribute_nfs_storage_class_to_all_consumers(nfs_sc):
    """
    This method is to distribute nfs storage class to Storage Consumers
    Function validates Storage Class is available on Client cluster and return combined result for all consumers.

    Args:
        nfs_sc (str): nfs storage class name

    Returns:
        bool: True if the nfs Storage Classes is distributed successfully to all consumers, False otherwise.

    """

    # to avoid overloading this module with imports, we import only when this fixture is called
    from ocs_ci.ocs.resources.storageconsumer import (
        get_ready_storage_consumers,
        check_storage_classes_on_clients,
    )

    consumers = get_ready_storage_consumers()
    consumers = [
        consumer
        for consumer in consumers
        if consumer.name != constants.INTERNAL_STORAGE_CONSUMER_NAME
    ]
    ready_consumer_names = [consumer.name for consumer in consumers]

    if not ready_consumer_names:
        log.warning("No ready storage consumers found")
        return
    storage_class_names = get_autodistributed_storage_classes()
    storage_class_names.append(nfs_sc)
    log.info(f"storage classes: {storage_class_names}")
    for consumer in consumers:
        log.info(f"Distributing storage classes to consumer {consumer.name}")
        consumer.set_storage_classes(storage_class_names)

    return check_storage_classes_on_clients(ready_consumer_names)


def nfs_access_for_clients(nfs_sc):
    """
    This method is for client clusters to be able to access nfs

    Args:
        nfs_sc (str): storage class name

    Returns:
        str: nfs-ganesha pod name
        str: host details

    """
    provider_namespace = constants.OPENSHIFT_STORAGE_NAMESPACE
    # switch to provider
    config.switch_to_provider()
    provider_storage_cluster_obj = ocp.OCP(
        kind=constants.STORAGECLUSTER, namespace=provider_namespace
    )
    provider_config_map_obj = ocp.OCP(
        kind=constants.CONFIGMAP, namespace=provider_namespace
    )
    provider_pod_obj = ocp.OCP(kind=constants.POD, namespace=provider_namespace)

    # Enable nfs
    nfs_ganesha_pod = nfs_enable(
        provider_storage_cluster_obj,
        provider_config_map_obj,
        provider_pod_obj,
        provider_namespace,
    )

    # Create nfs-load balancer service
    hostname_add = create_nfs_load_balancer_service(provider_storage_cluster_obj)

    # Distribute the scs to consumers
    distribute_nfs_storage_class_to_all_consumers(nfs_sc)

    # switch to consumer
    config.switch_to_consumer()

    return nfs_ganesha_pod, hostname_add


def disable_nfs_service_from_provider(nfs_sc_obj, nfs_ganesha_pod_name):
    """
    This method is for disabling nfs feature from provider cluster

    Args:
        nfs_ganesha_pod_name (str): rook-ceph-nfs * pod name
        nfs_sc_obj (obj): storage class object

    """
    provider_namespace = constants.OPENSHIFT_STORAGE_NAMESPACE
    # switch to provider
    config.switch_to_provider()
    provider_storage_cluster_obj = ocp.OCP(
        kind=constants.STORAGECLUSTER, namespace=provider_namespace
    )
    provider_config_map_obj = ocp.OCP(
        kind=constants.CONFIGMAP, namespace=provider_namespace
    )
    provider_pod_obj = ocp.OCP(kind=constants.POD, namespace=provider_namespace)

    # Disable nfs
    nfs_disable(
        provider_storage_cluster_obj,
        provider_config_map_obj,
        provider_pod_obj,
        nfs_sc_obj,
        nfs_ganesha_pod_name,
    )

    # Delete load balancer service
    delete_nfs_load_balancer_service(provider_storage_cluster_obj)

    # switch to consumer
    config.switch_to_consumer()


def check_cluster_resources_for_nfs(min_cpu=12, min_memory=32 * 10**9):
    """
    Check if cluster has sufficient resources for NFS deployment.

    Args:
        min_cpu (int): Minimum CPU cores per worker node (default: 12)
        min_memory (int): Minimum memory in bytes per worker node (default: 32GB)

    Returns:
        bool: True if cluster meets NFS resource requirements, False otherwise
    """
    try:
        from ocs_ci.ocs.node import get_nodes

        # Check worker nodes only (OCS/ODF runs on worker nodes)
        worker_nodes = get_nodes(node_type=constants.WORKER_MACHINE)
        if not worker_nodes:
            log.warning("No worker nodes found for NFS resource check")
            return True

        for node in worker_nodes:
            node_data = node.get()
            capacity = node_data.get("status", {}).get("capacity", {})

            real_cpu = int(capacity.get("cpu", 0))
            memory_str = capacity.get("memory", "0")

            try:
                real_memory = convert_device_size(memory_str, "BY")
            except Exception:
                real_memory = 0

            if real_cpu < min_cpu or real_memory < min_memory:
                log.info(
                    f"Insufficient resources for NFS. Node has {real_cpu} CPUs "
                    f"and {real_memory / 10**9:.1f}GB RAM (required: {min_cpu} CPUs, "
                    f"{min_memory / 10**9}GB RAM)"
                )
                return False

        log.info("Cluster has sufficient resources for NFS deployment")
        return True

    except Exception as e:
        log.warning(f"Unable to check NFS resource requirements: {e}")
        return True  # Don't block deployment on check failure


def update_nfs_endpoint():
    """
    This method is to pass nfs external endpoint under storagecluster.spec.nfs

    """
    storage_cluster_obj = ocp.OCP(
        kind=constants.STORAGECLUSTER, namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
    )
    nfs_spec_enable = '{"spec": {"nfs":{ExternalEndpoint}}}'
    assert storage_cluster_obj.patch(
        resource_name="ocs-storagecluster",
        params=nfs_spec_enable,
        format_type="merge",
    ), "storagecluster.ocs.openshift.io/ocs-storagecluster not patched"


def fetch_nfs_spec_details():
    """
    This method is to fetch nfs spec details

    Returns:
        dict: nfs spec details
    """
    storage_cluster_obj = ocp.OCP(
        kind=constants.STORAGECLUSTER, namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
    )
    return storage_cluster_obj.get().get("spec").get("nfs")
