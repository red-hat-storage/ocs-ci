"""
General PVC object
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import UnavailableResourceException
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.resources import pod
from ocs_ci.framework import config
from ocs_ci.utility.utils import run_cmd
from ocs_ci.utility.utils import TimeoutSampler, convert_device_size
from ocs_ci.utility import templating, version
from ocs_ci.helpers import helpers

log = logging.getLogger(__name__)


class PVC(OCS):
    """
    A basic PersistentVolumeClaim kind resource
    """

    def __init__(self, **kwargs):
        """
        Initializer function
        kwargs:
            See parent class for kwargs information
        """
        super(PVC, self).__init__(**kwargs)

    @property
    def size(self):
        """
        Returns the PVC size pvc_name in namespace

        Returns:
            int: PVC size
        """
        unformatted_size = (
            self.data.get("spec").get("resources").get("requests").get("storage")
        )
        return convert_device_size(unformatted_size, "GB")

    @property
    def status(self):
        """
        Returns the PVC status

        Returns:
            str: PVC status
        """
        return self.data.get("status").get("phase")

    @property
    def backed_pv(self):
        """
        Returns the backed PV name of pvc_name in namespace

        Returns:
            str: PV name
        """
        return self.data.get("spec").get("volumeName")

    @property
    def backed_pv_obj(self):
        """
        Returns the backed PV object of pvc_name in namespace

        Returns:
            OCS: An OCS instance for PV
        """
        self.reload()
        data = dict()
        data["api_version"] = self.api_version
        data["kind"] = "PersistentVolume"
        data["metadata"] = {"name": self.backed_pv, "namespace": self.namespace}
        pv_obj = OCS(**data)
        pv_obj.ocp.cluster_kubeconfig = self.ocp.cluster_kubeconfig
        pv_obj.reload()
        return pv_obj

    @property
    def image_uuid(self):
        """
        Fetch image uuid associated with PVC

        Returns:
            str: Image uuid associated with PVC
        """
        spec_volhandle = "'{.spec.csi.volumeHandle}'"
        cmd = f"oc get pv/{self.backed_pv} -o jsonpath={spec_volhandle}"
        out = run_cmd(cmd=cmd)
        image_uuid = "-".join(out.split("-")[-5:])
        return image_uuid

    @property
    def get_pvc_access_mode(self):
        """
        Function to get pvc access_mode

        Returns:
            (str): The accessModes Value of pvc_obj
        """
        return self.data.get("spec").get("accessModes")[0]

    @property
    def backed_sc(self):
        """
        Returns the storage class of pvc object in namespace

        Returns:
            str: Storage class name
        """
        return self.data.get("spec").get("storageClassName")

    @property
    def get_pvc_vol_mode(self):
        """
        Function to get pvc volume_mode

        Returns:
            str: The volumeMode Value of pvc_obj

        """
        return self.data.get("spec").get("volumeMode")

    @property
    def reclaim_policy(self):
        """
        Get the reclaim policy of PV associated with the PVC

        Returns:
            str: Reclaim policy. eg: Reclaim, Delete
        """
        return self.backed_pv_obj.get().get("spec").get("persistentVolumeReclaimPolicy")

    @property
    def provisioner(self):
        return self.get()["metadata"]["annotations"][
            "volume.beta.kubernetes.io/storage-provisioner"
        ]

    @property
    def get_rbd_image_name(self):
        """
        Fetch image name associated with the RBD PVC

        Returns:
            str: Image name associated with the RBD PVC
        """
        return self.backed_pv_obj.get()["spec"]["csi"]["volumeAttributes"]["imageName"]

    @property
    def get_pv_volume_handle_name(self):
        """
        Fetch volume handle name from PV

        Returns:
            str: volume handle name from pv
        """
        return self.backed_pv_obj.get()["spec"]["csi"]["volumeHandle"]

    def resize_pvc(self, new_size, verify=False):
        """
        Modify the capacity of PVC

        Args:
            new_size (int): New size of PVC in Gi
            verify (bool): True to verify the change is reflected on PVC,
                False otherwise

        Returns:
            bool: True if operation succeeded, False otherwise
        """
        patch_param = f'{{"spec": {{"resources": {{"requests": {{"storage": "{new_size}Gi"}}}}}}}}'

        # Modify size of PVC
        assert self.ocp.patch(
            resource_name=self.name,
            params=patch_param,
        ), f"Patch command to modify size of PVC {self.name} has failed."

        if verify:
            for pvc_data in TimeoutSampler(240, 2, self.get):
                capacity = pvc_data.get("status").get("capacity").get("storage")
                if capacity == f"{new_size}Gi":
                    break
                log.info(
                    f"Capacity of PVC {self.name} is not {new_size}Gi as "
                    f"expected, but {capacity}. Retrying."
                )
            log.info(
                f"Verified that the capacity of PVC {self.name} is changed to "
                f"{new_size}Gi."
            )
        return True

    def get_attached_pods(self):
        """
        Get the pods attached to the PVC represented by this object instance

        Returns:
            list: A list of pod objects attached to the PVC

        """
        # Importing from pod inside, because of unsolvable import loop
        from ocs_ci.ocs.resources.pod import get_all_pods, get_pvc_name

        attached_pods = []
        all_pods = get_all_pods()
        for pod_obj in all_pods:
            try:
                pvc = get_pvc_name(pod_obj)
            except UnavailableResourceException:
                continue
            if pvc == self.name:
                attached_pods.append(pod_obj)
        return attached_pods

    def create_snapshot(self, snapshot_name=None, wait=False):
        """
        Take snapshot of the PVC

        Args:
            snapshot_name (str): Name to be provided for snapshot
            wait (bool): True to wait for snapshot to be ready, False otherwise

        Returns:
            OCS: Kind Snapshot

        """
        from ocs_ci.utility.lvmo_utils import get_lvm_cluster_name

        assert self.provisioner in constants.OCS_PROVISIONERS, "Unknown provisioner"
        if "rbd.csi.ceph.com" in self.provisioner:
            snap_yaml = constants.CSI_RBD_SNAPSHOT_YAML
            snapshotclass = helpers.default_volumesnapshotclass(
                constants.CEPHBLOCKPOOL
            ).name
        elif "cephfs.csi.ceph.com" in self.provisioner:
            snap_yaml = constants.CSI_CEPHFS_SNAPSHOT_YAML
            snapshotclass = helpers.default_volumesnapshotclass(
                constants.CEPHFILESYSTEM
            ).name
        elif self.provisioner in [
            constants.LVM_PROVISIONER_4_11,
            constants.LVM_PROVISIONER,
        ]:
            lvm_name = get_lvm_cluster_name()
            if "lvms" in lvm_name:
                snap_yaml = constants.CSI_LVMS_SNAPSHOT_YAML
                snapshotclass = constants.DEFAULT_VOLUMESNAPSHOTCLASS_LVMS
            else:
                snap_yaml = constants.CSI_LVM_SNAPSHOT_YAML
                snapshotclass = constants.DEFAULT_VOLUMESNAPSHOTCLASS_LVM

        snapshot_name = snapshot_name or f"{self.name}-snapshot-{uuid4().hex}"
        snapshot_obj = create_pvc_snapshot(
            pvc_name=self.name,
            snap_yaml=snap_yaml,
            snap_name=snapshot_name,
            namespace=self.namespace,
            sc_name=snapshotclass,
            wait=wait,
        )
        snapshot_obj.parent_access_mode = self.get_pvc_access_mode
        snapshot_obj.parent_sc = self.backed_sc
        snapshot_obj.parent_volume_mode = self.get()["spec"]["volumeMode"]
        return snapshot_obj

    def create_reclaim_space_job(self):
        """
        Create ReclaimSpaceJob to invoke reclaim space operation on RBD volume

        Returns:
            ocs_ci.ocs.resources.ocs.OCS: An OCS object representing ReclaimSpaceJob

        """
        assert (
            self.provisioner == constants.RBD_PROVISIONER
        ), "Only RBD PVC is supported"
        reclaim_space_job = helpers.create_reclaim_space_job(self.name)
        return reclaim_space_job

    def create_reclaim_space_cronjob(self, schedule=None):
        """
        Create ReclaimSpaceCronJob to invoke reclaim space operation on RBD volume

        Returns:
            ocs_ci.ocs.resources.ocs.OCS: An OCS object representing ReclaimSpaceCronJob

        """
        assert (
            self.provisioner == constants.RBD_PROVISIONER
        ), "Only RBD PVC is supported"
        reclaim_space_job = helpers.create_reclaim_space_cronjob(
            self.name, schedule=schedule
        )
        return reclaim_space_job


def delete_pvcs(pvc_objs, concurrent=False):
    """
    Deletes list of the pvc objects

    Args:
        pvc_objs (list): List of the pvc objects to be deleted
        concurrent (bool): Determines if the delete operation should be
            executed with multiple thread in parallel

    Returns:
        bool: True if deletion is successful
    """
    if concurrent:
        with ThreadPoolExecutor() as executor:
            for pvc in pvc_objs:
                executor.submit(pvc.delete)
    else:
        for pvc in pvc_objs:
            pvc.delete()
    return True


def get_all_pvcs(namespace=None, selector=None):
    """
    Gets all pvc in given namespace

    Args:
        namespace (str): Name of namespace  ('all-namespaces' to get all namespaces)
        selector (str): The label selector to look for

    Returns:
         dict: Dict of all pvc in namespaces
    """
    all_ns = True if namespace == "all-namespaces" else False
    if not namespace:
        namespace = config.ENV_DATA["cluster_namespace"]
    ocp_pvc_obj = OCP(kind=constants.PVC, namespace=namespace)

    out = ocp_pvc_obj.get(selector=selector, all_namespaces=all_ns)
    return out


def get_all_pvc_objs(namespace=None, selector=None):
    """
    Gets all PVCs objects in given namespace

    Args:
        namespace (str): Name of namespace
        selector (str): The label selector to look for

    Returns:
         list: Instances of PVC

    """
    all_pvcs = get_all_pvcs(namespace=namespace, selector=selector)
    err_msg = f"Failed to get the PVCs for namespace {namespace}"
    if selector:
        err_msg = err_msg + f" and selector {selector}"
    assert all_pvcs, err_msg
    return [PVC(**pvc) for pvc in all_pvcs["items"]]


def get_all_pvcs_in_storageclass(storage_class):
    """
    This function returen all the PVCs in a given storage class

    Args:
        storage_class (str): name of the storage class

    Returns:
        out: list of PVC objects

    """
    ocp_pvc_obj = OCP(kind=constants.PVC)
    pvc_list = ocp_pvc_obj.get(all_namespaces=True)["items"]
    out = []
    for pvc in pvc_list:
        pvc_obj = PVC(**pvc)
        if pvc_obj.backed_sc == storage_class:
            out.append(pvc_obj)

    return out


def get_deviceset_pvcs():
    """
    Get the deviceset PVCs

    Returns:
        list: The deviceset PVCs OCS objects

    Raises:
        AssertionError: In case the deviceset PVCs are not found

    """
    storage_cluster_obj = OCP(
        kind=constants.STORAGECLUSTER,
        resource_name=constants.DEFAULT_CLUSTERNAME,
        namespace=config.ENV_DATA["cluster_namespace"],
    )
    ocs_pvc_obj = get_all_pvc_objs(namespace=config.ENV_DATA["cluster_namespace"])
    deviceset_prefix = (
        storage_cluster_obj.get().get("spec").get("storageDeviceSets")[0].get("name")
    )
    deviceset_pvcs = []
    for pvc_obj in ocs_pvc_obj:
        if pvc_obj.name.startswith(deviceset_prefix):
            deviceset_pvcs.append(pvc_obj)
    assert deviceset_pvcs, "Failed to find the deviceset PVCs"
    return deviceset_pvcs


def get_deviceset_pvs():
    """
    Get the deviceset PVs

    Returns:
        list: the deviceset PVs OCS objects

    Raises:
        AssertionError: In case the deviceset PVCs are not found

    """
    deviceset_pvcs = get_deviceset_pvcs()
    return [pvc.backed_pv_obj for pvc in deviceset_pvcs]


def create_pvc_snapshot(
    pvc_name, snap_yaml, snap_name, namespace, sc_name=None, wait=False
):
    """
    Create snapshot of a PVC

    Args:
        pvc_name (str): Name of the PVC
        snap_yaml (str): The path of snapshot yaml
        snap_name (str): The name of the snapshot to be created
        namespace (str): The namespace for the snapshot creation
        sc_name (str): The name of the snapshot class
        wait (bool): True to wait for snapshot to be ready, False otherwise

    Returns:
        OCS object
    """
    ocp_version = version.get_semantic_ocp_version_from_config()
    snapshot_data = templating.load_yaml(snap_yaml)
    if ocp_version < version.VERSION_4_9:
        snapshot_data["apiVersion"] = "snapshot.storage.k8s.io/v1beta1"
    snapshot_data["metadata"]["name"] = snap_name
    snapshot_data["metadata"]["namespace"] = namespace
    if sc_name:
        snapshot_data["spec"]["volumeSnapshotClassName"] = sc_name
    snapshot_data["spec"]["source"]["persistentVolumeClaimName"] = pvc_name
    ocs_obj = OCS(**snapshot_data)
    created_snap = ocs_obj.create(do_reload=True)
    assert created_snap, f"Failed to create snapshot {snap_name}"
    if wait:
        ocs_obj.ocp.wait_for_resource(
            condition="true",
            resource_name=ocs_obj.name,
            column=constants.STATUS_READYTOUSE,
            timeout=60,
        )
    return ocs_obj


def create_restore_pvc(
    sc_name,
    snap_name,
    namespace,
    size,
    pvc_name,
    volume_mode=None,
    restore_pvc_yaml=constants.CSI_RBD_PVC_RESTORE_YAML,
    access_mode=constants.ACCESS_MODE_RWO,
):
    """
    Create PVC from snapshot

    Args:
        sc_name (str): The name of the storageclass
        snap_name (str): The name of the snapshot from which pvc would
        be created
        namespace (str): The namespace for the PVC creation
        size (str): Size of pvc being created
        pvc_name (str): The name of the PVC being created
        volume_mode (str): Volume mode for rbd RWX pvc i.e. 'Block'
        restore_pvc_yaml (str): The location of pvc-restore.yaml
        access_mode (str): The access mode to be used for the PVC

    Returns:
        PVC: PVC instance
    """
    pvc_data = templating.load_yaml(restore_pvc_yaml)
    pvc_data["metadata"]["name"] = pvc_name
    pvc_data["metadata"]["namespace"] = namespace
    pvc_data["spec"]["storageClassName"] = sc_name
    pvc_data["spec"]["resources"]["requests"]["storage"] = size
    if volume_mode:
        pvc_data["spec"]["volumeMode"] = volume_mode
    pvc_data["spec"]["dataSource"]["name"] = snap_name
    if access_mode:
        pvc_data["spec"]["accessModes"] = [access_mode]
    pvc_obj = PVC(**pvc_data)
    created_pvc = pvc_obj.create(do_reload=True)
    assert created_pvc, f"Failed to create resource {pvc_name}"
    return pvc_obj


def create_pvc_clone(
    sc_name,
    parent_pvc,
    clone_yaml,
    namespace,
    pvc_name=None,
    do_reload=True,
    storage_size=None,
    access_mode=None,
    volume_mode=None,
):
    """
    Create a cloned pvc from existing pvc

    Args:
        sc_name (str): The name of storage class.
        parent_pvc (str): Name of the parent pvc, whose clone is to be created.
        namespace (str): Namespace of the parent pvc
        pvc_name (str): The name of the PVC being created
        do_reload (bool): True for wait for reloading PVC after its creation,
            False otherwise
        storage_size (str): Size of the clone, if not passed will use the
            default "storage" value from pvc-clone.yaml. eg: '5Gi'
        access_mode (str): This decides the access mode to be used for
                the cloned PVC. eg: ReadWriteOnce, ReadOnlyMany, ReadWriteMany
        volume_mode (str): Volume mode for PVC. This should match the
            volume mode of parent PVC
    Returns:
        PVC: PVC instance

    """
    pvc_data = templating.load_yaml(clone_yaml)
    pvc_data["metadata"]["name"] = (
        pvc_name
        if pvc_name
        else helpers.create_unique_resource_name(parent_pvc, "clone")
    )
    pvc_data["metadata"]["namespace"] = namespace
    pvc_data["spec"]["storageClassName"] = sc_name
    pvc_data["spec"]["dataSource"]["name"] = parent_pvc
    if storage_size:
        pvc_data["spec"]["resources"]["requests"]["storage"] = storage_size
    if volume_mode:
        pvc_data["spec"]["volumeMode"] = volume_mode
    if access_mode:
        pvc_data["spec"]["accessModes"] = [access_mode]
    ocs_obj = PVC(**pvc_data)
    created_pvc = ocs_obj.create(do_reload=do_reload)
    assert created_pvc, f"Failed to create resource {pvc_name}"
    ocs_obj.reload()
    return ocs_obj


def get_pvc_objs(
    pvc_names,
    namespace=config.ENV_DATA["cluster_namespace"],
):
    """
    Get the PVC objects of the specified names

    Args:
        pvc_names (list): The list of the pvc names to get their objects
        namespace (str): Name of cluster namespace(default: config.ENV_DATA["cluster_namespace"])

    Returns:
        list: The PVC objects of the specified names

    """

    pvc_names_set = set(pvc_names)
    pvcs = get_all_pvc_objs(namespace=namespace)
    return [pvc for pvc in pvcs if pvc.name in pvc_names_set]


def scale_down_pods_and_remove_pvcs(sc_name):
    """
    Removes the mon and osd pods and also removes PVC's

    Args:
        sc_name (str): Name of storage class backing PVC
    """
    # To prevent circular dependencies importing from here
    from ocs_ci.ocs.resources.pod import (
        get_mon_pods,
        get_deployment_name,
        get_osd_pods,
        get_osd_prepare_pods,
        delete_pods,
    )

    # scale down mon pods
    namespace = config.ENV_DATA["cluster_namespace"]
    mon_pod_obj_list = get_mon_pods()
    for mon_pod_obj in mon_pod_obj_list:
        mon_deployment_name = get_deployment_name(mon_pod_obj.name)
        run_cmd(
            f"oc scale deployment {mon_deployment_name} --replicas=0 -n {namespace}"
        )

    # scale down osd pods
    osd_pod_obj_list = get_osd_pods()
    for osd_pod_obj in osd_pod_obj_list:
        osd_deployment_name = get_deployment_name(osd_pod_obj.name)
        run_cmd(
            f"oc scale deployment {osd_deployment_name} --replicas=0 -n {namespace}"
        )

    # delete osd-prepare pods
    osd_prepare_pod_obj_list = get_osd_prepare_pods()
    delete_pods(osd_prepare_pod_obj_list)

    # delete PVC's
    pvcs_objs = get_all_pvc_objs(namespace=namespace)
    for pvc_obj in pvcs_objs:
        if pvc_obj.backed_sc == sc_name or pvc_obj.backed_sc == f"{sc_name}-odf":
            pvc_name = pvc_obj.name
            pv_name = pvc_obj.backed_pv

            # set finalizers to null for both pvc and pv
            pvc_patch_cmd = (
                f"oc patch pvc {pvc_name} -n {namespace} -p "
                '\'{"metadata":{"finalizers":null}}\''
            )
            run_cmd(pvc_patch_cmd)
            pv_patch_cmd = (
                f"oc patch pv {pv_name} -n {namespace} -p "
                '\'{"metadata":{"finalizers":null}}\''
            )
            run_cmd(pv_patch_cmd)

            time.sleep(10)
            delete_pvcs([pvc_obj])


def flatten_image(clone_obj):
    """
    Flatten the image of clone

    Args:
        clone_obj: Object of clone of which image to be flatten
    """
    image_name = clone_obj.get_rbd_image_name
    pool_name = constants.DEFAULT_CEPHBLOCKPOOL

    tool_pod = pod.get_ceph_tools_pod()
    out = tool_pod.exec_ceph_cmd(
        ceph_cmd=f"rbd flatten {pool_name}/{image_name}",
        format=None,
    )
    log.info(f"{out}")
    log.info(f"Successfully flatten the image of {clone_obj.name}")


def get_pvc_size(pvc_obj, convert_size=1024):
    """
    Returns the PVC size in GB

    Args:
        pvc_obj (ocs_ci.ocs.resources.ocs.OCS): The pvc object
        convert_size (int): set convert by 1024 or 1000

    Returns:
        int: PVC size

    """
    unformatted_size = (
        pvc_obj.data.get("spec").get("resources").get("requests").get("storage")
    )
    return convert_device_size(unformatted_size, "GB", convert_size)
