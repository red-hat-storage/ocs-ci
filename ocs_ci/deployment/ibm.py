"""
This module implements the OCS deployment for IBM Power platform
Base code in deployment.py contains the required changes to keep
code duplication to minimum. Only destroy_ocs is retained here.
"""

import json
import logging
import subprocess
import time

from ocs_ci.deployment.deployment import Deployment

from ocs_ci.framework import config
from ocs_ci.ocs import ocp, constants
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.resources import pvc

logger = logging.getLogger(__name__)


class IBMDeployment(Deployment):
    """
    Implementation of Deploy for IBM Power architecture
    """

    def destroy_ocs(self):
        """
        Handle OCS destruction. Remove storage classes, PVCs, Storage
        Cluster, Openshift-storage namespace, LocalVolume, unlabel
        worker-storage nodes, delete ocs CRDs, etc.
        """
        cluster_namespace = config.ENV_DATA["cluster_namespace"]

        # https://access.redhat.com/documentation/en-us/red_hat_openshift_container_storage/4.5/html/deploying_openshift_container_storage_using_bare_metal_infrastructure/assembly_uninstalling-openshift-container-storage_rhocs

        # Section 3.1 Step 1
        # Deleting PVCs
        rbd_pvcs = [
            p
            for p in pvc.get_all_pvcs_in_storageclass(constants.CEPHBLOCKPOOL_SC)
            if not (
                p.data["metadata"]["namespace"] == cluster_namespace
                and p.data["metadata"]["labels"]["app"] == "noobaa"
            )
        ]
        pvc.delete_pvcs(rbd_pvcs)
        cephfs_pvcs = pvc.get_all_pvcs_in_storageclass(constants.CEPHFILESYSTEM_SC)

        # Section 3.1 Step 2
        # Section 3.3 Step 1
        # Removing OpenShift Container Platform registry from OpenShift Container Storage
        registry_conf_name = "configs.imageregistry.operator.openshift.io"
        registry_conf = ocp.OCP().exec_oc_cmd(f"get {registry_conf_name} -o yaml")
        if registry_conf["items"][0]["spec"].get("storage", dict()).get("pvc"):
            patch = dict(spec=dict(storage=dict(emptyDir=dict(), pvc=None)))
            ocp.OCP().exec_oc_cmd(
                f"patch {registry_conf_name} cluster --type merge "
                f"-p '{json.dumps(patch)}'"
            )
        # Section 3.3 Step 2
        pvc.delete_pvcs(cephfs_pvcs)

        # Section 3.1 Step 3
        try:
            ocp.OCP().exec_oc_cmd(
                f"delete -n {cluster_namespace} storagecluster --all --wait=true"
            )
        except (CommandFailed, subprocess.TimeoutExpired):
            pass

        # Section 3.1 Step 4
        ocp.OCP().exec_oc_cmd("project default")
        ocp.OCP().exec_oc_cmd(
            f"delete project {cluster_namespace} --wait=true --timeout=5m"
        )
        tried = 0
        leftovers = True
        while tried < 5:
            # We need to loop here until the project can't be found
            try:
                ocp.OCP().exec_oc_cmd(
                    f"get project {cluster_namespace}",
                    out_yaml_format=False,
                )
            except CommandFailed:
                leftovers = False
                break
            time.sleep(60)
            tried += 1
        if leftovers:
            # https://access.redhat.com/documentation/en-us/red_hat_openshift_container_storage/4.5/html/troubleshooting_openshift_container_storage/troubleshooting-and-deleting-remaining-resources-during-uninstall_rhocs
            leftover_types = [
                "cephfilesystem.ceph.rook.io",
                "cephobjectstore.ceph.rook.io",
                "cephobjectstoreuser.ceph.rook.io",
                "storagecluster.ocs.openshift.io",
            ]
            patch = dict(metadata=dict(finalizers=None))
            for obj_type in leftover_types:
                try:
                    objs = ocp.OCP(kind=obj_type).get()
                except CommandFailed:
                    continue
                for obj in objs["items"]:
                    name = obj["metadata"]["name"]
                    ocp.OCP().exec_oc_cmd(
                        f"oc patch -n {cluster_namespace} {obj_type} {name} --type=merge -p '{json.dumps(patch)}'"
                    )

        # Section 3.1 Step 5
        nodes = ocp.OCP().exec_oc_cmd(
            "get node -l cluster.ocs.openshift.io/openshift-storage= -o yaml"
        )
        for node in nodes["items"]:
            node_name = node["metadata"]["name"]
            ocp.OCP().exec_oc_cmd(
                f"debug node/{node_name} -- chroot /host rm -rfv /var/lib/rook"
            )

        # Section 3.1 Step 6
        ocp.OCP().exec_oc_cmd(
            "delete storageclass  openshift-storage.noobaa.io --wait=true --timeout=5m"
        )

        # Section 3.1 Step 7
        ocp.OCP().exec_oc_cmd(
            "label nodes  --all cluster.ocs.openshift.io/openshift-storage-"
        )
        ocp.OCP().exec_oc_cmd("label nodes  --all topology.rook.io/rack-")

        # Section 3.1 Step 8
        pvs = ocp.OCP(kind="PersistentVolume").get()
        for pv in pvs["items"]:
            pv_name = pv["metadata"]["name"]
            if pv_name.startswith("ocs-storagecluster-ceph"):
                ocp.OCP().exec_oc_cmd(f"delete pv {pv_name}")

        # Section 3.1 Step 9
        # Note that the below process differs from the documentation slightly.
        # Instead of deleting all CRDs at once and calling the job done, we
        # iterate over a list of them, noting which ones don't delete fully and
        # applying the standard workaround of removing the finalizers from any
        # CRs and also the CRD. Finally, the documentation leaves out a few
        # CRDs that we've seen in deployed clusters.
        crd_types = [
            "backingstores.noobaa.io",
            "bucketclasses.noobaa.io",
            "cephblockpools.ceph.rook.io",
            "cephclients.ceph.rook.io",
            "cephclusters.ceph.rook.io",
            "cephfilesystems.ceph.rook.io",
            "cephnfses.ceph.rook.io",
            "cephobjectrealms.ceph.rook.io",  # not in doc
            "cephobjectstores.ceph.rook.io",
            "cephobjectstoreusers.ceph.rook.io",
            "cephobjectzonegroups.ceph.rook.io",  # not in doc
            "cephobjectzones.ceph.rook.io",  # not in doc
            "cephrbdmirrors.ceph.rook.io",  # not in doc
            "noobaas.noobaa.io",
            "ocsinitializations.ocs.openshift.io",
            "storageclusterinitializations.ocs.openshift.io",
            "storageclusters.ocs.openshift.io",
        ]
        cr_patch = json.dumps(dict(finalizers=None))
        crd_patch = json.dumps(dict(metadata=dict(finalizers=None)))
        for crd_type in crd_types:
            try:
                ocp.OCP().exec_oc_cmd(
                    f"delete crd {crd_type} --wait=true --timeout=30s",
                    out_yaml_format=False,
                )
            except CommandFailed:
                pass
            crs = []
            try:
                crs = ocp.OCP(kind=crd_type).get(all_namespaces=True)["items"]
            except CommandFailed:
                continue
            for cr in crs:
                cr_md = cr["metadata"]
                ocp.OCP().exec_oc_cmd(
                    f"patch -n {cr_md['namespace']} {crd_type} {cr_md['name']} --type=merge -p '{cr_patch}'"
                )
            try:
                crs = ocp.OCP(kind=crd_type).get(all_namespaces=True)["items"]
            except CommandFailed:
                continue
            ocp.OCP().exec_oc_cmd(f"patch crd {crd_type} --type=merge -p '{crd_patch}'")

        # End sections from above documentation
        ocp.OCP().exec_oc_cmd(
            f"delete catalogsource {constants.OPERATOR_CATALOG_SOURCE_NAME} "
            f"-n {constants.MARKETPLACE_NAMESPACE}"
        )

        storageclasses = ocp.OCP(kind="StorageClass").get(all_namespaces=True)
        for sc in storageclasses["items"]:
            if sc["provisioner"].startswith("openshift-storage."):
                sc_name = sc["metadata"]["name"]
                ocp.OCP().exec_oc_cmd(f"delete storageclass {sc_name}")
        volumesnapclasses = ocp.OCP(kind="VolumeSnapshotClass").get(all_namespaces=True)
        for vsc in volumesnapclasses["items"]:
            if vsc["driver"].startswith("openshift-storage."):
                vsc_name = vsc["metadata"]["name"]
                ocp.OCP().exec_oc_cmd(f"delete volumesnapshotclass {vsc_name}")

        self.destroy_lso()

    def destroy_lso(self):
        lso_namespace = config.ENV_DATA["local_storage_namespace"]
        lso_name = constants.LOCAL_STORAGE_CSV_PREFIX
        subscription = ocp.OCP(
            kind="Subscription",
            resource_name=lso_name,
            namespace=lso_namespace,
        ).get()
        currentCSV = subscription["status"]["currentCSV"]
        ocp.OCP().exec_oc_cmd(f"delete subscription {lso_name} -n {lso_namespace}")
        ocp.OCP().exec_oc_cmd(
            f"delete clusterserviceversion -n {lso_namespace} {currentCSV}"
        )
        ocp.OCP().exec_oc_cmd(
            f"delete project {lso_namespace} --wait=true --timeout=5m"
        )
        ocp.OCP().exec_oc_cmd("delete storageclass localblock")
        try:
            ocp.OCP().exec_oc_cmd(
                f"delete localvolumediscovery auto-discover-devices -n {lso_namespace}"
            )
            ocp.OCP().exec_oc_cmd(
                f"delete localvolumeset localblock -n {lso_namespace}"
            )
        except CommandFailed:
            pass
        pvs = ocp.OCP(kind="PersistentVolume").get()
        for pv in pvs["items"]:
            pv_name = pv["metadata"]["name"]
            if pv["spec"].get("storageClassName") == "localblock":
                ocp.OCP().exec_oc_cmd(f"delete pv {pv_name}")
