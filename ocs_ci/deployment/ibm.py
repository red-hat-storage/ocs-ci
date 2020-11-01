"""
This module implements the OCS deployment for IBM Power platform
Base code in deployment.py contains the required changes to keep
code duplication to minimum. Only destroy_ocs is retained here.
"""
import logging

from ocs_ci.deployment.deployment import Deployment

from ocs_ci.framework import config
from ocs_ci.ocs import ocp, constants
from ocs_ci.utility.utils import (
    run_cmd
)

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
        cluster_namespace = config.ENV_DATA['cluster_namespace']
        local_storage_namespace = config.ENV_DATA['local_storage_namespace']

        # STEP 2 & 3 not relevant. There are no PVCs using the storage class
        # provisioners. other than noobaa.

        # STEP 1 & 4
        # Get a list of storage classes
        storage_classes = ocp.OCP(
            kind='StorageClass').get()
        scnoprovisioner = []
        scopenshiftprovisioner = []
        for storage_class in storage_classes.get('items', []):
            name = storage_class['metadata']['name']
            if storage_class['provisioner'] == 'kubernetes.io/no-provisioner':
                scnoprovisioner.append(name)
            else:
                scopenshiftprovisioner.append(name)

        storage_clusters = ocp.OCP(
            kind='StorageCluster',
            namespace=cluster_namespace
        ).get()
        sclassindevicesets = []
        for storage_cluster in storage_clusters.get('items', []):
            device_sets = storage_cluster['spec']['storageDeviceSets']
            sclassindevicesets.append([
                i['dataPVCTemplate']['spec']['storageClassName'] for i in device_sets
            ])

        # sclassindevicesets is a list of lists. Each outer list is for
        # a specific storage cluster. All storage classes in the inner
        # list are same (same storage class used across all devicesets
        # in a storage cluster. So pick only the first one.

        # for each storage class, get LocalVolume (owner).

        if len(sclassindevicesets) == 0:
            logger.warning("No storage class in device sets. Quitting.")
            return

        # compare scnoprovisioner and sclassindevicesets
        tmplist = []
        for sclist in sclassindevicesets:
            for sc in sclist:
                if sc in scnoprovisioner:
                    if sc not in tmplist:
                        tmplist.append(sc)

        scandlvlist = []
        for sc in tmplist:
            storage_class = ocp.OCP(
                kind='StorageClass',
                resource_name=sc
            )
            localvolumemeta = storage_class.get().get('metadata')
            localvolume = localvolumemeta['labels']['local.storage.openshift.io/owner-name']
            scandlvlist.append((sc[0], localvolume))

        alldevicepaths = []
        for lv in scandlvlist:
            # Get localvolume and corresponding devices
            localvolume = ocp.OCP(
                kind='LocalVolume',
                namespace=local_storage_namespace,
                resource_name=lv[1]).get().get('spec')
            scdevices = localvolume['storageClassDevices']
            devicepaths = [sub['devicePaths'] for sub in scdevices]
            alldevicepaths.append(devicepaths[0][0])

        # STEP 5
        # delete storage cluster - all of them.
        scdelete = f"oc delete -n {cluster_namespace} storagecluster --all --wait=true"
        logger.info(scdelete)
        out = run_cmd(scdelete)
        logger.info(out)

        # STEP 6
        spdelete = f"oc delete project {cluster_namespace} --wait=true --timeout=5m"
        logger.info(spdelete)
        out = run_cmd(spdelete)
        logger.info(out)

        default = 'oc project default'
        logger.info(default)
        out = run_cmd(default)
        logger.info(out)

        # STEP 7 & 9
        nodes = ocp.OCP(kind='node').get().get('items', [])
        worker_nodes = [
            node for node in nodes if "cluster.ocs.openshift.io/openshift-storage"
            in node['metadata']['labels']
        ]
        worker_node_names = [node['metadata']['name'] for node in worker_nodes]
        logger.warning(worker_node_names)

        for worker_node in worker_node_names:
            rookstring = 'oc debug node/' + worker_node + \
                         ' -- chroot /host rm -rfv /var/lib/rook'
            logger.info(rookstring)
            out = run_cmd(rookstring)
            logger.info(out)

            for devicepath in alldevicepaths:
                sgdiskstring = 'oc debug node/' + worker_node + \
                    ' -- chroot /host sgdisk --zap-all ' + devicepath
                logger.info(sgdiskstring)
                # out = run_cmd(sgdiskstring)
                # logger.info(out)

        # STEP 8
        for sc in scandlvlist:
            lvdelete = f"oc delete localvolume -n {local_storage_namespace} --wait=true {sc[0]}"
            logger.info(lvdelete)
            out = run_cmd(lvdelete)
            logger.info(out)
            pvdelete = 'oc delete pv -l \
                storage.openshift.com/local-volume-owner-name=' + sc[0] \
                + ' --wait --timeout=5m'
            logger.info(pvdelete)
            out = run_cmd(pvdelete)
            logger.info(out)
            scdelete = 'oc delete storageclass ' + sc[1] + ' --wait=true --timeout=5m'
            logger.info(scdelete)
            out = run_cmd(scdelete)
            logger.info(out)

            for worker_node in worker_node_names:
                rookstring = 'oc debug node/' + worker_node + \
                             ' -- chroot /host rm -rfv /mnt/local-storage/ ' + sc[1]
                logger.info(rookstring)
                out = run_cmd(rookstring)
                logger.info(out)

        # Delete all PV's for openshift provisioner storageclass
        pvall = 'oc delete pv --all'
        logger.info(pvall)
        out = run_cmd(pvall)
        logger.info(out)

        # STEP 10
        for sc in scopenshiftprovisioner:
            scdelete = 'oc delete storageclass ' + sc + ' --wait=true --timeout=5m'
            logger.info(scdelete)
            out = run_cmd(scdelete)
            logger.info(out)

        # STEP 11
        # Unlabel nodes
        unlabel1 = 'oc label nodes  --all cluster.ocs.openshift.io/openshift-storage='
        logger.info(unlabel1)
        # out = run_cmd(unlabel1)
        logger.info(out)

        unlabel2 = ' oc label nodes  --all topology.rook.io/rack='
        logger.info(unlabel2)
        # out = run_cmd(unlabel2)
        logger.info(out)

        # STEP 12
        # Remove local storage namespace
        spdelete = f"oc delete project {local_storage_namespace} --wait=true --timeout=5m"
        logger.info(spdelete)
        out = run_cmd(spdelete)
        logger.info(out)

        # STEP 13
        catsource = f"oc delete catalogsource ocs-catalogsource -n {constants.MARKETPLACE_NAMESPACE}"
        logger.info(catsource)
        out = run_cmd(catsource)
        logger.info(out)

        # STEP 14
        mondelete = f"oc delete configmap cluster-monitoring-config -n {constants.OPENSHIFT_MONITORING_NAMESPACE}"
        logger.info(mondelete)
        out = run_cmd(mondelete)
        logger.info(out)

        # STEP 15
        pvcdelete = f"oc delete pvc registry-cephfs-rwx-pvc -n {constants.OPENSHIFT_IMAGE_REGISTRY_NAMESPACE}"
        logger.info(pvcdelete)
        out = run_cmd(pvcdelete)
        logger.info(out)
