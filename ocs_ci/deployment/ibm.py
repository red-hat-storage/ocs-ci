"""
This module implements the OCS deployment for IBM Power platform
Base code in deployment.py contains the required changes to keep
code duplication to minimum. Only destroy_ocs is retained here.
"""
import logging

from ocs_ci.deployment.deployment import Deployment

from ocs_ci.ocs import ocp
from ocs_ci.ocs.exceptions import (
    CommandFailed
)
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

        # STEP 2 & 3 not relevant. There are no PVCs using the storage class
        # provisioners. other than noobaa.

        # STEP 1 & 4
        scopenshiftprovisioner = []
        # Get a list of storage classes
        storage_classes = ocp.OCP(
            kind='StorageClass')
        try:
            sclassess = [(sub['metadata']['name'], sub['provisioner'])
                         for sub in storage_classes.get().get('items')]
            scnoprovisioner = [sub[0] for sub in sclassess
                               if sub[1] == 'kubernetes.io/no-provisioner']
            scopenshiftprovisioner = [sub[0] for sub in sclassess
                                      if not sub[1] == 'kubernetes.io/no-provisioner']
        except (IndexError, CommandFailed):
            logger.warning("Error")

        storage_clusters = ocp.OCP(
            kind='StorageCluster')
        try:
            scdevicesets = [sub['spec']['storageDeviceSets']
                            for sub in storage_clusters.get().get('items')]
            sclassindevicesets = \
                [[sub1['dataPVCTemplate']['spec']['storageClassName']
                    for sub1 in sub] for sub in scdevicesets]
        except (IndexError, CommandFailed):
            logger.warning("Error")

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
            localvolume = ocp.OCP(kind='LocalVolume',
                                  namespace='local-storage',
                                  resource_name=lv[1]).get().get('spec')
            scdevices = localvolume['storageClassDevices']
            devicepaths = [sub['devicePaths'] for sub in scdevices]
            alldevicepaths.append(devicepaths[0][0])

        # STEP 5
        # delete storage cluster - all of them.
        scdelete = 'oc delete -n openshift-storage storagecluster --all --wait=true'
        logger.info(scdelete)
        out = run_cmd(scdelete)
        logger.info(out)

        # STEP 6
        spdelete = 'oc delete project openshift-storage --wait=true --timeout=5m'
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
            lvdelete = 'oc delete localvolume -n local-storage --wait=true ' + sc[0]
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
        # Remove local-storage namespace
        spdelete = 'oc delete project local-storage --wait=true --timeout=5m'
        logger.info(spdelete)
        out = run_cmd(spdelete)
        logger.info(out)

        # STEP 13
        catsource = 'oc delete catalogsource ocs-catalogsource -n openshift-marketplace'
        logger.info(catsource)
        out = run_cmd(catsource)
        logger.info(out)

        # STEP 14
        mondelete = 'oc delete configmap cluster-monitoring-config -n openshift-monitoring'
        logger.info(mondelete)
        out = run_cmd(mondelete)
        logger.info(out)

        # STEP 15
        pvcdelete = 'oc delete pvc registry-cephfs-rwx-pvc -n openshift-image-registry'
        logger.info(pvcdelete)
        out = run_cmd(pvcdelete)
        logger.info(out)
