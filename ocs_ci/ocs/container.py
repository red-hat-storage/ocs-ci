
import logging

from ocs_ci.ocs import constants, defaults
from ocs_ci.utility import templating
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.utility.utils import run_cmd
from ocs_ci.ocs.exceptions import CommandFailed

from tests.helpers import (create_unique_resource_name,
                           get_admin_key, get_cephfs_name,
                           get_cephfs_data_pool_name)

log = logging.getLogger(__name__)


class ResourceStor(object):
    """
    Container Storage class

    Args:
        interface_type (str): type of interface to use RBD or FS
        block_pool_name (str): name of the ceph block pool name
        reclaim_policy (str): pvc recliam policy (retain/delete)
        sc_name (str): Storage class metadata name
        pvc_name (str): PVC metadata name
        access_mode (str): Access mode for PVC - RWO/RWX/ROX
        namespace (str): namespace for the PVC
        wait (bool): whether to wait for PVC to be in BOUND state
        size (int): Size of the PVC in GB
        cleanup (bool): If true, the previously created objects are cleanedup

    """
    def __init__(
        self,
        interface_type=constants.CEPHBLOCKPOOL,
        block_pool_name=None,
        reclaim_policy=constants.RECLAIM_POLICY_DELETE,
        sc_name=None,
        pvc_name=None,
        num_pvc=1,
        access_mode=constants.ACCESS_MODE_RWO,
        namespace=defaults.ROOK_CLUSTER_NAMESPACE,
        wait=True,
        size=None,
        cleanup=False,
    ):
        self.interface_type = interface_type
        self.pool_name = block_pool_name
        self.sc_name = sc_name
        self.pvc_name = pvc_name
        self.access_mode = access_mode
        self.reclaim_policy = reclaim_policy
        self.namespace = namespace
        self.size = size
        self.wait = wait
        self.sc_data = dict()
        self.block_data = dict()
        self.secret_data = dict()
        self.pvc_data = dict()
        if cleanup:
            self._cleanup()
        if interface_type == constants.CEPHBLOCKPOOL:
            self._setup_rbd_secret()
            self._setup_ceph_blockpool()
            self._setup_rbd_storageclass()
        else:
            self._setup_fs_secret()
            self._setup_fs_storageclass()
        self._setup_pvc()

    def _setup_rbd_secret(self):
        """"
        Setup RBD secret
        """
        self.secret_data = templating.load_yaml_to_dict(
            constants.CSI_RBD_SECRET_YAML
        )
        self.secret_data['stringData']['userID'] = constants.ADMIN_USER
        self.secret_data['stringData']['userKey'] = get_admin_key()
        self._setup_secret_data_name()

    def _setup_fs_secret(self):
        """
        Setup FS secret
        """
        self.secret_data = templating.load_yaml_to_dict(
            constants.CSI_CEPHFS_SECRET_YAML
        )
        del self.secret_data['stringData']['userID']
        del self.secret_data['stringData']['userKey']
        self.secret_data['stringData']['adminID'] = constants.ADMIN_USER
        self.secret_data['stringData']['adminKey'] = get_admin_key()
        self._setup_secret_data_name()

    def _setup_secret_data_name(self):
        """
        Setup secret data name
        """
        self.secret_data['metadata']['namespace'] = (
            defaults.ROOK_CLUSTER_NAMESPACE)
        self.secret_data_name = create_unique_resource_name('test', 'secret')
        self.secret_data['metadata']['name'] = self.secret_data_name

    def _setup_ceph_blockpool(self):
        """
        Setup ceph block pool data
        """
        self.block_data = templating.load_yaml_to_dict(constants.CEPHBLOCKPOOL_YAML)
        if self.pool_name is None:
            self.pool_name = create_unique_resource_name('test', 'cbp')
        self.block_data['metadata']['name'] = self.pool_name
        self.block_data['metadata']['namespace'] = defaults.ROOK_CLUSTER_NAMESPACE

    def _setup_rbd_storageclass(self):
        """
        Setup  RBD storageclass
        """
        self.sc_data = templating.load_yaml_to_dict(
            constants.CSI_RBD_STORAGECLASS_YAML
        )
        self.sc_data['parameters'][
            'csi.storage.k8s.io/node-stage-secret-name'
        ] = self.secret_data_name
        self.sc_data['parameters'][
            'csi.storage.k8s.io/node-stage-secret-namespace'
        ] = defaults.ROOK_CLUSTER_NAMESPACE
        self.sc_data['provisioner'] = defaults.RBD_PROVISIONER
        self.sc_data['parameters']['pool'] = self.pool_name
        self._setup_sc()

    def _setup_fs_storageclass(self):
        """
        Setup CephFS Storage class
        """
        self.sc_data = templating.load_yaml_to_dict(
            constants.CSI_CEPHFS_STORAGECLASS_YAML
        )
        self.sc_data['parameters'][
            'csi.storage.k8s.io/node-stage-secret-name'
        ] = self.secret_data_name
        self.sc_data['parameters'][
            'csi.storage.k8s.io/node-stage-secret-namespace'
        ] = defaults.ROOK_CLUSTER_NAMESPACE
        self.sc_data['parameters']['fsName'] = get_cephfs_name()
        self.sc_data['provisioner'] = defaults.CEPHFS_PROVISIONER
        self.sc_data['parameters']['pool'] = get_cephfs_data_pool_name()
        self._setup_sc()

    def _setup_sc(self):
        """
        Setup reminder of storage class data
        """

        if self.sc_name is None:
            self.sc_name = create_unique_resource_name('test', 'storageclass')
        self.sc_data['metadata']['name'] = self.sc_name
        self.sc_data['metadata']['namespace'] = defaults.ROOK_CLUSTER_NAMESPACE
        self.sc_data['parameters'][
            'csi.storage.k8s.io/provisioner-secret-name'
        ] = self.secret_data_name
        self.sc_data['parameters'][
            'csi.storage.k8s.io/provisioner-secret-namespace'
        ] = defaults.ROOK_CLUSTER_NAMESPACE

        self.sc_data['parameters']['clusterID'] = defaults.ROOK_CLUSTER_NAMESPACE
        self.sc_data['reclaimPolicy'] = self.reclaim_policy

    def _setup_pvc(self):
        """
         Setup PVC data
        """
        self.pvc_data = templating.load_yaml_to_dict(constants.CSI_PVC_YAML)
        if self.pvc_name is None:
            self.pvc_name = create_unique_resource_name('test', 'pvc')
        self.pvc_data['metadata']['name'] = self.pvc_name
        self.pvc_data['metadata']['namespace'] = self.namespace
        self.pvc_data['spec']['accessModes'] = [self.access_mode]
        self.pvc_data['spec']['storageClassName'] = self.sc_name
        if self.size:
            self.pvc_data['spec']['resources']['requests']['storage'] = self.size

    def _create_rbd_pvc(self):
        """
         Create a PVC backed by RBD
        """
        self.rbd_secret_obj = self._create_ocs(self.secret_data)
        self.ceph_block_pool_obj = self._create_ocs(self.block_data)
        self.rbd_sc_obj = self._create_ocs(self.sc_data)
        self.pvc_obj = self._create_ocs(self.pvc_data, constants.STATUS_BOUND)

    def _create_fs_pvc(self):
        """
         Create a PVC backed by CephFS
        """
        self.cephfs_secret_obj = self._create_ocs(self.secret_data)
        self.fs_sc_obj = self._create_ocs(self.sc_data)
        self.pvc_obj = self._create_ocs(self.pvc_data, constants.STATUS_BOUND)

    def _delete_rbd_pvc(self):
        """
         delete rbd pvc
        """
        self._delete_ocs(self.pvc_obj)
        self._delete_ocs(self.rbd_sc_obj)
        self._delete_ocs(self.ceph_block_pool_obj)
        self._delete_ocs(self.rbd_secret_obj)

    def _delete_fs_pvc(self):
        """
         Delete fs pvc
        """
        self._delete_ocs(self.pvc_obj)
        self._delete_ocs(self.fs_sc_obj)
        self._delete_ocs(self.cephfs_secret_obj)

    def _create_ocs(self, data, state=None):
        """
        Create OCS object
        """
        obj = OCS(**data)
        obj.create(do_reload=True)
        # add label so that we can delete them with ease
        obj.ocp.add_label(obj.name, constants.OCS4_TEST_LABEL)
        if self.wait and state is not None:
            obj.ocp.wait_for_resource(
                condition=state,
                resource_name=obj.name,
                timeout=60
            )
        return obj

    def _delete_ocs(self, obj):
        """"
        Delete OCS object
        """
        obj.delete()
        obj.ocp.wait_for_delete(obj.name)

    def create(self):
        """
        Create pvc and all required dependency objects
        """
        if self.interface_type == constants.CEPHBLOCKPOOL:
            self._create_rbd_pvc()
        else:
            self._create_fs_pvc()

    def delete(self):
        """
        Delete pvc and all required dependency objects
        """
        if self.interface_type == constants.CEPHBLOCKPOOL:
            self._delete_rbd_pvc()
        else:
            self._delete_fs_pvc()

    def _cleanup(self):
        """
        Cleanup resources created by Container object
        """
        label = constants.OCS4_TEST_LABEL
        test_label = label.split('=')[0]
        ns = defaults.ROOK_CLUSTER_NAMESPACE
        try:
            run_cmd(f'oc delete pvc -l {test_label} -n {ns}')
        except CommandFailed:
            log.info("No previous pvc objects found")
        try:
            run_cmd(f'oc delete storageclass -l {test_label} -n {ns}')
        except CommandFailed:
            log.info('No previous storageclass objects found')
        try:
            run_cmd(f'oc delete secret -l {test_label} -n {ns}')
        except CommandFailed:
            log.info("No previous secret objects found")
