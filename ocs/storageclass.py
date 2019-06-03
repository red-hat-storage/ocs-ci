import os
import logging
import yaml

from munch import munchify

from ocs import defaults, ocp, kinds
from ocs.rook import OCSCluster
from utility.templating import generate_yaml_from_jinja2_template_with_data

logger = logging.getLogger(__name__)

class CephFSStorageClass(object):
    """
    Handling CephFS storage from openshift storageclass perspective
    # TODO: Handle secret for storageclasses, may be a seperate class
    """
    def __init__(self, **kwargs):
        self._name = kwargs.get('name', defaults.CEPHFS_STORAGE_CLASS)
        self._namespace = kwargs.get(
            'namespace',
            defaults.ROOK_CLUSTER_NAMESPACE
        )
        self.FSSTORAGECLASS =ocp.OCP(
            kind='StorageClass',
            namespace=self._namespace,
        )
        self.storageclass = "CephFS"
        # cluster object which will be used by this storageclass
        self.cluster = kwargs.get('cluster', None)
        self.driver_type = kwargs.get('driver', 'csi')

        if self.driver_type == 'csi':
            self.INITIAL_CONFIG = os.path.join(
                defaults.TEMPLATE_DIR, "CSI/cephfs/storageclass.yaml"
            )
        else:
            #TODO: Figure out other types and template path
            pass

        #If user passes resource yaml path to override default
        if kwargs.get('yaml_path'):
            self.INITIAL_CONFIG = kwargs.get('yaml_path')
        self.reclaim_policy = kwargs.get('reclaim-policy', 'Delete')
        self.CURRENT_CONFIG = "cephfsstorageclass_tmp.yaml"

        # Create cephfs
        logger.info("creating cephfs")
        assert self.cluster.create_cephfs()
        logger.info(f"Created cephfs {self.cluster.cephfs.name}")

        #TODO: How to handle user and keys ?

        resource = generate_yaml_from_jinja2_template_with_data(
            self.INITIAL_CONFIG,
            **kwargs,
        )
        with open(self.CURRENT_CONFIG, 'w') as conf:
            yaml.dump(resource, conf, default_flow_style=False)

        logger.info(f"Creating cephfsstorageclass {self._name}")
        assert self.FSSTORAGECLASS.create(yaml_file=self.CURRENT_CONFIG)

    def delete(self):
        """
        Delete a storage class

        Returns:
            bool: True if deleted else False

            #TODO: Decide to delete associated CephFS as well ?
        """
        logger.info(f'Deleting Storageclass {self._name}')
        out = self.FSSTORAGECLASS.delete(resource_name=self._name)
        if f'"{self._name}" deleted' in out:
            # double check
            if f'NotFound' in self.FSSTORAGECLASS.get(resource_name=self._name):
                return True
        return False
