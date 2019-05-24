"""
A module which consists of kubevolume related operations
"""

import os
import logging

from ocs import defaults
from ocs import kinds
from kubernetes import config
from openshift.dynamic import DynamicClient
from utility.utils import get_random_str
from utility.templating import generate_yaml_from_jinja2_template_with_data

logger = logging.getLogger(__name__)

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
TOP_DIR = os.path.dirname(THIS_DIR)
TEMPLATE_DIR = os.path.join(TOP_DIR, "templates/ocs-deployment/")


class KubeVolume(object):
    """
    Base class for cluster volumes

    Attributes:
        name (str): name of the RBD volume
        namepsace (str): namespace to create RBD volume

    """
    def __init__(self, name, namespace):
        k8s_client = config.new_client_from_config()
        self.dyn_client = DynamicClient(k8s_client)
        self.name = name
        self.namespace = namespace


class CephRBDVolume(KubeVolume):
    """
    Class which contains Ceph RBD related functionality

    Attributes:
        name (str): name of the RBD volume
        namepsace (str): namespace to create RBD volume

    """
    def __init__(self, name=None, namespace='default'):
        KubeVolume.__init__(self, name, namespace)
        self.kind = kinds.CEPHBLOCKPOOL
        self.api_version = defaults.OPENSHIFT_REST_CLIENT_API_VERSION
        self.service_cbp = self.dyn_client.resources.get(
            api_version=self.api_version,
            kind=self.kind
        )
        self.template_path = os.path.join(TEMPLATE_DIR, "cephblockpool.yaml")

    def create_cephblockpool(
        self,
        cephblockpool_name_prefix="autotests-cephblockpool",
        failureDomain="host",
        size="3"
    ):
        """
        Creates cephblock pool

        Args:
            cephblockpool_name_prefix (str): prefix given to cephblockpool
            failureDomain (str): The failure domain across which the
                                   replicas or chunks of data will be spread
            size (int): The number of copies of the data in the pool.

        Returns:
            str : name of the cephblockpool created

        Raises:
            KeyError when error occured

        Examples:
            create_cephblockpool(
                cephblockpool_name_prefix="autotests-blockpool",
                failureDomain="host",
                size=3)

        """
        if self.name:
            cbp_name = self.name
        else:
            cbp_name = f"{cephblockpool_name_prefix}-{get_random_str()}"
        # overwrite the namespace with openshift-storage, since cephblockpool
        # is tied-up with openshift-storage
        self.namespace = defaults.ROOK_CLUSTER_NAMESPACE

        cephblockpool_data = {}
        cephblockpool_data['cephblockpool_name'] = cbp_name
        cephblockpool_data['rook_api_version'] = defaults.ROOK_API_VERSION
        cephblockpool_data['failureDomain'] = failureDomain
        cephblockpool_data['size'] = size

        data = generate_yaml_from_jinja2_template_with_data(
            self.template_path,
            **cephblockpool_data
        )
        self.service_cbp.create(body=data, namespace=self.namespace)

        return cbp_name


class StorageClass(KubeVolume):
    """
    Class which contains StorageClass related functionality

    Attributes:
        name (str): name of the RBD volume
        namepsace (str): namespace to create RBD volume

    """
    def __init__(self, name=None, namespace='default'):
        KubeVolume.__init__(self, name, namespace)
        self.kind = kinds.STORAGECLASS
        self.api_version = defaults.OPENSHIFT_REST_CLIENT_API_VERSION
        self.service_sc = self.dyn_client.resources.get(
            api_version=self.api_version,
            kind=self.kind
        )
        self.template_path = os.path.join(TEMPLATE_DIR, "storageclass.yaml")

    def create_storageclass(
        self,
        blockPool,
        sc_name_prefix="autotests-sc",
        allow_volume_expansion=True,
        reclaim_policy="Delete",
        fstype="xfs",
        clusterNamespace=defaults.ROOK_CLUSTER_NAMESPACE,
    ):
        """
        Creates storage class using data provided

        Args:
            blockPool (str): name of the block pool
            sc_name_prefix (str): sc name will consist of this prefix and
                                  random str.
            allow_volume_expansion (bool): either True or False
            reclaim_policy (str): Reclaim Policy type. Either Retain,
                                  Recycle or Delete
            fstype (str): filesystem type
            clusterNamespace (str): namespace where rook cluster exists

        Returns:
            str: name of the storage class created

        Example:
            create_storageclass(
                blockPool,
                sc_name_prefix="autotests-sc",
                allow_volume_expansion=True,
                reclaim_policy="Delete",
                fstype="xfs"
                clusternamespace="openshift-storage",
            )

        """
        if self.name:
            sc_name = self.name
        else:
            sc_name = f"{sc_name_prefix}-{get_random_str()}"

        sc_data = {}
        sc_data['k8s_api_version'] = defaults.STORAGE_API_VERSION
        sc_data['storageclass_name'] = sc_name
        sc_data['volume_expansion'] = allow_volume_expansion
        sc_data['reclaimPolicy'] = reclaim_policy
        sc_data['blockPool'] = blockPool
        sc_data['clusterNamespace'] = clusterNamespace
        sc_data['fstype'] = fstype

        data = generate_yaml_from_jinja2_template_with_data(
            self.template_path,
            **sc_data
        )
        self.service_sc.create(body=data)

        return sc_name


class PVC(KubeVolume):
    """
    Class which contains PVC related functionality

    Attributes:
        name (str): name of the PVC volume
        namespace (str): namespace to create PVC

    """
    def __init__(self, name=None, namespace='default'):
        KubeVolume.__init__(self, name, namespace)
        self.kind = kinds.PVC
        self.api_version = defaults.OPENSHIFT_REST_CLIENT_API_VERSION
        self.service_pvc = self.dyn_client.resources.get(
            api_version=self.api_version,
            kind=self.kind
        )
        self.template_path = os.path.join(TEMPLATE_DIR, "pvc.yaml")

    def create_pvc(
        self,
        storageclass,
        accessmode="ReadWriteOnce",
        pvc_name_prefix="autotests-pvc",
        pvc_size=3
    ):
        """
        Creates PVC using data provided

        Args:
            storageclass (str): name of storageclass to create PVC
            accessmode (str): access mode for PVC
            pvc_name_prefix (str): prefix given to PVC name
            pvc_size (int): size of PVC in Gb

        Returns:
            str: name of the pvc created

        Examples:
            create_pvc(
                storageclass,
                accessmode="ReadWriteOnce",
                pvc_size=3
            )
            create_pvc(
                storageclass,
                accessmode="ReadWriteOnce ReadOnlyMany",
                pvc_size=5
            )

        """
        if self.name:
            pvc_name = self.name
        else:
            pvc_name = f"{pvc_name_prefix}-{get_random_str()}"
        pvc_size = f"{pvc_size}Gi"
        accessmode = accessmode.split()

        pvc_data = {}
        pvc_data['pvc_name'] = pvc_name
        pvc_data['cluster_namespace'] = self.namespace
        pvc_data['storageclass_namespace'] = storageclass
        pvc_data['storage'] = pvc_size
        pvc_data['access_mode'] = accessmode

        data = generate_yaml_from_jinja2_template_with_data(
            self.template_path,
            **pvc_data
        )
        self.service_pvc.create(body=data, namespace=self.namespace)

        return pvc_name

