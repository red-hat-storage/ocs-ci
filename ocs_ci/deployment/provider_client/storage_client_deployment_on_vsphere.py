"""
This module provides installation of ODF in provider mode and storage-client creation
on the hosting cluster.
"""
# import json
# import logging
# import os
# import tempfile
# import time
# import base64
# import yaml


class Storage_Client_Deployment(object):
    """
    1. set control nodes as scheduleable
    2. allow ODF to be deployed on all nodes
    3. allow hosting cluster domain to be usable by hosted clusters
    4. Enable nested virtualization on vSphere nodes
    5. Install ODF
    6. Install LSO, create LocalVolumeDiscovery and LocalVolumeSet
    7. Disable ROOK_CSI_ENABLE_CEPHFS and ROOK_CSI_ENABLE_RBD
    8. Create storage profile


    """

    # set control nodes as scheduleable
    # master_node_schedulable = '{"spec": {"nfs":{"enable": true}}}'
    # rook_csi_config_enable = '{"data":{"ROOK_CSI_ENABLE_NFS": "true"}}'

    # # Enable nfs feature for storage-cluster using patch command
    # assert storage_cluster_obj.patch(
    #     resource_name="ocs-storagecluster",
    #     params=nfs_spec_enable,
    #     format_type="merge",
    # ), "storagecluster.ocs.openshift.io/ocs-storagecluster not patched"
