"""
Defaults module. All the defaults used by OSCCI framework should
reside in this module.
PYTEST_DONT_REWRITE - avoid pytest to rewrite, keep this msg here please!
"""
import os

from ocs import constants

STORAGE_API_VERSION = 'storage.k8s.io/v1'
ROOK_API_VERSION = 'ceph.rook.io/v1'
OCP_API_VERSION = 'project.openshift.io/v1'
OPENSHIFT_REST_CLIENT_API_VERSION = 'v1'

# Be aware that variables defined above and below are not used anywhere in the
# config files and their sections when we rendering config!

INSTALLER_VERSION = '4.1.2'
CLIENT_VERSION = INSTALLER_VERSION
ROOK_CLUSTER_NAMESPACE = 'openshift-storage'
KUBECONFIG_LOCATION = 'auth/kubeconfig'  # relative from cluster_dir
API_VERSION = "v1"

TEMP_YAML = os.path.join(constants.TEMPLATE_DIR, "temp.yaml")

TOOL_POD_DICT = os.path.join(
    constants.TEMPLATE_DEPLOYMENT_DIR, "toolbox_pod.yaml"
)

CEPHFILESYSTEM_YAML = os.path.join(
    constants.TEMPLATE_CSI_FS_DIR, "CephFileSystem.yaml"
)

CEPHBLOCKPOOL_YAML = os.path.join(
    constants.TEMPLATE_DEPLOYMENT_DIR, "cephblockpool.yaml"
)

CSI_RBD_STORAGECLASS_DICT = os.path.join(
    constants.TEMPLATE_CSI_RBD_DIR, "storageclass.yaml"
)

CSI_CEPHFS_STORAGECLASS_DICT = os.path.join(
    constants.TEMPLATE_CSI_FS_DIR, "storageclass.yaml"
)

CSI_PVC_DICT = os.path.join(
    constants.TEMPLATE_PV_PVC_DIR, "PersistentVolumeClaim.yaml"
)

CSI_RBD_POD_DICT = os.path.join(
    constants.TEMPLATE_CSI_RBD_DIR, "pod.yaml"
)

CSI_RBD_SECRET = os.path.join(
    constants.TEMPLATE_CSI_RBD_DIR, "secret.yaml"
)

CSI_CEPHFS_SECRET = os.path.join(
    constants.TEMPLATE_CSI_FS_DIR, "secret.yaml"
)

CSI_CEPHFS_PVC = os.path.join(
    constants.TEMPLATE_CSI_FS_DIR, "pvc.yaml"
)


CSI_RBD_PVC = os.path.join(
    constants.TEMPLATE_CSI_RBD_DIR, "pvc.yaml"
)
