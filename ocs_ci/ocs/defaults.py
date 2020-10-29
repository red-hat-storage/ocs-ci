"""
Defaults module. All the defaults used by OSCCI framework should
reside in this module.
PYTEST_DONT_REWRITE - avoid pytest to rewrite, keep this msg here please!
"""
import os

from ocs_ci.ocs import constants

STORAGE_API_VERSION = 'storage.k8s.io/v1'
ROOK_API_VERSION = 'ceph.rook.io/v1'
OCP_API_VERSION = 'project.openshift.io/v1'
OPENSHIFT_REST_CLIENT_API_VERSION = 'v1'

# Be aware that variables defined above and below are not used anywhere in the
# config files and their sections when we rendering config!

INSTALLER_VERSION = '4.1.4'
CLIENT_VERSION = INSTALLER_VERSION
ROOK_CLUSTER_NAMESPACE = 'openshift-storage'
OCS_MONITORING_NAMESPACE = 'openshift-monitoring'
KUBECONFIG_LOCATION = 'auth/kubeconfig'  # relative from cluster_dir
API_VERSION = "v1"
CEPHFILESYSTEM_NAME = 'ocs-storagecluster-cephfilesystem'
RBD_PROVISIONER = f'{ROOK_CLUSTER_NAMESPACE}.rbd.csi.ceph.com'
CEPHFS_PROVISIONER = f'{ROOK_CLUSTER_NAMESPACE}.cephfs.csi.ceph.com'
CSI_PROVISIONERS = {CEPHFS_PROVISIONER, RBD_PROVISIONER}

TEMP_YAML = os.path.join(constants.TEMPLATE_DIR, "temp.yaml")

PROMETHEUS_ROUTE = 'prometheus-k8s'

# Default device size in Gigs
DEVICE_SIZE = 100

OCS_OPERATOR_NAME = "ocs-operator"
LOCAL_STORAGE_OPERATOR_NAME = "local-storage-operator"
LIVE_CONTENT_SOURCE = "redhat-operators"

# Noobaa S3 bucket website configurations
website_config = {
    'ErrorDocument': {'Key': 'error.html'},
    'IndexDocument': {'Suffix': 'index.html'},
}
index = "<html><body><h1>My Static Website on S3</h1></body></html>"
error = "<html><body><h1>Oh. Something bad happened!</h1></body></html>"

# pyipmi
IPMI_INTERFACE_TYPE = "lanplus"
IPMI_RMCP_PORT = 623
IPMI_IPMB_ADDRESS = 0x20

# Background load FIO pod name
BG_LOAD_NAMESPACE = "bg-fio-load"
