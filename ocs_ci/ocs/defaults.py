"""
Defaults module. All the defaults used by OSCCI framework should
reside in this module.
PYTEST_DONT_REWRITE - avoid pytest to rewrite, keep this msg here please!
"""
import os

from ocs_ci.ocs import constants

STORAGE_API_VERSION = "storage.k8s.io/v1"
ROOK_API_VERSION = "ceph.rook.io/v1"
OCP_API_VERSION = "project.openshift.io/v1"
OPENSHIFT_REST_CLIENT_API_VERSION = "v1"

# Be aware that variables defined above and below are not used anywhere in the
# config files and their sections when we rendering config!

INSTALLER_VERSION = "4.1.4"
CLIENT_VERSION = INSTALLER_VERSION
SRE_BUILD_TEST_NAMESPACE = "openshift-build-test"
ROOK_CLUSTER_NAMESPACE = "openshift-storage"
OCS_MONITORING_NAMESPACE = "openshift-monitoring"
KUBECONFIG_LOCATION = "auth/kubeconfig"  # relative from cluster_dir
API_VERSION = "v1"
CEPHFILESYSTEM_NAME = "ocs-storagecluster-cephfilesystem"
RBD_PROVISIONER = f"{ROOK_CLUSTER_NAMESPACE}.rbd.csi.ceph.com"
CEPHFS_PROVISIONER = f"{ROOK_CLUSTER_NAMESPACE}.cephfs.csi.ceph.com"
CSI_PROVISIONERS = {CEPHFS_PROVISIONER, RBD_PROVISIONER}

TEMP_YAML = os.path.join(constants.TEMPLATE_DIR, "temp.yaml")

PROMETHEUS_ROUTE = "prometheus-k8s"

# Default device size in Gigs
DEVICE_SIZE = 100

OCS_OPERATOR_NAME = "ocs-operator"
ODF_OPERATOR_NAME = "odf-operator"
LOCAL_STORAGE_OPERATOR_NAME = "local-storage-operator"
LIVE_CONTENT_SOURCE = "redhat-operators"

# Noobaa S3 bucket website configurations
website_config = {
    "ErrorDocument": {"Key": "error.html"},
    "IndexDocument": {"Suffix": "index.html"},
}
index = "<html><body><h1>My Static Website on S3</h1></body></html>"
error = "<html><body><h1>Oh. Something bad happened!</h1></body></html>"

# pyipmi
IPMI_INTERFACE_TYPE = "lanplus"
IPMI_RMCP_PORT = 623
IPMI_IPMB_ADDRESS = 0x20

# Background load FIO pod name
BG_LOAD_NAMESPACE = "bg-fio-load"

# pool related data
MAX_BYTES_IN_POOL_AFTER_DATA_DELETE = 250000

# Elastic search parameters
ELASTICSEARCH_DEV_IP = "10.0.144.152"
ELASTICSEARCE_PORT = 9200

# Local storage namespace
LOCAL_STORAGE_NAMESPACE = "openshift-local-storage"

# Vault related defaults
VAULT_DEFAULT_CA_CERT = "ocs-kms-ca-secret"
VAULT_DEFAULT_CLIENT_CERT = "ocs-kms-client-cert"
VAULT_DEFAULT_CLIENT_KEY = "ocs-kms-client-key"
VAULT_DEFAULT_BACKEND_VERSION = "v1"
# To be used for adding additional vault connections
# to csi-kms-connection-details resource
VAULT_CSI_CONNECTION_CONF = {
    "1-vault": {
        "KMS_PROVIDER": "vaulttokens",
        "KMS_SERVICE_NAME": "vault",
        "VAULT_ADDR": "https://vault.qe.rh-ocs.com:8200",
        "VAULT_BACKEND_PATH": "kv-v2",
        "VAULT_CACERT": "ocs-kms-ca-secret",
        "VAULT_TLS_SERVER_NAME": "",
        "VAULT_NAMESPACE": "",
        "VAULT_TOKEN_NAME": "ocs-kms-token",
        "VAULT_CACERT_FILE": "fullchain.pem",
        "VAULT_CLIENT_CERT_FILE": "cert.pem",
        "VAULT_CLIENT_KEY_FILE": "privkey.pem",
        "VAULT_BACKEND": "kv-v2",
    }
}
