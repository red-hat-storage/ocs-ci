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
CERT_MANAGER_NAMESPACE = "cert-manager-operator"
FUSION_NAMESPACE = "ibm-spectrum-fusion-ns"
SRE_BUILD_TEST_NAMESPACE = "openshift-build-test"
ROOK_CLUSTER_NAMESPACE = "openshift-storage"
OCS_MONITORING_NAMESPACE = "openshift-monitoring"
KUBECONFIG_LOCATION = "auth/kubeconfig"  # relative from cluster_dir
API_VERSION = "v1"
CEPHFILESYSTEM_NAME = "ocs-storagecluster-cephfilesystem"
RBD_PROVISIONER = f"{ROOK_CLUSTER_NAMESPACE}.rbd.csi.ceph.com"
RBD_NAME = "rbd"
RHCS_CLUSTER_NAME = "ceph"
CEPHFS_PROVISIONER = f"{ROOK_CLUSTER_NAMESPACE}.cephfs.csi.ceph.com"
CSI_PROVISIONERS = {CEPHFS_PROVISIONER, RBD_PROVISIONER}

TEMP_YAML = os.path.join(constants.TEMPLATE_DIR, "temp.yaml")

PROMETHEUS_ROUTE = "prometheus-k8s"

# Default device size in Gigs
DEVICE_SIZE = 100
DEVICE_SIZE_IBM_CLOUD_MANAGED = 512

OCS_OPERATOR_NAME = "ocs-operator"
ODF_OPERATOR_NAME = "odf-operator"
ROOK_CEPH_OPERATOR = "rook-ceph-operator"
ODF_PROMETHEUS_OPERATOR = "odf-prometheus-operator"
ODF_CLIENT_OPERATOR = "ocs-client-operator"
RECIPE_OPERATOR = "recipe"
HCI_CLIENT_ODF_OPERATOR_NAME = "ocs-client-operator"
NOOBAA_OPERATOR = "noobaa-operator"
MCG_OPERATOR = "mcg-operator"
ODF_CSI_ADDONS_OPERATOR = "odf-csi-addons-operator"
LOCAL_STORAGE_OPERATOR_NAME = "local-storage-operator"
CERT_MANAGER_OPERATOR_NAME = "cert-manager-operator"
FUSION_OPERATOR_NAME = "isf-operator"
FUSION_CATALOG_NAME = "isf-data-foundation-catalog"
LIVE_CONTENT_SOURCE = "redhat-operators"
OCS_CLIENT_OPERATOR_NAME = "ocs-client-operator"
CEPHCSI_OPERATOR = "cephcsi-operator"
ODF_DEPENDENCIES = "odf-dependencies"
MCO_OPERATOR_NAME = "odf-multicluster-orchestrator"
DR_HUB_OPERATOR_NAME = "odr-hub-operator"
DR_CLUSTER_OPERATOR_NAME = "odr-cluster-operator"

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
ELASTICSEARCH_DEV_IP = "10.0.144.103"
ELASTICSEARCE_PORT = 9200
ELASTICSEARCE_SCHEME = "http"

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
        "VAULT_TOKEN_NAME": "ceph-csi-kms-token",
        "VAULT_CACERT_FILE": "fullchain.pem",
        "VAULT_CLIENT_CERT_FILE": "cert.pem",
        "VAULT_CLIENT_KEY_FILE": "privkey.pem",
        "VAULT_BACKEND": "kv-v2",
    }
}
VAULT_TENANT_SA_CONNECTION_CONF = {
    "1-vault": {
        "encryptionKMSType": "vaulttenantsa",
        "vaultAddress": "https://vault.qe.rh-ocs.com:8200",
        "vaultAuthPath": "",
        "vaultAuthNamespace": "",
        "vaultNamespace": "",
        "vaultBackendPath": "kv-v2",
        "vaultCAFromSecret": "ocs-kms-ca-secret",
        "vaultClientCertFromSecret": "ocs-kms-client-cert",
        "vaultClientCertKeyFromSecret": "ocs-kms-client-key",
        "vaultBackend": "kv-v2",
    }
}

# External cluster username
EXTERNAL_CLUSTER_USER = "client.healthchecker"
EXTERNAL_CLUSTER_OBJECT_STORE_USER = "rgw-admin-ops-user"

# External cluster CSI users
ceph_csi_users = [
    "client.csi-cephfs-node",
    "client.csi-cephfs-provisioner",
    "client.csi-rbd-node",
    "client.csi-rbd-provisioner",
]

# Hpcs related defaults
#
# To be used for adding additional hpcs connections
# to csi-kms-connection-details resource
HPCS_CSI_CONNECTION_CONF = {
    "1-hpcs": {
        "KMS_PROVIDER": "ibmkeyprotect",
        "KMS_SERVICE_NAME": "1-hpcs",
        "IBM_KP_SERVICE_INSTANCE_ID": "",
        "IBM_KP_SECRET_NAME": "ibm-kp-kms-test-secret",
        "IBM_KP_BASE_URL": "",
        "IBM_KP_TOKEN_URL": "https://iam.cloud.ibm.com/oidc/token",
    }
}

# KMIP csi-kms-connection-details
KMIP_CSI_CONNECTION_CONF = {
    "1-kmip": {
        "KMS_PROVIDER": "kmip",
        "KMS_SERVICE_NAME": "1-kmip",
        "KMIP_ENDPOINT": "",
        "KMIP_SECRET_NAME": "thales-kmip-csi-secret",
        "TLS_SERVER_NAME": "kmip_all.ciphertrustmanager.local",
    }
}

# Must-gather:
MUST_GATHER_UPSTREAM_IMAGE = "quay.io/ocs-dev/ocs-must-gather"
MUST_GATHER_UPSTREAM_TAG = "latest"
MUST_GATHER_TIMEOUT = 3600

# CrushDeviceClass
CRUSH_DEVICE_CLASS = "ssd"


# IBM Cloud
IBM_CLOUD_LOAD_BALANCER_QUOTA = 100
IBM_CLOUD_REGIONS = {"us-south", "us-east"}

# HyperShift defaults
HYPERSHIFT_NODEPOOL_REPLICAS_DEFAULT = 2
HYPERSHIFT_MEMORY_DEFAULT = "12Gi"
HYPERSHIFT_CPU_CORES_DEFAULT = 6
HOSTED_ODF_REGISTRY_DEFAULT = "quay.io/rhceph-dev/ocs-registry"

# Custom Ingress SSL certificate, key and CA certificate related defaults
INGRESS_SSL_CERT = os.path.join(constants.DATA_DIR, "ingress-cert.crt")
INGRESS_SSL_KEY = os.path.join(constants.DATA_DIR, "ingress-cert.key")
INGRESS_SSL_CA_CERT = os.path.join(constants.DATA_DIR, "ca.crt")
API_SSL_CERT = os.path.join(constants.DATA_DIR, "api-cert.crt")
API_SSL_KEY = os.path.join(constants.DATA_DIR, "api-cert.key")
API_SSL_CA_CERT = os.path.join(constants.DATA_DIR, "ca.crt")
