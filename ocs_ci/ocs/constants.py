"""
Constants module.

This module contains any values that are widely used across the framework,
utilities, or tests that will predominantly remain unchanged.

In the event values here have to be changed it should be under careful review
and with consideration of the entire project.

"""
import os

# Directories
TOP_DIR = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
CONF_DIR = os.path.join(TOP_DIR, 'conf')
OCP_VERSION_CONF_DIR = os.path.join(CONF_DIR, "ocp_version")
TEMPLATE_DIR = os.path.join(TOP_DIR, "ocs_ci", "templates")
TEMPLATE_CLEANUP_DIR = os.path.join(TEMPLATE_DIR, "cleanup")
REPO_DIR = os.path.join(TOP_DIR, "ocs_ci", "repos")
EXTERNAL_DIR = os.path.join(TOP_DIR, "external")
TEMPLATE_DEPLOYMENT_DIR = os.path.join(TEMPLATE_DIR, "ocs-deployment")
TEMPLATE_CSI_DIR = os.path.join(TEMPLATE_DIR, "CSI")
TEMPLATE_CSI_RBD_DIR = os.path.join(TEMPLATE_CSI_DIR, "rbd")
TEMPLATE_CSI_FS_DIR = os.path.join(TEMPLATE_CSI_DIR, "cephfs")
TEMPLATE_PV_PVC_DIR = os.path.join(TEMPLATE_DIR, "pv_pvc")
TEMPLATE_APP_POD_DIR = os.path.join(TEMPLATE_DIR, "app-pods")
TEMPLATE_WORKLOAD_DIR = os.path.join(TEMPLATE_DIR, "workloads")
TEMPLATE_FIO_DIR = os.path.join(TEMPLATE_WORKLOAD_DIR, "fio")
TEMPLATE_SMALLFILE_DIR = os.path.join(TEMPLATE_WORKLOAD_DIR, "smallfile")
TEMPLATE_PGSQL_DIR = os.path.join(TEMPLATE_WORKLOAD_DIR, "pgsql")
TEMPLATE_VDBENCH_DIR = os.path.join(TEMPLATE_WORKLOAD_DIR, "vdbench")
TEMPLATE_PGSQL_SERVER_DIR = os.path.join(TEMPLATE_PGSQL_DIR, "server")
TEMPLATE_COUCHBASE_DIR = os.path.join(TEMPLATE_WORKLOAD_DIR, "couchbase")
TEMPLATE_COUCHBASE_SERVER_DIR = os.path.join(TEMPLATE_COUCHBASE_DIR, "server")
TEMPLATE_PILLOWFIGHT_DIR = os.path.join(TEMPLATE_COUCHBASE_SERVER_DIR, "pillowfight")
TEMPLATE_MCG_DIR = os.path.join(TEMPLATE_DIR, "mcg")
TEMPLATE_OPENSHIFT_INFRA_DIR = os.path.join(
    TEMPLATE_DIR, "openshift-infra/"
)
TEMPLATE_CONFIGURE_PVC_MONITORING_POD = os.path.join(
    TEMPLATE_OPENSHIFT_INFRA_DIR, "monitoring/"
)
TEMPLATE_DEPLOYMENT_LOGGING = os.path.join(
    TEMPLATE_OPENSHIFT_INFRA_DIR, "logging-deployment"
)
TEMPLATE_DEPLOYMENT_EO = os.path.join(
    TEMPLATE_DEPLOYMENT_LOGGING, "elasticsearch_operator"
)
TEMPLATE_DEPLOYMENT_CLO = os.path.join(
    TEMPLATE_DEPLOYMENT_LOGGING, "clusterlogging_operator"
)
DATA_DIR = os.path.join(TOP_DIR, 'data')
ROOK_REPO_DIR = os.path.join(DATA_DIR, 'rook')
ROOK_EXAMPLES_DIR = os.path.join(
    ROOK_REPO_DIR, "cluster", "examples", "kubernetes", "ceph"
)
ROOK_CSI_RBD_DIR = os.path.join(ROOK_EXAMPLES_DIR, "csi", "rbd")
ROOK_CSI_CEPHFS_DIR = os.path.join(ROOK_EXAMPLES_DIR, "csi", "cephfs")
CLEANUP_YAML = "cleanup.yaml.j2"


# Statuses
STATUS_PENDING = 'Pending'
STATUS_CONTAINER_CREATING = 'ContainerCreating'
STATUS_AVAILABLE = 'Available'
STATUS_RUNNING = 'Running'
STATUS_TERMINATING = 'Terminating'
STATUS_BOUND = 'Bound'
STATUS_RELEASED = 'Released'
STATUS_COMPLETED = 'Completed'

# NooBaa statuses
BS_AUTH_FAILED = 'AUTH_FAILED'
BS_OPTIMAL = 'OPTIMAL'

# Resources / Kinds
CEPHFILESYSTEM = "CephFileSystem"
CEPHBLOCKPOOL = "CephBlockPool"
DEPLOYMENT = "Deployment"
JOB = "Job"
STORAGECLASS = "StorageClass"
PV = "PersistentVolume"
PVC = "PersistentVolumeClaim"
POD = "Pod"
ROUTE = "Route"
NODE = "Node"
DEPLOYMENTCONFIG = "deploymentconfig"
CONFIG = "Config"
MACHINESETS = 'machinesets'
STORAGECLUSTER = 'storagecluster'
CLUSTER_OPERATOR = 'ClusterOperator'
MONITORING = 'monitoring'
CLUSTER_SERVICE_VERSION = 'csv'
JOB = 'job'

# Provisioners
AWS_EFS_PROVISIONER = "openshift.org/aws-efs"
ROLE = 'Role'
ROLEBINDING = "Rolebinding"
SUBSCRIPTION = "Subscription"
NAMESPACES = "Namespaces"
CLUSTER_LOGGING = "ClusterLogging"
OPERATOR_GROUP = "OperatorGroup"
SERVICE_ACCOUNT = "Serviceaccount"
SCC = "SecurityContextConstraints"
PRIVILEGED = "privileged"
CLUSTER_SERVICE_VERSION = 'csv'

# Other
SECRET = "Secret"
NAMESPACE = 'Namespace'
IGNORE_SC_GP2 = "gp2"
IGNORE_SC_FLEX = "rook-ceph-block"
TEST_FILES_BUCKET = "ocsci-test-files"
ROOK_REPOSITORY = "https://github.com/rook/rook.git"
OPENSHIFT_MACHINE_API_NAMESPACE = "openshift-machine-api"
OPENSHIFT_LOGGING_NAMESPACE = "openshift-logging"
OPENSHIFT_OPERATORS_REDHAT_NAMESPACE = "openshift-operators-redhat"
OPENSHIFT_IMAGE_REGISTRY_NAMESPACE = "openshift-image-registry"
OPENSHIFT_IMAGE_REGISTRY_DEPLOYMENT = "image-registry"
OPENSHIFT_IMAGE_SELECTOR = "docker-registry=default"
OPENSHIFT_INGRESS_NAMESPACE = "openshift-ingress"
MASTER_MACHINE = "master"
WORKER_MACHINE = "worker"
MOUNT_POINT = '/var/lib/www/html'
OCP_QE_MISC_REPO = (
    "http://git.host.prod.eng.bos.redhat.com/git/openshift-misc.git"
)
CRITICAL_ERRORS = [
    "core dumped", "oom_reaper"
]
must_gather_pod_label = "must-gather"

OCS_WORKLOADS = "https://github.com/red-hat-storage/ocs-workloads"

UPI_INSTALL_SCRIPT = "upi_on_aws-install.sh"

DEFAULT_CLUSTERNAME = 'ocs-storagecluster'
DEFAULT_BLOCKPOOL = f'{DEFAULT_CLUSTERNAME}-cephblockpool'
METADATA_POOL = f'{DEFAULT_CLUSTERNAME}-cephfilesystem-metadata'
DATA_POOL = f'{DEFAULT_CLUSTERNAME}-cephfilesystem-data0'
DEFAULT_ROUTE_CRT = "router-certs-default"
DEFAULT_NAMESPACE = "default"
IMAGE_REGISTRY_RESOURCE_NAME = "cluster"
IMAGE_REGISTRY_CONFIG = "configs.imageregistry.operator.openshift.io/cluster"

# Default StorageClass
DEFAULT_STORAGECLASS_CEPHFS = f'{DEFAULT_CLUSTERNAME}-cephfs'
DEFAULT_STORAGECLASS_RBD = f'{DEFAULT_CLUSTERNAME}-ceph-rbd'

# encoded value of 'admin'
ADMIN_USER = 'admin'
GB = 1024 ** 3
GB2KB = 1024 ** 2

# Reclaim Policy
RECLAIM_POLICY_RETAIN = 'Retain'
RECLAIM_POLICY_DELETE = 'Delete'

# Access Mode
ACCESS_MODE_RWO = 'ReadWriteOnce'
ACCESS_MODE_ROX = 'ReadOnlyMany'
ACCESS_MODE_RWX = 'ReadWriteMany'

# Pod label
MON_APP_LABEL = "app=rook-ceph-mon"
MDS_APP_LABEL = "app=rook-ceph-mds"
TOOL_APP_LABEL = "app=rook-ceph-tools"
MGR_APP_LABEL = "app=rook-ceph-mgr"
OSD_APP_LABEL = "app=rook-ceph-osd"
OSD_PREPARE_APP_LABEL = "app=rook-ceph-osd-prepare"
RGW_APP_LABEL = "app=rook-ceph-rgw"
OPERATOR_LABEL = "app=rook-ceph-operator"
CSI_CEPHFSPLUGIN_PROVISIONER_LABEL = "app=csi-cephfsplugin-provisioner"
CSI_RBDPLUGIN_PROVISIONER_LABEL = "app=csi-rbdplugin-provisioner"
CSI_CEPHFSPLUGIN_LABEL = "app=csi-cephfsplugin"
CSI_RBDPLUGIN_LABEL = "app=csi-rbdplugin"
OCS_OPERATOR_LABEL = "name=ocs-operator"
LOCAL_STORAGE_OPERATOR_LABEL = "name=local-storage-operator"
NOOBAA_APP_LABEL = "app=noobaa"
NOOBAA_CORE_POD_LABEL = "noobaa-core=noobaa"
NOOBAA_OPERATOR_POD_LABEL = "noobaa-operator=deployment"
DEFAULT_DEVICESET_PVC_NAME = "ocs-deviceset"
DEFAULT_MON_PVC_NAME = "rook-ceph-mon"
OSD_PVC_GENERIC_LABEL = "ceph.rook.io/DeviceSet"
CEPH_ROOK_IO_PVC_LABEL = 'ceph.rook.io/pvc'


# YAML paths
TOOL_POD_YAML = os.path.join(
    TEMPLATE_DEPLOYMENT_DIR, "toolbox_pod.yaml"
)

CEPHFILESYSTEM_YAML = os.path.join(
    TEMPLATE_CSI_FS_DIR, "CephFileSystem.yaml"
)

CEPHBLOCKPOOL_YAML = os.path.join(
    TEMPLATE_DEPLOYMENT_DIR, "cephblockpool.yaml"
)

CSI_RBD_STORAGECLASS_YAML = os.path.join(
    TEMPLATE_CSI_RBD_DIR, "storageclass.yaml"
)

ROOK_CSI_RBD_STORAGECLASS_YAML = os.path.join(
    ROOK_CSI_RBD_DIR, "storageclass.yaml"
)

CSI_CEPHFS_STORAGECLASS_YAML = os.path.join(
    TEMPLATE_CSI_FS_DIR, "storageclass.yaml"
)

ROOK_CSI_CEPHFS_STORAGECLASS_YAML = os.path.join(
    ROOK_CSI_CEPHFS_DIR, "storageclass.yaml"
)

CSI_PVC_YAML = os.path.join(
    TEMPLATE_PV_PVC_DIR, "PersistentVolumeClaim.yaml"
)

MCG_OBC_YAML = os.path.join(
    TEMPLATE_MCG_DIR, "ObjectBucketClaim.yaml"
)

MCG_AWS_CREDS_YAML = os.path.join(
    TEMPLATE_MCG_DIR, "AwsCreds.yaml"
)

MCG_BACKINGSTORE_SECRET_YAML = os.path.join(
    TEMPLATE_MCG_DIR, "BackingStoreSecret.yaml"
)

MCG_BACKINGSTORE_YAML = os.path.join(
    TEMPLATE_MCG_DIR, "BackingStore.yaml"
)

PV_BACKINGSTORE_YAML = os.path.join(
    TEMPLATE_MCG_DIR, "PVBackingStore.yaml"
)

MCG_BUCKETCLASS_YAML = os.path.join(
    TEMPLATE_MCG_DIR, "BucketClass.yaml"
)

CSI_RBD_POD_YAML = os.path.join(
    TEMPLATE_CSI_RBD_DIR, "pod.yaml"
)

CSI_RBD_RAW_BLOCK_POD_YAML = os.path.join(
    TEMPLATE_APP_POD_DIR, "raw_block_pod.yaml"
)

CSI_CEPHFS_POD_YAML = os.path.join(
    TEMPLATE_CSI_FS_DIR, "pod.yaml"
)
CSI_RBD_SECRET_YAML = os.path.join(
    TEMPLATE_CSI_RBD_DIR, "secret.yaml"
)

CSI_CEPHFS_SECRET_YAML = os.path.join(
    TEMPLATE_CSI_FS_DIR, "secret.yaml"
)

CSI_CEPHFS_PVC_YAML = os.path.join(
    TEMPLATE_CSI_FS_DIR, "pvc.yaml"
)

CSI_RBD_PVC_YAML = os.path.join(
    TEMPLATE_CSI_RBD_DIR, "pvc.yaml"
)
CONFIGURE_PVC_ON_MONITORING_POD = os.path.join(
    TEMPLATE_CONFIGURE_PVC_MONITORING_POD, "configuring_pvc.yaml"
)

PGSQL_SERVICE_YAML = os.path.join(
    TEMPLATE_PGSQL_SERVER_DIR, "Service.yaml"
)

PGSQL_CONFIGMAP_YAML = os.path.join(
    TEMPLATE_PGSQL_SERVER_DIR, "ConfigMap.yaml"
)

PGSQL_STATEFULSET_YAML = os.path.join(
    TEMPLATE_PGSQL_SERVER_DIR, "StatefulSet.yaml"
)

PGSQL_BENCHMARK_YAML = os.path.join(
    TEMPLATE_PGSQL_DIR, "PGSQL_Benchmark.yaml"
)

SMALLFILE_BENCHMARK_YAML = os.path.join(
    TEMPLATE_SMALLFILE_DIR, "SmallFile.yaml"
)

VDBENCH_BENCHMARK_YAML = os.path.join(
    TEMPLATE_VDBENCH_DIR, "VDBench.yaml"
)

COUCHBASE_ADMISSION_SERVICE_ACCOUNT_YAML = os.path.join(
    TEMPLATE_COUCHBASE_SERVER_DIR, "admissionServiceAccount.yaml"
)

COUCHBASE_ADMISSION_CLUSTER_ROLE_YAML = os.path.join(
    TEMPLATE_COUCHBASE_SERVER_DIR, "admissionClusterRole.yaml"
)

COUCHBASE_ADMISSION_CLUSTER_ROLE_BINDING_YAML = os.path.join(
    TEMPLATE_COUCHBASE_SERVER_DIR, "admissionClusterRoleBinding.yaml"
)

COUCHBASE_ADMISSION_SECRET_YAML = os.path.join(
    TEMPLATE_COUCHBASE_SERVER_DIR, "admissionSecret.yaml"
)

COUCHBASE_ADMISSION_DEPLOYMENT_YAML = os.path.join(
    TEMPLATE_COUCHBASE_SERVER_DIR, "admissionDeployment.yaml"
)

COUCHBASE_ADMISSION_SERVICE_YAML = os.path.join(
    TEMPLATE_COUCHBASE_SERVER_DIR, "admissionService.yaml"
)

COUCHBASE_MUTATING_WEBHOOK_YAML = os.path.join(
    TEMPLATE_COUCHBASE_SERVER_DIR, "MutatingWebhookConfiguration.yaml"
)

COUCHBASE_VALIDATING_WEBHOOK_YAML = os.path.join(
    TEMPLATE_COUCHBASE_SERVER_DIR, "ValidatingWebhookConfiguration.yaml"
)

COUCHBASE_CRD_YAML = os.path.join(
    TEMPLATE_COUCHBASE_SERVER_DIR, "couchbaseCrd.yaml"
)

COUCHBASE_OPERATOR_ROLE = os.path.join(
    TEMPLATE_COUCHBASE_SERVER_DIR, "operator-role.yaml"
)

COUCHBASE_OPERATOR_DEPLOY = os.path.join(
    TEMPLATE_COUCHBASE_SERVER_DIR, "operator-deployment.yaml"
)

COUCHBASE_WORKER_SECRET = os.path.join(
    TEMPLATE_COUCHBASE_SERVER_DIR, "couchbase-worker-secret.yaml"
)

COUCHBASE_WORKER_EXAMPLE = os.path.join(
    TEMPLATE_COUCHBASE_SERVER_DIR, "couchbase-worker-example.yaml"
)

NGINX_POD_YAML = os.path.join(
    TEMPLATE_APP_POD_DIR, "nginx.yaml"
)

AWSCLI_POD_YAML = os.path.join(
    TEMPLATE_APP_POD_DIR, "awscli.yaml"
)

SERVICE_ACCOUNT_YAML = os.path.join(
    TEMPLATE_DEPLOYMENT_DIR, "service_account.yaml"
)

FEDORA_DC_YAML = os.path.join(
    TEMPLATE_APP_POD_DIR, "fedora_dc.yaml"
)

RHEL_7_7_POD_YAML = os.path.join(
    TEMPLATE_APP_POD_DIR, "rhel-7_7.yaml"
)

# Openshift-logging elasticsearch operator deployment yamls
EO_NAMESPACE_YAML = os.path.join(
    TEMPLATE_DEPLOYMENT_EO, "eo-project.yaml"
)

EO_OG_YAML = os.path.join(
    TEMPLATE_DEPLOYMENT_EO, "eo-og.yaml"
)
EO_RBAC_YAML = os.path.join(
    TEMPLATE_DEPLOYMENT_EO, "eo-rbac.yaml"
)
EO_SUB_YAML = os.path.join(
    TEMPLATE_DEPLOYMENT_EO, "eo-sub.yaml"
)

OLM_YAML = os.path.join(
    TEMPLATE_DEPLOYMENT_DIR, "deploy-with-olm.yaml"
)

CATALOG_SOURCE_YAML = os.path.join(
    TEMPLATE_DEPLOYMENT_DIR, "catalog-source.yaml"
)

SUBSCRIPTION_YAML = os.path.join(
    TEMPLATE_DEPLOYMENT_DIR, "subscription.yaml"
)

STORAGE_CLUSTER_YAML = os.path.join(
    TEMPLATE_DEPLOYMENT_DIR, "storage-cluster.yaml"
)

OPERATOR_SOURCE_SECRET_YAML = os.path.join(
    TEMPLATE_DEPLOYMENT_DIR, "operator-source-secret.yaml"
)

OPERATOR_SOURCE_YAML = os.path.join(
    TEMPLATE_DEPLOYMENT_DIR, "operator-source.yaml"
)

OPERATOR_SOURCE_NAME = "ocs-operatorsource"

OPERATOR_SOURCE_SECRET_NAME = "ocs-operatorsource-secret"

# Openshift-logging clusterlogging operator deployment yamls
CL_NAMESPACE_YAML = os.path.join(
    TEMPLATE_DEPLOYMENT_CLO, "cl-namespace.yaml"
)
CL_OG_YAML = os.path.join(
    TEMPLATE_DEPLOYMENT_CLO, "cl-og.yaml"
)
CL_SUB_YAML = os.path.join(
    TEMPLATE_DEPLOYMENT_CLO, "cl-sub.yaml"
)
CL_INSTANCE_YAML = os.path.join(
    TEMPLATE_DEPLOYMENT_CLO, "instance.yaml"
)

# Workload-io yamls
FIO_IO_PARAMS_YAML = os.path.join(
    TEMPLATE_FIO_DIR, "workload_io.yaml"
)
FIO_IO_RW_PARAMS_YAML = os.path.join(
    TEMPLATE_FIO_DIR, "workload_io_rw.yaml"
)

# Openshift infra yamls:
RSYNC_POD_YAML = os.path.join(
    TEMPLATE_OPENSHIFT_INFRA_DIR, "rsync-pod.yaml"
)
MACHINESET_YAML = os.path.join(
    TEMPLATE_OPENSHIFT_INFRA_DIR, "machine-set.yaml"
)

ANSIBLE_INVENTORY_YAML = os.path.join(
    "ocp-deployment", "inventory.yaml.j2"
)
# constants
RBD_INTERFACE = 'rbd'
CEPHFS_INTERFACE = 'cephfs'
RAW_BLOCK_DEVICE = '/dev/rbdblock'

# Constant values for IOPS and Throughput is set
# considering gp2 interface, EBS volumes and EC2 instances
IOPS_FOR_1TiB_OSD = 3000
THROUGHPUT_LIMIT_OSD = 250

# EC2 instance statuses
INSTANCE_PENDING = 0
INSTANCE_STOPPING = 64
INSTANCE_STOPPED = 80
INSTANCE_RUNNING = 16
INSTANCE_SHUTTING_DOWN = 32
INSTANCE_TERMINATED = 48

# vSphere VM power statuses
VM_POWERED_OFF = 'poweredOff'
VM_POWERED_ON = 'poweredOn'

# Node statuses
NODE_READY = 'Ready'
NODE_NOT_READY = 'NotReady'
NODE_READY_SCHEDULING_DISABLED = 'Ready,SchedulingDisabled'

# Volume modes
VOLUME_MODE_BLOCK = 'Block'
VOLUME_MODE_FILESYSTEM = 'Filesystem'

# Alert labels
ALERT_CLUSTERERRORSTATE = 'CephClusterErrorState'
ALERT_CLUSTERWARNINGSTATE = 'CephClusterWarningState'
ALERT_DATARECOVERYTAKINGTOOLONG = 'CephDataRecoveryTakingTooLong'
ALERT_MGRISABSENT = 'CephMgrIsAbsent'
ALERT_MONQUORUMATRISK = 'CephMonQuorumAtRisk'
ALERT_OSDDISKNOTRESPONDING = 'CephOSDDiskNotResponding'
ALERT_PGREPAIRTAKINGTOOLONG = 'CephPGRepairTakingTooLong'
ALERT_BUCKETREACHINGQUOTASTATE = 'NooBaaBucketReachingQuotaState'
ALERT_BUCKETERRORSTATE = 'NooBaaBucketErrorState'
ALERT_BUCKETEXCEEDINGQUOTASTATE = 'NooBaaBucketExceedingQuotaState'
ALERT_CLUSTERNEARFULL = 'CephClusterNearFull'
ALERT_CLUSTERCRITICALLYFULL = 'CephClusterCriticallyFull'

# OCS Deployment related constants
OPERATOR_NODE_LABEL = "cluster.ocs.openshift.io/openshift-storage=''"
OPERATOR_NODE_TAINT = "node.ocs.openshift.io/storage=true:NoSchedule"
OPERATOR_CATALOG_SOURCE_NAME = "ocs-catalogsource"
MARKETPLACE_NAMESPACE = "openshift-marketplace"
MONITORING_NAMESPACE = "openshift-monitoring"
OPERATOR_INTERNAL_SELECTOR = "ocs-operator-internal=true"
OPERATOR_CS_QUAY_API_QUERY = (
    'https://quay.io/api/v1/repository/rhceph-dev/ocs-olm-operator/'
    'tag/?onlyActiveTags=true&limit={tag_limit}'
)

# OCP related constants
OPENSHIFT_UPGRADE_INFO_API = (
    "https://api.openshift.com/api/upgrades_info/v1/graph?channel={channel}"
)

# VDbench benchmark related constants
APP_NODE_LABEL = 'app-node'
VDBENCH_NODE_LABEL = 'vdbench'
VDBENCH_RESULTS_FILE = '/tmp/Results.tar.gz'

# Platforms
AWS_PLATFORM = 'aws'
VSPHERE_PLATFORM = 'vsphere'
CLOUD_PLATFORMS = (AWS_PLATFORM)

# Default SC based on platforms
DEFAULT_SC_AWS = "gp2"
DEFAULT_SC_VSPHERE = "thin"

# ignition files
BOOTSTRAP_IGN = "bootstrap.ign"
MASTER_IGN = "master.ign"
WORKER_IGN = "worker.ign"

# vSphere related constants
VSPHERE_INSTALLER_REPO = "https://github.com/openshift/installer.git"
VSPHERE_SCALEUP_REPO = "https://code.engineering.redhat.com/gerrit/openshift-misc"
VSPHERE_DIR = os.path.join(EXTERNAL_DIR, "installer/upi/vsphere/")
INSTALLER_IGNITION = os.path.join(VSPHERE_DIR, "machine/ignition.tf")
INSTALLER_ROUTE53 = os.path.join(VSPHERE_DIR, "route53/main.tf")
INSTALLER_MACHINE_CONF = os.path.join(VSPHERE_DIR, "machine/main.tf")
VSPHERE_CONFIG_PATH = os.path.join(TOP_DIR, "conf/ocsci/vsphere_upi_vars.yaml")
VSPHERE_MAIN = os.path.join(VSPHERE_DIR, "main.tf")
TERRAFORM_DATA_DIR = "terraform_data"
SCALEUP_TERRAFORM_DATA_DIR = "scaleup_terraform_data"
SCALEUP_VSPHERE_DIR = os.path.join(
    EXTERNAL_DIR,
    "openshift-misc/v4-testing-misc/v4-scaleup/vsphere/"
)
SCALEUP_VSPHERE_MAIN = os.path.join(SCALEUP_VSPHERE_DIR, "main.tf")
SCALEUP_VSPHERE_VARIABLES = os.path.join(SCALEUP_VSPHERE_DIR, "variables.tf")
SCALEUP_VSPHERE_ROUTE53 = os.path.join(SCALEUP_VSPHERE_DIR, "route53/vsphere-rhel-dns.tf")
SCALEUP_VSPHERE_ROUTE53_VARIABLES = os.path.join(SCALEUP_VSPHERE_DIR, "route53/variables.tf")
SCALEUP_VSPHERE_MACHINE_CONF = os.path.join(SCALEUP_VSPHERE_DIR, "machines/vsphere-rhel-machine.tf")
TERRAFORM_VARS = "terraform.tfvars"
VM_DISK_TYPE = "thin"
VM_DISK_MODE = "persistent"
INSTALLER_DEFAULT_DNS = "1.1.1.1"

# Config related constants
config_keys_patterns_to_censor = ['passw', 'token', 'secret']

# packages
RHEL_POD_PACKAGES = ["openssh-clients", "openshift-ansible", "openshift-clients", "jq"]

# common locations
POD_UPLOADPATH = RHEL_TMP_PATH = "/tmp/"
YUM_REPOS_PATH = "/etc/yum.repos.d/"
PEM_PATH = "/etc/pki/ca-trust/source/anchors/"

# Upgrade related constants, keeping some space between, so we can add
# additional order.
ORDER_BEFORE_UPGRADE = 10
ORDER_BEFORE_OCP_UPGRADE = 20
ORDER_OCP_UPGRADE = 30
ORDER_AFTER_OCP_UPGRADE = 40
ORDER_BEFORE_OCS_UPGRADE = 50
ORDER_OCS_UPGRADE = 60
ORDER_AFTER_OCS_UPGRADE = 70
ORDER_AFTER_UPGRADE = 80

# Deployment constants
OCS_CSV_PREFIX = 'ocs-operator'
LOCAL_STORAGE_CSV_PREFIX = 'local-storage-operator'
LATEST_TAGS = (
    'latest', 'latest-stable', '4.2-rc', 'latest-4.2', 'latest-stable-4.2',
    '4.3-rc', 'latest-4.3', 'latest-stable-4.3',
)
INTERNAL_MIRROR_PEM_FILE = "ops-mirror.pem"
EC2_USER = "ec2-user"
OCS_SUBSCRIPTION = "ocs-subscription"

# UI Deployment constants
HTPASSWD_SECRET_NAME = "htpass-secret"
HTPASSWD_SECRET_YAML = "frontend/integration-tests/data/htpasswd-secret.yaml"
HTPASSWD_PATCH_YAML = "frontend/integration-tests/data/patch-htpasswd.yaml"
CHROME_BROWSER = "chrome"
SUPPORTED_BROWSERS = (CHROME_BROWSER)

# Inventory
INVENTORY_TEMPLATE = "inventory.yaml.j2"
INVENTORY_FILE = "inventory.yaml"

# users
VM_RHEL_USER = "test"

# PEM
OCP_PEM = "ops-mirror.pem"

# playbooks
SCALEUP_ANSIBLE_PLAYBOOK = "/usr/share/ansible/openshift-ansible/playbooks/scaleup.yml"

# labels
MASTER_LABEL = "node-role.kubernetes.io/master"
WORKER_LABEL = "node-role.kubernetes.io/worker"

# Cluster name limits
CLUSTER_NAME_MIN_CHARACTERS = 5
CLUSTER_NAME_MAX_CHARACTERS = 17

STAGE_CA_FILE = os.path.join(
    TEMPLATE_DIR, "ocp-deployment", "stage-ca.crt"
)

# PDB NAMES
MDS_PDB = "rook-ceph-mds-ocs-storagecluster-cephfilesystem"
OSD_PDB = "rook-ceph-osd-"
MON_PDB = "rook-ceph-mon-pdb"

# Root Disk size
CURRENT_VM_ROOT_DISK_SIZE = '60'
VM_ROOT_DISK_SIZE = '120'

# Secrets
RBD_PROVISIONER_SECRET = 'rook-csi-rbd-provisioner'
RBD_NODE_SECRET = 'rook-csi-rbd-node'
CEPHFS_PROVISIONER_SECRET = 'rook-csi-cephfs-provisioner'
CEPHFS_NODE_SECRET = 'rook-csi-cephfs-node'

# JSON Schema
OSD_TREE_ROOT = {
    'type': 'object',
    'properties': {
        'id': {'type': 'integer'}, 'name': {'const': 'default'},
        'type': {'const': 'root'}, 'type_id': {'const': 11},
        'children': {'type': 'array', 'items': {'type': 'integer'}}
    },
    'required': ['children', 'id', 'name', 'type', 'type_id'],
    'additionalProperties': False
}

OSD_TREE_RACK = {
    'type': 'object',
    'properties': {
        'id': {'type': 'integer'}, 'name': {'type': 'string'},
        'type': {'const': 'rack'}, 'type_id': {'const': 3},
        'pool_weights': {'type': 'object'},
        'children': {'type': 'array', 'items': {'type': 'integer'}}
    },
    'required': ['children', 'id', 'name', 'pool_weights', 'type', 'type_id'],
    'additionalProperties': False
}

OSD_TREE_HOST = {
    'type': 'object',
    'properties': {
        'id': {'type': 'integer'}, 'name': {'type': 'string'},
        'type': {'const': 'host'}, 'type_id': {'const': 1},
        'pool_weights': {'type': 'object'},
        'children': {'type': 'array', 'items': {'type': 'integer'}}
    },
    'required': ['children', 'id', 'name', 'pool_weights', 'type', 'type_id'],
    'additionalProperties': False
}

OSD_TREE_OSD = {
    'type': 'object',
    'properties': {
        'id': {'type': 'integer'}, 'device_class': {'type': 'string'},
        'name': {'pattern': 'osd[.][0-9]+'}, 'type': {'const': 'osd'},
        'type_id': {'const': 0},
        'crush_weight': {'type': 'number'},
        'depth': {'type': 'integer'}, 'pool_weights': {'type': 'object'},
        'exists': {'type': 'integer'}, 'status': {'const': 'up'},
        'reweight': {'type': 'integer'},
        'primary_affinity': {'type': 'integer'}
    },
    'required': [
        'crush_weight', 'depth', 'device_class', 'exists', 'id', 'name',
        'pool_weights', 'primary_affinity', 'reweight', 'status', 'type',
        'type_id'
    ],
    'additionalProperties': False
}

OSD_TREE_REGION = {
    'type': 'object',
    'properties': {
        'id': {'type': 'integer'}, 'name': {'type': 'string'},
        'type': {'const': 'region'}, 'type_id': {'const': 10},
        'pool_weights': {'type': 'object'},
        'children': {'type': 'array', 'items': {'type': 'integer'}}
    },
    'required': ['children', 'id', 'name', 'pool_weights', 'type', 'type_id'],
    'additionalProperties': False
}

OSD_TREE_ZONE = {
    'type': 'object',
    'properties': {
        'id': {'type': 'integer'}, 'name': {'type': 'string'},
        'type': {'const': 'zone'}, 'type_id': {'const': 9},
        'pool_weights': {'type': 'object'},
        'children': {'type': 'array', 'items': {'type': 'integer'}}
    },
    'required': ['children', 'id', 'name', 'pool_weights', 'type', 'type_id'],
    'additionalProperties': False
}

# gather bootstrap
GATHER_BOOTSTRAP_PATTERN = 'openshift-install gather bootstrap --help'

# must-gather commands output files
MUST_GATHER_COMMANDS = [
    'ceph_versions', 'ceph_status', 'ceph_report', 'ceph_pg_dump',
    'ceph_osd_tree', 'ceph_osd_stat', 'ceph_osd_dump', 'ceph_osd_df_tree',
    'ceph_osd_crush_show-tunables', 'ceph_osd_crush_dump', 'ceph_mon_stat',
    'ceph_mon_dump', 'ceph_mgr_dump', 'ceph_mds_stat', 'ceph_health_detail',
    'ceph_fs_ls', 'ceph_fs_dump', 'ceph_df', 'ceph_auth_list',
    'ceph-volume_lvm_list'
]

MUST_GATHER_COMMANDS_JSON = [
    'ceph_versions_--format_json-pretty', 'ceph_status_--format_json-pretty',
    'ceph_report_--format_json-pretty', 'ceph_pg_dump_--format_json-pretty',
    'ceph_osd_tree_--format_json-pretty', 'ceph_osd_stat_--format_json-pretty',
    'ceph_osd_dump_--format_json-pretty',
    'ceph_osd_df_tree_--format_json-pretty',
    'ceph_osd_crush_show-tunables_--format_json-pretty',
    'ceph_osd_crush_dump_--format_json-pretty',
    'ceph_mon_stat_--format_json-pretty', 'ceph_mon_dump_--format_json-pretty',
    'ceph_mgr_dump_--format_json-pretty', 'ceph_mds_stat_--format_json-pretty',
    'ceph_health_detail_--format_json-pretty',
    'ceph_fs_ls_--format_json-pretty', 'ceph_fs_dump_--format_json-pretty',
    'ceph_df_--format_json-pretty', 'ceph_auth_list_--format_json-pretty'
]

# local storage
LOCAL_STORAGE_OPERATOR = os.path.join(
    TEMPLATE_DEPLOYMENT_DIR, "local-storage-operator.yaml"
)
LOCAL_VOLUME_YAML = os.path.join(
    TEMPLATE_DEPLOYMENT_DIR, "local-volume.yaml"
)

# All worker default config files
RHEL_WORKERS_CONF = os.path.join(CONF_DIR, 'ocsci/aws_upi_rhel_workers.yaml')

# Users
NOOBAA_SERVICE_ACCOUNT = "system:serviceaccount:openshift-storage:noobaa"

# Miscellaneous
NOOBAA_OPERATOR_POD_CLI_PATH = "/usr/local/bin/noobaa-operator"
