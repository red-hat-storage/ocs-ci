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
TEMPLATE_CEPH_DIR = os.path.join(TEMPLATE_DIR, "ceph")
TEMPLATE_CSI_DIR = os.path.join(TEMPLATE_DIR, "CSI")
TEMPLATE_CSI_RBD_DIR = os.path.join(TEMPLATE_CSI_DIR, "rbd")
TEMPLATE_CSI_FS_DIR = os.path.join(TEMPLATE_CSI_DIR, "cephfs")
TEMPLATE_PV_PVC_DIR = os.path.join(TEMPLATE_DIR, "pv_pvc")
TEMPLATE_APP_POD_DIR = os.path.join(TEMPLATE_DIR, "app-pods")
TEMPLATE_WORKLOAD_DIR = os.path.join(TEMPLATE_DIR, "workloads")
TEMPLATE_FIO_DIR = os.path.join(TEMPLATE_WORKLOAD_DIR, "fio")
TEMPLATE_SMALLFILE_DIR = os.path.join(TEMPLATE_WORKLOAD_DIR, "smallfile")
TEMPLATE_PGSQL_DIR = os.path.join(TEMPLATE_WORKLOAD_DIR, "pgsql")
TEMPLATE_JENKINS_DIR = os.path.join(TEMPLATE_WORKLOAD_DIR, "jenkins")
TEMPLATE_VDBENCH_DIR = os.path.join(TEMPLATE_WORKLOAD_DIR, "vdbench")
TEMPLATE_PGSQL_SERVER_DIR = os.path.join(TEMPLATE_PGSQL_DIR, "server")
TEMPLATE_COUCHBASE_DIR = os.path.join(TEMPLATE_WORKLOAD_DIR, "couchbase")
TEMPLATE_COUCHBASE_SERVER_DIR = os.path.join(TEMPLATE_COUCHBASE_DIR, "server")
TEMPLATE_PILLOWFIGHT_DIR = os.path.join(TEMPLATE_COUCHBASE_SERVER_DIR, "pillowfight")
TEMPLATE_MCG_DIR = os.path.join(TEMPLATE_DIR, "mcg")
TEMPLATE_AMQ_DIR = os.path.join(TEMPLATE_WORKLOAD_DIR, "amq")
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
STATUS_ERROR = 'Error'
STATUS_CLBO = 'CrashLoopBackOff'
STATUS_READYTOUSE = 'READYTOUSE'

# NooBaa statuses
BS_AUTH_FAILED = 'AUTH_FAILED'
BS_OPTIMAL = 'OPTIMAL'
HEALTHY_OB = 'OPTIMAL'
HEALTHY_OBC = STATUS_BOUND
HEALTHY_OBC_CLI_PHASE = 'Phase:Bound'
HEALTHY_OB_CLI_MODE = 'Mode:OPTIMAL'

# Resources / Kinds
CEPHFILESYSTEM = "CephFileSystem"
CEPHBLOCKPOOL = "CephBlockPool"
CEPHBLOCKPOOL_SC = "ocs-storagecluster-ceph-rbd"
CEPHFILESYSTEM_SC = "ocs-storagecluster-cephfs"
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
LOCAL_VOLUME = 'localvolume'
PROXY = 'Proxy'
MACHINECONFIGPOOL = "MachineConfigPool"
VOLUMESNAPSHOTCLASS = "VolumeSnapshotClass"

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
TEST = "test"
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
OPENSHIFT_MONITORING_NAMESPACE = "openshift-monitoring"
MASTER_MACHINE = "master"
WORKER_MACHINE = "worker"
MOUNT_POINT = '/var/lib/www/html'

OCP_QE_MISC_REPO = (
    "https://gitlab.cee.redhat.com/aosqe/flexy-templates.git"
)
CRITICAL_ERRORS = [
    "core dumped", "oom_reaper"
]
must_gather_pod_label = "must-gather"
drain_canary_pod_label = "rook-ceph-drain-canary"

# AMQ
AMQ_NAMESPACE = "myproject"
KAFKA_OPERATOR = "https://github.com/strimzi/strimzi-kafka-operator"
OCS_WORKLOADS = "https://github.com/red-hat-storage/ocs-workloads"
CODESPEED_URL = 'http://10.0.78.167:8000/'

UPI_INSTALL_SCRIPT = "upi_on_aws-install.sh"

DEFAULT_CLUSTERNAME = 'ocs-storagecluster'
DEFAULT_CLUSTERNAME_EXTERNAL_MODE = 'ocs-external-storagecluster'
DEFAULT_BLOCKPOOL = f'{DEFAULT_CLUSTERNAME}-cephblockpool'
METADATA_POOL = f'{DEFAULT_CLUSTERNAME}-cephfilesystem-metadata'
DATA_POOL = f'{DEFAULT_CLUSTERNAME}-cephfilesystem-data0'
DEFAULT_ROUTE_CRT = "router-certs-default"
DEFAULT_NAMESPACE = "default"
IMAGE_REGISTRY_RESOURCE_NAME = "cluster"
IMAGE_REGISTRY_CONFIG = "configs.imageregistry.operator.openshift.io/cluster"
DEFAULT_NOOBAA_BACKINGSTORE = "noobaa-default-backing-store"
RIPSAW_NAMESPACE = "my-ripsaw"
JENKINS_BUILD = "jax-rs-build"
JENKINS_BUILD_COMPLETE = "Complete"
RIPSAW_CRD = "resources/crds/ripsaw_v1alpha1_ripsaw_crd.yaml"
RIPSAW_DROP_CACHE = os.path.join(TEMPLATE_FIO_DIR, "drop_cache_pod.yaml")
OCP_QE_DEVICEPATH_REPO = "https://github.com/anubhav-here/device-by-id-ocp.git"
LOCAL_STORAGE_NAMESPACE = 'local-storage'


# Default StorageClass
DEFAULT_STORAGECLASS_CEPHFS = f'{DEFAULT_CLUSTERNAME}-cephfs'
DEFAULT_STORAGECLASS_RBD = f'{DEFAULT_CLUSTERNAME}-ceph-rbd'
DEFAULT_STORAGECLASS_RGW = f'{DEFAULT_CLUSTERNAME}-ceph-rgw'

# Independent mode default StorageClasses
DEFAULT_EXTERNAL_MODE_STORAGECLASS_RGW = (
    f'{DEFAULT_CLUSTERNAME_EXTERNAL_MODE}-ceph-rgw'
)

# Default StorageClass for External-mode
DEFAULT_EXTERNAL_MODE_STORAGECLASS_CEPHFS = (
    f'{DEFAULT_CLUSTERNAME_EXTERNAL_MODE}-cephfs'
)
DEFAULT_EXTERNAL_MODE_STORAGECLASS_RBD = (
    f'{DEFAULT_CLUSTERNAME_EXTERNAL_MODE}-ceph-rbd'
)

# Default VolumeSnapshotClass
DEFAULT_VOLUMESNAPSHOTCLASS_CEPHFS = (
    f'{DEFAULT_CLUSTERNAME}-cephfsplugin-snapclass'
)
DEFAULT_VOLUMESNAPSHOTCLASS_RBD = f'{DEFAULT_CLUSTERNAME}-rbdplugin-snapclass'

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
NOOBAA_DB_LABEL = "noobaa-db=noobaa"
NOOBAA_ENDPOINT_POD_LABEL = "noobaa-s3=noobaa"
DEFAULT_DEVICESET_PVC_NAME = "ocs-deviceset"
DEFAULT_MON_PVC_NAME = "rook-ceph-mon"
OSD_PVC_GENERIC_LABEL = "ceph.rook.io/DeviceSet"
CEPH_ROOK_IO_PVC_LABEL = 'ceph.rook.io/pvc'
PGSQL_APP_LABEL = "app=postgres"
HOSTNAME_LABEL = "kubernetes.io/hostname"

# Auth Yaml
OCSCI_DATA_BUCKET = 'ocs-ci-data'
AUTHYAML = 'auth.yaml'

# OBJ File representing serialized data
NODE_OBJ_FILE = "node_file.objs"
NODE_FILE = "nodes.objs"
INSTANCE_FILE = "instances.objs"

# Ceph keyring template
CEPH_KEYRING = "ceph-keyring.j2"

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

CSI_RBD_PVC_CLONE_YAML = os.path.join(
    TEMPLATE_CSI_RBD_DIR, "pvc-clone.yaml"
)

CSI_CEPHFS_STORAGECLASS_YAML = os.path.join(
    TEMPLATE_CSI_FS_DIR, "storageclass.yaml"
)

CSI_CEPHFS_PVC_CLONE_YAML = os.path.join(
    TEMPLATE_CSI_FS_DIR, "pvc-clone.yaml"
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

RGW_OBC_YAML = os.path.join(
    TEMPLATE_MCG_DIR, "ObjectBucketClaim-RGW.yaml"
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

CSI_CEPHFS_PVC_RESTORE_YAML = os.path.join(
    TEMPLATE_CSI_FS_DIR, "pvc-restore.yaml"
)

CSI_CEPHFS_SNAPSHOT_YAML = os.path.join(
    TEMPLATE_CSI_FS_DIR, "snapshot.yaml"
)

CSI_CEPHFS_SNAPSHOTCLASS_YAML = os.path.join(
    TEMPLATE_CSI_FS_DIR, "snapshotclass.yaml"
)

CSI_RBD_PVC_YAML = os.path.join(
    TEMPLATE_CSI_RBD_DIR, "pvc.yaml"
)

CSI_RBD_PVC_RESTORE_YAML = os.path.join(
    TEMPLATE_CSI_RBD_DIR, "pvc-restore.yaml"
)

CSI_RBD_SNAPSHOT_YAML = os.path.join(
    TEMPLATE_CSI_RBD_DIR, "snapshot.yaml"
)

CSI_RBD_SNAPSHOTCLASS_YAML = os.path.join(
    TEMPLATE_CSI_RBD_DIR, "snapshotclass.yaml"
)

CONFIGURE_PVC_ON_MONITORING_POD = os.path.join(
    TEMPLATE_CONFIGURE_PVC_MONITORING_POD, "configuring_pvc.yaml"
)

FIO_CR_YAML = os.path.join(
    TEMPLATE_FIO_DIR, "benchmark_fio.yaml"
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

JENKINS_BUILDCONFIG_YAML = os.path.join(
    TEMPLATE_JENKINS_DIR, "buildconfig.yaml"
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

COUCHBASE_OPERATOR = 'couchbase-operator-namespace'

HELLO_WORLD_PRODUCER_YAML = os.path.join(
    TEMPLATE_AMQ_DIR, "hello-world-producer.yaml"
)

HELLO_WORLD_CONSUMER_YAML = os.path.join(
    TEMPLATE_AMQ_DIR, "hello-world-consumer.yaml"
)

AMQ_RBAC_YAML = os.path.join(
    TEMPLATE_AMQ_DIR, "rbac.yaml"
)

AMQ_BENCHMARK_POD_YAML = os.path.join(
    TEMPLATE_AMQ_DIR, "benchmark"
)

AMQ_BENCHMARK_VALUE_YAML = os.path.join(
    AMQ_BENCHMARK_POD_YAML, "values.yaml"
)

AMQ_DRIVER_KAFKA_YAML = os.path.join(
    TEMPLATE_AMQ_DIR, "driver-kafka.yaml"
)

AMQ_WORKLOAD_YAML = os.path.join(
    TEMPLATE_AMQ_DIR, "amq_workload.yaml"
)

AMQ_SIMPLE_WORKLOAD_YAML = os.path.join(
    TEMPLATE_AMQ_DIR, "amq_simple_workload.yaml"
)

NGINX_POD_YAML = os.path.join(
    TEMPLATE_APP_POD_DIR, "nginx.yaml"
)

AWSCLI_SERVICE_CA_YAML = os.path.join(
    TEMPLATE_MCG_DIR, "aws-cli-service-ca-configmap.yaml"
)

AWSCLI_POD_YAML = os.path.join(
    TEMPLATE_APP_POD_DIR, "awscli.yaml"
)

AWSCLI_MULTIARCH_POD_YAML = os.path.join(
    TEMPLATE_APP_POD_DIR, "awscli_multiarch.yaml"
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

EXTERNAL_STORAGE_CLUSTER_YAML = os.path.join(
    TEMPLATE_DEPLOYMENT_DIR, "external-storage-cluster.yaml"
)

EXTERNAL_CLUSTER_SECRET_YAML = os.path.join(
    TEMPLATE_DEPLOYMENT_DIR, "external-cluster-secret.yaml"
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
FIO_DC_YAML = os.path.join(
    TEMPLATE_FIO_DIR, "fio_dc.yaml"
)

# fio configuration files
FIO_S3 = os.path.join(
    TEMPLATE_FIO_DIR, 'config_s3.fio'
)

# Openshift infra yamls:
RSYNC_POD_YAML = os.path.join(
    TEMPLATE_OPENSHIFT_INFRA_DIR, "rsync-pod.yaml"
)
MACHINESET_YAML = os.path.join(
    TEMPLATE_OPENSHIFT_INFRA_DIR, "machine-set.yaml"
)
PODS_PER_NODE_COUNT_YAML = os.path.join(
    TEMPLATE_OPENSHIFT_INFRA_DIR, "max-pods-per-node.yaml"
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
NODE_NOT_READY_SCHEDULING_DISABLED = 'NotReady,SchedulingDisabled'

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
INFRA_NODE_LABEL = "node-role.kubernetes.io/infra=''"
NODE_SELECTOR_ANNOTATION = "openshift.io/node-selector="
TOPOLOGY_ROOK_LABEL = "topology.rook.io/rack"
OPERATOR_NODE_TAINT = "node.ocs.openshift.io/storage=true:NoSchedule"
OPERATOR_CATALOG_SOURCE_NAME = "ocs-catalogsource"
MARKETPLACE_NAMESPACE = "openshift-marketplace"
MONITORING_NAMESPACE = "openshift-monitoring"
OPERATOR_INTERNAL_SELECTOR = "ocs-operator-internal=true"
OPERATOR_CS_QUAY_API_QUERY = (
    'https://quay.io/api/v1/repository/rhceph-dev/{image}/'
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
VDBENCH_WIDTH = 4  # the width of the directory tree  that will be created
VDBENCH_DEPTH = 4  # the depth of the directory tree  that will be created
VDBENCH_FILE_SIZE = 1  # the file size in MB that will be created
VDBENCH_CAP_PER_POD = 80000  # the Maximum capacity (in MB) per pod in the test
VDBENCH_MIN_CAPACITY = 300  # minimum storage capacity (in GB) for the test to run

# Platforms
AWS_PLATFORM = 'aws'
AZURE_PLATFORM = 'azure'
GCP_PLATFORM = 'gcp'
VSPHERE_PLATFORM = 'vsphere'
BAREMETAL_PLATFORM = 'baremetal'
ON_PREM_PLATFORMS = [VSPHERE_PLATFORM, BAREMETAL_PLATFORM]
CLOUD_PLATFORMS = [AWS_PLATFORM, AZURE_PLATFORM, GCP_PLATFORM]
BAREMETALPSI_PLATFORM = 'baremetalpsi'

# ignition files
BOOTSTRAP_IGN = "bootstrap.ign"
MASTER_IGN = "master.ign"
WORKER_IGN = "worker.ign"

# terraform provider constants
TERRAFORM_IGNITION_PROVIDER_VERSION = "v2.1.0"

# vSphere related constants
VSPHERE_NODE_USER = "core"
VSPHERE_INSTALLER_BRANCH = "release-4.3"
VSPHERE_INSTALLER_REPO = "https://github.com/openshift/installer.git"
VSPHERE_SCALEUP_REPO = "https://code.engineering.redhat.com/gerrit/openshift-misc"
VSPHERE_CLUSTER_LAUNCHER = "https://gitlab.cee.redhat.com/aosqe/cluster-launcher.git"
VSPHERE_DIR = os.path.join(EXTERNAL_DIR, "installer/upi/vsphere/")
INSTALLER_IGNITION = os.path.join(VSPHERE_DIR, "machine/ignition.tf")
VM_IFCFG = os.path.join(VSPHERE_DIR, "vm/ifcfg.tmpl")
INSTALLER_ROUTE53 = os.path.join(VSPHERE_DIR, "route53/main.tf")
INSTALLER_MACHINE_CONF = os.path.join(VSPHERE_DIR, "machine/main.tf")
VM_MAIN = os.path.join(VSPHERE_DIR, "vm/main.tf")
VSPHERE_CONFIG_PATH = os.path.join(TOP_DIR, "conf/ocsci/vsphere_upi_vars.yaml")
VSPHERE_MAIN = os.path.join(VSPHERE_DIR, "main.tf")
VSPHERE_VAR = os.path.join(VSPHERE_DIR, "variables.tf")
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

# cluster-launcher
CLUSTER_LAUNCHER_VSPHERE_DIR = os.path.join(
    EXTERNAL_DIR,
    "cluster-launcher/v4-scaleup/ocp4-rhel-scaleup/"
)
CLUSTER_LAUNCHER_MACHINE_CONF = "vsphere/machines/vsphere-rhel-machine.tf"

TERRAFORM_VARS = "terraform.tfvars"
VM_DISK_TYPE = "thin"
VM_DISK_MODE = "persistent"
INSTALLER_DEFAULT_DNS = "1.1.1.1"

LIFECYCLE = 'lifecycle { ignore_changes = ["disk"] }'
CSR_BOOTSTRAPPER_NODE = "node-bootstrapper"

# VMware Datastore types
VMFS = "VMFS"
VSAN = "vsan"

# terraform haproxy service
TERRAFORM_HAPROXY_SERVICE = os.path.join(VSPHERE_DIR, "lb/haproxy.service")

# Config related constants
config_keys_patterns_to_censor = ['passw', 'token', 'secret']

# packages
RHEL_POD_PACKAGES = ["openssh-clients", "openshift-ansible", "openshift-clients", "jq"]

# common locations
POD_UPLOADPATH = RHEL_TMP_PATH = "/tmp/"
YUM_REPOS_PATH = "/etc/yum.repos.d/"
PEM_PATH = "/etc/pki/ca-trust/source/anchors/"
FIPS_LOCATION = "/proc/sys/crypto/fips_enabled"

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
    'latest', 'latest-stable',
    '4.2-rc', 'latest-4.2', 'latest-stable-4.2',
    '4.3-rc', 'latest-4.3', 'latest-stable-4.3',
    '4.4-rc', 'latest-4.4', 'latest-stable-4.4',
    '4.5-rc', 'latest-4.5', 'latest-stable-4.5',
    '4.6-rc', 'latest-4.6', 'latest-stable-4.6',
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

INVENTORY_TEMPLATE_HAPROXY = "inventory_haproxy.yaml.j2"
INVENTORY_FILE_HAPROXY = "inventory_haproxy.yaml"

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
RHCOS_WORKER_CONF = os.path.join(CONF_DIR, 'ocsci/aws_upi_rhcos_workers.yaml')
AWS_WORKER_NODE_TEMPLATE = "06_cluster_worker_node.yaml"
AWS_S3_UPI_BUCKET = "ocs-qe-upi"
AWS_WORKER_LOGICAL_RESOURCE_ID = "Worker0"
RHEL_WORKERS_CONF = os.path.join(CONF_DIR, 'ocsci/aws_upi_rhel_workers.yaml')

# Users
NOOBAA_SERVICE_ACCOUNT = "system:serviceaccount:openshift-storage:noobaa"

# Miscellaneous
NOOBAA_OPERATOR_POD_CLI_PATH = "/usr/local/bin/noobaa-operator"
NOOBAA_OPERATOR_LOCAL_CLI_PATH = os.path.join(DATA_DIR, "mcg-cli")
DEFAULT_INGRESS_CRT = "router-ca.crt"
DEFAULT_INGRESS_CRT_LOCAL_PATH = f"{DATA_DIR}/mcg-{DEFAULT_INGRESS_CRT}"
SERVICE_CA_CRT = "service-ca.crt"
SERVICE_CA_CRT_AWSCLI_PATH = f"/cert/{SERVICE_CA_CRT}"
AWSCLI_RELAY_POD_NAME = "awscli-relay-pod"
AWSCLI_SERVICE_CA_CONFIGMAP_NAME = "awscli-service-ca"

# Storage classes provisioners
OCS_PROVISIONERS = [
    'openshift-storage.rbd.csi.ceph.com',
    'openshift-storage.cephfs.csi.ceph.com',
    'openshift-storage.noobaa.io/obc'
]

# Bucket Policy action lists
bucket_website_action_list = ['PutBucketWebsite', 'GetBucketWebsite', 'PutObject']
bucket_version_action_list = ['PutBucketVersioning', 'GetBucketVersioning']
object_version_action_list = ['PutObject', 'GetObjectVersion', 'DeleteObjectVersion']

# Flexy config constants
FLEXY_MNT_CONTAINER_DIR = '/mnt'
FLEXY_HOST_DIR = 'flexy-dir'
FLEXY_HOST_DIR_PATH = os.path.join(DATA_DIR, FLEXY_HOST_DIR)
FLEXY_DEFAULT_ENV_FILE = "ocs-osp.env"
OPENSHIFT_MISC_BASE = "private-openshift-misc/functionality-testing"
FLEXY_BAREMETAL_UPI_TEMPLATE = "upi-on-baremetal/versioned-installer-openstack"
FLEXY_GIT_CRYPT_KEYFILE = os.path.join(DATA_DIR, "git-crypt-keyfile")
NTP_CHRONY_CONF = os.path.join(
    TEMPLATE_DIR, "ocp-deployment", "ntp_chrony.yaml"
)
FLEXY_DEFAULT_PRIVATE_CONF_REPO = 'https://gitlab.cee.redhat.com/ocs/flexy-ocs-private.git'
FLEXY_JENKINS_USER = "jenkins"
JENKINS_NFS_CURRENT_CLUSTER_DIR = "/home/jenkins/current-cluster-dir"
FLEXY_DEFAULT_PRIVATE_CONF_BRANCH = "master"
OPENSHIFT_CONFIG_NAMESPACE = "openshift-config"
FLEXY_RELATIVE_CLUSTER_DIR = "flexy/workdir/install-dir"
FLEXY_IMAGE_URL = "docker-registry.upshift.redhat.com/aosqe/flexy:poc"
FLEXY_ENV_FILE_UPDATED = os.path.join(
    FLEXY_HOST_DIR_PATH, 'ocs-flexy-env-file-updated.env'
)
REGISTRY_SVC = "registry.svc.ci.openshift.org/ocp/release"

# PSI-openstack constants
NOVA_CLNT_VERSION = "2.0"
CINDER_CLNT_VERSION = "3.0"

# URLs
AUTH_CONFIG_DOCS = (
    'https://ocs-ci.readthedocs.io/en/latest/docs/getting_started.html'
    '#authentication-config'
)

# Conversions
TP_CONVERSION = {
    ' B/s': 0.000000976562, ' KiB/s': 0.000976562, ' MiB/s': 1
}

# LSO
ROOT_DISK_NAME = "sda"
RDM = "RDM"
VMDK = "VMDK"
DIRECTPATH = "VMDirectPath"
DISK_MODE = "independent_persistent"
COMPATABILITY_MODE = "physicalMode"
DISK_PATH_PREFIX = "/vmfs/devices/disks/"

# OS
RHEL_OS = "RHEL"
RHCOS = "RHCOS"

# Scale constants
SCALE_NODE_SELECTOR = {'scale-label': 'app-scale'}
SCALE_LABEL = 'scale-label=app-scale'
# TODO: Revisit the dict value once there is change in instance/vm/server type
# TODO: Generic worker count value to support all kind of pods.
# Note: Below worker count value is based on nginx pod
# aws dict value is based on the manual execution result with m5.4xlarge instance and nginx pod
# vmware dict value is based on each worker vm config of min 12CPU and 64G RAM
# bm dict value is based on each worker BM machine of config 40CPU and 256G/184G RAM
# azure dict value is based on assumption similar to vmware vms min worker config of 12CPU and 64G RAM
SCALE_WORKER_DICT = {
    1500: {'aws': 12, 'vmware': 15, 'bm': 5, 'azure': 15},
    3000: {'aws': 24, 'vmware': 30, 'bm': 10, 'azure': 30},
    4500: {'aws': 36, 'vmware': 45, 'bm': 15, 'azure': 45},
}

# Elasticsearch and codespeed constants
ES_SERVER_IP = '10.0.78.167'
ES_SERVER_PORT = '9200'
ES_SERVER_URL = 'https://10.0.78.167:9200'

# Cluster metrics
THROUGHPUT_QUERY = "(sum(rate(ceph_pool_wr_bytes[1m]) + rate(ceph_pool_rd_bytes[1m])))"
LATENCY_QUERY = "cluster:ceph_disk_latency:join_ceph_node_disk_irate1m"
IOPS_QUERY = "sum(rate(ceph_pool_wr[1m])) + sum(rate(ceph_pool_rd[1m]))"
USED_SPACE_QUERY = "ceph_cluster_total_used_bytes"

# files
REMOTE_FILE_URL = "http://download.ceph.com/tarballs/ceph_15.1.0.orig.tar.gz"
FILE_PATH = '/tmp/ceph.tar.gz'

# terraform tfstate modules
BOOTSTRAP_MODULE = "module.ipam_bootstrap"
LOAD_BALANCER_MODULE = "module.ipam_lb"
COMPUTE_MODULE = "module.ipam_compute"
CONTROL_PLANE = "module.ipam_control_plane"

# proxy location
HAPROXY_LOCATION = "/etc/haproxy/haproxy.conf"

# chrony conf
CHRONY_CONF = "/etc/chrony.conf"

# NTP server
RH_NTP_CLOCK = "clock.redhat.com"

# Disruptions pod names
OSD = 'osd'
ROOK_OPERATOR = 'operator'
MON_DAEMON = 'mon'

# cluster expansion
MAX_OSDS = 15

# Minimum cluster requirements in term of node specs
MIN_NODE_CPU = 16
MIN_NODE_MEMORY = 64 * 10 ** 9

# aws tags
AWS_CLOUDFORMATION_TAG = 'aws:cloudformation:stack-name'

# Bare Metal constants
BOOTSTRAP_PXE_FILE = os.path.join(
    TEMPLATE_DIR, "baremetal-pxefile", "bootstrap"
)
MASTER_PXE_FILE = os.path.join(
    TEMPLATE_DIR, "baremetal-pxefile", "master"
)
WORKER_PXE_FILE = os.path.join(
    TEMPLATE_DIR, "baremetal-pxefile", "worker"
)
PXE_CONF_FILE = os.path.join(
    TEMPLATE_DIR, "ocp-deployment", "dnsmasq.pxe.conf"
)
COMMON_CONF_FILE = os.path.join(
    TEMPLATE_DIR, "ocp-deployment", "dnsmasq.common.conf"
)
RHCOS_IMAGES_FILE = os.path.join(
    TEMPLATE_DIR, "ocp-deployment", "rhcos_images.yaml"
)
PXE_FILE = os.path.join(
    TEMPLATE_DIR, "baremetal-pxefile"
)
coreos_url_prefix = "https://mirror.openshift.com/pub/openshift-v4/dependencies/rhcos"
BM_DEFAULT_CLUSTER_NAME = "ocp-baremetal-auto"
BM_STATUS_ABSENT = "ABSENT"
BM_STATUS_PRESENT = "PRESENT"
BM_STATUS_RESPONSE_UPDATED = "UPDATED"

# MCG namespace constants
MCG_NS_AWS_ENDPOINT = 'https://s3.amazonaws.com'
MCG_NS_RESOURCE = 'ns_resource'
MCG_NS_BUCKET = 'ns-bucket'
MCG_NS_AWS_CONNECTION = 'aws_connection'
