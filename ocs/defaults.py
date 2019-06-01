"""
Defaults module
"""
from getpass import getuser

INSTALLER_VERSION = '4.1.0-rc.3'
AWS_REGION = 'us-east-2'
ROOK_CLUSTER_NAMESPACE = 'openshift-storage'
TEST_NAMESPACE = 'ocs-qe'
KUBECONFIG_LOCATION = 'auth/kubeconfig'  # relative from cluster_dir
CLUSTER_NAME = f"ocs-ci-cluster-{getuser()}"
API_VERSION = "v1"
CEPH_IMAGE = "ceph/ceph:v14.2.0-20190410"
ROOK_IMAGE = "rook/ceph:master"
DEPLOYMENT_PLATFORM = 'AWS'
GB = 1024 ** 3

ENV_DATA = {
    'platform': DEPLOYMENT_PLATFORM,
    'cluster_name': CLUSTER_NAME,
    'cluster_namespace': ROOK_CLUSTER_NAMESPACE,
    'region': AWS_REGION,
    'ceph_image': CEPH_IMAGE,
    'rook_image': ROOK_IMAGE,
}
