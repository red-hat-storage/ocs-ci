"""
Defaults module
"""
from getpass import getuser

INSTALLER_VERSION = '4.1.0-rc.3'
AWS_REGION = 'us-east-2'
ROOK_CLUSTER_NAMESPACE = 'openshift-storage'
KUBECONFIG_LOCATION = 'auth/kubeconfig'  # relative from cluster_dir
CLUSTER_NAME = f"ocs-ci-cluster-{getuser()}"
API_VERSION = "v1"
CEPH_IMAGE = "quay.io/rhceph-dev/rhceph-4.0-rhel-8"
ROOK_IMAGE = "quay.io/rhceph-dev/rook"
DEPLOYMENT_PLATFORM = 'AWS'

ENV_DATA = {
    'platform': DEPLOYMENT_PLATFORM,
    'cluster_name': CLUSTER_NAME,
    'cluster_namespace': ROOK_CLUSTER_NAMESPACE,
    'region': AWS_REGION,
    'ceph_image': CEPH_IMAGE,
    'rook_image': ROOK_IMAGE,
}
k8s_api_version = 'storage.k8s.io/v1'
rook_api_version = 'ceph.rook.io/v1'
ocp_api_version = 'project.openshift.io/v1'
