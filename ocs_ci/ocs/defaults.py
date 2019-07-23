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
ROOK_CLUSTER_NAMESPACE = 'rook-ceph'
OCS_MONITORING_NAMESPACE = 'openshift-monitoring'
KUBECONFIG_LOCATION = 'auth/kubeconfig'  # relative from cluster_dir
API_VERSION = "v1"
CEPHFILESYSTEM_NAME = 'ocsci-cephfs'

TEMP_YAML = os.path.join(constants.TEMPLATE_DIR, "temp.yaml")

PROMETHEUS_ROUTE = 'prometheus-k8s'
