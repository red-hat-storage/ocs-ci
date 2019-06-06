"""
Defaults module.

This module is automatically loaded with variables defined in
conf/ocsci/default_config.yaml in its DEFAULTS section.

If the variable can be used in some config file sections from ocsci/config.py
module, plese put your defaults rather to mentioned default_config.yaml file!

See the documentation in conf/README.md file to understand this config file.

PYTEST_DONT_REWRITE - avoid pytest to rewrite, keep this msg here please!
"""
import os
from getpass import getuser

from utility.templating import load_yaml_to_dict

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
TOP_DIR = os.path.dirname(THIS_DIR)
TEMPLATE_DIR = os.path.join(TOP_DIR, "templates")
TEMPLATE_DEPLOYMENT_DIR = os.path.join(TEMPLATE_DIR, "ocs-deployment")
TEMPLATE_CSI_DIR = os.path.join(TEMPLATE_DIR, "CSI")
TEMPLATE_CSI_RBD_DIR = os.path.join(TEMPLATE_CSI_DIR, "rbd")
TEMPLATE_CSI_FS_DIR = os.path.join(TEMPLATE_CSI_DIR, "cephfs")
TEMPLATE_PV_PVC_DIR = os.path.join(TEMPLATE_DIR, "pv_pvc")
TEMPLATE_APP_POD_DIR = os.path.join(TEMPLATE_DIR, "app-pods")
STORAGE_API_VERSION = 'storage.k8s.io/v1'
ROOK_API_VERSION = 'ceph.rook.io/v1'
OCP_API_VERSION = 'project.openshift.io/v1'
OPENSHIFT_REST_CLIENT_API_VERSION = 'v1'

CEPHFILESYSTEM = "CephFileSystem"
CEPHBLOCKPOOL = "CephBlockPool"
PVC = "PersistentVolumeClaim"
STORAGECLASS = "StorageClass"
SECRET = "Secret"

# Those variables below are duplicate at the moment from default_config.yaml
# and once we drop support for old runner we will remove those variables from
# here and will have them defined only on one place.

# Be aware that variables defined above and below are not used anywhere in the
# config files and their sections when we rendering config!

INSTALLER_VERSION = '4.1.0-rc.5'
CLIENT_VERSION = INSTALLER_VERSION
AWS_REGION = 'us-east-2'
ROOK_CLUSTER_NAMESPACE = 'openshift-storage'
KUBECONFIG_LOCATION = 'auth/kubeconfig'  # relative from cluster_dir
CLUSTER_NAME = f"ocs-ci-cluster-{getuser()}"
API_VERSION = "v1"
CEPH_IMAGE = "ceph/ceph:v14.2.0-20190410"
ROOK_IMAGE = "rook/ceph:master"
DEPLOYMENT_PLATFORM = 'AWS'

STATUS_PENDING = 'Pending'
STATUS_AVAILABLE = 'Available'
STATUS_RUNNING = 'Running'
STATUS_TERMINATING = 'Terminating'

# This section is suppose to be available just from ocsci/config.py module from
# ENV_DATA dictionary. Once we drop support of old runner we will delete this
# data from here as well.
ENV_DATA = {
    'platform': DEPLOYMENT_PLATFORM,
    'cluster_name': CLUSTER_NAME,
    'cluster_namespace': ROOK_CLUSTER_NAMESPACE,
    'region': AWS_REGION,
    'ceph_image': CEPH_IMAGE,
    'rook_image': ROOK_IMAGE,
}

DEPLOYMENT = {
    'installer_version': INSTALLER_VERSION,
}

TEMPLATES_DIR = "templates"
TEMP_YAML = os.path.join(TEMPLATES_DIR, "temp.yaml")

TOOL_POD_DICT = load_yaml_to_dict(
    os.path.join(
        TEMPLATE_DEPLOYMENT_DIR, "toolbox_pod.yaml"
    )
)
CEPHFILESYSTEM_DICT = load_yaml_to_dict(
    os.path.join(
        TEMPLATE_CSI_FS_DIR, "CephFileSystem.yaml"
    )
)
PVC_DICT = load_yaml_to_dict(
    os.path.join(
        TEMPLATE_PV_PVC_DIR, "PersistentVolumeClaim.yaml"
    )
)
