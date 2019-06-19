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

from ocs import constants
from utility.templating import load_yaml_to_dict

STORAGE_API_VERSION = 'storage.k8s.io/v1'
ROOK_API_VERSION = 'ceph.rook.io/v1'
OCP_API_VERSION = 'project.openshift.io/v1'
OPENSHIFT_REST_CLIENT_API_VERSION = 'v1'

# Those variables below are duplicate at the moment from default_config.yaml
# and once we drop support for old runner we will remove those variables from
# here and will have them defined only on one place.

# Be aware that variables defined above and below are not used anywhere in the
# config files and their sections when we rendering config!

INSTALLER_VERSION = '4.1.2'
CLIENT_VERSION = INSTALLER_VERSION
AWS_REGION = 'us-east-2'
ROOK_CLUSTER_NAMESPACE = 'openshift-storage'
KUBECONFIG_LOCATION = 'auth/kubeconfig'  # relative from cluster_dir
CLUSTER_NAME = "ocs-ci"
API_VERSION = "v1"
CEPH_IMAGE = "ceph/ceph:v14"
ROOK_IMAGE = "rook/ceph:master"
DEPLOYMENT_PLATFORM = 'AWS'

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

REPORTING = {
    'email': {
        'address': 'ocs-ci@redhat.com',
    },
    'polarion': {
        'project_id': 'OpenShiftContainerStorage',
    }
}

RUN = {
    'log_dir': '/tmp',
    'run_id': None,
    'kubeconfig_location': 'auth/kubeconfig',
    'cli_params': {},
    'client_version': DEPLOYMENT['installer_version'],
    'bin_dir': './bin',
}

TEMP_YAML = os.path.join(constants.TEMPLATE_DIR, "temp.yaml")

TOOL_POD_DICT = load_yaml_to_dict(
    os.path.join(
        constants.TEMPLATE_DEPLOYMENT_DIR, "toolbox_pod.yaml"
    )
)
CEPHFILESYSTEM_DICT = load_yaml_to_dict(
    os.path.join(
        constants.TEMPLATE_CSI_FS_DIR, "CephFileSystem.yaml"
    )
)
CEPHBLOCKPOOL_DICT = load_yaml_to_dict(
    os.path.join(
        constants.TEMPLATE_DEPLOYMENT_DIR, "cephblockpool.yaml"
    )
)
CSI_RBD_STORAGECLASS_DICT = load_yaml_to_dict(
    os.path.join(
        constants.TEMPLATE_CSI_RBD_DIR, "storageclass.yaml"
    )
)
CSI_CEPHFS_STORAGECLASS_DICT = load_yaml_to_dict(
    os.path.join(
        constants.TEMPLATE_CSI_FS_DIR, "storageclass.yaml"
    )
)
CSI_PVC_DICT = load_yaml_to_dict(
    os.path.join(
        constants.TEMPLATE_PV_PVC_DIR, "PersistentVolumeClaim.yaml"
    )
)
CSI_RBD_POD_DICT = load_yaml_to_dict(
    os.path.join(
        constants.TEMPLATE_CSI_RBD_DIR, "pod.yaml"
    )
)
CSI_RBD_SECRET = load_yaml_to_dict(
    os.path.join(
        constants.TEMPLATE_CSI_RBD_DIR, "secret.yaml"
    )
)
CSI_CEPHFS_SECRET = load_yaml_to_dict(
    os.path.join(
        constants.TEMPLATE_CSI_FS_DIR, "secret.yaml"
    )
)

CSI_CEPHFS_STORAGECLASS_DICT = load_yaml_to_dict(
    os.path.join(
        constants.TEMPLATE_CSI_FS_DIR, "storageclass.yaml"
    )
)

CSI_CEPHFS_PVC = load_yaml_to_dict(
    os.path.join(
        constants.TEMPLATE_CSI_FS_DIR, "pvc.yaml"
    )
)

CSI_RBD_PVC = load_yaml_to_dict(
    os.path.join(
        constants.TEMPLATE_CSI_RBD_DIR, "pvc.yaml"
    )
)
