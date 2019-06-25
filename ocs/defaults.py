"""
Defaults module. All the defaults used by OSCCI framework should
reside in this module.
PYTEST_DONT_REWRITE - avoid pytest to rewrite, keep this msg here please!
"""
import os

from ocs import constants


#############################################################################
# Default Configuration - access those vars from config and not from here!  #
#############################################################################
ENV_DATA = {
    'platform': 'AWS',
    'cluster_name': 'ocs-ci',
    'cluster_namespace': 'openshift-storage',
    'region': 'us-east-2',
    'ceph_image': "ceph/ceph:v14",
    'rook_image': "rook/ceph:master",
}

DEPLOYMENT = {
    'installer_version': "4.1.2",
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

#############################################################################
# End of Default Configuration                                              #
#############################################################################

TEMP_YAML = os.path.join(constants.TEMPLATE_DIR, "temp.yaml")

# This variable is changed in deployment fixture. Don't want to change the
# logic of this in this PR, so this needs to be probably solved better way.
CEPHFILESYSTEM_NAME = 'ocsci-cephfs'
