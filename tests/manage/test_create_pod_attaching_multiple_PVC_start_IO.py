"""
Test Case OCS-260, Create Cirros POD with multiple PVC attached and Start IO
"""

import logging
import pytest
import random
import time

from ocs_ci.ocs import constants, exceptions
from ocs_ci.framework.testlib import tier1, ManageTest
from ocs_ci.ocs import ocp
from ocs_ci.ocs.resources import pvc
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.utility import templating
from tests import helpers
from ocs_ci.framework import config

log = logging.getLogger(__name__)


@pytest.fixture(scope='class')
def ocs260_fixture(request):

    self = request.node.cls

    def finalizer():
        teardown(self)
    request.addfinalizer(finalizer)
    setup()


def setup():
    """
    Setting up the environment for the test
    """
    log.info("Create Ceph Block Pool")
    global RBD_POOL
    RBD_POOL = helpers.create_ceph_block_pool(pool_name="cirrostest")

    log.info("Create RBD Secret")
    global RBD_POOL_SECRET_OBJ
    RBD_POOL_SECRET_OBJ = helpers.create_secret(constants.CEPHBLOCKPOOL)

    log.info("Create RBD Storage Class")
    global RBD_STORAGE_CLASS_OBJ
    RBD_STORAGE_CLASS_OBJ = helpers.create_storage_class(
        constants.CEPHBLOCKPOOL,
        interface_name=RBD_POOL.name,
        secret_name=RBD_POOL_SECRET_OBJ.name
    )


def teardown(self):
    """
    Tearing down the environment
    """
    dc = ocp.OCP(
        kind=constants.DEPLOYMENTCONFIG, namespace=config.ENV_DATA['cluster_namespace']
    )
    dc.delete(resource_name=self.cirros_name[0], wait=True)
    pvc.delete_all_pvcs()
    RBD_STORAGE_CLASS_OBJ.delete()
    RBD_POOL_SECRET_OBJ.delete()
    RBD_POOL.delete()


def create_pvc(pvc_name_prefix, pvc_count):
    """
    Function to create multiple PVC based on count and name

    Args:
        pvc_name_prefix (str): User defined pvc prefix name
        pvc_count (int): Specify number of pvc count to be create

    Returns:
        pvc_list (list): Returns the pvc created
    """
    pvc_list = []
    for count in range(pvc_count):
        name = pvc_name_prefix + str(count)
        pvc_obj = helpers.create_pvc(sc_name=RBD_STORAGE_CLASS_OBJ.name, pvc_name=name)
        pvc_list.append(pvc_obj.name)
    return pvc_list


def create_pod(pvc_list):
    """
    Function to create Cirros pod with list of pvc.

    Args:
        pvc_list (list): List of PVC to be mounted in the cirros pod

    Returns:
        cirros_pod_name (str): Cirros pod deployment config name
    """
    temp_dict1 = []
    temp_dict2 = []
    temp_str = ''
    cirros_yaml_data = templating.load_yaml_to_dict(constants.CIRROS_APP_POD_YAML)
    # Following code collects the existing cirros yaml data, removes some
    # values which will be updated as per pvc_list argument
    cirros_pod_name = cirros_yaml_data['metadata']['name']
    cirros_yaml_data['metadata']['namespace'] = config.ENV_DATA['cluster_namespace']
    temp_list1 = cirros_yaml_data['spec']['template']['spec']['containers'][0]['args']
    temp_list1.pop(len(temp_list1) - 1)
    temp_list2 = cirros_yaml_data['spec']['template']['spec']['containers'][0][
        'livenessProbe'
    ]['exec']['command']
    temp_list2.pop(len(temp_list2) - 1)
    # for loop which creates the values, which will be added in cirros.yaml
    for pv in pvc_list:
        temp_dict1.append(
            {'name': pv, 'persistentVolumeClaim': {'claimName': pv}}
        )
        temp_dict2.append({'mountPath': '/mnt' + pv, 'name': pv})
        temp_list2.append(
            'mount | grep /mnt' + pv + ' && head -c 1024 '
                                       '< /dev/urandom >> /mnt' + pv + '/random-data.log'
        )
        temp_str = temp_str + '(mount | grep /mnt' + pv + ') && (head -c ' \
                              '1000 < /dev/urandom > /mnt' + pv + '/random-data.log) || ' \
                              'exit 1; '
    del cirros_yaml_data['spec']['template']['spec']['volumes']
    del cirros_yaml_data['spec']['template']['spec']['containers'][0]['volumeMounts']
    del cirros_yaml_data['spec']['template']['spec']['containers'][0][
        'livenessProbe'
    ]['exec']['command']
    # Updating the yaml with pv_list arg values, which will be mounted
    # in the cirros pod.
    cirros_yaml_data['spec']['template']['spec']['volumes'] = temp_dict1
    cirros_yaml_data['spec']['template']['spec']['containers'][0][
        'volumeMounts'
    ] = temp_dict2
    temp_list1.append('while true; do ' + temp_str + ' sleep 20 ; done')
    cirros_yaml_data['spec']['template']['spec']['containers'][0][
        'livenessProbe'
    ]['exec']['command'] = temp_list2
    # Create pod with cirros_yaml_data
    ocs_obj = OCS(**cirros_yaml_data)
    assert ocs_obj.create()
    try:
        # Check for pod is up and running
        pod = ocp.OCP(
            kind=constants.POD, namespace=config.ENV_DATA['cluster_namespace']
        )
        pod.wait_for_resource(
            condition='Running', selector='deploymentconfig=cirrospod',
            timeout=180, sleep=20
        )
        log.info(f"Pod {cirros_pod_name} creation succeeded")
        return cirros_pod_name
    except exceptions.CommandFailed:
        log.info(f"{cirros_pod_name} not in Running state")
        return False


@tier1
@pytest.mark.usefixtures(
    ocs260_fixture.__name__,
)
@pytest.mark.polarion_id("OCS-260")
class TestCreatePodWithMultiplePVCAndStartIO(ManageTest):
    """
    Creating Cirros POD with mutliple PVC attached and Start IO
    """
    # create random number of PVC to be mounted in POD.
    pvc_count = random.randint(5, 10)
    io_duration_sec = random.randint(180, 300)  # amount of time IO needs to executed
    cirros_name = []
    pvc_list = []

    def test_create_cirros_pod(self):
        """
        Test Creating Cirros POD with multiple PVC attached to it.
        """
        self.pvc_list = create_pvc(pvc_name_prefix="cirrospvc", pvc_count=self.pvc_count)
        self.cirros_name.append(create_pod(self.pvc_list))
        log.info(f"Running IO's for {self.io_duration_sec} seconds")
        time.sleep(self.io_duration_sec)
