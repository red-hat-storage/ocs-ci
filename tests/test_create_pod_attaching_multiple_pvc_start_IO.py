import os
import yaml
import logging
import pytest
import ocs.ocp
import ocs.pod
import ocs.defaults as defaults

from ocs import exceptions
from time import sleep
from ocsci import tier1, ManageTest
from utility import templating

log = logging.getLogger(__name__)

CBP = ocs.ocp.OCP(
    kind='CephBlockPool', namespace=defaults.ROOK_CLUSTER_NAMESPACE
)
SEC = ocs.ocp.OCP(
    kind='Secret', namespace=defaults.ROOK_CLUSTER_NAMESPACE
)
SC = ocs.ocp.OCP(
    kind='StorageClass', namespace=defaults.ROOK_CLUSTER_NAMESPACE
)
PVC = ocs.ocp.OCP(
    kind='PersistentVolumeClaim', namespace=defaults.ROOK_CLUSTER_NAMESPACE
)
POD = ocs.ocp.OCP(
    kind='Pod', namespace=defaults.ROOK_CLUSTER_NAMESPACE
)
OCP = ocs.ocp.OCP(
    kind='Service', namespace=defaults.ROOK_CLUSTER_NAMESPACE
)
DC = ocs.ocp.OCP(
    kind='DeploymentConfig', namespace=defaults.ROOK_CLUSTER_NAMESPACE
)

TEMP_YAML = os.path.join("templates/ocs-deployment", "temp.yaml")
TEMPLATES_DIR = "templates/CSI/rbd"
CIRROS_DIR = "templates/app-pod-yamls"
TIMEOUT = 180
SLEEP_TIME = 20


@pytest.fixture(scope='class')
def test_fixture(request):
    """
    pytest fixture function
    """
    self = request.node.cls

    def finalizer():
        teardown(self)
    request.addfinalizer(finalizer)
    setup()


def setup():
    """
    Setting up the environment for the test
    """
    create_ceph_block_pool()
    create_rbd_secret()
    create_rbd_storage_class()


def teardown(self):
    """
    Teardown the created Environment
    """
    log.info(f"Cirros pod name {self.cirros_name[0]}")
    delete_pod(self.cirros_name[0])
    delete_persistent_volume(self.pv_list)
    delete_storage_class()
    delete_pool()
    delete_secret()
    os.remove(TEMP_YAML)


def create_ceph_block_pool():
    """
    Create ceph block pool
    """
    template = os.path.join(TEMPLATES_DIR, "CephBlockPool.yaml")
    log.info(f"Create a ceph block pool")
    data = templating.generate_yaml_from_jinja2_template_with_data(
        template
    )
    ceph_block_pool = data['metadata']['name']
    with open(TEMP_YAML, 'w') as yaml_file:
        yaml.dump(data, yaml_file)
    assert CBP.create(yaml_file=TEMP_YAML)
    open(TEMP_YAML, 'w').close()
    sleep(5)  # Added sleep to avoid timing issue of resource creation
    # Check for Pool creation succeeded in OC
    try:
        CBP.get(ceph_block_pool)
        log.info(f"Ceph Block Pool {ceph_block_pool} creation succeeded")
    except exceptions.CommandFailed:
        assert log.info(
            f'Failed to create {ceph_block_pool}'
        )
        return False


def create_rbd_secret():
    """
    Get rbd secret from ceph and create secret in oc
    """
    secret_data = {}
    secret_data['base64_encoded_admin_password'] = \
        ocs.ocp.getbase64_ceph_secret("client.admin")
    template = os.path.join(TEMPLATES_DIR, "secret.yaml")
    log.info(f"Create secret for RBD storageclass")
    data = templating.generate_yaml_from_jinja2_template_with_data(
        template, **secret_data
    )
    secret_name = data['metadata']['name']
    with open(TEMP_YAML, 'w') as yaml_file:
        yaml.dump(data, yaml_file)
    assert SEC.create(yaml_file=TEMP_YAML)
    open(TEMP_YAML, 'w').close()
    sleep(5)  # Added sleep to avoid timing issue of resource creation
    try:
        # Check for secret creation succeeded in oc
        SEC.get(secret_name)
        log.info(f"Secret {secret_name} creation succeeded")
        return True
    except exceptions.CommandFailed:
        assert log.info(f'Failed to create Secret {secret_name}')
        return False


def create_rbd_storage_class():
    """
    Create Storage class in oc
    """
    template = os.path.join(TEMPLATES_DIR, "storageclass.yaml")
    log.info(f"Create rbd csi storageclass")
    data = templating.generate_yaml_from_jinja2_template_with_data(
        template
    )
    storage_class_name = data['metadata']['name']
    with open(TEMP_YAML, 'w') as yaml_file:
        yaml.dump(data, yaml_file)
    assert SC.create(yaml_file=TEMP_YAML)
    open(TEMP_YAML, 'w').close()
    sleep(5)  # Added sleep to avoid timing issue of resource creation
    try:
        # Check rbd storageclass creation succeeded in OC
        SC.get(storage_class_name)
        log.info(f"SC {storage_class_name} creation succeeded")
        return True
    except exceptions.CommandFailed:
        log.info(f'Failed to create SC {storage_class_name}')
        return False


def create_persistent_volume(pvc_name):
    """
    Create a new Persistent Volume Claim

    Args:
        pvc_name : Name of pvc to be created
    """
    rbd_pvc_data = {}
    rbd_pvc_data['rbd_pvc_name'] = pvc_name
    template = os.path.join(TEMPLATES_DIR, "pvc.yaml")
    log.info(f"Create Persistent Volume Claim ")
    data = templating.generate_yaml_from_jinja2_template_with_data(
        template, **rbd_pvc_data
    )
    with open(TEMP_YAML, 'w') as yaml_file:
        yaml.dump(data, yaml_file)
    assert PVC.create(yaml_file=TEMP_YAML)
    open(TEMP_YAML, 'w').close()
    # Check for pv creation succeed in oc
    try:
        PVC.wait_for_resource(
            resource_name=pvc_name, condition='Bound', timeout=TIMEOUT
        )
        log.info(f"PVC {pvc_name} creation succeeded")
        return True
    except exceptions.CommandFailed:
        log.info(f"PVC {pvc_name} is not in Bound state")
        return False


def create_pod(pv_list):
    """
    Generate cirros yaml with multiple pvc and run IO's

    Args:
        pv_list : List of pv's to be mounted in pod

    Returns:
        cirros_pod_name: Cirros pod deployment config name
    """
    temp_dict1 = []
    temp_dict2 = []
    temp_str = ''
    yaml_data = os.path.join(CIRROS_DIR, "cirros.yaml")
    data = templating.generate_yaml_from_jinja2_template_with_data(
        yaml_data
    )
    # Following code collects the existing cirros data, also removes some
    # values which will be updated as per pv_list args
    cirros_pod_name = data['metadata']['name']
    temp_list1 = data['spec']['template']['spec']['containers'][0]['args']
    temp_list1.pop(len(temp_list1) - 1)
    temp_list2 = data['spec']['template']['spec']['containers'][0][
        'livenessProbe'
    ]['exec']['command']
    temp_list2.pop(len(temp_list2) - 1)
    # for loop which creates the values which will be added in cirros.yaml
    for pv in pv_list:
        temp_dict1.append(
            {'name': pv, 'persistentVolumeClaim': {'claimName': pv}}
        )
        temp_dict2.append({'mountPath': '/mnt' + pv, 'name': pv})
        temp_list2.append(
            'mount | grep /mnt' + pv + ' && head -c 1024 '
            '< /dev/urandom >> /mnt' + pv + '/random-data.log'
        )
        temp_str = temp_str + '(mount | grep /mnt' + pv + ') && (head -c ' \
            '1000 < /dev/urandom > /mnt' + pv + '/random-data.log) || exit 1;'
    del data['spec']['template']['spec']['volumes']
    del data['spec']['template']['spec']['containers'][0]['volumeMounts']
    del data['spec']['template']['spec']['containers'][0][
        'livenessProbe'
    ]['exec']['command']
    # Updating the yaml with pv_list arg values, which will be mounted
    # in the cirros pod.
    data['spec']['template']['spec']['volumes'] = temp_dict1
    data['spec']['template']['spec']['containers'][0][
        'volumeMounts'
    ] = temp_dict2
    temp_list1.append('while true; do ' + temp_str + ' sleep 20 ; done')
    data['spec']['template']['spec']['containers'][0][
        'livenessProbe'
    ]['exec']['command'] = temp_list2
    with open(TEMP_YAML, 'w') as yaml_data:
        yaml.dump(data, yaml_data, default_flow_style=False)
    log.info(f"Create cirros pod with all the pvc")
    assert DC.create(yaml_file=TEMP_YAML)
    open(TEMP_YAML, 'w').close()
    try:
        # Check for pod is up and running
        POD.wait_for_resource(
            condition='Running', selector='deploymentconfig=cirrospod',
            timeout=TIMEOUT, sleep=SLEEP_TIME
        )
        log.info(f"Pod {cirros_pod_name} creation succeeded")
        return cirros_pod_name
    except exceptions.CommandFailed:
        log.info(f"{cirros_pod_name} not in Running state")
        return False


def delete_pod(pod_name):
    """
    Delete the cirros pod

    Args:
        pod_name: Name of the cirros_pod to be deleted.
    """
    try:
        cmd = f"delete deploymentconfig {pod_name}"
        assert DC.exec_oc_cmd(command=cmd)
        # Wait for pod deletion to complete, else pvc remove will fail
        sleep(SLEEP_TIME)
        return True
    except exceptions.CommandFailed:
        log.info(f'Exception POD {pod_name} not terminated ')
        return False


def delete_persistent_volume(pv_list):
    """
    Delete the Persistent Volumes

    Args:
        pv_list : list of the pvc to be cleaned/deleted
    """
    failed_pv_list = []
    for pv in pv_list:
        try:
            cmd = f"delete pvc {pv}"
            PVC.exec_oc_cmd(command=cmd)
        except exceptions.CommandFailed:
            failed_pv_list.append(pv)
    if not failed_pv_list:
        log.info(f"All PVC delete succeeded")
    else:
        log.info(f"Failed to delete PVC {failed_pv_list}")
        return False


def delete_storage_class():
    """
    Delete the Storage class
    """
    template = os.path.join(TEMPLATES_DIR, "storageclass.yaml")
    log.info(f"Delete the PVC")
    data = templating.generate_yaml_from_jinja2_template_with_data(
        template
    )
    sc_name = data['metadata']['name']
    with open(TEMP_YAML, 'w') as yaml_file:
        yaml.dump(data, yaml_file)
    try:
        SC.delete(yaml_file=TEMP_YAML)
        open(TEMP_YAML, 'w').close()
        return True
    except exceptions.CommandFailed:
        log.info(f"Exception SC {sc_name} not deleted")
        return False


def delete_pool():
    """
    Delete the pool
    """
    template = os.path.join(TEMPLATES_DIR, "CephBlockPool.yaml")
    log.info(f"Delete the PVC")
    data = templating.generate_yaml_from_jinja2_template_with_data(
        template,
    )
    pool_name = data['metadata']['name']
    with open(TEMP_YAML, 'w') as yaml_file:
        yaml.dump(data, yaml_file)
    try:
        CBP.delete(yaml_file=TEMP_YAML)
        open(TEMP_YAML, 'w').close()
        return True
    except exceptions.CommandFailed:
        log.info(f"Exception CephBlockPool {pool_name} not deleted")
        return False


def delete_secret():
    """
    Delete the Secret
    """
    template = os.path.join(TEMPLATES_DIR, "secret.yaml")
    log.info(f"Delete the PVC")
    data = templating.generate_yaml_from_jinja2_template_with_data(
        template
    )
    secret_name = data['metadata']['name']
    with open(TEMP_YAML, 'w') as yaml_file:
        yaml.dump(data, yaml_file)
    try:
        SEC.delete(yaml_file=TEMP_YAML)
        open(TEMP_YAML, 'w').close()
        return True
    except exceptions.CommandFailed:
        log.info(f"Exception Secret {secret_name} not deleted")
        return False


@tier1
@pytest.mark.usefixtures(
    test_fixture.__name__,
)
class TestCaseOCS260(ManageTest):
    """
    Create a pod attached with multiple PVC and run IO
    https://polarion.engineering.redhat.com/polarion/#/project
    /OpenShiftContainerStorage/workitem?id=OCS-260
    TC Description: To verify either pod can be attached with multiple PVCs.
    """
    # create 10 PVC to be mounted in POD, don't increase it beyond 25
    pvc_count = 20
    io_duration_sec = 180  # amount of time IO needs to executed
    pv_list = []
    cirros_name = []

    def test_run_ocs_260(self):
        """
        Test case OCS-260
        """
        for pv in range(self.pvc_count):
            self.pv_list.append('rbd-pvc' + str(pv))
            create_persistent_volume(self.pv_list[pv])
            log.info(f"pvc {self.pv_list[pv]} created success")
        self.cirros_name.append(create_pod(self.pv_list))
        log.info(f"Running IO's for {self.io_duration_sec} seconds")
        sleep(self.io_duration_sec)
