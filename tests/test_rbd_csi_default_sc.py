"""
Test for creating a pvc with default RBD StorageClass - CSI
"""
import os
from time import sleep

from kubernetes import client, config
from ocs import pod
import yaml
from ocsci.enums import StatusOfTest
from utility import templating
from ocs import ocp
from ocsci import run_this, EcosystemTest, tier1
import logging

log = logging.getLogger(__name__)

RBD_POOL_YAML = os.path.join("templates/ocs-deployment", "pool.yaml")
SC_RBD_YAML = os.path.join("templates/ocs-deployment",
                           "storageclass-csi-rbd.yaml")
SECRET_RBD_YAML = os.path.join("templates/ocs-deployment", "secret_rbd.yaml")
PVC_RBD_YAML = os.path.join("templates/ocs-deployment", "pvc-rbd.yaml")
TEMP_YAML_FILE = 'test_RbdCSI.yaml'

SC = ocp.OCP(
    kind='StorageClass', namespace="rook-ceph"
)
POOL = ocp.OCP(
    kind='CephBlockPool', namespace="rook-ceph"
)
DEPLOYMENT = ocp.OCP(
    kind='Deployment', namespace="rook-ceph"
)
SECRET = ocp.OCP(
    kind='Secret', namespace="default"
)
PVC = ocp.OCP(
    kind='PersistentVolumeClaim', namespace="rook-ceph"
)

def create_rbd_pool():
    """
    Create Blockpool with default values
    """
    file_y = templating.generate_yaml_from_jinja2_template_with_data(
        RBD_POOL_YAML
    )
    with open(TEMP_YAML_FILE, 'w') as yaml_file:
        yaml.dump(file_y, yaml_file, default_flow_style=False)
    log.info(f"Creating a new CephBlockPool with default name")
    assert POOL.create(yaml_file=TEMP_YAML_FILE)
    sleep(5)
    log.info(f"Sleeping for 5 sec to let pool get created")
    return True

def create_storageclass_rbd():
    """
    This Creates a default CSI StorageClass
    """
    file_y = templating.generate_yaml_from_jinja2_template_with_data(
        SC_RBD_YAML
    )
    with open(TEMP_YAML_FILE, 'w') as yaml_file:
        yaml.dump(file_y, yaml_file, default_flow_style=False)
    log.info(f"Creating a RBD StorageClass with default values")
    assert SC.create(yaml_file=TEMP_YAML_FILE)
    log.info(f"Sleeping for 5 sec to let StorageClass get created")
    sleep(2)
    return True

def validate_pool_creation(pool_name):
    """
    Check whether default blockpool is created or not at ceph and as well
    OCS side

    :param pool_name:
    :return:
    """
    ceph_validate = False
    OCS_validate = False
    cmd = "ceph osd lspools|grep " + str(pool_name) + " |awk '{print$2}'"
    took_box = get_toolbox_pod_handler()
    out, err, ret = took_box.exec_command(cmd=cmd, timeout=20)
    if out:
        log.info(f"{pool_name} pool is created successfully at CEPH side")
        ceph_validate = True
    else:
        log.error(f"{pool_name} pool failed to get created at CEPH side")
    pool_obj = (POOL.get(resource_name=""))
    sample = pool_obj['items']
    for item in sample:
        if item.metadata.name == pool_name:
            OCS_validate = True
    if ceph_validate and OCS_validate:
        log.info("Pool got created successfully from Ceph and OCS side")
        return True
    else:
        return False

def get_toolbox_pod_handler():
    """
    Used to get handler of toolbox_pod

    :return:
    """
    config.load_kube_config()
    v1 = client.CoreV1Api()
    ret = v1.list_pod_for_all_namespaces(
        watch=False,
        label_selector='app=rook-ceph-tools'
    )

    for i in ret.items:
        namespace = i.metadata.namespace
        name = i.metadata.name
        break
    tool_box = pod.Pod(name, namespace)
    return tool_box

def validate_storageclass(sc_name):
    """
    Validate if storageClass is been created or not
    """
    sc_obj = SC.get(resource_name="")
    sample = sc_obj['items']
    for item in sample:
        if item.metadata.name in sc_name:
            log.info(f"StorageClass got created successfully")
            return True
    return False

def create_secret_rbd(**kwargs):
    """
    This will create Secret file which will be used for creating StorageClass

    :return:
    """
    file_y = templating.generate_yaml_from_jinja2_template_with_data(
        SECRET_RBD_YAML, **kwargs
    )
    with open(TEMP_YAML_FILE, 'w') as yaml_file:
        yaml.dump(file_y, yaml_file, default_flow_style=False)
    assert SECRET.create(yaml_file=TEMP_YAML_FILE)
    return True

def get_client_admin_keyring():
    """
    This will fetch client admin keyring from Ceph

    :return:
    """
    cmd = f"ceph auth get-key client.admin|base64"
    tool_box = get_toolbox_pod_handler()
    out, err, ret = tool_box.exec_command(cmd=cmd, timeout=20)
    if out:
        out = out.rstrip('\n')
        secret_data = {}
        secret_data['client_admin_key'] = out
        assert create_secret_rbd(**secret_data)
    else:
        log.error(f"Failed to get client auth key")
        return StatusOfTest.FAILED

def create_pvc():
    """
    This will create PVC with default value

    :return:
    """

    file_y = templating.generate_yaml_from_jinja2_template_with_data(
        PVC_RBD_YAML,
    )
    with open(TEMP_YAML_FILE, 'w') as yaml_file:
        yaml.dump(file_y, yaml_file, default_flow_style=False)
    assert PVC.create(yaml_file=TEMP_YAML_FILE)
    log.info(f"Waiting for 3 sec for PVC to get created")
    sleep(3)
    return True

def validate_pvc(pvc_name):
    """
    This will check if PVC created is bound successfully or not

    :param pvc_name:
    :return:
    """
    pvc_list = PVC.get(resource_name=pvc_name)
    pvc_status=pvc_list['status'].phase
    if pvc_status == "Bound":
        log.info(f"PVC is created and Status is {pvc_status}")
        return True
    else:
        log.info(f"Failed: PVC status PVC in {pvc_status}")
        return False

@tier1
class TestCaseOCS347(EcosystemTest):
    def test_347(self):

        assert create_rbd_pool()
        assert validate_pool_creation("rbd")
        assert create_storageclass_rbd()
        assert validate_storageclass("ocsci-csi-rbd")
        get_client_admin_keyring()
        assert create_pvc()
        assert validate_pvc("rbd-pvc")
        return StatusOfTest.PASSED