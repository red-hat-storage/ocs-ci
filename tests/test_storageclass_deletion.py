"""
Automates te following test:-
https://polarion.engineering.redhat.com/polarion/#/project/OpenShiftContainerStorage/workitem?id=OCS-297

1) Create a Storage Class
2) Create a PVC
3) Delete the corresponding storage class
"""
import logging
import yaml
import os
from utility.utils import run_cmd
from ocs import ocp
from openshift.dynamic import DynamicClient
from utility import utils, templating
from kubernetes import config
from ocsci.enums import StatusOfTest

k8s_client = config.new_client_from_config()
dyn_client = DynamicClient(k8s_client)
log = logging.getLogger(__name__)

PVC_YAML = os.path.join(
    "templates/ocs-deployment", "PersistentVolumeClaim.yaml"
)
SC_YAML = os.path.join(
    "templates/ocs-deployment", "StorageClass.yaml"
)
TEMP_PVC_YAML_FILE = '/tmp/pvc_test.yaml'
TEMP_POD_YAML_FILE = '/tmp/pod_test.yaml'
TEMP_SC_YAML_FILE = '/tmp/sc_test.yaml'

PVC = ocp.OCP(kind='PersistentVolumeClaim')
PV = ocp.OCP(kind='PersistentVolume')
SC = ocp.OCP(kind='StorageClass')
NAMESPACE = ocp.OCP(kind='Project')


def create_namespace(**kwargs):
    '''
    Creating a project
    '''
    project_name = (kwargs['project_name'])
    project_get = NAMESPACE.get()
    namespaces = []
    for i in range(len(project_get['items'])):
        namespaces.append(project_get['items'][i]['metadata']['name'])
        log.info(f'checking id project {project_name} already exists')
    if project_name in namespaces:
        log.info(
            f'project {project_name} exists, using the existing namespace'
        )
        return True
    else:
        log.info(f'creating a new project {project_name}')
        return run_cmd(f'oc new-project {project_name}')


def create_storageclass(**kwargs):
    '''
    Creating a storage class
    '''
    sc_name = (kwargs['sc_name'])
    sc_get = SC.get()
    storage_classes = []
    for i in range(len(sc_get['items'])):
        storage_classes.append(sc_get['items'][i]['metadata']['name'])
    log.info(f'checking if {sc_name} exists already')
    if sc_name in storage_classes:
        log.info(f'storage class {sc_name} exists, using {sc_name} ')
        return True
    else:
        file_sc = templating.generate_yaml_from_jinja2_template_with_data(
            SC_YAML, **kwargs
        )

        with open(TEMP_SC_YAML_FILE, 'w') as yaml_file:
            yaml.dump(file_sc, yaml_file, default_flow_style=False)
        return SC.create(yaml_file=TEMP_SC_YAML_FILE)


def create_pvc(**kwargs):
    '''
    Creates a Persistent Volume Claim
    '''

    file_pvc = templating.generate_yaml_from_jinja2_template_with_data(
        PVC_YAML, **kwargs
    )
    with open(TEMP_PVC_YAML_FILE, 'w') as yaml_file:
        yaml.dump(file_pvc, yaml_file, default_flow_style=False)
        log.info(f"Creating new Persistent Volume Claim")
    assert PVC.create(yaml_file=TEMP_PVC_YAML_FILE)
    return PVC.wait_for_resource_status(kwargs['pvc_name'], 'Bound')


def delete_pvc(**kwargs):
    '''
    Deletes a PVC and its underlying PV if Reclaim policy
    is Retain'
    '''

    pvc_name = kwargs['pvc_name']
    log.info(f"Deleting the Persistent Volume Claim {pvc_name}")
    pvc_get = PVC.get(pvc_name)
    pv_get = PV.get(pvc_get.spec.volumeName)
    if pv_get.spec.persistentVolumeReclaimPolicy == 'Retain':
        assert run_cmd(f'oc delete pvc {pvc_name}')
        return run_cmd(f'oc delete pv {pvc_get.spec.volumeName}')
    elif pv_get.spec.persistentVolumeReclaimPolicy == 'Delete':
        return run_cmd(f'oc delete pvc {pvc_name}')


def delete_storageclass(**kwargs):
    '''
    Deletes a storageclass
    '''
    sc_name = kwargs['sc_name']
    log.info(f'deleting storage class: {sc_name}')
    return run_cmd(f'oc delete sc {sc_name}')


def delete_namespace(**kwargs):
    '''
    Deletes a project
    '''

    project_name = kwargs['project_name']
    return run_cmd(f'oc delete project {project_name}')


def run(**kwargs):
    '''
    Running the code in test steps defined.
    '''

    data = {}
    pvc_name = 'claim'
    data['pvc_name'] = pvc_name
    data['project_name'] = 'ocs-qe'
    data['sc_name'] = 'ocs-qe-sc'
    assert create_namespace(**data)
    assert create_storageclass(**data)
    assert create_pvc(**data)
    assert delete_pvc(**data)
    assert delete_storageclass(**data)
    assert delete_namespace(**data)
    utils.delete_file(TEMP_POD_YAML_FILE)
    utils.delete_file(TEMP_PVC_YAML_FILE)
    utils.delete_file(TEMP_SC_YAML_FILE)
    return StatusOfTest.PASSED
