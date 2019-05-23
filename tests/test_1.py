"""
PVC test plan
"""
import os
import logging
import oc.openshift_ops
import ocs.ocp
import ocs.defaults as defaults
from utility.utils import templating

ocp = oc.openshift_ops.OCP()
logger = logging.getLogger(__name__)
from ipdb import set_trace

OCP = ocs.ocp.OCP(
    kind='Service', namespace=defaults.ROOK_CLUSTER_NAMESPACE
)

PVC = ocs.ocp.OCP(
    kind='PersistentVolumeClaim', namespace=defaults.ROOK_CLUSTER_NAMESPACE
)
CBP = ocs.ocp.OCP(
    kind='CephBlockPool', namespace=defaults.ROOK_CLUSTER_NAMESPACE
)
Pod = ocs.ocp.OCP(
    kind='Pod', namespace=defaults.ROOK_CLUSTER_NAMESPACE
)
SC = ocs.ocp.OCP(
    kind='StorageClass', namespace=defaults.ROOK_CLUSTER_NAMESPACE
)

temp_yaml = os.path.join("templates/ocs-deployment", "temp.yaml")





def dump_to_temp_yaml(source_file, dst_file, **kwargs):
    """

    """
    data = generate_yaml_from_jinja2_template_with_data(source_file, **kwargs)
    with open(dst_file, 'w') as yaml_file:
        yaml.dump(data, yaml_file, default_flow_style=False)


def create_ceph_block_pool():
    """
    Create a Ceph block pool
    """
    template = os.path.join("templates/ocs-deployment", "CephBlockPool.yaml")
    logger.info(f"Creating a Ceph block pool")
    templating.dump_to_temp_yaml(template, temp_yaml)
    set_trace()
    assert CBP.create(yaml_file=temp_yaml)
    open(temp_yaml, 'w').close()
    # wait()
    # implement validation


def delete_ceph_block_pool():
    """
    """
    template = os.path.join("templates/ocs-deployment", "CephBlockPool.yaml")
    logger.info(f"Deleting a Ceph block pool")
    templating.dump_to_temp_yaml(template, temp_yaml)
    set_trace()
    assert CBP.delete(yaml_file=temp_yaml)
    open(temp_yaml, 'w').close()


def create_storageclass():
    """
    Create a storage class
    """
    template = os.path.join("templates/ocs-deployment", "StorageClass.yaml")
    logger.info(f"Creating a storage class")
    templating.dump_to_temp_yaml(template, temp_yaml)
    assert SC.create(yaml_file=temp_yaml)
    open(temp_yaml, 'w').close()
    # wait()
    # implement validation


def delete_storageclass():
    """
    """
    template = os.path.join("templates/ocs-deployment", "StorageClass.yaml")
    logger.info(f"Creating a storage class")
    templating.dump_to_temp_yaml(template, temp_yaml)
    assert SC.delete(yaml_file=temp_yaml)
    open(temp_yaml, 'w').close()


def create_project():
    """
    Create a project
    """
    project_name = 'test-project'
    logger.info(f"Creating a project {project_name}")
    assert ocp.create_project(project=project_name)
    # wait()
    # implement validation


def delete_project():
    """
    Create a project
    """
    project_name = 'test-project'
    logger.info(f"Deleting a project {project_name}")
    assert OCP.exec_oc_cmd(f"delete project {project_name}")


def create_pvc():
    """
    Create a PVC
    """
    template = os.path.join("templates/ocs-deployment", "PersistentVolumeClaim.yaml")
    logger.info(f"Creating a PVC")
    templating.dump_to_temp_yaml(template, temp_yaml)
    assert PVC.create(yaml_file=temp_yaml)
    open(temp_yaml, 'w').close()
    # wait()
    # implement validation


def delete_pvc():
    """
    """
    template = os.path.join("templates/ocs-deployment", "PersistentVolumeClaim.yaml")
    logger.info(f"Deleting a PVC")
    templating.dump_to_temp_yaml(template, temp_yaml)
    assert PVC.delete(yaml_file=temp_yaml)
    open(temp_yaml, 'w').close()


def create_pod():
    """
    Create a pod
    """
    template = os.path.join("templates/ocs-deployment", "Pod.yaml")
    logger.info(f"Creating a pod")
    templating.dump_to_temp_yaml(template, temp_yaml)
    assert Pod.create(yaml_file=temp_yaml)
    open(temp_yaml, 'w').close()
    # wait()
    # implement validation


def delete_pod():
    """
    """
    template = os.path.join("templates/ocs-deployment", "Pod.yaml")
    logger.info(f"Deleting a pod")
    templating.dump_to_temp_yaml(template, temp_yaml)
    assert Pod.delete(yaml_file=temp_yaml)
    open(temp_yaml, 'w').close()


def run_io_on_pvc(pvc_name):
    """

    """
    logger.info(f"Running IO on PVC {pvc_name}")
    OCP.exec_oc_cmd(f'')



def run(**kwargs):
    set_trace()
    create_ceph_block_pool()
    set_trace()
    create_storageclass()
    set_trace()
    create_project()
    set_trace()
    create_pvc()
    set_trace()
    create_pod()
    set_trace()

    delete_pod()
    set_trace()
    delete_pvc()
    set_trace()
    delete_project()
    set_trace()
    delete_storageclass()
    set_trace()
    delete_ceph_block_pool()

