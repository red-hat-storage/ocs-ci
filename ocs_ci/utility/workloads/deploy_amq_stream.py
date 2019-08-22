import logging
import json

from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.resources.pod import get_all_pods, get_pod_obj
from ocs_ci.utility import templating
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.utility.retry import retry
from tests import helpers
from tests.fixtures import (
    create_ceph_block_pool,
    create_rbd_secret, create_cephfs_secret
)

logger = logging.getLogger(__name__)

@pytest.mark.usefixtures(
    create_cephfs_secret.__name__,
    create_cephfs_storageclass.__name__,
)

def create_namespace(yaml_file):
    """
    Creation of namespace "my-project")
    Args:
        yaml_file (str): Path to yaml file to create namespace
    Example:
        create_namespace(yaml_file=constants.STREAM_NAMESPACE_YAML)
    """

    namespaces = ocp.OCP(kind=constants.NAMESPACES)

    logger.info("Namespace creation in progress
    assert namespaces.create(yaml_file=yaml_file), 'Not able to create Namespace
    logger.info("Namespace creation success")


def deploy_cluster_op():
    """
    Install multiple yaml files
    """
    logger.info("Installing cluster Operator files...")
    assert ocp.OCP.apply(constants.TEMPLATE_DEPLOYMENT_CS)
    logger.info("Cluster operator files are getting deployed")
    assert ocp.OCP.apply(constants.TEMPLATE_DEPLOYMENT_CP)


def kafka_cluster(yaml_file, resource_name):
    """
    This will set the cephfs default storage class
    and setup persistent pods using cephfs volume
    :param yaml_file:
    :return:
    """
    cmd =  oc patch storageclass {self.sc_obj.name} -p /
    '{"metadata": {"annotations":{"storageclass.kubernetes.io/is-default-class":"true"}}}'
    assert self.ocp.exec_oc_cmd(cmd)
    logger.info("storage class set to default")

    amq_operator_group = ocp.OCP(
        kind=constants.OPERATOR_GROUP, namespace='my-project'
    )

    amq_operator_group.create(yaml_file=yaml_file)
    try:
        amq_operator_group.get(resource_name, out_yaml_format=True)
        logger.info('The amq kafka is created successfully')
    except CommandFailed:
        logger.error('The resource is not found')
        return False
    return True

def kafka_connect(yaml_file, resource_name):
    """
    This function will apply
    :param yaml_file:
    :param resource_name:
    :return:
    """
    kafcon_operator_group = ocp.OCP(
        kind=constants.OPERATOR_GROUP, namespace='my-project'
    )
    kafcon_operator_group.create(yaml_file=yaml_file)
    try:
        kafcon_operator_group.get(resource_name, out_yaml_format=True)
        logger.info('The amq kafka is created successfully')
    except CommandFailed:
        logger.error('The resource is not found')
        return False
    return True

def kafka_bridge(yaml_file, resource):
    """

    :param yaml_file:
    :param resource_name:
    :return:
    """
    kafbridge_operator_group = ocp.OCP(
        kind=constants.OPERATOR_GROUP, namespace='my-project'
    )
    kafbridge_operator_group.create(yaml_file=yaml_file)
    try:
        kafbridge_operator_group.get(resource_name, out_yaml_format=True)
        logger.info('The amq kafka is created successfully')
    except CommandFailed:
        logger.error('The resource is not found')
        return False
    return True