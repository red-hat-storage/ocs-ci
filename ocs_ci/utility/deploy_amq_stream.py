import logging
import pytest
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.exceptions import CommandFailed
from tests.fixtures import (
    create_cephfs_secret, create_cephfs_storageclass
)


logger = logging.getLogger(__name__)


def create_namespace(yaml_file):
    """
    Creation of namespace
    Args:
        yaml_file (str): Path to yaml file to create namespace
    Example:
        create_namespace
    """
    namespaces = ocp.OCP(kind=constants.NAMESPACES)

    logger.info("Namespace creation in progress")
    assert namespaces.create(yaml_file=yaml_file), 'Not able to create Namespace'
    logger.info("Namespace creation success")


def deploy_cluster_op():
    """
    Install yaml files to bring up the pods
    and connection building
    """
    logger.info("Installing cluster Operator files...")
    assert ocp.OCP.apply(constants.TEMPLATE_DEPLOYMENT_CS)
    logger.info("Cluster operator files are getting deployed")
    assert ocp.OCP.apply(constants.TEMPLATE_DEPLOYMENT_CP)


def kafka_cluster(yaml_file, resource_name):
    """
    This will set the cephfs default storage class
    and setup persistent pods using cephfs volume

    yaml_file: PATH to the yaml file
    resource_name: Name of the operator group

    return: True is PASSED else FALSE
    """

    @pytest.mark.usefixtures(
        create_cephfs_secret.__name__,
        create_cephfs_storageclass.__name__,
    )
    cmd = f"oc patch storageclass {self.sc_obj.name} -p " \
          f"'{"metadata": {"annotations":{"storageclass." \
          f"kubernetes.io/is-default-class":"true"}}}'"
    assert self.ocp.exec_oc_cmd(cmd)
    logger.info("storage class set to default")

    amq_operator_group = ocp.OCP(
        kind=constants.OPERATOR_GROUP, namespace='my-project'
    )

    amq_operator_group.apply(yaml_file=yaml_file)
    try:
        amq_operator_group.get(resource_name, out_yaml_format=True)
        logger.info('The kafka cluster is deployed phase 1')
    except CommandFailed:
        logger.error('The resource is not found')
        return False
    return True


def kafka_connect(yaml_file, resource_name):
    """
    This function will setup connection with the
    stream pods

    yaml_file: Path to the Yaml file

    return: True if PASSED else False
    """
    kafcon_operator_group = ocp.OCP(
        kind=constants.OPERATOR_GROUP, namespace='my-project'
    )
    kafcon_operator_group.apply(yaml_file=yaml_file)
    try:
        kafcon_operator_group.get(resource_name, out_yaml_format=True)
        logger.info('The kafka connect is created successfully')
    except CommandFailed:
        logger.error('The resource is not found')
        return False
    return True


def kafka_bridge(yaml_file, resource_name):
    """
    Kafka bridge is created between outside world and pods
    yaml_file: PATH to the yaml file
    :return: True if PASSED else False
    """
    kafbridge_operator_group = ocp.OCP(
        kind=constants.OPERATOR_GROUP, namespace='my-project'
    )
    kafbridge_operator_group.apply(yaml_file=yaml_file)
    try:
        kafbridge_operator_group.get(resource_name, out_yaml_format=True)
        logger.info('The kafka bridge is created successfully Phase 2')
    except CommandFailed:
        logger.error('The resource is not found')
        return False
    return True
