# -*- coding: utf8 -*-


import logging
import pytest

from ocs_ci.ocs.platform_nodes import IBMCloud
from ocs_ci.framework.testlib import libtest
from ocs_ci.ocs import node
from ocs_ci.framework.pytest_customization.marks import ibmcloud_platform_required

logger = logging.getLogger(__name__)


@pytest.fixture
def get_volume(request):
    """
    Create and return volume
    """

    def finalizer():
        ibmcloud.delete_volume_id(volume)

    request.addfinalizer(finalizer)

    ibmcloud = IBMCloud()

    volume = ibmcloud.get_volume_id()

    return volume


@pytest.fixture
def get_attached_volume(request, get_volume):
    """
    Attached volume
    """

    def finalizer():
        worker_node = ibmcloud.get_node_by_attached_volume(get_volume)
        ibmcloud.detach_volume(get_volume, worker_node)

    request.addfinalizer(finalizer)
    ibmcloud = IBMCloud()

    worker_nodes = node.get_nodes(node_type="worker", num_of_nodes=1)
    ibmcloud.attach_volume(get_volume, worker_nodes)

    worker_id = ibmcloud.get_node_by_attached_volume(get_volume)
    return worker_id


@libtest
@ibmcloud_platform_required
def test_rebootnodes():
    """
    Check basic consistency in platform handling.
    """
    ibmcloud = IBMCloud()
    worker_nodes = node.get_nodes(node_type="worker")
    ibmcloud.restart_nodes(worker_nodes)


@libtest
@ibmcloud_platform_required
def test_attachvolume(get_volume):
    """
    Check basic consistency in platform handling.
    """
    ibmcloud = IBMCloud()

    worker_nodes = node.get_nodes(node_type="worker", num_of_nodes=1)
    ibmcloud.attach_volume(get_volume, worker_nodes)


@libtest
@ibmcloud_platform_required
def test_detachvolume(get_volume, get_attached_volume):
    """
    Check basic consistency in platform handling.
    """
    ibmcloud = IBMCloud()

    worker_node = get_attached_volume
    ibmcloud.detach_volume(get_volume, worker_node)


@libtest
@ibmcloud_platform_required
def test_get_node_by_attached_volume(get_volume, get_attached_volume):
    """
    Check basic consistency in platform handling.
    """
    worker_id = get_attached_volume
    logger.info(f"volume is  attached to node: {worker_id}")


@libtest
@ibmcloud_platform_required
def test_get_data_volumes():
    """
    Check basic consistency in platform handling.
    """
    ibmcloud = IBMCloud()

    vol_ids = ibmcloud.get_data_volumes()
    logger.info(f"volume ids are : {vol_ids}")


@libtest
@ibmcloud_platform_required
def test_wait_for_volume_attach(get_volume):
    """
    Check basic consistency in platform handling.
    """
    ibmcloud = IBMCloud()

    ibmcloud.wait_for_volume_attach(get_volume)


@libtest
@ibmcloud_platform_required
def test_restart_nodes_by_stop_and_start():
    """
    Check basic consistency in platform handling.
    """
    ibmcloud = IBMCloud()
    worker_nodes = node.get_nodes(node_type="worker")
    ibmcloud.restart_nodes_by_stop_and_start(worker_nodes)


@libtest
@ibmcloud_platform_required
def test_restart_nodes_by_stop_and_start_teardown():
    """
    Check basic consistency in platform handling.
    """
    ibmcloud = IBMCloud()
    ibmcloud.restart_nodes_by_stop_and_start_teardown()


@libtest
@ibmcloud_platform_required
def test_create_nodes():
    """
    Check basic consistency in platform handling.
    """
    ibmcloud = IBMCloud()
    node_conf = {}
    node_type = "RHEL"
    num_nodes = 1
    ibmcloud.create_nodes(node_conf, node_type, num_nodes)


@libtest
@ibmcloud_platform_required
def test_create_and_attach_nodes_to_cluster():
    """
    Check basic consistency in platform handling.
    """
    ibmcloud = IBMCloud()
    node_conf = {}
    node_type = "RHEL"
    num_nodes = 1
    ibmcloud.create_and_attach_nodes_to_cluster(node_conf, node_type, num_nodes)
