import logging
import pytest
import threading
import time

from ocs_ci.framework import config
from ocs_ci.framework.testlib import ManageTest, tier1, skipif_external_mode, bugzilla
from ocs_ci.ocs.must_gather.must_gather import MustGather
from ocs_ci.ocs.must_gather.const_must_gather import GATHER_COMMANDS_VERSION
from ocs_ci.ocs import node


logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def mustgather(request):

    mustgather = MustGather()
    mustgather.collect_must_gather()

    def teardown():
        mustgather.cleanup()

    request.addfinalizer(teardown)
    return mustgather


class TestMustGather(ManageTest):
    @tier1
    @pytest.mark.parametrize(
        argnames=["log_type"],
        argvalues=[
            pytest.param(
                *["CEPH"],
                marks=[pytest.mark.polarion_id("OCS-1583"), skipif_external_mode],
            ),
            pytest.param(
                *["JSON"],
                marks=[pytest.mark.polarion_id("OCS-1583"), skipif_external_mode],
            ),
            pytest.param(*["OTHERS"], marks=pytest.mark.polarion_id("OCS-1583")),
        ],
    )
    @pytest.mark.skipif(
        float(config.ENV_DATA["ocs_version"]) not in GATHER_COMMANDS_VERSION,
        reason=(
            "Skipping must_gather test, because there is not data for this version"
        ),
    )
    def test_must_gather(self, mustgather, log_type):
        """
        Tests functionality of: oc adm must-gather

        """
        mustgather.log_type = log_type
        mustgather.validate_must_gather()

    @tier1
    @bugzilla("1884546")
    def test_must_gather_network_failure(self, nodes):
        """
        Test Must Gather Network Failure
        """
        logging.info("Create must_gather_thread and network_failure_thread")
        mustgather = MustGather()
        must_gather_thread = threading.Thread(target=mustgather.collect_must_gather)
        network_failure_thread = threading.Thread(
            target=self.disconnect_worker_nodes, nodes=nodes
        )

        logging.info("Starting must gather thread")
        must_gather_thread.start()

        logging.info("Sleep 15 Sec")
        time.sleep(15)

        logging.info("Starting network failure thread")
        network_failure_thread.start()

        logging.info("wait until network_failure thread is completely executed")
        network_failure_thread.join()

        logging.info("wait until must_gather thread is completely executed")
        must_gather_thread.join()

        mustgather.log_type = "OTHERS"
        mustgather.validate_must_gather()

    def disconnect_worker_nodes(self, nodes):
        """
        Disconnect Worker Nodes

        """
        worker_node_names = node.get_worker_nodes()
        node.node_network_failure(node_names=worker_node_names, wait=True)
        logger.info(f"Stop and start the worker nodes: {worker_node_names}")
        nodes.restart_nodes_by_stop_and_start(node.get_node_objs(worker_node_names))
