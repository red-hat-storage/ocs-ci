import logging
import pytest
import threading

from ocs_ci.framework import config
from ocs_ci.framework.testlib import (
    ManageTest,
    tier1,
    skipif_external_mode,
    bugzilla,
    tier2,
)
from ocs_ci.ocs.must_gather.must_gather import MustGather
from ocs_ci.ocs.must_gather.const_must_gather import GATHER_COMMANDS_VERSION


logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def mustgather(request):

    mustgather = MustGather()
    mustgather.collect_must_gather()

    def teardown():
        mustgather.cleanup()

    request.addfinalizer(teardown)
    return mustgather


@pytest.fixture(scope="function")
def mustgather_restart(request, nodes):

    mustgather = MustGather()

    def teardown():
        mustgather.cleanup()
        nodes.restart_nodes_by_stop_and_start_teardown()

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

    @tier2
    @bugzilla("1884546")
    def test_must_gather_restart(self, mustgather_restart, nodes):
        """
        Restart worker node where "must-gather" pod running
        while collect the "must-gather"

        """
        logging.info("Create must_gather_thread and restart_thread")
        must_gather_thread = threading.Thread(
            target=mustgather_restart.collect_must_gather
        )
        restart_thread = threading.Thread(
            target=mustgather_restart.restart_node_where_must_gather_pod_running,
            args=(nodes,),
        )

        logging.info("Starting must gather thread")
        must_gather_thread.start()

        logging.info("Starting node restart thread")
        restart_thread.start()

        logging.info("wait until restart thread is completely executed")
        restart_thread.join()

        logging.info("wait until must_gather thread is completely executed")
        must_gather_thread.join()

        logging.info("Validate must-gather after restart")
        mustgather_restart.validate_must_gather_restart()
